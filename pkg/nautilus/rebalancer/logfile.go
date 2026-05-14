// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/util/atomicfs"
)

// logFileVersion is the on-disk schema version for the rebalancer's
// persisted logs. Bump when changing the JSON shape and add a
// migration in load() for any prior version we still want to read.
const logFileVersion = 1

// logFileData is the JSON envelope shared by both the assignment log
// and the readcache-assignment log. We persist them in separate
// files so the rebalancer can mount independent data sources during
// migrations.
type logFileData struct {
	Version int `json:"version"`

	// Exactly one of the two slices is populated, depending on the
	// file kind. Marshalled with omitempty so the on-disk file
	// reflects only the relevant kind.
	Entries          []assignment.LogEntry           `json:"entries,omitempty"`
	ReadcacheEntries []readcacheassignment.LogEntry  `json:"readcache_entries,omitempty"`
}

// logFile is a small atomic-write helper for the rebalancer logs.
//
// Modelled on pkg/storage/ingest/offset_file.go: writes go through
// pkg/util/atomicfs.CreateFile (write to .tmp, fsync, rename) so a
// crash mid-write never leaves the on-disk file in a half-written
// state. Reads tolerate missing-or-corrupt files by returning ok=false
// — the rebalancer falls back to its FineEvenSplit seed in that case.
type logFile struct {
	filePath string
	logger   log.Logger

	mu sync.Mutex
}

func newLogFile(filePath string, logger log.Logger) *logFile {
	return &logFile{filePath: filePath, logger: logger}
}

// readAssignmentLog returns the persisted entries plus ok=true on
// success. Returns (nil, false) when:
//   - the file doesn't exist (cold start),
//   - the file is unreadable or unparseable (corrupted), or
//   - the file's schema version is unrecognised.
//
// The corruption path is logged at error level but does not propagate
// an error: the rebalancer treats it as a cold start so a single
// unrecoverable on-disk state can't pin the rebalancer in a crash
// loop.
func (f *logFile) readAssignmentLog() ([]assignment.LogEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.filePath)
	if os.IsNotExist(err) {
		return nil, false
	}
	if err != nil {
		level.Error(f.logger).Log("msg", "failed to read rebalancer log file", "file", f.filePath, "err", err)
		return nil, false
	}

	var parsed logFileData
	if err := json.Unmarshal(data, &parsed); err != nil {
		level.Error(f.logger).Log("msg", "failed to parse rebalancer log file", "file", f.filePath, "err", err)
		return nil, false
	}
	if parsed.Version != logFileVersion {
		level.Error(f.logger).Log("msg", "rebalancer log file has unknown version", "file", f.filePath, "version", parsed.Version)
		return nil, false
	}
	return parsed.Entries, true
}

// writeAssignmentLog atomically replaces the file's contents with
// entries. Empty entries is a valid persisted state (the rebalancer
// briefly observes it on cold start).
func (f *logFile) writeAssignmentLog(entries []assignment.LogEntry) error {
	return f.write(logFileData{Version: logFileVersion, Entries: entries})
}

// readReadcacheLog mirrors readAssignmentLog for the (partition ->
// readcache instance) log.
func (f *logFile) readReadcacheLog() ([]readcacheassignment.LogEntry, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.filePath)
	if os.IsNotExist(err) {
		return nil, false
	}
	if err != nil {
		level.Error(f.logger).Log("msg", "failed to read rebalancer readcache log file", "file", f.filePath, "err", err)
		return nil, false
	}

	var parsed logFileData
	if err := json.Unmarshal(data, &parsed); err != nil {
		level.Error(f.logger).Log("msg", "failed to parse rebalancer readcache log file", "file", f.filePath, "err", err)
		return nil, false
	}
	if parsed.Version != logFileVersion {
		level.Error(f.logger).Log("msg", "rebalancer readcache log file has unknown version", "file", f.filePath, "version", parsed.Version)
		return nil, false
	}
	return parsed.ReadcacheEntries, true
}

func (f *logFile) writeReadcacheLog(entries []readcacheassignment.LogEntry) error {
	return f.write(logFileData{Version: logFileVersion, ReadcacheEntries: entries})
}

func (f *logFile) write(data logFileData) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if err := os.MkdirAll(filepath.Dir(f.filePath), 0o755); err != nil {
		return fmt.Errorf("creating rebalancer data dir: %w", err)
	}
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshalling rebalancer log: %w", err)
	}
	if err := atomicfs.CreateFile(f.filePath, bytes.NewReader(jsonBytes)); err != nil {
		return fmt.Errorf("writing rebalancer log file: %w", err)
	}
	return nil
}

// assignmentLogFilename is the on-disk filename for the (hash range
// -> partition) log. Kept stable across versions so PVC contents
// survive rebalancer upgrades.
const assignmentLogFilename = "assignment-log.json"

// readcacheLogFilename is the on-disk filename for the (partition ->
// readcache instance) log.
const readcacheLogFilename = "readcache-assignment-log.json"
