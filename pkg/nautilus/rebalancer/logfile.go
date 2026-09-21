// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
	"github.com/grafana/mimir/pkg/util/atomicfs"
)

// The assignment log has its own version because its tenant and heartbeat
// migration must not invalidate the independently persisted readcache and
// cooldown files.
const (
	logFileVersion           = 1
	assignmentLogFileVersion = 2
)

// logFileData is the JSON envelope shared by the assignment log, the
// readcache-assignment log, and the move-cooldown state. We persist
// them in separate files so the rebalancer can mount independent
// data sources during migrations.
type logFileData struct {
	Version int `json:"version"`

	// Exactly one of the fields below is populated, depending on the
	// file kind. Marshalled with omitempty so the on-disk file
	// reflects only the relevant kind.
	Entries          []assignment.LogEntry          `json:"entries,omitempty"`
	ReadcacheEntries []readcacheassignment.LogEntry `json:"readcache_entries,omitempty"`

	AssignmentGeneration uint64    `json:"assignment_generation,omitempty"`
	AssignmentValidUntil time.Time `json:"assignment_valid_until,omitempty"`

	// Cooldowns use Trace.Cooldowns' tenant-aware string encoding.
	MoveCooldowns               map[string]time.Time `json:"move_cooldowns,omitempty"`
	StructuralCooldowns         map[string]time.Time `json:"structural_cooldowns,omitempty"`
	RecentSourcePartitions      map[int32]time.Time  `json:"recent_source_partitions,omitempty"`
	RecentDestinationPartitions map[int32]time.Time  `json:"recent_destination_partitions,omitempty"`
}

// logFile is a small atomic-write helper for the rebalancer logs.
//
// Modelled on pkg/storage/ingest/offset_file.go: writes go through
// pkg/util/atomicfs.CreateFile (write to .tmp, fsync, rename) so a
// crash mid-write never leaves the on-disk file in a half-written
// state. Reads tolerate missing-or-corrupt files by returning ok=false;
// the rebalancer then reconstructs tenant placements from readcache
// reports or starts with an empty tier-1 assignment.
type logFile struct {
	filePath string
	logger   log.Logger

	mu sync.Mutex
}

func newLogFile(filePath string, logger log.Logger) *logFile {
	return &logFile{filePath: filePath, logger: logger}
}

// readAssignmentLog returns the persisted state plus ok=true on
// success. Returns a zero state and false when:
//   - the file doesn't exist (cold start),
//   - the file is unreadable or unparseable (corrupted), or
//   - the file's schema version is unrecognised.
//
// The corruption path is logged at error level but does not propagate
// an error: the rebalancer treats it as a cold start so a single
// unrecoverable on-disk state can't pin the rebalancer in a crash
// loop.
func (f *logFile) readAssignmentLog() (assignment.LogState, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.filePath)
	if os.IsNotExist(err) {
		return assignment.LogState{}, false
	}
	if err != nil {
		level.Error(f.logger).Log("msg", "failed to read rebalancer log file", "file", f.filePath, "err", err)
		return assignment.LogState{}, false
	}

	var parsed logFileData
	if err := json.Unmarshal(data, &parsed); err != nil {
		level.Error(f.logger).Log("msg", "failed to parse rebalancer log file", "file", f.filePath, "err", err)
		return assignment.LogState{}, false
	}
	switch parsed.Version {
	case logFileVersion:
		// Version 1 predates tenant IDs and generation metadata. Missing
		// tenant_id fields naturally migrate into the legacy empty tenant.
		generation := uint64(0)
		if len(parsed.Entries) > 0 {
			generation = 1
		}
		return assignment.LogState{Entries: parsed.Entries, Generation: generation}, true
	case assignmentLogFileVersion:
		return assignment.LogState{
			Entries:    parsed.Entries,
			Generation: parsed.AssignmentGeneration,
			ValidUntil: parsed.AssignmentValidUntil,
		}, true
	default:
		level.Error(f.logger).Log("msg", "rebalancer log file has unknown version", "file", f.filePath, "version", parsed.Version)
		return assignment.LogState{}, false
	}
}

// writeAssignmentLog atomically replaces the file's contents with
// entries. Empty entries is a valid persisted state (the rebalancer
// briefly observes it on cold start).
func (f *logFile) writeAssignmentLog(state assignment.LogState) error {
	return f.write(logFileData{
		Version:              assignmentLogFileVersion,
		Entries:              state.Entries,
		AssignmentGeneration: state.Generation,
		AssignmentValidUntil: state.ValidUntil,
	})
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

// readMoveCooldowns mirrors readAssignmentLog for the per-range
// move-cooldown state. The same missing/corrupt/unknown-version
// handling applies: any failure reads as a cold start (no cooldowns)
// rather than an error, because stale or absent cooldowns only relax
// churn protection — they can't corrupt routing.
func (f *logFile) readMoveCooldowns() (map[string]time.Time, bool) {
	state, ok := f.readCooldownState()
	return state.MoveCooldowns, ok
}

func (f *logFile) readCooldownState() (logFileData, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.filePath)
	if os.IsNotExist(err) {
		return logFileData{}, false
	}
	if err != nil {
		level.Error(f.logger).Log("msg", "failed to read rebalancer move-cooldowns file", "file", f.filePath, "err", err)
		return logFileData{}, false
	}

	var parsed logFileData
	if err := json.Unmarshal(data, &parsed); err != nil {
		level.Error(f.logger).Log("msg", "failed to parse rebalancer move-cooldowns file", "file", f.filePath, "err", err)
		return logFileData{}, false
	}
	if parsed.Version != logFileVersion {
		level.Error(f.logger).Log("msg", "rebalancer move-cooldowns file has unknown version", "file", f.filePath, "version", parsed.Version)
		return logFileData{}, false
	}
	return parsed, true
}

func (f *logFile) writeMoveCooldowns(cooldowns map[string]time.Time) error {
	return f.write(logFileData{Version: logFileVersion, MoveCooldowns: cooldowns})
}

func (f *logFile) writeCooldownState(move, structural map[string]time.Time, roles partitionRoleCooldowns) error {
	return f.write(logFileData{
		Version:                     logFileVersion,
		MoveCooldowns:               move,
		StructuralCooldowns:         structural,
		RecentSourcePartitions:      roles.recentSources,
		RecentDestinationPartitions: roles.recentDestinations,
	})
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

// moveCooldownsFilename is the on-disk filename for the per-range
// move-cooldown state. Persisted so a rebalancer restart doesn't
// forget in-flight cooldowns and immediately re-move ranges it just
// relocated.
const moveCooldownsFilename = "move-cooldowns.json"
