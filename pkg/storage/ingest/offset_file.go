// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/util/atomicfs"
)

// offsetFileVersion is the version of the offset file format for future evolution.
const offsetFileVersion = 1

// offsetFileData is the JSON structure persisted to disk.
type offsetFileData struct {
	Version     int   `json:"version"`
	PartitionID int32 `json:"partition_id"`
	Offset      int64 `json:"offset"`
}

// offsetFile handles reading and writing offsets to a file on disk.
type offsetFile struct {
	filePath    string
	partitionID int32
	logger      log.Logger
	mu          sync.Mutex
}

// newOffsetFile creates a new offsetFile. Returns nil if filePath is empty.
func newOffsetFile(filePath string, partitionID int32, logger log.Logger) *offsetFile {
	if filePath == "" {
		return nil
	}

	return &offsetFile{
		filePath:    filePath,
		partitionID: partitionID,
		logger:      logger,
	}
}

// Read reads the last committed offset from the file.
// Returns the offset and true if the file exists, contains valid data, and the partition ID matches.
// Returns 0 and false if the file doesn't exist, contains invalid data, or the partition ID does not match.
func (f *offsetFile) Read() (int64, bool) {
	if f == nil {
		return 0, false
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	data, err := os.ReadFile(f.filePath)
	if os.IsNotExist(err) {
		level.Info(f.logger).Log("msg", "offset file does not exist, will replay entire backlog", "file", f.filePath)
		return 0, false
	}
	if err != nil {
		level.Error(f.logger).Log("msg", "failed to read offset file, will replay entire backlog", "file", f.filePath, "err", err)
		return 0, false
	}

	var parsed offsetFileData
	if err := json.Unmarshal(data, &parsed); err != nil {
		level.Error(f.logger).Log("msg", "failed to parse offset file, will replay entire backlog", "file", f.filePath, "err", err)
		return 0, false
	}
	if parsed.Version != offsetFileVersion {
		level.Error(f.logger).Log("msg", "offset file has unknown version, will replay entire backlog", "file", f.filePath, "version", parsed.Version)
		return 0, false
	}
	if parsed.PartitionID != f.partitionID {
		level.Error(f.logger).Log("msg", "offset file partition ID does not match expected partition, wrong volume may be mounted", "file", f.filePath, "expected_partition", f.partitionID, "file_partition", parsed.PartitionID)
		return 0, false
	}

	level.Info(f.logger).Log("msg", "read last committed offset from file", "file", f.filePath, "offset", parsed.Offset)
	return parsed.Offset, true
}

// Write writes the offset to the file atomically.
func (f *offsetFile) Write(offset int64) error {
	if f == nil {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	dir := filepath.Dir(f.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	data := offsetFileData{
		Version:     offsetFileVersion,
		PartitionID: f.partitionID,
		Offset:      offset,
	}
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal offset file data: %w", err)
	}

	if err := atomicfs.CreateFile(f.filePath, bytes.NewReader(jsonBytes)); err != nil {
		return fmt.Errorf("failed to write offset file: %w", err)
	}

	level.Debug(f.logger).Log("msg", "wrote offset to file", "file", f.filePath, "offset", offset)
	return nil
}
