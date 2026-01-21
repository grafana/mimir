// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

// offsetFile handles reading and writing offsets to a file on disk.
type offsetFile struct {
	filePath string
	logger   log.Logger
	mu       sync.Mutex
}

// newOffsetFile creates a new offsetFile. Returns nil if filePath is empty.
func newOffsetFile(filePath string, logger log.Logger) *offsetFile {
	if filePath == "" {
		return nil
	}

	return &offsetFile{
		filePath: filePath,
		logger:   logger,
	}
}

// Read reads the last committed offset from the file.
// Returns the offset and true if the file exists and contains a valid offset.
// Returns 0 and false if the file doesn't exist or contains invalid data.
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

	offsetStr := strings.TrimSpace(string(data))
	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		level.Error(f.logger).Log("msg", "failed to parse offset from file, will replay entire backlog", "file", f.filePath, "content", offsetStr, "err", err)
		return 0, false
	}

	level.Info(f.logger).Log("msg", "read last committed offset from file", "file", f.filePath, "offset", offset)
	return offset, true
}

// Write writes the offset to the file atomically.
func (f *offsetFile) Write(offset int64) error {
	if f == nil {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Create the parent directory if it doesn't exist
	dir := filepath.Dir(f.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Write to a temporary file first, then rename for atomicity
	tmpFile := f.filePath + ".tmp"
	data := []byte(fmt.Sprintf("%d\n", offset))

	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write to temporary offset file %s: %w", tmpFile, err)
	}

	if err := os.Rename(tmpFile, f.filePath); err != nil {
		// Clean up the temporary file if rename fails
		_ = os.Remove(tmpFile)
		return fmt.Errorf("failed to rename temporary offset file %s to %s: %w", tmpFile, f.filePath, err)
	}

	level.Debug(f.logger).Log("msg", "wrote offset to file", "file", f.filePath, "offset", offset)
	return nil
}
