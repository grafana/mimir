// SPDX-License-Identifier: AGPL-3.0-only

package bucketindex

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/runutil"

	"github.com/grafana/mimir/pkg/util/atomicfs"
)

const (
	inProgressIndexFilename = "in-progress-index.json.gz"
	cleanerCacheDir         = "cleaner-cache"
)

// InProgressIndex stores an in-progress bucket index along with metadata
// needed to determine if it's still valid to use.
type InProgressIndex struct {
	// The in-progress bucket index.
	Index *Index `json:"index"`

	// BaseUpdatedAt is the UpdatedAt timestamp of the bucket index that was read
	// from object storage before running UpdateIndex(). This is used to detect
	// if another compactor updated the bucket index while we were working.
	// If 0, it means we started from scratch (no index existed in object storage).
	BaseUpdatedAt int64 `json:"base_updated_at"`

	// CreatedAt is a unix timestamp (seconds precision) of when this in-progress
	// index was saved.
	CreatedAt int64 `json:"created_at"`
}

// inProgressIndexPath returns the path to the in-progress index file for a user.
func inProgressIndexPath(dataDir, userID string) string {
	return filepath.Join(dataDir, cleanerCacheDir, userID, inProgressIndexFilename)
}

// WriteInProgressIndex writes the in-progress index to disk atomically.
// The file is written to a temporary location and then renamed to ensure
// atomicity. If dataDir is empty, this function returns nil without doing anything.
func WriteInProgressIndex(dataDir, userID string, idx *InProgressIndex, logger log.Logger) error {
	if dataDir == "" {
		return nil
	}

	path := inProgressIndexPath(dataDir, userID)

	// Ensure directory exists.
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	// Marshal the index to JSON.
	content, err := json.Marshal(idx)
	if err != nil {
		return err
	}

	// Compress it.
	var gzipContent bytes.Buffer
	gzipWriter := gzip.NewWriter(&gzipContent)
	gzipWriter.Name = inProgressIndexFilename

	if _, err := gzipWriter.Write(content); err != nil {
		return err
	}
	if err := gzipWriter.Close(); err != nil {
		return err
	}

	// Write atomically using atomicfs.
	if err := atomicfs.CreateFile(path, bytes.NewReader(gzipContent.Bytes())); err != nil {
		return err
	}

	level.Debug(logger).Log("msg", "wrote in-progress index to disk", "path", path, "created_at", time.Unix(idx.CreatedAt, 0))
	return nil
}

// ReadInProgressIndex reads the in-progress index from disk.
// Returns (nil, nil) if the file doesn't exist.
// If dataDir is empty, returns (nil, nil).
func ReadInProgressIndex(dataDir, userID string, logger log.Logger) (*InProgressIndex, error) {
	if dataDir == "" {
		return nil, nil
	}

	path := inProgressIndexPath(dataDir, userID)

	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithLogOnErr(logger, f, "close in-progress index file")

	gzipReader, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer runutil.CloseWithLogOnErr(logger, gzipReader, "close in-progress index gzip reader")

	var idx InProgressIndex
	if err := json.NewDecoder(gzipReader).Decode(&idx); err != nil {
		return nil, err
	}

	level.Debug(logger).Log("msg", "read in-progress index from disk", "path", path, "created_at", time.Unix(idx.CreatedAt, 0), "base_updated_at", idx.BaseUpdatedAt)
	return &idx, nil
}

// DeleteInProgressIndex removes the in-progress index file.
// It's a no-op if the file doesn't exist or dataDir is empty.
func DeleteInProgressIndex(dataDir, userID string, logger log.Logger) error {
	if dataDir == "" {
		return nil
	}

	path := inProgressIndexPath(dataDir, userID)
	err := os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err == nil {
		level.Debug(logger).Log("msg", "deleted in-progress index file", "path", path)
	}
	return err
}
