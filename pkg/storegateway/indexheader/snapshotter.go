// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"

	"github.com/grafana/mimir/pkg/util/atomicfs"
)

const lazyLoadedHeadersListFileName = "lazy-loaded.json"

type SnapshotterConfig struct {
	// Path stores where lazy loaded blocks will be tracked in a single file per tenant
	Path   string
	UserID string
}

// Snapshotter manages the snapshots of lazy loaded blocks.
type Snapshotter struct {
	logger log.Logger
	conf   SnapshotterConfig

	// when the running group is positive, this indicates the Snapshotter is active
	running sync.WaitGroup
	stop    chan struct{}
}

func NewSnapshotter(logger log.Logger, conf SnapshotterConfig) *Snapshotter {
	return &Snapshotter{
		logger: logger,
		conf:   conf,
		stop:   make(chan struct{}),
	}
}

type blocksLoader interface {
	LoadedBlocks() map[ulid.ULID]int64
}

// Start spawns a background job that periodically persists the list of lazy-loaded index headers.
func (s *Snapshotter) Start(ctx context.Context, bl blocksLoader) {
	s.running.Add(1)
	go func() {
		defer s.running.Done()

		err := s.PersistLoadedBlocks(bl)
		if err != nil {
			// Note, the decision here is to only log the error but not failing the job. We may reconsider that later.
			level.Warn(s.logger).Log("msg", "failed to persist initial list of lazy-loaded index headers", "err", err)
		}

		tick := time.NewTicker(time.Minute)
		defer tick.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-s.stop:
				return
			case <-tick.C:
				if err := s.PersistLoadedBlocks(bl); err != nil {
					level.Warn(s.logger).Log("msg", "failed to persist list of lazy-loaded index headers", "err", err)
				}
			}
		}
	}()
}

func (s *Snapshotter) Stop() {
	close(s.stop)
	s.running.Wait()
}

func (s *Snapshotter) PersistLoadedBlocks(bl blocksLoader) error {
	snapshot := &indexHeadersSnapshot{
		IndexHeaderLastUsedTime: bl.LoadedBlocks(),
		UserID:                  s.conf.UserID,
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	// Create temporary path for fsync.
	// We don't use temporary folder because the process might not have access to the temporary folder.
	tmpPath := filepath.Join(s.conf.Path, "tmp-"+lazyLoadedHeadersListFileName)
	// the actual path we want to store the file in
	finalPath := filepath.Join(s.conf.Path, lazyLoadedHeadersListFileName)

	return atomicfs.CreateFileAndMove(tmpPath, finalPath, bytes.NewReader(data))
}

func (s *Snapshotter) RestoreLoadedBlocks() map[ulid.ULID]int64 {
	var snapshot indexHeadersSnapshot
	fileName := filepath.Join(s.conf.Path, lazyLoadedHeadersListFileName)
	err := loadIndexHeadersSnapshot(fileName, &snapshot)
	if err != nil {
		if os.IsNotExist(err) {
			// We didn't find the snapshot. Could be because the previous binary didn't support eager loading.
			return nil
		}
		level.Warn(s.logger).Log(
			"msg", "loading the list of index-headers from snapshot file failed; not eagerly loading index-headers for tenant",
			"tenant", s.conf.UserID,
			"file", fileName,
			"err", err,
		)
		// We will remove the file only on error.
		// Note, in the case such as snapshot loading causing OOM, an operator will need to
		// remove the snapshot manually and let it lazy load after server restarts.
		// The current experience is that this is less of a problem than not eagerly loading
		// index headers after two consecutive restarts (ref grafana/mimir#8281).
		if err := os.Remove(fileName); err != nil {
			level.Warn(s.logger).Log("msg", "removing the lazy-loaded index-header snapshot failed", "file", fileName, "err", err)
		}
	}
	return snapshot.IndexHeaderLastUsedTime
}

type indexHeadersSnapshot struct {
	// IndexHeaderLastUsedTime is map of index header ulid.ULID to timestamp in millisecond.
	IndexHeaderLastUsedTime map[ulid.ULID]int64 `json:"index_header_last_used_time"`
	UserID                  string              `json:"user_id"`
}

func loadIndexHeadersSnapshot(fileName string, snapshot *indexHeadersSnapshot) error {
	snapshotBytes, err := os.ReadFile(fileName)
	if err != nil {
		return err
	}
	return json.Unmarshal(snapshotBytes, snapshot)
}
