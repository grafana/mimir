// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/multierror"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"

	"github.com/grafana/mimir/pkg/util/atomicfs"
)

const lazyLoadedHeadersListFileName = "lazy-loaded.json"

type SnapshotterConfig struct {
	PersistInterval time.Duration
	// Path stores where lazy loaded blocks will be tracked in a single file per tenant
	Path   string
	UserID string
}

// Snapshotter manages the snapshots of lazy loaded blocks.
type Snapshotter struct {
	services.Service

	logger log.Logger
	conf   SnapshotterConfig

	bl BlocksLoader

	// lastChecksum stores the checksum of the last persisted JSON data
	// to avoid writing the same data repeatedly, reducing IOPS.
	// This is useful when running with many tenants on low-performance disks.
	lastChecksum [sha256.Size]byte
}

func NewSnapshotter(logger log.Logger, conf SnapshotterConfig, bl BlocksLoader) *Snapshotter {
	s := &Snapshotter{
		logger: logger,
		conf:   conf,
		bl:     bl,
	}
	s.Service = services.NewTimerService(conf.PersistInterval, nil, s.persist, nil)
	return s
}

type BlocksLoader interface {
	LoadedBlocks() []ulid.ULID
}

func (s *Snapshotter) persist(context.Context) error {
	err := s.PersistLoadedBlocks()
	if err != nil {
		// Note, the decision here is to only log the error but not failing the job. We may reconsider that later.
		level.Warn(s.logger).Log("msg", "failed to persist list of lazy-loaded index headers", "err", err)
	}
	// Never return an error because we want to persist the list of lazy-loaded index headers as a best effort
	return nil
}

func (s *Snapshotter) PersistLoadedBlocks() error {
	loadedBlocks := s.bl.LoadedBlocks()

	// Convert to the format we store on disk for backward compatibility
	indexHeaderLastUsedTime := make(map[ulid.ULID]int64, len(loadedBlocks))
	for _, blockID := range loadedBlocks {
		// We use a constant timestamp since we no longer care about the actual timestamp
		indexHeaderLastUsedTime[blockID] = 1
	}

	snapshot := &indexHeadersSnapshot{
		IndexHeaderLastUsedTime: indexHeaderLastUsedTime,
		UserID:                  s.conf.UserID,
	}

	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}

	// The json marshalling is deterministic, so the checksum will be the same for the same map contents.
	checksum := sha256.Sum256(data)
	if checksum == s.lastChecksum {
		level.Debug(s.logger).Log("msg", "skipping persistence of index headers snapshot as data hasn't changed", "user", s.conf.UserID)
		return nil
	}

	finalPath := filepath.Join(s.conf.Path, lazyLoadedHeadersListFileName)
	err = atomicfs.CreateFile(finalPath, bytes.NewReader(data))
	if err == nil {
		// Only update the checksum if the write was successful
		s.lastChecksum = checksum
	}
	return err
}

func RestoreLoadedBlocks(directory string) (map[ulid.ULID]struct{}, error) {
	var (
		snapshot indexHeadersSnapshot
		multiErr = multierror.MultiError{}
	)
	fileName := filepath.Join(directory, lazyLoadedHeadersListFileName)
	err := loadIndexHeadersSnapshot(fileName, &snapshot)
	if err != nil {
		if os.IsNotExist(err) {
			// We didn't find the snapshot. Could be because the previous binary didn't support eager loading.
			return nil, nil
		}
		multiErr.Add(fmt.Errorf("reading list of index headers from snapshot: %w", err))
		// We will remove the file only on error.
		// Note, in the case such as snapshot loading causing OOM, an operator will need to
		// remove the snapshot manually and let it lazy load after server restarts.
		// The current experience is that this is less of a problem than not eagerly loading
		// index headers after two consecutive restarts (ref grafana/mimir#8281).
		if err := os.Remove(fileName); err != nil {
			multiErr.Add(fmt.Errorf("removing the lazy-loaded index-header snapshot: %w", err))
		}
	}

	// Snapshots used to be stored with their last-used timestamp. But that wasn't used and lead to constant file churn, so we removed it.
	result := make(map[ulid.ULID]struct{}, len(snapshot.IndexHeaderLastUsedTime))
	for blockID := range snapshot.IndexHeaderLastUsedTime {
		result[blockID] = struct{}{}
	}

	return result, multiErr.Err()
}

type indexHeadersSnapshot struct {
	// IndexHeaderLastUsedTime is map of index header ulid.ULID to the number 1.
	// The number used to be the last-used timestamp of each block.
	// We keep this format for backward compatibility, but we no longer care about the timestamps.
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
