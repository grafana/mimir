// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/reader_pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

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
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/util/atomicfs"
)

var lazyLoadedHeadersListFile = "lazy-loaded.json"

// ReaderPoolMetrics holds metrics tracked by ReaderPool.
type ReaderPoolMetrics struct {
	lazyReader   *LazyBinaryReaderMetrics
	streamReader *StreamBinaryReaderMetrics
}

// NewReaderPoolMetrics makes new ReaderPoolMetrics.
func NewReaderPoolMetrics(reg prometheus.Registerer) *ReaderPoolMetrics {
	return &ReaderPoolMetrics{
		lazyReader:   NewLazyBinaryReaderMetrics(reg),
		streamReader: NewStreamBinaryReaderMetrics(reg),
	}
}

// ReaderPool is used to istantiate new index-header readers and keep track of them.
// When the lazy reader is enabled, the pool keeps track of all instantiated readers
// and automatically close them once the idle timeout is reached. A closed lazy reader
// will be automatically re-opened upon next usage.
type ReaderPool struct {
	lazyReaderEnabled      bool
	lazyReaderIdleTimeout  time.Duration
	eagerLoadReaderEnabled bool
	logger                 log.Logger
	metrics                *ReaderPoolMetrics

	// Channel used to signal once the pool is closing.
	close chan struct{}

	// Keep track of all readers managed by the pool.
	lazyReadersMx             sync.Mutex
	lazyReaders               map[*LazyBinaryReader]struct{}
	lazyLoadedHeadersSnapshot *lazyLoadedHeadersSnapshot
}

// LazyLoadedHeadersSnapshotConfig stores information needed to track lazy loaded index headers.
type LazyLoadedHeadersSnapshotConfig struct {
	Path                                   string
	UserID                                 string
	IndexHeadersEagerLoadingStartupEnabled bool
}

type lazyLoadedHeadersSnapshot struct {
	// IndexHeaderLastUsedTime is map of index header ulid.ULID to timestamp in millisecond.
	IndexHeaderLastUsedTime map[ulid.ULID]int64 `json:"index_header_last_used_time"`
	UserID                  string              `json:"user_id"`
}

// persist atomically writes this snapshot to persistDir.
func (l lazyLoadedHeadersSnapshot) persist(persistDir string) error {
	// Create temporary path for fsync.
	// We don't use temporary folder because the process might not have access to the temporary folder.
	tmpPath := filepath.Join(persistDir, "tmp-"+lazyLoadedHeadersListFile)
	// the actual path we want to store the file in
	finalPath := filepath.Join(persistDir, lazyLoadedHeadersListFile)

	data, err := json.Marshal(l)
	if err != nil {
		return err
	}

	return atomicfs.CreateFileAndMove(tmpPath, finalPath, bytes.NewReader(data))
}

// NewReaderPool makes a new ReaderPool. If lazy-loading is enabled, NewReaderPool also starts a background task for unloading idle Readers and persisting a list of loaded Readers to disk.
func NewReaderPool(logger log.Logger, lazyReaderEnabled bool, lazyReaderIdleTimeout time.Duration, metrics *ReaderPoolMetrics, lazyLoadedSnapshotConfig LazyLoadedHeadersSnapshotConfig) *ReaderPool {
	var snapshot *lazyLoadedHeadersSnapshot
	if lazyReaderEnabled && lazyLoadedSnapshotConfig.IndexHeadersEagerLoadingStartupEnabled {
		lazyLoadedSnapshotFileName := filepath.Join(lazyLoadedSnapshotConfig.Path, lazyLoadedHeadersListFile)
		var err error
		snapshot, err = loadLazyLoadedHeadersSnapshot(lazyLoadedSnapshotFileName)
		if err != nil {
			level.Warn(logger).Log("msg", "loading lazy-loaded index header failed; removing the file", "file", lazyLoadedSnapshotFileName, "err", err)
			err := os.Remove(lazyLoadedSnapshotFileName)
			if err != nil {
				level.Warn(logger).Log("msg", "removing lazy-loaded index header failed", "file", lazyLoadedSnapshotFileName, "err", err)
			}
		}
	}

	p := newReaderPool(logger, lazyReaderEnabled, lazyReaderIdleTimeout, lazyLoadedSnapshotConfig.IndexHeadersEagerLoadingStartupEnabled, metrics, snapshot)

	// Start a goroutine to close idle readers (only if required).
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		checkFreq := p.lazyReaderIdleTimeout / 10

		go func() {
			tickerIdleReader := time.NewTicker(checkFreq)
			defer tickerIdleReader.Stop()

			var lazyLoadC <-chan time.Time

			if p.eagerLoadReaderEnabled {
				tickerLazyLoadPersist := time.NewTicker(time.Minute)
				defer tickerLazyLoadPersist.Stop()

				lazyLoadC = tickerLazyLoadPersist.C
			}

			for {
				select {
				case <-p.close:
					return
				case <-tickerIdleReader.C:
					p.closeIdleReaders()
				case <-lazyLoadC:
					snapshot := lazyLoadedHeadersSnapshot{
						IndexHeaderLastUsedTime: p.LoadedBlocks(),
						UserID:                  lazyLoadedSnapshotConfig.UserID,
					}

					if err := snapshot.persist(lazyLoadedSnapshotConfig.Path); err != nil {
						level.Warn(p.logger).Log("msg", "failed to persist list of lazy-loaded index headers", "err", err)
					}
				}
			}

		}()
	}

	return p
}

// newReaderPool makes a new ReaderPool.
func newReaderPool(logger log.Logger, lazyReaderEnabled bool, lazyReaderIdleTimeout time.Duration, eagerLoadReaderEnabled bool, metrics *ReaderPoolMetrics, lazyLoadedHeadersSnapshot *lazyLoadedHeadersSnapshot) *ReaderPool {
	return &ReaderPool{
		logger:                    logger,
		metrics:                   metrics,
		lazyReaderEnabled:         lazyReaderEnabled,
		lazyReaderIdleTimeout:     lazyReaderIdleTimeout,
		eagerLoadReaderEnabled:    eagerLoadReaderEnabled,
		lazyReaders:               make(map[*LazyBinaryReader]struct{}),
		close:                     make(chan struct{}),
		lazyLoadedHeadersSnapshot: lazyLoadedHeadersSnapshot,
	}
}

func loadLazyLoadedHeadersSnapshot(fileName string) (*lazyLoadedHeadersSnapshot, error) {
	snapshotBytes, err := os.ReadFile(fileName)
	if err != nil {
		return nil, err
	}
	snapshot := &lazyLoadedHeadersSnapshot{}
	err = json.Unmarshal(snapshotBytes, snapshot)
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

// NewBinaryReader creates and returns a new binary reader. If the pool has been configured
// with lazy reader enabled, this function will return a lazy reader. The returned lazy reader
// is tracked by the pool and automatically closed once the idle timeout expires.
func (p *ReaderPool) NewBinaryReader(ctx context.Context, logger log.Logger, bkt objstore.BucketReader, dir string, id ulid.ULID, postingOffsetsInMemSampling int, cfg Config) (Reader, error) {
	var readerFactory func() (Reader, error)
	var reader Reader
	var err error

	readerFactory = func() (Reader, error) {
		return NewStreamBinaryReader(ctx, logger, bkt, dir, id, postingOffsetsInMemSampling, p.metrics.streamReader, cfg)
	}

	if p.lazyReaderEnabled {
		lazyBinaryReader, lazyErr := NewLazyBinaryReader(ctx, readerFactory, logger, bkt, dir, id, p.metrics.lazyReader, p.onLazyReaderClosed)
		if p.eagerLoadReaderEnabled && p.lazyLoadedHeadersSnapshot != nil {
			if p.lazyLoadedHeadersSnapshot.IndexHeaderLastUsedTime[id] > 0 {
				lazyBinaryReader.EagerLoad()
			}
		}
		reader, err = lazyBinaryReader, lazyErr
	} else {
		reader, err = readerFactory()
	}

	if err != nil {
		return nil, err
	}

	// Keep track of lazy readers only if required.
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		p.lazyReadersMx.Lock()
		p.lazyReaders[reader.(*LazyBinaryReader)] = struct{}{}
		p.lazyReadersMx.Unlock()
	}

	return reader, err
}

// Close the pool and stop checking for idle readers. No reader tracked by this pool
// will be closed. It's the caller responsibility to close readers.
func (p *ReaderPool) Close() {
	close(p.close)
}

func (p *ReaderPool) closeIdleReaders() {
	idleTimeoutAgo := time.Now().Add(-p.lazyReaderIdleTimeout).UnixNano()

	for _, r := range p.getIdleReadersSince(idleTimeoutAgo) {
		if err := r.unloadIfIdleSince(idleTimeoutAgo); err != nil && !errors.Is(err, errNotIdle) {
			level.Warn(p.logger).Log("msg", "failed to close idle index-header reader", "err", err)
		}
	}
}

func (p *ReaderPool) getIdleReadersSince(ts int64) []*LazyBinaryReader {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	var idle []*LazyBinaryReader
	for r := range p.lazyReaders {
		if r.isIdleSince(ts) {
			idle = append(idle, r)
		}
	}

	return idle
}

func (p *ReaderPool) isTracking(r *LazyBinaryReader) bool {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	_, ok := p.lazyReaders[r]
	return ok
}

func (p *ReaderPool) onLazyReaderClosed(r *LazyBinaryReader) {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	// When this function is called, it means the reader has been closed NOT because was idle
	// but because the consumer closed it. By contract, a reader closed by the consumer can't
	// be used anymore, so we can automatically remove it from the pool.
	delete(p.lazyReaders, r)
}

// LoadedBlocks returns a new map of lazy-loaded block IDs and the last time they were used in milliseconds.
func (p *ReaderPool) LoadedBlocks() map[ulid.ULID]int64 {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	blocks := make(map[ulid.ULID]int64, len(p.lazyReaders))
	for r := range p.lazyReaders {
		if r.reader != nil {
			blocks[r.blockID] = r.usedAt.Load() / int64(time.Millisecond)
		}
	}

	return blocks
}
