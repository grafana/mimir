// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/reader_pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/util/fsync"
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
	lazyReaderEnabled     bool
	lazyReaderIdleTimeout time.Duration
	logger                log.Logger
	metrics               *ReaderPoolMetrics

	// Channel used to signal once the pool is closing.
	close chan struct{}

	// Keep track of all readers managed by the pool.
	lazyReadersMx sync.Mutex
	lazyReaders   map[*LazyBinaryReader]struct{}
}

// LazyLoadedHeadersSnapshotConfig stores information needed to track lazy loaded index headers.
type LazyLoadedHeadersSnapshotConfig struct {
	Path   string
	UserID string
}

type lazyLoadedHeadersSnapshot struct {
	// HeaderLastUsedTime is map of index header ulid.ULID to timestamp in millisecond.
	HeaderLastUsedTime map[ulid.ULID]int64 `json:"header_last_used_time"`
	UserID             string              `json:"user_id"`
}

// persist creates a file to store lazy loaded index header in atomic way.
func (l lazyLoadedHeadersSnapshot) persist(persistDir string) error {
	// Create temporary path for fsync.
	// We don't use temporary folder because the process might not have access to the temporary folder.
	tmpPath := filepath.Join(persistDir, strings.Join([]string{"tmp", lazyLoadedHeadersListFile}, "-"))
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer os.Remove(tmpFile.Name())

	data, err := json.Marshal(l)
	if err != nil {
		return err
	}

	if err := fsync.CreateFile(tmpFile, func(f *os.File) (int, error) {
		return f.Write(data)
	}); err != nil {
		return err
	}
	
	// move the written file to the actual path
	finalPath := filepath.Join(persistDir, lazyLoadedHeadersListFile)
	return os.Rename(tmpFile.Name(), finalPath)
}

// NewReaderPool makes a new ReaderPool. If lazy-loading is enabled, NewReaderPool also starts a background task for unloading idle Readers and persisting a list of loaded Readers to disk.
func NewReaderPool(logger log.Logger, lazyReaderEnabled bool, lazyReaderIdleTimeout time.Duration, metrics *ReaderPoolMetrics, lazyLoadedSnapshotConfig LazyLoadedHeadersSnapshotConfig) *ReaderPool {
	p := newReaderPool(logger, lazyReaderEnabled, lazyReaderIdleTimeout, metrics)

	// Start a goroutine to close idle readers (only if required).
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		checkFreq := p.lazyReaderIdleTimeout / 10

		go func() {
			tickerLazyLoad := time.NewTicker(time.Minute)
			defer tickerLazyLoad.Stop()

			tickerIdleReader := time.NewTicker(checkFreq)
			defer tickerIdleReader.Stop()

			for {
				select {
				case <-p.close:
					return
				case <-tickerIdleReader.C:
					p.closeIdleReaders()
				case <-tickerLazyLoad.C:
					snapshot := lazyLoadedHeadersSnapshot{
						HeaderLastUsedTime: p.LoadedBlocks(),
						UserID:             lazyLoadedSnapshotConfig.UserID,
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
func newReaderPool(logger log.Logger, lazyReaderEnabled bool, lazyReaderIdleTimeout time.Duration, metrics *ReaderPoolMetrics) *ReaderPool {
	return &ReaderPool{
		logger:                logger,
		metrics:               metrics,
		lazyReaderEnabled:     lazyReaderEnabled,
		lazyReaderIdleTimeout: lazyReaderIdleTimeout,
		lazyReaders:           make(map[*LazyBinaryReader]struct{}),
		close:                 make(chan struct{}),
	}
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
		reader, err = NewLazyBinaryReader(ctx, readerFactory, logger, bkt, dir, id, p.metrics.lazyReader, p.onLazyReaderClosed)
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
