// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/reader_pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"
)

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
	services.Service

	lazyReaderEnabled     bool
	lazyReaderIdleTimeout time.Duration
	logger                log.Logger
	metrics               *ReaderPoolMetrics

	// Gate used to limit the number of concurrent index-header loads.
	lazyLoadingGate gate.Gate

	// Keep track of all readers managed by the pool.
	lazyReadersMx sync.Mutex
	lazyReaders   map[*LazyBinaryReader]struct{}
}

// NewReaderPool makes a new ReaderPool. If lazy-loading is enabled, NewReaderPool also starts a background task for unloading idle Readers.
func NewReaderPool(logger log.Logger, indexHeaderConfig Config, lazyLoadingGate gate.Gate, metrics *ReaderPoolMetrics) *ReaderPool {
	p := newReaderPool(logger, indexHeaderConfig, lazyLoadingGate, metrics)
	if !p.lazyReaderEnabled || p.lazyReaderIdleTimeout <= 0 {
		p.Service = services.NewIdleService(nil, nil)
	} else {
		p.Service = services.NewTimerService(p.lazyReaderIdleTimeout/10, nil, p.unloadIdleReaders, nil)
	}
	return p
}

// newReaderPool makes a new ReaderPool.
func newReaderPool(logger log.Logger, indexHeaderConfig Config, lazyLoadingGate gate.Gate, metrics *ReaderPoolMetrics) *ReaderPool {
	return &ReaderPool{
		logger:                logger,
		metrics:               metrics,
		lazyReaderEnabled:     indexHeaderConfig.LazyLoadingEnabled,
		lazyReaderIdleTimeout: indexHeaderConfig.LazyLoadingIdleTimeout,
		lazyReaders:           make(map[*LazyBinaryReader]struct{}),
		lazyLoadingGate:       lazyLoadingGate,
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
		reader, err = NewLazyBinaryReader(ctx, readerFactory, logger, bkt, dir, id, p.metrics.lazyReader, p.onLazyReaderClosed, p.lazyLoadingGate)
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

func (p *ReaderPool) unloadIdleReaders(context.Context) error {
	idleTimeoutAgo := time.Now().Add(-p.lazyReaderIdleTimeout).UnixNano()

	for _, r := range p.getIdleReadersSince(idleTimeoutAgo) {
		if err := r.unloadIfIdleSince(idleTimeoutAgo); err != nil && !errors.Is(err, errNotIdle) {
			level.Warn(p.logger).Log("msg", "failed to close idle index-header reader", "err", err)
		}
	}
	return nil // always return nil to avoid stopping the service
}

func (p *ReaderPool) getIdleReadersSince(ts int64) []*LazyBinaryReader {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	var idle []*LazyBinaryReader
	for r := range p.lazyReaders {
		if r.IsIdleSince(ts) {
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
		usedAt := r.usedAt.Load()
		if usedAt != 0 {
			blocks[r.blockID] = usedAt / int64(time.Millisecond)
		}
	}

	return blocks
}
