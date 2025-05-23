// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/reader_pool.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"context"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/gate"
	"github.com/grafana/dskit/services"
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
<<<<<<< HEAD
	"github.com/prometheus-community/parquet-common/storage"
=======
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/indexheader"
)

// ReaderPoolMetrics holds metrics tracked by ReaderPool.
type ReaderPoolMetrics struct {
	lazyReader *LazyParquetReaderMetrics
	// streamReader *StreamBinaryReaderMetrics
}

// NewReaderPoolMetrics makes new ReaderPoolMetrics.
func NewReaderPoolMetrics(reg prometheus.Registerer) *ReaderPoolMetrics {
	return &ReaderPoolMetrics{
		lazyReader: NewLazyParquetReaderMetrics(reg),
		// streamReader: NewStreamBinaryReaderMetrics(reg),
	}
}

// ReaderPool is used to istantiate new index-header readers and keep track of them.
// When the lazy reader is enabled, the pool keeps track of all instantiated readers
// and automatically close them once the idle timeout is reached. A closed lazy reader
// will be automatically re-opened upon next usage.
type ReaderPool struct {
	services.Service

	lazyReaderEnabled      bool
	earlyValidationEnabled bool
	lazyReaderIdleTimeout  time.Duration
	logger                 log.Logger
	metrics                *ReaderPoolMetrics

	// Gate used to limit the number of concurrent index-header loads.
	lazyLoadingGate gate.Gate

	// Keep track of all readers managed by the pool.
	lazyReadersMx sync.Mutex
<<<<<<< HEAD
	lazyReaders   map[LazyReader]struct{}
=======
	lazyReaders   map[*LazyReaderLocalLabelsBucketChunks]struct{}
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
}

// NewReaderPool makes a new ReaderPool. If lazy-loading is enabled, NewReaderPool also starts a background task for unloading idle Readers.
func NewReaderPool(
	indexHeaderConfig indexheader.Config,
	lazyLoadingGate gate.Gate,
	logger log.Logger,
	metrics *ReaderPoolMetrics,
) *ReaderPool {
	p := newReaderPool(indexHeaderConfig, lazyLoadingGate, logger, metrics)
	if !p.lazyReaderEnabled || p.lazyReaderIdleTimeout <= 0 {
		panic("not implemented: parquet block reader pool without lazy loading")
		// p.Service = services.NewIdleService(nil, nil)
	} else {
		p.Service = services.NewTimerService(p.lazyReaderIdleTimeout/10, nil, p.unloadIdleReaders, nil)
	}
	return p
}

// newReaderPool makes a new ReaderPool.
func newReaderPool(
	indexHeaderConfig indexheader.Config,
	lazyLoadingGate gate.Gate,
	logger log.Logger,
	metrics *ReaderPoolMetrics,
) *ReaderPool {
	return &ReaderPool{
		logger:                 logger,
		metrics:                metrics,
		lazyReaderEnabled:      indexHeaderConfig.LazyLoadingEnabled,
		earlyValidationEnabled: false,
		lazyReaderIdleTimeout:  indexHeaderConfig.LazyLoadingIdleTimeout,
<<<<<<< HEAD
		lazyReaders:            make(map[LazyReader]struct{}),
=======
		lazyReaders:            make(map[*LazyReaderLocalLabelsBucketChunks]struct{}),
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
		lazyLoadingGate:        lazyLoadingGate,
	}
}

// GetReader creates and returns a new binary reader. If the pool has been configured
// with lazy reader enabled, this function will return a lazy reader. The returned lazy reader
// is tracked by the pool and automatically closed once the idle timeout expires.
func (p *ReaderPool) GetReader(
	ctx context.Context,
	blockID ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
	localDir string,
<<<<<<< HEAD
	loadIndexToDisk bool,
	fileOpts []storage.FileOption,
	logger log.Logger,
) (Reader, error) {
	var reader LazyReader
	var err error

	if loadIndexToDisk {
		reader, err = NewLazyReaderLocalLabelsBucketChunks(
			ctx,
			blockID,
			bkt,
			localDir,
			fileOpts,
			p.metrics.lazyReader,
			p.onLazyReaderClosed,
			p.lazyLoadingGate,
			p.earlyValidationEnabled,
			logger,
		)
	} else {
		reader, err = NewLazyReaderBucketLabelsAndChunks(
			ctx,
			blockID,
			bkt,
			localDir,
			fileOpts,
			p.metrics.lazyReader,
			p.onLazyReaderClosed,
			nil,
			p.earlyValidationEnabled,
			logger,
		)
	}
=======
	logger log.Logger,
) (Reader, error) {
	var reader Reader
	var err error

	reader, err = NewLazyReaderLocalLabelsBucketChunks(
		ctx,
		blockID,
		bkt,
		localDir,
		p.metrics.lazyReader,
		p.onLazyReaderClosed,
		p.lazyLoadingGate,
		p.earlyValidationEnabled,
		logger,
	)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))

	if err != nil {
		return nil, err
	}

	// Keep track of lazy readers only if required.
	if p.lazyReaderEnabled && p.lazyReaderIdleTimeout > 0 {
		p.lazyReadersMx.Lock()
<<<<<<< HEAD
		p.lazyReaders[reader] = struct{}{}
=======
		p.lazyReaders[reader.(*LazyReaderLocalLabelsBucketChunks)] = struct{}{}
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
		p.lazyReadersMx.Unlock()
	}

	return reader, err
}

func (p *ReaderPool) unloadIdleReaders(context.Context) error {
	idleTimeoutAgo := time.Now().Add(-p.lazyReaderIdleTimeout).UnixNano()

	for _, r := range p.getIdleReadersSince(idleTimeoutAgo) {
<<<<<<< HEAD
		if err := r.UnloadIfIdleSince(idleTimeoutAgo); err != nil && !errors.Is(err, errNotIdle) {
=======
		if err := r.unloadIfIdleSince(idleTimeoutAgo); err != nil && !errors.Is(err, errNotIdle) {
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
			level.Warn(p.logger).Log("msg", "failed to close idle index-header reader", "err", err)
		}
	}
	return nil // always return nil to avoid stopping the service
}

<<<<<<< HEAD
func (p *ReaderPool) getIdleReadersSince(ts int64) []LazyReader {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	var idle []LazyReader
=======
func (p *ReaderPool) getIdleReadersSince(ts int64) []*LazyReaderLocalLabelsBucketChunks {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	var idle []*LazyReaderLocalLabelsBucketChunks
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	for r := range p.lazyReaders {
		if r.IsIdleSince(ts) {
			idle = append(idle, r)
		}
	}

	return idle
}

<<<<<<< HEAD
func (p *ReaderPool) onLazyReaderClosed(r LazyReader) {
=======
func (p *ReaderPool) onLazyReaderClosed(r *LazyReaderLocalLabelsBucketChunks) {
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	// When this function is called, it means the reader has been closed NOT because was idle
	// but because the consumer closed it. By contract, a reader closed by the consumer can't
	// be used anymore, so we can automatically remove it from the pool.
	delete(p.lazyReaders, r)
}

func (p *ReaderPool) LoadedBlocks() []ulid.ULID {
	p.lazyReadersMx.Lock()
	defer p.lazyReadersMx.Unlock()

	blocks := make([]ulid.ULID, 0, len(p.lazyReaders))
	for r := range p.lazyReaders {
<<<<<<< HEAD
		usedAt := r.UsedAt()
		if usedAt != 0 {
			blocks = append(blocks, r.BlockID())
=======
		usedAt := r.usedAt.Load()
		if usedAt != 0 {
			blocks = append(blocks, r.blockID)
>>>>>>> bb537b2d7a (bring in prometheus/parquet-common code to new package (#11490))
		}
	}

	return blocks
}
