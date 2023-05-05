// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/lazy_binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	streamindex "github.com/grafana/mimir/pkg/storegateway/indexheader/index"
)

var (
	errNotIdle              = errors.New("the reader is not idle")
	errUnloadedWhileLoading = errors.New("the index-header has been concurrently unloaded")
)

// LazyBinaryReaderMetrics holds metrics tracked by LazyBinaryReader.
type LazyBinaryReaderMetrics struct {
	loadCount         prometheus.Counter
	loadFailedCount   prometheus.Counter
	unloadCount       prometheus.Counter
	unloadFailedCount prometheus.Counter
	loadDuration      prometheus.Histogram
}

// NewLazyBinaryReaderMetrics makes new LazyBinaryReaderMetrics.
func NewLazyBinaryReaderMetrics(reg prometheus.Registerer) *LazyBinaryReaderMetrics {
	return &LazyBinaryReaderMetrics{
		loadCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_lazy_load_total",
			Help: "Total number of index-header lazy load operations.",
		}),
		loadFailedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_lazy_load_failed_total",
			Help: "Total number of failed index-header lazy load operations.",
		}),
		unloadCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_lazy_unload_total",
			Help: "Total number of index-header lazy unload operations.",
		}),
		unloadFailedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "indexheader_lazy_unload_failed_total",
			Help: "Total number of failed index-header lazy unload operations.",
		}),
		loadDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "indexheader_lazy_load_duration_seconds",
			Help:    "Duration of the index-header lazy loading in seconds.",
			Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 15, 30, 60, 120, 300},
		}),
	}
}

// LazyBinaryReader wraps BinaryReader and loads (mmap or streaming read) the index-header only upon
// the first Reader function is called.
type LazyBinaryReader struct {
	logger   log.Logger
	filepath string
	metrics  *LazyBinaryReaderMetrics
	onClosed func(*LazyBinaryReader)

	readerMx      sync.RWMutex
	reader        Reader
	readerErr     error
	readerFactory func() (Reader, error)

	// Keep track of the last time it was used.
	usedAt *atomic.Int64
}

// NewLazyBinaryReader makes a new LazyBinaryReader. If the index-header does not exist
// on the local disk at dir location, this function will build it downloading required
// sections from the full index stored in the bucket. However, this function doesn't load
// (mmap or streaming read) the index-header; it will be loaded at first Reader function call.
func NewLazyBinaryReader(
	ctx context.Context,
	readerFactory func() (Reader, error),
	logger log.Logger,
	bkt objstore.BucketReader,
	dir string,
	id ulid.ULID,
	metrics *LazyBinaryReaderMetrics,
	onClosed func(*LazyBinaryReader),
) (*LazyBinaryReader, error) {
	path := filepath.Join(dir, id.String(), block.IndexHeaderFilename)

	// If the index-header doesn't exist we should download it.
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, errors.Wrap(err, "read index header")
		}

		level.Debug(logger).Log("msg", "the index-header doesn't exist on disk; recreating", "path", path)

		start := time.Now()
		if err := WriteBinary(ctx, bkt, id, path); err != nil {
			return nil, errors.Wrap(err, "write index header")
		}

		level.Debug(logger).Log("msg", "built index-header file", "path", path, "elapsed", time.Since(start))
	}

	return &LazyBinaryReader{
		logger:        logger,
		filepath:      path,
		metrics:       metrics,
		usedAt:        atomic.NewInt64(time.Now().UnixNano()),
		onClosed:      onClosed,
		readerFactory: readerFactory,
	}, nil
}

// Close implements Reader. It unloads the index-header from memory (releasing the mmap
// area), but a subsequent call to any other Reader function will automatically reload it.
func (r *LazyBinaryReader) Close() error {
	if r.onClosed != nil {
		defer r.onClosed(r)
	}

	// Unload without checking if idle.
	return r.unloadIfIdleSince(0)
}

// IndexVersion implements Reader.
func (r *LazyBinaryReader) IndexVersion() (int, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.load(); err != nil {
		return 0, err
	}

	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.IndexVersion()
}

// PostingsOffset implements Reader.
func (r *LazyBinaryReader) PostingsOffset(name, value string) (index.Range, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.load(); err != nil {
		return index.Range{}, err
	}

	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.PostingsOffset(name, value)
}

// LookupSymbol implements Reader.
func (r *LazyBinaryReader) LookupSymbol(o uint32) (string, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.load(); err != nil {
		return "", err
	}

	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.LookupSymbol(o)
}

// LabelValuesOffsets implements Reader.
func (r *LazyBinaryReader) LabelValuesOffsets(name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.load(); err != nil {
		return nil, err
	}

	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.LabelValuesOffsets(name, prefix, filter)
}

// LabelNames implements Reader.
func (r *LazyBinaryReader) LabelNames() ([]string, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	if err := r.load(); err != nil {
		return nil, err
	}

	r.usedAt.Store(time.Now().UnixNano())
	return r.reader.LabelNames()
}

// load ensures the underlying binary index-header reader has been successfully loaded. Returns
// an error on failure. This function MUST be called with the read lock already acquired.
func (r *LazyBinaryReader) load() (returnErr error) {
	// Nothing to do if we already tried loading it.
	if r.reader != nil {
		return nil
	}
	if r.readerErr != nil {
		return r.readerErr
	}

	// Take the write lock to ensure we'll try to load it only once. Take again
	// the read lock once done.
	r.readerMx.RUnlock()
	r.readerMx.Lock()
	defer func() {
		r.readerMx.Unlock()
		r.readerMx.RLock()

		// Between the write unlock and the subsequent read lock, the unload() may have run,
		// so we make sure to catch this edge case.
		if returnErr == nil && r.reader == nil {
			returnErr = errUnloadedWhileLoading
		}
	}()

	// Ensure none else tried to load it in the meanwhile.
	if r.reader != nil {
		return nil
	}
	if r.readerErr != nil {
		return r.readerErr
	}

	level.Debug(r.logger).Log("msg", "lazy loading index-header file", "path", r.filepath)
	r.metrics.loadCount.Inc()
	startTime := time.Now()

	reader, err := r.readerFactory()
	if err != nil {
		r.metrics.loadFailedCount.Inc()
		r.readerErr = err
		return errors.Wrapf(err, "lazy load index-header file at %s", r.filepath)
	}

	r.reader = reader
	elapsed := time.Since(startTime)

	level.Debug(r.logger).Log("msg", "lazy loaded index-header file", "path", r.filepath, "elapsed", elapsed)
	r.metrics.loadDuration.Observe(elapsed.Seconds())

	return nil
}

// unloadIfIdleSince closes underlying BinaryReader if the reader is idle since given time (as unix nano). If idleSince is 0,
// the check on the last usage is skipped. Calling this function on a already unloaded reader is a no-op.
func (r *LazyBinaryReader) unloadIfIdleSince(ts int64) error {
	r.readerMx.Lock()
	defer r.readerMx.Unlock()

	// Nothing to do if already unloaded.
	if r.reader == nil {
		return nil
	}

	// Do not unloadIfIdleSince if not idle.
	if ts > 0 && r.usedAt.Load() > ts {
		return errNotIdle
	}

	r.metrics.unloadCount.Inc()
	if err := r.reader.Close(); err != nil {
		r.metrics.unloadFailedCount.Inc()
		return err
	}

	r.reader = nil
	return nil
}

// isIdleSince returns true if the reader is idle since given time (as unix nano).
func (r *LazyBinaryReader) isIdleSince(ts int64) bool {
	if r.usedAt.Load() > ts {
		return false
	}

	// A reader can be considered idle only if it's loaded.
	r.readerMx.RLock()
	loaded := r.reader != nil
	r.readerMx.RUnlock()

	return loaded
}
