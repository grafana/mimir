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
	"github.com/grafana/dskit/gate"
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
	logger          log.Logger
	filepath        string
	metrics         *LazyBinaryReaderMetrics
	onClosed        func(*LazyBinaryReader)
	lazyLoadingGate gate.Gate
	ctx             context.Context

	readerMx      sync.RWMutex
	reader        Reader
	readerErr     error
	readerInUse   sync.WaitGroup // Only increased when readerMx is held.
	readerFactory func() (Reader, error)

	// Keep track of the last time it was used.
	usedAt *atomic.Int64

	blockID ulid.ULID
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
	lazyLoadingGate gate.Gate,
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
		logger:          logger,
		filepath:        path,
		metrics:         metrics,
		usedAt:          atomic.NewInt64(0),
		onClosed:        onClosed,
		readerFactory:   readerFactory,
		blockID:         id,
		lazyLoadingGate: lazyLoadingGate,
		ctx:             ctx,
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
	reader, wg, err := r.getOrLoadReader()
	if err != nil {
		return 0, err
	}
	defer wg.Done()

	return reader.IndexVersion()
}

// PostingsOffset implements Reader.
func (r *LazyBinaryReader) PostingsOffset(name, value string) (index.Range, error) {
	reader, wg, err := r.getOrLoadReader()
	if err != nil {
		return index.Range{}, err
	}
	defer wg.Done()

	return reader.PostingsOffset(name, value)
}

// LookupSymbol implements Reader.
func (r *LazyBinaryReader) LookupSymbol(o uint32) (string, error) {
	reader, wg, err := r.getOrLoadReader()
	if err != nil {
		return "", err
	}
	defer wg.Done()

	return reader.LookupSymbol(o)
}

// SymbolsReader implements Reader.
func (r *LazyBinaryReader) SymbolsReader() (streamindex.SymbolsReader, error) {
	reader, wg, err := r.getOrLoadReader()
	if err != nil {
		return nil, err
	}

	sr, err := reader.SymbolsReader()
	if err != nil {
		wg.Done()
		return nil, err
	}
	return newLazySymbolsReader(sr, wg), nil
}

// LabelValuesOffsets implements Reader.
func (r *LazyBinaryReader) LabelValuesOffsets(ctx context.Context, name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error) {
	reader, wg, err := r.getOrLoadReader()
	if err != nil {
		return nil, err
	}
	defer wg.Done()

	return reader.LabelValuesOffsets(ctx, name, prefix, filter)
}

// LabelNames implements Reader.
func (r *LazyBinaryReader) LabelNames() ([]string, error) {
	reader, wg, err := r.getOrLoadReader()
	if err != nil {
		return nil, err
	}
	defer wg.Done()

	return reader.LabelNames()
}

// EagerLoad attempts to eagerly load this index header.
func (r *LazyBinaryReader) EagerLoad() {
	_, wg, err := r.getOrLoadReader()
	if err != nil {
		level.Warn(r.logger).Log("msg", "eager loading of lazy loaded index-header failed; skipping", "err", err)
		return
	}
	wg.Done()
}

// getOrLoadReader ensures the underlying binary index-header reader has been successfully loaded.
// Returns the reader, wait group that should be used to signal that usage of reader is finished, and an error on failure.
// Must be called without lock.
func (r *LazyBinaryReader) getOrLoadReader() (Reader, *sync.WaitGroup, error) {
	r.readerMx.RLock()
	defer r.readerMx.RUnlock()

	// Nothing to do if we already tried loading it.
	if r.reader != nil {
		r.usedAt.Store(time.Now().UnixNano())
		r.readerInUse.Add(1)

		return r.reader, &r.readerInUse, nil
	}
	if r.readerErr != nil {
		return nil, nil, r.readerErr
	}

	// Release the read lock, so that loadReader can take write lock. Take the read lock again once done.
	r.readerMx.RUnlock()
	err := r.loadReader()
	// Re-acquire read lock.
	r.readerMx.RLock()

	if err != nil {
		return nil, nil, err
	}
	// Between the write lock release and the subsequent read lock, the unload() may have run,
	// so we make sure to catch this edge case.
	if r.reader == nil {
		return nil, nil, errUnloadedWhileLoading
	}

	r.usedAt.Store(time.Now().UnixNano())
	r.readerInUse.Add(1)
	return r.reader, &r.readerInUse, nil
}

// loadReader is called from getOrLoadReader, without any locks.
func (r *LazyBinaryReader) loadReader() error {
	// lazyLoadingGate implementation: blocks load if too many are happening at once.
	// It's important to get permit from the Gate when NOT holding the read-lock, otherwise we risk that multiple goroutines
	// that enter `load()` will deadlock themselves. (If Start() allows one goroutine to continue, but blocks another one,
	// then goroutine that continues would not be able to get Write lock.)
	err := r.lazyLoadingGate.Start(r.ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to wait for turn")
	}
	defer r.lazyLoadingGate.Done()

	r.readerMx.Lock()
	defer r.readerMx.Unlock()

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

	// Wait until all users finished using current reader.
	r.readerInUse.Wait()

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

type lazySymbolsReader struct {
	sr   streamindex.SymbolsReader
	wg   *sync.WaitGroup
	once sync.Once
}

func newLazySymbolsReader(sr streamindex.SymbolsReader, wg *sync.WaitGroup) *lazySymbolsReader {
	return &lazySymbolsReader{
		sr: sr,
		wg: wg,
	}
}

func (l *lazySymbolsReader) Read(u uint32) (string, error) {
	return l.sr.Read(u)
}

func (l *lazySymbolsReader) Close() error {
	err := l.sr.Close()
	l.once.Do(l.wg.Done)
	return err
}
