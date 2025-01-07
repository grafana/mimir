// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/lazy_binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package indexheader

import (
	"context"
	"fmt"
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

	loadedReader chan readerRequest
	unloadReq    chan unloadRequest

	// Keep track of the last time it was used.
	usedAt *atomic.Int64

	readerFactory func() (Reader, error)

	blockID ulid.ULID
	done    chan struct{}
}

type readerRequest struct {
	response chan loadedReader
}

// loadedReader represents an attempt to load a Reader. If the attempt failed, then err is set and reader is nil.
// If the attempt succeeded, then err is nil, and inUse and reader are set.
// If the attempt succeeded, then inUse must be signalled when the reader is no longer in use.
type loadedReader struct {
	reader Reader
	inUse  *sync.WaitGroup

	err error
}

// unloadRequest is a request to unload a binary reader.
type unloadRequest struct {
	// response will receive a single error with the result of the unload operation.
	// response will not be closed.
	response chan error
	// idleSinceNanos is the unix nano timestamp of the last time this reader was used.
	// If idleSinceNanos is 0, the check on the last usage is skipped.
	idleSinceNanos int64
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

	reader := &LazyBinaryReader{
		logger:          logger,
		filepath:        path,
		metrics:         metrics,
		usedAt:          atomic.NewInt64(0),
		onClosed:        onClosed,
		readerFactory:   readerFactory,
		blockID:         id,
		lazyLoadingGate: lazyLoadingGate,
		ctx:             ctx,

		loadedReader: make(chan readerRequest),
		unloadReq:    make(chan unloadRequest),
		done:         make(chan struct{}),
	}

	go reader.controlLoop()
	return reader, nil
}

// Close implements Reader.
func (r *LazyBinaryReader) Close() error {
	select {
	case <-r.done:
		return nil // already closed
	default:
	}
	if r.onClosed != nil {
		defer r.onClosed(r)
	}

	// Unload without checking if idle.
	if err := r.unloadIfIdleSince(0); err != nil {
		return fmt.Errorf("unload index-header: %w", err)
	}

	close(r.done)
	return nil
}

// IndexVersion implements Reader.
func (r *LazyBinaryReader) IndexVersion(ctx context.Context) (int, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return 0, loaded.err
	}
	defer loaded.inUse.Done()

	return loaded.reader.IndexVersion(ctx)
}

// PostingsOffset implements Reader.
func (r *LazyBinaryReader) PostingsOffset(ctx context.Context, name string, value string) (index.Range, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return index.Range{}, loaded.err
	}
	defer loaded.inUse.Done()

	return loaded.reader.PostingsOffset(ctx, name, value)
}

// LookupSymbol implements Reader.
func (r *LazyBinaryReader) LookupSymbol(ctx context.Context, o uint32) (string, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return "", loaded.err
	}
	defer loaded.inUse.Done()

	return loaded.reader.LookupSymbol(ctx, o)
}

// SymbolsReader implements Reader.
func (r *LazyBinaryReader) SymbolsReader(ctx context.Context) (streamindex.SymbolsReader, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return nil, loaded.err
	}

	sr, err := loaded.reader.SymbolsReader(ctx)
	if err != nil {
		loaded.inUse.Done()
		return nil, err
	}
	return newLazySymbolsReader(sr, loaded.inUse), nil
}

// LabelValuesOffsets implements Reader.
func (r *LazyBinaryReader) LabelValuesOffsets(ctx context.Context, name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return nil, loaded.err
	}
	defer loaded.inUse.Done()

	return loaded.reader.LabelValuesOffsets(ctx, name, prefix, filter)
}

// LabelNames implements Reader.
func (r *LazyBinaryReader) LabelNames(ctx context.Context) ([]string, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return nil, loaded.err
	}
	defer loaded.inUse.Done()

	return loaded.reader.LabelNames(ctx)
}

// getOrLoadReader ensures the underlying binary index-header reader has been successfully loaded.
// Returns the reader, wait group that should be used to signal that usage of reader is finished, and an error on failure.
// Must be called without lock.
func (r *LazyBinaryReader) getOrLoadReader(ctx context.Context) loadedReader {
	readerReq := readerRequest{response: make(chan loadedReader)}
	select {
	case <-r.done:
		return loadedReader{err: errors.New("lazy reader is closed; this shouldn't happen")}
	case r.loadedReader <- readerReq:
		select {
		case loadedR := <-readerReq.response:
			return loadedR
		case <-ctx.Done():
			// We will get a response on the channel, and if it's a loaded reader we need to signal that we're no longer using it.
			// This should be rare, so spinning up a goroutine shouldn't be too expensive.
			go r.waitAndCloseReader(readerReq)
			return loadedReader{err: context.Cause(ctx)}
		}
	case <-ctx.Done():
		return loadedReader{err: context.Cause(ctx)}
	}
}

// loadReader is called from getOrLoadReader, without any locks.
func (r *LazyBinaryReader) loadReader() (Reader, error) {
	// lazyLoadingGate implementation: blocks load if too many are happening at once.
	// It's important to get permit from the Gate when NOT holding the read-lock, otherwise we risk that multiple goroutines
	// that enter `load()` will deadlock themselves. (If Start() allows one goroutine to continue, but blocks another one,
	// then goroutine that continues would not be able to get Write lock.)
	err := r.lazyLoadingGate.Start(r.ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to wait for turn")
	}
	defer r.lazyLoadingGate.Done()

	level.Debug(r.logger).Log("msg", "lazy loading index-header file", "path", r.filepath)
	r.metrics.loadCount.Inc()
	startTime := time.Now()

	reader, err := r.readerFactory()
	if err != nil {
		r.metrics.loadFailedCount.Inc()
		return nil, errors.Wrapf(err, "lazy load index-header file at %s", r.filepath)
	}

	elapsed := time.Since(startTime)

	level.Debug(r.logger).Log("msg", "lazy loaded index-header file", "path", r.filepath, "elapsed", elapsed)
	r.metrics.loadDuration.Observe(elapsed.Seconds())

	return reader, nil
}

func (r *LazyBinaryReader) waitAndCloseReader(req readerRequest) {
	resp := <-req.response
	if resp.reader != nil {
		resp.inUse.Done()
	}
}

// unloadIfIdleSince closes underlying BinaryReader if the reader is idle since given time (as unix nano). If idleSince is 0,
// the check on the last usage is skipped. Calling this function on a already unloaded reader is a no-op.
func (r *LazyBinaryReader) unloadIfIdleSince(tsNano int64) error {
	req := unloadRequest{
		// The channel is unbuffered because we will read the response. It should be buffered if we can give up before reading from it
		response:       make(chan error),
		idleSinceNanos: tsNano,
	}
	select {
	case r.unloadReq <- req:
		return <-req.response
	case <-r.done:
		return nil // if the control loop has returned we can't do much other than return.
	}
}

func (r *LazyBinaryReader) controlLoop() {
	var loaded loadedReader

	for {
		select {
		case <-r.done:
			return
		case readerReq := <-r.loadedReader:
			if loaded.reader == nil {
				// Try to load the reader if it hasn't been loaded before or if the previous loading failed.
				loaded = loadedReader{}
				loaded.reader, loaded.err = r.loadReader()
				if loaded.reader != nil {
					loaded.inUse = &sync.WaitGroup{}
				}
			}
			if loaded.reader != nil {
				loaded.inUse.Add(1)
				r.usedAt.Store(time.Now().UnixNano())
			}
			readerReq.response <- loaded

		case unloadPromise := <-r.unloadReq:
			if loaded.reader == nil {
				// Nothing to do if already unloaded.
				unloadPromise.response <- nil
				continue
			}

			// Do not unloadIfIdleSince if not idle.
			if ts := unloadPromise.idleSinceNanos; ts > 0 && r.usedAt.Load() > ts {
				unloadPromise.response <- errNotIdle
				continue
			}

			// Wait until all users finished using current reader.
			waitReadersOrPanic(loaded.inUse)

			r.metrics.unloadCount.Inc()
			if err := loaded.reader.Close(); err != nil {
				r.metrics.unloadFailedCount.Inc()
				unloadPromise.response <- fmt.Errorf("closing binary reader: %w", err)
				continue
			}

			loaded = loadedReader{}
			r.usedAt.Store(0)
			unloadPromise.response <- nil
		}
	}
}

func waitReadersOrPanic(wg *sync.WaitGroup) {
	// timeout is long enough for any request to finish.
	// The idea is that we don't want to wait forever, but surface a bug.
	const timeout = time.Hour
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-time.After(timeout):
		// It is illegal to leave the hanging wg.Wait() and later call wg.Add() on the same instance.
		// So we panic here.
		panic(fmt.Sprintf("timed out waiting for readers after %s, there is probably a bug keeping readers open, please report this", timeout))
	}
}

// IsIdleSince returns true if the reader is idle since given time (as unix nano).
func (r *LazyBinaryReader) IsIdleSince(ts int64) bool {
	lastUse := r.LoadedLastUse()
	return lastUse != 0 && lastUse <= ts
}

// LoadedLastUse returns 0 if the reader is not loaded.
// LoadedLastUse returns a timestamp in nanoseconds of the last time this reader was used.
func (r *LazyBinaryReader) LoadedLastUse() int64 {
	return r.usedAt.Load()
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
