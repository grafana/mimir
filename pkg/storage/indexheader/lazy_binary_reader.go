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
	"github.com/oklog/ulid/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/thanos-io/objstore"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"

	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
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

	// current is the currently loaded reader, or nil if the reader isn't loaded.
	// It's only written by controlLoop, and allows callers to use an already loaded reader
	// without a round trip through controlLoop.
	current atomic.Pointer[pinnedReader]

	// Keep track of the last time it was used.
	usedAt *atomic.Int64

	readerFactory func(ctx context.Context) (Reader, error)

	blockID ulid.ULID
	done    chan struct{}
}

type readerRequest struct {
	// ctx is the context of whichever caller's getOrLoadReader triggers the actual load.
	// Only its trace span is used (as the parent of the load's own span), not its cancellation.
	ctx      context.Context
	response chan loadedReader
}

// loadedReader represents an attempt to load a Reader. If the attempt failed, then err is set and pinned is nil.
// If the attempt succeeded, then err is nil, and pinned is set and pinned on behalf of the requester.
// If the attempt succeeded, then pinned.unpin must be called when the reader is no longer in use.
type loadedReader struct {
	pinned *pinnedReader

	err error
}

// unloadingBit is set in pinnedReader.refs once the reader starts unloading.
const unloadingBit = int64(1) << 62

// pinnedReader is a loaded Reader together with a count of the callers currently using it.
// Callers pin the reader before using it and unpin it when done, so the reader is never closed while in use.
type pinnedReader struct {
	reader Reader

	// refs is the number of callers currently using reader, plus unloadingBit once unloading has started.
	refs atomic.Int64

	// drained is closed once unloading has started and no caller is using reader anymore.
	drained chan struct{}
}

func newPinnedReader(reader Reader) *pinnedReader {
	return &pinnedReader{
		reader:  reader,
		drained: make(chan struct{}),
	}
}

// tryPin registers a new user of the reader. It returns false if the reader started unloading,
// in which case the reader must not be used.
func (p *pinnedReader) tryPin() bool {
	for {
		refs := p.refs.Load()
		if refs&unloadingBit != 0 {
			return false
		}
		if p.refs.CompareAndSwap(refs, refs+1) {
			return true
		}
	}
}

// unpin must be called exactly once after each successful tryPin, once the reader is no longer in use.
func (p *pinnedReader) unpin() {
	if p.refs.Dec() == unloadingBit {
		close(p.drained)
	}
}

// startUnloading prevents any new user from pinning the reader, and returns a channel which is closed
// once all current users have unpinned it. It must be called at most once.
func (p *pinnedReader) startUnloading() <-chan struct{} {
	if p.refs.Add(unloadingBit) == unloadingBit {
		close(p.drained)
	}
	return p.drained
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
// NewLazyBinaryReader tries to download the sparse index header from the bucket and save it on disk.
// It does not try to parse the sparse index header.
func NewLazyBinaryReader(
	ctx context.Context,
	cfg Config,
	readerFactory func(ctx context.Context) (Reader, error),
	logger log.Logger,
	bkt objstore.InstrumentedBucketReader,
	localDir string,
	id ulid.ULID,
	metrics *LazyBinaryReaderMetrics,
	onClosed func(*LazyBinaryReader),
	lazyLoadingGate gate.Gate,
) (*LazyBinaryReader, error) {
	localBlockDir := filepath.Join(localDir, id.String())
	headerPath := indexHeaderPath(localBlockDir, cfg.Version)

	if df, err := os.Open(localBlockDir); err != nil && os.IsNotExist(err) {
		if err := os.MkdirAll(localBlockDir, os.ModePerm); err != nil {
			return nil, fmt.Errorf("cannot create index-header dir: %w", err)
		}
	} else {
		_ = df.Close()
	}

	g := errgroup.Group{}
	g.Go(func() error {
		return ensureIndexHeaderOnDisk(ctx, id, bkt, localDir, cfg, logger)
	})

	g.Go(func() error {
		err := downloadSparseHeaderToDisk(ctx, id, bkt, localDir, logger)
		if err != nil {
			level.Info(logger).Log("msg", "could not download sparse index-header from bucket; will reconstruct when the block is queried", "err", err)
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}

	reader := &LazyBinaryReader{
		logger:          logger,
		filepath:        headerPath,
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

// ensureIndexHeaderOnDisk builds an index-header of the version required from the bucket if it is not already on disk,
// and removes any other version that may already have existed.
func ensureIndexHeaderOnDisk(
	ctx context.Context,
	blockID ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
	dir string,
	cfg Config,
	logger log.Logger,
) error {
	localBlockDir := filepath.Join(dir, blockID.String())

	headers, err := IndexHeadersOnDisk(localBlockDir)
	if err != nil {
		return fmt.Errorf("list index-headers on disk: %w", err)
	}

	for i, h := range headers {
		if h.Version == cfg.Version {
			// We already have the right header; update headers to remove the correct version, and sweep the rest
			headers = append(headers[:i], headers[i+1:]...)
			return removeIndexHeaders(localBlockDir, headers...)
		}
	}

	level.Debug(logger).Log(
		"msg", "index-header for required version not found on disk; will build from bucket",
		"path", indexHeaderPath(localBlockDir, cfg.Version), "requiredVersion", cfg.Version,
	)

	start := time.Now()
	if err := buildRequiredIndexHeader(ctx, bkt, blockID, localBlockDir, cfg.Version, headers, logger); err != nil {
		level.Error(logger).Log("msg", "failed to create index-header", "err", err)
		return err
	}

	level.Debug(logger).Log("msg", "built index-header file", "path", indexHeaderPath(localBlockDir, cfg.Version), "elapsed", time.Since(start))

	// headers was created before we built the correct index-header file, we can remove all of these
	return removeIndexHeaders(localBlockDir, headers...)
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
	defer loaded.pinned.unpin()

	return loaded.pinned.reader.IndexVersion(ctx)
}

// PostingsOffset implements Reader.
func (r *LazyBinaryReader) PostingsOffset(ctx context.Context, name string, value string) (index.Range, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return index.Range{}, loaded.err
	}
	defer loaded.pinned.unpin()

	return loaded.pinned.reader.PostingsOffset(ctx, name, value)
}

// LookupSymbol implements Reader.
func (r *LazyBinaryReader) LookupSymbol(ctx context.Context, o uint32) (string, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return "", loaded.err
	}
	defer loaded.pinned.unpin()

	return loaded.pinned.reader.LookupSymbol(ctx, o)
}

// SymbolsReader implements Reader.
func (r *LazyBinaryReader) SymbolsReader(ctx context.Context) (streamindex.SymbolsReader, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return nil, loaded.err
	}

	sr, err := loaded.pinned.reader.SymbolsReader(ctx)
	if err != nil {
		loaded.pinned.unpin()
		return nil, err
	}
	return newLazySymbolsReader(sr, loaded.pinned), nil
}

// LabelValuesOffsets implements Reader.
func (r *LazyBinaryReader) LabelValuesOffsets(ctx context.Context, name string, prefix string, filter func(string) bool) ([]streamindex.PostingListOffset, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return nil, loaded.err
	}
	defer loaded.pinned.unpin()

	return loaded.pinned.reader.LabelValuesOffsets(ctx, name, prefix, filter)
}

// LabelNames implements Reader.
func (r *LazyBinaryReader) LabelNames(ctx context.Context) ([]string, error) {
	loaded := r.getOrLoadReader(ctx)
	if loaded.err != nil {
		return nil, loaded.err
	}
	defer loaded.pinned.unpin()

	return loaded.pinned.reader.LabelNames(ctx)
}

// getOrLoadReader ensures the underlying binary index-header reader has been successfully loaded.
// Returns the pinned reader, which must be unpinned once the caller finished using it, or an error on failure.
// Must be called without lock.
func (r *LazyBinaryReader) getOrLoadReader(ctx context.Context) loadedReader {
	// Fast path: the reader is already loaded, so we pin it without a round trip through controlLoop.
	// This is the common case, and it's called for every symbol lookup, so it must not serialize callers.
	if pinned := r.current.Load(); pinned != nil && pinned.tryPin() {
		r.usedAt.Store(time.Now().UnixNano())
		return loadedReader{pinned: pinned}
	}

	// Slow path: the reader isn't loaded or is being unloaded, so let controlLoop load it.
	readerReq := readerRequest{ctx: ctx, response: make(chan loadedReader)}
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
func (r *LazyBinaryReader) loadReader(callerCtx context.Context) (Reader, error) {
	ctx := trace.ContextWithSpanContext(r.ctx, trace.SpanContextFromContext(callerCtx))

	// lazyLoadingGate implementation: blocks load if too many are happening at once.
	// It's important to get permit from the Gate when NOT holding the read-lock, otherwise we risk that multiple goroutines
	// that enter `load()` will deadlock themselves. (If Start() allows one goroutine to continue, but blocks another one,
	// then goroutine that continues would not be able to get Write lock.)
	err := r.lazyLoadingGate.Start(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to wait for turn")
	}
	defer r.lazyLoadingGate.Done()

	level.Debug(r.logger).Log("msg", "lazy loading index-header file", "path", r.filepath)
	r.metrics.loadCount.Inc()
	startTime := time.Now()

	reader, err := r.readerFactory(ctx)
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
	if resp.pinned != nil {
		resp.pinned.unpin()
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
	// loaded is the currently loaded reader, or nil. controlLoop is the only goroutine which loads, publishes
	// and unloads it, so it's never unloading while controlLoop handles a readerRequest.
	var loaded *pinnedReader

	for {
		select {
		case <-r.done:
			return
		case readerReq := <-r.loadedReader:
			if loaded == nil {
				// Try to load the reader if it hasn't been loaded before or if the previous loading failed.
				reader, err := r.loadReader(readerReq.ctx)
				if err != nil {
					readerReq.response <- loadedReader{err: err}
					continue
				}
				loaded = newPinnedReader(reader)
				r.current.Store(loaded)
			}
			// The pin can't fail, because only controlLoop starts unloading the reader.
			loaded.tryPin()
			r.usedAt.Store(time.Now().UnixNano())
			readerReq.response <- loadedReader{pinned: loaded}

		case unloadPromise := <-r.unloadReq:
			if loaded == nil {
				// Nothing to do if already unloaded.
				unloadPromise.response <- nil
				continue
			}

			// Do not unloadIfIdleSince if not idle.
			if ts := unloadPromise.idleSinceNanos; ts > 0 && r.usedAt.Load() > ts {
				unloadPromise.response <- errNotIdle
				continue
			}

			// Stop handing out the current reader, and wait until all users finished using it.
			r.current.Store(nil)
			waitReadersOrPanic(loaded.startUnloading())

			// The reader can't be used anymore once we try to close it, even if closing fails,
			// so we forget it either way and load a new one on the next request.
			closeErr := loaded.reader.Close()
			loaded = nil
			r.usedAt.Store(0)

			r.metrics.unloadCount.Inc()
			if closeErr != nil {
				r.metrics.unloadFailedCount.Inc()
				unloadPromise.response <- fmt.Errorf("closing binary reader: %w", closeErr)
				continue
			}
			unloadPromise.response <- nil
		}
	}
}

func waitReadersOrPanic(drained <-chan struct{}) {
	// timeout is long enough for any request to finish.
	// The idea is that we don't want to wait forever, but surface a bug.
	const timeout = time.Hour
	select {
	case <-drained:
		return
	case <-time.After(timeout):
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
	sr     streamindex.SymbolsReader
	pinned *pinnedReader
	once   sync.Once
}

func newLazySymbolsReader(sr streamindex.SymbolsReader, pinned *pinnedReader) *lazySymbolsReader {
	return &lazySymbolsReader{
		sr:     sr,
		pinned: pinned,
	}
}

func (l *lazySymbolsReader) Read(u uint32) (string, error) {
	return l.sr.Read(u)
}

func (l *lazySymbolsReader) Close() error {
	err := l.sr.Close()
	l.once.Do(l.pinned.unpin)
	return err
}
