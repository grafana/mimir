// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/lazy_binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/gate"
	"github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
)

var (
	errNotIdle = errors.New("the reader is not idle")
)

// LazyParquetReaderMetrics holds metrics tracked by LazyReaderLocalLabelsBucketChunks.
type LazyParquetReaderMetrics struct {
	loadCount         prometheus.Counter
	loadFailedCount   prometheus.Counter
	unloadCount       prometheus.Counter
	unloadFailedCount prometheus.Counter
	loadDuration      prometheus.Histogram
}

// NewLazyParquetReaderMetrics makes new LazyParquetReaderMetrics.
func NewLazyParquetReaderMetrics(reg prometheus.Registerer) *LazyParquetReaderMetrics {
	return &LazyParquetReaderMetrics{
		loadCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "parquet_reader_lazy_load_total",
			Help: "Total number of parquet reader lazy load operations.",
		}),
		loadFailedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "parquet_reader_lazy_load_failed_total",
			Help: "Total number of failed parquet reader lazy load operations.",
		}),
		unloadCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "parquet_reader_lazy_unload_total",
			Help: "Total number of parquet reader lazy unload operations.",
		}),
		unloadFailedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "parquet_reader_lazy_unload_failed_total",
			Help: "Total number of failed parquet reader lazy unload operations.",
		}),
		loadDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    "parquet_reader_lazy_load_duration_seconds",
			Help:    "Duration of the parquet reader lazy loading in seconds.",
			Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 15, 30, 60, 120, 300},
		}),
	}
}

type LazyReader interface {
	Reader
	UsedAt() int64

	// IsIdleSince returns true if the reader is idle since given time (as unix nano).
	IsIdleSince(tsNano int64) bool

	// UnloadIfIdleSince closes underlying Reader if the reader is idle since given time (as unix nano).
	// If idleSince is 0, the check on the last usage is skipped.
	// Calling this function on an already-unloaded reader is a no-op.
	UnloadIfIdleSince(tsNano int64) error
}

type readerRequest struct {
	response chan loadedReader
}

// loadedReader represents an attempt to load a Reader. If the attempt failed, then err is set and reader is nil.
// If the attempt succeeded, then err is nil, and inUse and reader are set.
// If the attempt succeeded, then inUse must be signalled when the reader is no longer in use.
type loadedReader struct {
	reader Reader

	// nolint:unused
	inUse *sync.WaitGroup

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

type lazyReaderLoader struct {
	ctx context.Context

	blockID  ulid.ULID
	shardIdx int

	labelsFileOpener storage.ParquetOpener
	chunksFileOpener storage.ParquetOpener
	fileOpts         []storage.FileOption

	lazyLoadingGate gate.Gate
	loadedReader    chan readerRequest
	unloadReq       chan unloadRequest
	usedAt          *atomic.Int64
	done            chan struct{}

	metrics *LazyParquetReaderMetrics
	logger  log.Logger
}

func (r *lazyReaderLoader) controlLoop() {
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
					// TODO: this should be necessary but is not currently working
					loaded.inUse = &sync.WaitGroup{}
				}
			}
			if loaded.reader != nil {
				// TODO: this should be necessary but is not currently working
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

			// Do not UnloadIfIdleSince if not idle.
			if ts := unloadPromise.idleSinceNanos; ts > 0 && r.usedAt.Load() > ts {
				unloadPromise.response <- errNotIdle
				continue
			}

			// TODO: this should be necessary but is not currently working
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

// getOrLoadReader ensures the underlying binary index-header reader has been successfully loaded.
// Returns the reader, wait group that should be used to signal that usage of reader is finished, and an error on failure.
// Must be called without lock.
func (r *lazyReaderLoader) getOrLoadReader(ctx context.Context) loadedReader {
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
func (r *lazyReaderLoader) loadReader() (Reader, error) {
	level.Debug(r.logger).Log("msg", "load reader for block", "block_id", r.blockID)
	// lazyLoadingGate implementation: blocks load if too many are happening at once.
	// It's important to get permit from the Gate when NOT holding the read-lock, otherwise we risk that multiple goroutines
	// that enter `load()` will deadlock themselves. (If Start() allows one goroutine to continue, but blocks another one,
	// then goroutine that continues would not be able to get Write lock.)
	if r.lazyLoadingGate != nil {
		err := r.lazyLoadingGate.Start(r.ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to wait for turn")
		}
		defer r.lazyLoadingGate.Done()
	}

	level.Debug(r.logger).Log("msg", "start lazy open parquet labels file")
	r.metrics.loadCount.Inc()
	startTime := time.Now()

	reader, err := NewBasicReader(
		r.ctx,
		r.blockID,
		r.shardIdx,
		r.labelsFileOpener,
		r.chunksFileOpener,
		r.fileOpts...,
	)

	if err != nil {
		r.metrics.loadFailedCount.Inc()
		return nil, errors.Wrapf(err, "lazy open parquet labels file")
	}

	elapsed := time.Since(startTime)
	level.Debug(r.logger).Log("msg", "finish lazy open parquet labels file", "elapsed", time.Since(startTime))

	r.metrics.loadDuration.Observe(elapsed.Seconds())

	return reader, nil
}

// IsIdleSince returns true if the reader is idle since given time (as unix nano).
func (r *lazyReaderLoader) IsIdleSince(ts int64) bool {
	lastUse := r.LoadedLastUse()
	return lastUse != 0 && lastUse <= ts
}

// LoadedLastUse returns 0 if the reader is not loaded.
// LoadedLastUse returns a timestamp in nanoseconds of the last time this reader was used.
func (r *lazyReaderLoader) LoadedLastUse() int64 {
	return r.usedAt.Load()
}

func (r *lazyReaderLoader) unloadIfIdleSince(tsNano int64) error {
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

func (r *lazyReaderLoader) waitAndCloseReader(req readerRequest) {
	resp := <-req.response
	if resp.reader != nil {
		resp.inUse.Done()
	}
}

// LazyBucketReader implements the parquet block Reader interface.
// The Reader waits to open the labels file or the chunks file from the bucket
// until the respective calls to the Reader interface's LabelsFile() or ChunksFile() methods.
type LazyBucketReader struct {
	ctx context.Context

	blockID ulid.ULID

	// bkt to open the labels and chunks files from
	bkt objstore.InstrumentedBucketReader

	onClosed     func(LazyReader)
	readerLoader *lazyReaderLoader
	logger       log.Logger
}

// NewLazyBucketReader initializes a parquet block Reader from the bucket.
func NewLazyBucketReader(
	ctx context.Context,
	blockID ulid.ULID,
	shardIdx int,
	bkt objstore.InstrumentedBucketReader,
	fileOpts []storage.FileOption,
	metrics *LazyParquetReaderMetrics,
	onClosed func(LazyReader),
	lazyLoadingGate gate.Gate,
	logger log.Logger,
) (*LazyBucketReader, error) {
	bucketOpener := storage.NewParquetBucketOpener(bkt)

	reader := &LazyBucketReader{
		ctx:      ctx,
		blockID:  blockID,
		bkt:      bkt,
		onClosed: onClosed,

		readerLoader: &lazyReaderLoader{
			ctx:              ctx,
			blockID:          blockID,
			shardIdx:         shardIdx,
			labelsFileOpener: bucketOpener,
			chunksFileOpener: bucketOpener,

			fileOpts: append(fileOpts,
				storage.WithFileOptions(parquet.SkipBloomFilters(false)),
			),

			lazyLoadingGate: lazyLoadingGate,
			loadedReader:    make(chan readerRequest),
			unloadReq:       make(chan unloadRequest),
			usedAt:          atomic.NewInt64(0),
			done:            make(chan struct{}),

			metrics: metrics,
			logger:  logger,
		},

		logger: logger,
	}

	go reader.readerLoader.controlLoop()
	return reader, nil
}

func (r *LazyBucketReader) BlockID() ulid.ULID {
	return r.blockID
}

func (r *LazyBucketReader) LabelsFile() storage.ParquetFileView {
	loaded := r.readerLoader.getOrLoadReader(r.ctx)
	if loaded.err != nil {
		// TODO: the current interface does not allow to return an error
		level.Error(r.logger).Log("msg", "failed to get labels file from lazy reader", "err", loaded.err)
		return nil
	}
	defer loaded.inUse.Done()
	return loaded.reader.LabelsFile()
}

func (r *LazyBucketReader) ChunksFile() storage.ParquetFileView {
	loaded := r.readerLoader.getOrLoadReader(r.ctx)
	if loaded.err != nil {
		// TODO: the current interface does not allow to return an error
		level.Error(r.logger).Log("msg", "failed to get chunks file from lazy reader", "err", loaded.err)
		return nil
	}
	defer loaded.inUse.Done()
	return loaded.reader.ChunksFile()
}

func (r *LazyBucketReader) TSDBSchema() (*schema.TSDBSchema, error) {
	loaded := r.readerLoader.getOrLoadReader(r.ctx)
	if loaded.err != nil {
		return nil, errors.Wrap(loaded.err, "get TSDB schema from lazy reader")
	}
	defer loaded.inUse.Done()
	return loaded.reader.TSDBSchema()
}

func (r *LazyBucketReader) UsedAt() int64 {
	return r.readerLoader.usedAt.Load()
}

// IsIdleSince returns true if the reader is idle since given time (as unix nano).
func (r *LazyBucketReader) IsIdleSince(ts int64) bool {
	return r.readerLoader.IsIdleSince(ts)
}

func (r *LazyBucketReader) UnloadIfIdleSince(tsNano int64) error {
	return r.readerLoader.unloadIfIdleSince(tsNano)
}

// Close implements Reader.
func (r *LazyBucketReader) Close() error {
	select {
	case <-r.readerLoader.done:
		return nil // already closed
	default:
	}
	if r.onClosed != nil {
		defer r.onClosed(r)
	}

	// Unload without checking if idle.
	if err := r.UnloadIfIdleSince(0); err != nil {
		return fmt.Errorf("unload index-header: %w", err)
	}

	close(r.readerLoader.done)
	return nil
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
