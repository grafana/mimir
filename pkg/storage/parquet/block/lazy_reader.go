// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/block/indexheader/lazy_binary_reader.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package block

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
	"github.com/grafana/dskit/runutil"
	"github.com/oklog/ulid/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/convert"
	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/thanos-io/objstore"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
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

// LazyReaderLocalLabelsBucketChunks implements the parquet block Reader interface.
// The Reader downloads the block shard's labels file from bucket to disk
// but does not open the labels file from local disk or the chunks file from the bucket
// until the respective calls to the Reader interface's LabelsFile() or ChunksFile() methods.
type LazyReaderLocalLabelsBucketChunks struct {
	ctx context.Context

	blockID  ulid.ULID
	shardIdx int

	// bkt to download the labels file to local disk and open the chunks file from the bucket
	bkt       objstore.InstrumentedBucketReader
	localDir  string
	shardOpts []storage.ShardOption

	metrics         *LazyParquetReaderMetrics
	onClosed        func(*LazyReaderLocalLabelsBucketChunks)
	lazyLoadingGate gate.Gate

	loadedReader chan readerRequest
	unloadReq    chan unloadRequest

	// Keep track of the last time it was used.
	usedAt *atomic.Int64

	done chan struct{}

	logger log.Logger
}

// NewLazyReaderLocalLabelsBucketChunks initializes a parquet block Reader
// and downloads the block shard's labels file from bucket to disk.
func NewLazyReaderLocalLabelsBucketChunks(
	ctx context.Context,
	blockID ulid.ULID,
	bkt objstore.InstrumentedBucketReader,
	localDir string,
	//shardOpts []storage.ShardOption,
	metrics *LazyParquetReaderMetrics,
	onClosed func(*LazyReaderLocalLabelsBucketChunks),
	lazyLoadingGate gate.Gate,
	logger log.Logger,
) (*LazyReaderLocalLabelsBucketChunks, error) {
	reader := &LazyReaderLocalLabelsBucketChunks{
		ctx:      ctx,
		blockID:  blockID,
		shardIdx: FirstShardIndex,
		bkt:      bkt,
		localDir: localDir,

		metrics:  metrics,
		usedAt:   atomic.NewInt64(0),
		onClosed: onClosed,

		lazyLoadingGate: lazyLoadingGate,

		loadedReader: make(chan readerRequest),
		unloadReq:    make(chan unloadRequest),
		done:         make(chan struct{}),

		logger: logger,
	}

	g := errgroup.Group{}
	g.Go(func() error {
		return reader.ensureLabelsFileToLocalDisk()
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}

	go reader.controlLoop()
	return reader, nil
}

func (r *LazyReaderLocalLabelsBucketChunks) labelsFileName() string {
	return schema.LabelsPfileNameForShard(r.blockID.String(), r.shardIdx)
}

func (r *LazyReaderLocalLabelsBucketChunks) labelsFileLocalPath() string {
	return filepath.Join(r.localDir, r.labelsFileName())
}

func (r *LazyReaderLocalLabelsBucketChunks) ensureLabelsFileToLocalDisk() error {
	localPath := r.labelsFileLocalPath()
	_, err := os.Stat(localPath)
	if err == nil {
		// file exists, nothing to do.
		return nil
	}
	if !os.IsNotExist(err) {
		return errors.Wrap(err, "read parquet labels file from disk")
	}

	level.Debug(r.logger).Log("msg", "parquet labels file not on disk; loading", "path", localPath)
	start := time.Now()
	if err := r.convertLabelsFileToLocalDisk(); err != nil {
		return errors.Wrap(err, "write labels file")
	}
	level.Debug(r.logger).Log("msg", "loaded parquet labels file to disk", "path", localPath, "elapsed", time.Since(start))
	return nil
}

// convertLabelsFileToLocalDisk utilizes the prometheus parquet convert package
// in order to download the labels file from the bucket to local disk.
// The usage of `convert` rather than a simple bucket download/copy is more CPU-intensive,
// but enables us to choose the output schema and projection of the labels file if desired
// and use the built-in read/write buffering and validation capabilities of the convert package.
func (r *LazyReaderLocalLabelsBucketChunks) convertLabelsFileToLocalDisk() error {
	labelsFileName := r.labelsFileName()
	bucketFileOpener := storage.NewParquetBucketOpener(r.bkt)

	bucketLabelsFile, err := bucketFileOpener.Open(r.ctx, labelsFileName, r.shardOpts...)
	if err != nil {
		return errors.Wrap(err, "open bucket parquet labels file")
	}
	defer runutil.CloseWithLogOnErr(r.logger, bucketLabelsFile, "close bucket labels file")
	bucketLabelsFileReader := parquet.NewGenericReader[any](bucketLabelsFile)
	defer runutil.CloseWithLogOnErr(r.logger, bucketLabelsFileReader, "close bucket labels file reader")
	labelsFileSchema, err := schema.FromLabelsFile(bucketLabelsFile.File)
	if err != nil {
		return errors.Wrap(err, "get schema from bucket parquet labels file")
	}
	labelsProjection, err := labelsFileSchema.LabelsProjection()
	if err != nil {
		return errors.Wrap(err, "get schema projection from bucket parquet labels file schema")
	}
	outSchemaProjections := []*schema.TSDBProjection{labelsProjection}

	pipeReaderFileWriter := convert.NewPipeReaderFileWriter(r.localDir)
	shardedBucketToFileWriter := convert.NewShardedWrite(
		bucketLabelsFileReader, labelsFileSchema, outSchemaProjections, pipeReaderFileWriter, &convert.DefaultConvertOpts,
	)
	err = shardedBucketToFileWriter.Write(r.ctx)
	if err != nil {
		return errors.Wrap(err, "convert bucket parquet labels file to disk")
	}
	return nil
}

func (r *LazyReaderLocalLabelsBucketChunks) LabelsFile() *storage.ParquetFile {
	loaded := r.getOrLoadReader(r.ctx)
	return loaded.reader.LabelsFile()
}

func (r *LazyReaderLocalLabelsBucketChunks) ChunksFile() *storage.ParquetFile {
	loaded := r.getOrLoadReader(r.ctx)
	return loaded.reader.ChunksFile()
}

func (r *LazyReaderLocalLabelsBucketChunks) TSDBSchema() (*schema.TSDBSchema, error) {
	loaded := r.getOrLoadReader(r.ctx)
	return loaded.reader.TSDBSchema()
}

// getOrLoadReader ensures the underlying binary index-header reader has been successfully loaded.
// Returns the reader, wait group that should be used to signal that usage of reader is finished, and an error on failure.
// Must be called without lock.
func (r *LazyReaderLocalLabelsBucketChunks) getOrLoadReader(ctx context.Context) loadedReader {
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
func (r *LazyReaderLocalLabelsBucketChunks) loadReader() (Reader, error) {
	// lazyLoadingGate implementation: blocks load if too many are happening at once.
	// It's important to get permit from the Gate when NOT holding the read-lock, otherwise we risk that multiple goroutines
	// that enter `load()` will deadlock themselves. (If Start() allows one goroutine to continue, but blocks another one,
	// then goroutine that continues would not be able to get Write lock.)
	err := r.lazyLoadingGate.Start(r.ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to wait for turn")
	}
	defer r.lazyLoadingGate.Done()

	level.Debug(r.logger).Log("msg", "start lazy open local parquet labels file", "path", r.localDir)
	r.metrics.loadCount.Inc()
	startTime := time.Now()

	labelsLocalFilePath := r.labelsFileLocalPath()
	labelsLocalFileOpener := NewParquetLocalFileOpener(r.localDir)
	chunksBucketOpener := storage.NewParquetBucketOpener(r.bkt)

	reader, err := NewBasicReader(
		r.ctx,
		r.blockID.String(),
		r.shardIdx,
		labelsLocalFileOpener,
		chunksBucketOpener,
		r.shardOpts...,
	)

	if err != nil {
		r.metrics.loadFailedCount.Inc()
		return nil, errors.Wrapf(err, "lazy open local parquet labels file %s", labelsLocalFilePath)
	}

	elapsed := time.Since(startTime)
	level.Debug(r.logger).Log("msg", "finish lazy open local parquet labels file", "path", r.localDir, "elapsed", time.Since(startTime))

	r.metrics.loadDuration.Observe(elapsed.Seconds())

	return reader, nil
}

// Close implements Reader.
func (r *LazyReaderLocalLabelsBucketChunks) Close() error {
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

func (r *LazyReaderLocalLabelsBucketChunks) waitAndCloseReader(req readerRequest) {
	resp := <-req.response
	if resp.reader != nil {
		resp.inUse.Done()
	}
}

// unloadIfIdleSince closes underlying BinaryReader if the reader is idle since given time (as unix nano). If idleSince is 0,
// the check on the last usage is skipped. Calling this function on a already unloaded reader is a no-op.
func (r *LazyReaderLocalLabelsBucketChunks) unloadIfIdleSince(tsNano int64) error {
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

func (r *LazyReaderLocalLabelsBucketChunks) controlLoop() {
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
func (r *LazyReaderLocalLabelsBucketChunks) IsIdleSince(ts int64) bool {
	lastUse := r.LoadedLastUse()
	return lastUse != 0 && lastUse <= ts
}

// LoadedLastUse returns 0 if the reader is not loaded.
// LoadedLastUse returns a timestamp in nanoseconds of the last time this reader was used.
func (r *LazyReaderLocalLabelsBucketChunks) LoadedLastUse() int64 {
	return r.usedAt.Load()
}
