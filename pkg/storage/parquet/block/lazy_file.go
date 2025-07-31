// SPDX-License-Identifier: AGPL-3.0-only

package block

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/gate"
	"github.com/pkg/errors"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/atomic"
)

// loadedReader represents an attempt to load a Reader. If the attempt failed, then err is set and reader is nil.
// If the attempt succeeded, then err is nil, and inUse and reader are set.
// If the attempt succeeded, then inUse must be signalled when the reader is no longer in use.
type loadedFile struct {
	file storage.ParquetFileView

	inUse *sync.WaitGroup

	err error
}

type fileLoadRequest struct {
	response chan loadedFile
}

// unloadRequest is a request to unload a binary reader.
type fileUnloadRequest struct {
	// response will receive a single error with the result of the unload operation.
	// response will not be closed.
	response chan error
	// idleSinceNanos is the unix nano timestamp of the last time this reader was used.
	// If idleSinceNanos is 0, the check on the last usage is skipped.
	idleSinceNanos int64
}

// LazyParquetFileMetrics holds metrics tracked by LazyParquetFileLoader.
type LazyParquetFileMetrics struct {
	loadCount         prometheus.Counter
	loadFailedCount   prometheus.Counter
	unloadCount       prometheus.Counter
	unloadFailedCount prometheus.Counter
	loadDuration      prometheus.Histogram
}

// NewLazyParquetFileMetrics makes new LazyParquetFileMetrics.
func NewLazyParquetFileMetrics(fileType string, reg prometheus.Registerer) *LazyParquetFileMetrics {
	return &LazyParquetFileMetrics{
		loadCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("parquet_%s_file_lazy_load_total", fileType),
			Help: "Total number of parquet file lazy load operations.",
		}),
		loadFailedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("parquet_%s_file_lazy_load_failed_total", fileType),
			Help: "Total number of failed parquet file lazy load operations.",
		}),
		unloadCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("parquet_%s_file_lazy_unload_total", fileType),
			Help: "Total number of parquet file lazy unload operations.",
		}),
		unloadFailedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: fmt.Sprintf("parquet_%s_file_lazy_unload_failed_total", fileType),
			Help: "Total number of failed parquet file lazy unload operations.",
		}),
		loadDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:    fmt.Sprintf("parquet_%s_file_lazy_load_duration_seconds", fileType),
			Help:    "Duration of the parquet file lazy loading in seconds.",
			Buckets: []float64{0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1, 2, 5, 15, 30, 60, 120, 300},
		}),
	}
}

// LazyParquetFileLoader implements the parquet-common storage.ParquetFileView interface
type LazyParquetFileLoader struct {
	ctx context.Context

	//file  storage.ParquetFileView
	fileName string
	//fileConfig storage.ExtendedFileConfig
	//fileOpts   []storage.FileOption
	fileOpener *ParquetBucketOpener
	inUse      *sync.WaitGroup

	lazyLoadingGate gate.Gate
	loadFileChan    chan fileLoadRequest
	unloadFileChan  chan fileUnloadRequest
	usedAt          *atomic.Int64
	done            chan struct{}
	onClosed        func(loader *LazyParquetFileLoader)

	metrics LazyParquetFileMetrics
	logger  log.Logger
}

func (f *LazyParquetFileLoader) controlLoop() {
	var loaded loadedFile

	for {
		select {
		case <-f.done:
			return
		case fileReq := <-f.loadFileChan:
			if loaded.file == nil {
				// Try to load the reader if it hasn't been loaded before or if the previous loading failed.
				loaded = loadedFile{}
				loaded.file, loaded.err = f.loadFile()
				if loaded.file != nil {
					// TODO: this should be necessary but is not currently working
					loaded.inUse = &sync.WaitGroup{}
				}
			}
			if loaded.file != nil {
				// TODO: this should be necessary but is not currently working
				loaded.inUse.Add(1)
				f.usedAt.Store(time.Now().UnixNano())
			}
			fileReq.response <- loaded

		case unloadPromise := <-f.unloadFileChan:
			if loaded.file == nil {
				// Nothing to do if already unloaded.
				unloadPromise.response <- nil
				continue
			}

			// Do not unloadIfIdleSince if not idle.
			if ts := unloadPromise.idleSinceNanos; ts > 0 && f.usedAt.Load() > ts {
				unloadPromise.response <- errNotIdle
				continue
			}

			// TODO: this should be necessary but is not currently working
			// Wait until all users finished using current reader.
			waitReadersOrPanic(loaded.inUse)

			f.metrics.unloadCount.Inc()
			if err := loaded.file.Close(); err != nil {
				f.metrics.unloadFailedCount.Inc()
				unloadPromise.response <- fmt.Errorf("closing binary reader: %w", err)
				continue
			}

			loaded = loadedFile{}
			f.usedAt.Store(0)
			unloadPromise.response <- nil
		}
	}
}

// getOrLoadReader ensures the underlying binary index-header reader has been successfully loaded.
// Returns the reader, wait group that should be used to signal that usage of reader is finished, and an error on failure.
// Must be called without lock.
func (f *LazyParquetFileLoader) getOrLoadFile(ctx context.Context) loadedFile {
	fileReq := fileLoadRequest{response: make(chan loadedFile)}
	select {
	case <-f.done:
		return loadedFile{err: errors.New("lazy reader is closed; this shouldn't happen")}
	case f.loadFileChan <- fileReq:
		select {
		case loadedF := <-fileReq.response:
			return loadedF
		case <-ctx.Done():
			// We will get a response on the channel, and if it's a loaded reader we need to signal that we're no longer using it.
			// This should be rare, so spinning up a goroutine shouldn't be too expensive.
			go f.waitAndCloseReader(fileReq)
			return loadedFile{err: context.Cause(ctx)}
		}
	case <-ctx.Done():
		return loadedFile{err: context.Cause(ctx)}
	}
}

// loadFile is called from getOrFile, without any locks.
func (f *LazyParquetFileLoader) loadFile() (storage.ParquetFileView, error) {
	//level.Debug(f.logger).Log("msg", "load reader for block", "block_id", f.blockID)
	// lazyLoadingGate implementation: blocks load if too many are happening at once.
	// It's important to get permit from the Gate when NOT holding the read-lock, otherwise we risk that multiple goroutines
	// that enter `load()` will deadlock themselves. (If Start() allows one goroutine to continue, but blocks another one,
	// then goroutine that continues would not be able to get Write lock.)
	if f.lazyLoadingGate != nil {
		err := f.lazyLoadingGate.Start(f.ctx)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to wait for turn")
		}
		defer f.lazyLoadingGate.Done()
	}

	level.Debug(f.logger).Log("msg", "start lazy open parquet labels file")
	f.metrics.loadCount.Inc()
	startTime := time.Now()

	file, err := f.fileOpener.Open(f.ctx, f.fileName)

	if err != nil {
		f.metrics.loadFailedCount.Inc()
		return nil, errors.Wrapf(err, "lazy open parquet labels file")
	}

	elapsed := time.Since(startTime)
	level.Debug(f.logger).Log("msg", "finish lazy open parquet labels file", "elapsed", time.Since(startTime))

	f.metrics.loadDuration.Observe(elapsed.Seconds())

	return file, nil
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
func (f *LazyParquetFileLoader) waitAndCloseReader(req fileLoadRequest) {
	resp := <-req.response
	if resp.file != nil {
		resp.inUse.Done()
	}
}

// unloadIfIdleSince closes underlying BinaryReader if the reader is idle since given time (as unix nano). If idleSince is 0,
// the check on the last usage is skipped. Calling this function on a already unloaded reader is a no-op.
func (f *LazyParquetFileLoader) unloadIfIdleSince(tsNano int64) error {
	req := fileUnloadRequest{
		// The channel is unbuffered because we will read the response. It should be buffered if we can give up before reading from it
		response:       make(chan error),
		idleSinceNanos: tsNano,
	}
	select {
	case f.unloadFileChan <- req:
		return <-req.response
	case <-f.done:
		return nil // if the control loop has returned we can't do much other than return.
	}
}

// Close implements Reader.
func (f *LazyParquetFileLoader) Close() error {
	select {
	case <-f.done:
		return nil // already closed
	default:
	}
	if f.onClosed != nil {
		defer f.onClosed(f)
	}

	// Unload without checking if idle.
	if err := f.unloadIfIdleSince(0); err != nil {
		return fmt.Errorf("unload index-header: %w", err)
	}

	close(f.done)
	return nil
}
