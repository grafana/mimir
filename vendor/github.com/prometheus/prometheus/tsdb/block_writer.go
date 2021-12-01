package tsdb

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
)

type blockWriter struct {
	chunkPool chunkenc.Pool // Where to return chunks after writing.

	chunkw ChunkWriter
	indexw IndexWriter

	closeSemaphore *semaphore.Weighted

	seriesChan chan seriesToWrite
	finishedCh chan blockWriterResult

	closed bool
	result blockWriterResult
}

type blockWriterResult struct {
	stats BlockStats
	err   error
}

type seriesToWrite struct {
	lbls labels.Labels
	chks []chunks.Meta
}

func newBlockWriter(chunkPool chunkenc.Pool, chunkw ChunkWriter, indexw IndexWriter, closeSema *semaphore.Weighted) *blockWriter {
	bw := &blockWriter{
		chunkPool:      chunkPool,
		chunkw:         chunkw,
		indexw:         indexw,
		seriesChan:     make(chan seriesToWrite, 128),
		finishedCh:     make(chan blockWriterResult, 1),
		closeSemaphore: closeSema,
	}

	go bw.loop()
	return bw
}

func (bw *blockWriter) loop() (res blockWriterResult) {
	defer func() {
		bw.finishedCh <- res
		close(bw.finishedCh)
	}()

	stats := BlockStats{}
	ref := storage.SeriesRef(0)
	for sw := range bw.seriesChan {
		if err := bw.chunkw.WriteChunks(sw.chks...); err != nil {
			return blockWriterResult{err: errors.Wrap(err, "write chunks")}
		}
		if err := bw.indexw.AddSeries(ref, sw.lbls, sw.chks...); err != nil {
			return blockWriterResult{err: errors.Wrap(err, "add series")}
		}

		stats.NumChunks += uint64(len(sw.chks))
		stats.NumSeries++
		for _, chk := range sw.chks {
			stats.NumSamples += uint64(chk.Chunk.NumSamples())
		}

		for _, chk := range sw.chks {
			if err := bw.chunkPool.Put(chk.Chunk); err != nil {
				return blockWriterResult{err: errors.Wrap(err, "put chunk")}
			}
		}
		ref++
	}

	err := bw.closeSemaphore.Acquire(context.Background(), 1)
	if err != nil {
		return blockWriterResult{err: errors.Wrap(err, "failed to acquire semaphore before closing writers")}
	}
	defer bw.closeSemaphore.Release(1)

	// If everything went fine with writing so far, close writers.
	if err := bw.chunkw.Close(); err != nil {
		return blockWriterResult{err: errors.Wrap(err, "closing chunk writer")}
	}
	if err := bw.indexw.Close(); err != nil {
		return blockWriterResult{err: errors.Wrap(err, "closing index writer")}
	}

	return blockWriterResult{stats: stats}
}

func (bw *blockWriter) addSeries(lbls labels.Labels, chks []chunks.Meta) error {
	select {
	case bw.seriesChan <- seriesToWrite{lbls: lbls, chks: chks}:
		return nil
	case result, ok := <-bw.finishedCh:
		if ok {
			bw.result = result
		}
		return fmt.Errorf("blockWriter doesn't run anymore")
	}
}

func (bw *blockWriter) closeAsync() {
	if !bw.closed {
		bw.closed = true

		close(bw.seriesChan)
	}
}

func (bw *blockWriter) waitFinished() (BlockStats, error) {
	// Wait for flusher to finish.
	result, ok := <-bw.finishedCh
	if ok {
		bw.result = result
	}

	return bw.result.stats, bw.result.err
}

type preventDoubleCloseIndexWriter struct {
	IndexWriter
	closed atomic.Bool
}

func newPreventDoubleCloseIndexWriter(iw IndexWriter) *preventDoubleCloseIndexWriter {
	return &preventDoubleCloseIndexWriter{IndexWriter: iw}
}

func (p *preventDoubleCloseIndexWriter) Close() error {
	if p.closed.CAS(false, true) {
		return p.IndexWriter.Close()
	}
	return nil
}

type preventDoubleCloseChunkWriter struct {
	ChunkWriter
	closed atomic.Bool
}

func newPreventDoubleCloseChunkWriter(cw ChunkWriter) *preventDoubleCloseChunkWriter {
	return &preventDoubleCloseChunkWriter{ChunkWriter: cw}
}

func (p *preventDoubleCloseChunkWriter) Close() error {
	if p.closed.CAS(false, true) {
		return p.ChunkWriter.Close()
	}
	return nil
}
