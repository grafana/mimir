// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
	"github.com/grafana/mimir/pkg/util/validation"
)

// StreamingSeries represents a single series used in evaluation of a query where the chunks for the series
// are streamed from one or more ingesters.
type StreamingSeries struct {
	Labels  labels.Labels
	Sources []StreamingSeriesSource
}

// StreamingSeriesSource holds the relationship between a stream of chunks from a SeriesChunksStreamReader
// and the expected position of a series' chunks in that stream.
type StreamingSeriesSource struct {
	StreamReader *SeriesChunksStreamReader
	SeriesIndex  uint64
}

// SeriesChunksStreamReader is responsible for managing the streaming of chunks from an ingester and buffering
// chunks in memory until they are consumed by the PromQL engine.
type SeriesChunksStreamReader struct {
	ctx                 context.Context
	client              Ingester_QueryStreamClient
	expectedSeriesCount int
	queryLimiter        *limiter.QueryLimiter
	cleanup             func()
	log                 log.Logger

	seriesBatchChan chan []QueryStreamSeriesChunks
	errorChan       chan error
	err             error
	seriesBatch     []QueryStreamSeriesChunks

	// Keeping the ingester name for debug logs.
	ingesterName string
}

func (s *SeriesChunksStreamReader) GetName() string {
	return s.ingesterName
}

func NewSeriesChunksStreamReader(ctx context.Context, client Ingester_QueryStreamClient, ingesterName string, expectedSeriesCount int, queryLimiter *limiter.QueryLimiter, cleanup func(), log log.Logger) *SeriesChunksStreamReader {
	return &SeriesChunksStreamReader{
		ctx:                 ctx,
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
		queryLimiter:        queryLimiter,
		cleanup:             cleanup,
		log:                 log,
		ingesterName:        ingesterName,
	}
}

// Close cleans up all resources associated with this SeriesChunksStreamReader.
// This method should only be directly called if StartBuffering is not called,
// otherwise StartBuffering will call it once done.
func (s *SeriesChunksStreamReader) Close() {
	if err := util.CloseAndExhaust[*QueryStreamResponse](s.client); err != nil {
		level.Warn(s.log).Log("msg", "closing ingester client stream failed", "err", err)
	}

	s.cleanup()
}

// StartBuffering begins streaming series' chunks from the ingester associated with
// this SeriesChunksStreamReader. Once all series have been consumed with GetChunks, all resources
// associated with this SeriesChunksStreamReader are cleaned up.
// If an error occurs while streaming, a subsequent call to GetChunks will return an error.
// To cancel buffering, cancel the context associated with this SeriesChunksStreamReader's client.Ingester_QueryStreamClient.
func (s *SeriesChunksStreamReader) StartBuffering() {
	s.seriesBatchChan = make(chan []QueryStreamSeriesChunks, 1)

	// Important: to ensure that the goroutine does not become blocked and leak, the goroutine must only ever write to errorChan at most once.
	s.errorChan = make(chan error, 1)

	go func() {
		log, _ := spanlogger.NewWithLogger(s.client.Context(), s.log, "SeriesChunksStreamReader.StartBuffering")

		defer func() {
			s.Close()

			close(s.seriesBatchChan)
			close(s.errorChan)
			log.Finish()
		}()

		if err := s.readStream(log); err != nil {
			s.errorChan <- err
			if errors.Is(err, context.Canceled) {
				return
			}
			level.Error(log).Log("msg", "received error while streaming chunks from ingester", "err", err)
			ext.Error.Set(log.Span, true)
		}
	}()
}

func (s *SeriesChunksStreamReader) readStream(log *spanlogger.SpanLogger) error {
	totalSeries := 0
	totalChunks := 0

	for {
		msg, err := s.client.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				if totalSeries < s.expectedSeriesCount {
					return fmt.Errorf("expected to receive %v series, but got EOF after receiving %v series", s.expectedSeriesCount, totalSeries)
				}

				log.DebugLog("msg", "finished streaming", "series", totalSeries, "chunks", totalChunks)
				return nil
			} else if errors.Is(err, context.Canceled) {
				// If there's a more detailed cancellation reason available, return that.
				if cause := context.Cause(s.ctx); cause != nil {
					return fmt.Errorf("aborted stream because query was cancelled: %w", cause)
				}
			}

			return err
		}

		if len(msg.StreamingSeriesChunks) == 0 {
			continue
		}

		totalSeries += len(msg.StreamingSeriesChunks)
		if totalSeries > s.expectedSeriesCount {
			return fmt.Errorf("expected to receive only %v series, but received at least %v series", s.expectedSeriesCount, totalSeries)
		}

		chunkBytes := 0

		for _, s := range msg.StreamingSeriesChunks {
			totalChunks += len(s.Chunks)

			for _, c := range s.Chunks {
				chunkBytes += c.Size()
			}
		}

		// The chunk count limit is enforced earlier, while we're reading series labels, so we don't need to do that here.
		if err := s.queryLimiter.AddChunkBytes(chunkBytes); err != nil {
			return err
		}

		select {
		case <-s.ctx.Done():
			// Why do we abort if the context is done?
			// We want to make sure that this goroutine is never leaked.
			// This goroutine could be leaked if nothing is reading from the buffer, but this method is still trying to send
			// more series to a full buffer: it would block forever.
			// So, here, we try to send the series to the buffer if we can, but if the context is cancelled, then we give up.
			// This only works correctly if the context is cancelled when the query request is complete or cancelled,
			// which is true at the time of writing.
			//
			// Note that we deliberately don't use the context from the gRPC client here: that context is cancelled when
			// the stream's underlying ClientConn is closed, which can happen if the querier decides that the ingester is no
			// longer healthy. If that happens, we want to return the more informative error we'll get from Recv() above, not
			// a generic 'context canceled' error.
			return fmt.Errorf("aborted stream because query was cancelled: %w", context.Cause(s.ctx))
		case s.seriesBatchChan <- msg.StreamingSeriesChunks:
			// Batch enqueued successfully, nothing else to do for this batch.
		}
	}
}

// GetChunks returns the chunks for the series with index seriesIndex.
// This method must be called with monotonically increasing values of seriesIndex.
func (s *SeriesChunksStreamReader) GetChunks(seriesIndex uint64) (_ []Chunk, err error) {
	if s.err != nil {
		// Why not just return s.err?
		// GetChunks should not be called once it has previously returned an error.
		// However, if this does not hold true, this may indicate a bug somewhere else (see https://github.com/grafana/mimir-prometheus/pull/540 for an example).
		// So it's valuable to return a slightly different error to indicate that something's not quite right if GetChunks is called after it's previously returned an error.
		return nil, fmt.Errorf("attempted to read series at index %v from ingester chunks stream, but the stream previously failed and returned an error: %w", seriesIndex, s.err)
	}

	defer func() {
		s.err = err
	}()

	if len(s.seriesBatch) == 0 {
		if err := s.readNextBatch(seriesIndex); err != nil {
			return nil, err
		}
	}

	series := s.seriesBatch[0]

	// Discard the series we just read.
	if len(s.seriesBatch) > 1 {
		s.seriesBatch = s.seriesBatch[1:]
	} else {
		s.seriesBatch = nil
	}

	if series.SeriesIndex != seriesIndex {
		return nil, fmt.Errorf("attempted to read series at index %v from ingester chunks stream, but the stream has series with index %v", seriesIndex, series.SeriesIndex)
	}

	if int(seriesIndex) == s.expectedSeriesCount-1 {
		// This is the last series we expect to receive. Wait for StartBuffering() to exit (which is signalled by returning an error or
		// closing errorChan).
		//
		// This ensures two things:
		// 1. If we receive more series than expected (likely due to a bug), or something else goes wrong after receiving the last series,
		//    StartBuffering() will return an error. This method will then return it, which will bubble up to the PromQL engine and report
		//    it, rather than it potentially being logged and missed.
		// 2. It ensures the gRPC stream is cleaned up before the PromQL engine cancels the context used for the query. If the context
		//    is cancelled before the gRPC stream's Recv() returns EOF, this can result in misleading context cancellation errors being
		//    logged and included in metrics and traces, when in fact the call succeeded.
		if err := <-s.errorChan; err != nil {
			return nil, fmt.Errorf("attempted to read series at index %v from ingester chunks stream, but the stream has failed: %w", seriesIndex, err)
		}
	}

	return series.Chunks, nil
}

func (s *SeriesChunksStreamReader) readNextBatch(seriesIndex uint64) error {
	batch, channelOpen := <-s.seriesBatchChan

	if !channelOpen {
		// If there's an error, report it.
		select {
		case err, haveError := <-s.errorChan:
			if haveError {
				if validation.IsLimitError(err) {
					return err
				}
				return fmt.Errorf("attempted to read series at index %v from ingester chunks stream, but the stream has failed: %w", seriesIndex, err)
			}
		default:
		}

		return fmt.Errorf("attempted to read series at index %v from ingester chunks stream, but the stream has already been exhausted (was expecting %v series)", seriesIndex, s.expectedSeriesCount)
	}

	s.seriesBatch = batch
	return nil
}
