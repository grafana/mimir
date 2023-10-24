// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
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
	client              Ingester_QueryStreamClient
	expectedSeriesCount int
	queryLimiter        *limiter.QueryLimiter
	cleanup             func()
	log                 log.Logger

	seriesBatchChan chan []QueryStreamSeriesChunks
	errorChan       chan error
	err             error
	seriesBatch     []QueryStreamSeriesChunks
}

func NewSeriesChunksStreamReader(client Ingester_QueryStreamClient, expectedSeriesCount int, queryLimiter *limiter.QueryLimiter, cleanup func(), log log.Logger) *SeriesChunksStreamReader {
	return &SeriesChunksStreamReader{
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
		queryLimiter:        queryLimiter,
		cleanup:             cleanup,
		log:                 log,
	}
}

// Close cleans up all resources associated with this SeriesChunksStreamReader.
// This method should only be called if StartBuffering is not called.
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
	ctxDone := s.client.Context().Done()

	go func() {
		log, _ := spanlogger.NewWithLogger(s.client.Context(), s.log, "SeriesChunksStreamReader.StartBuffering")

		defer func() {
			s.Close()

			close(s.seriesBatchChan)
			close(s.errorChan)
			log.Span.Finish()
		}()

		onError := func(err error) {
			s.errorChan <- err
			level.Error(log).Log("msg", "received error while streaming chunks from ingester", "err", err)
			ext.Error.Set(log.Span, true)
		}

		totalSeries := 0
		totalChunks := 0

		for {
			msg, err := s.client.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if totalSeries < s.expectedSeriesCount {
						onError(fmt.Errorf("expected to receive %v series, but got EOF after receiving %v series", s.expectedSeriesCount, totalSeries))
					} else {
						level.Debug(log).Log("msg", "finished streaming", "series", totalSeries, "chunks", totalChunks)
					}
				} else {
					onError(err)
				}

				return
			}

			if len(msg.StreamingSeriesChunks) == 0 {
				continue
			}

			totalSeries += len(msg.StreamingSeriesChunks)
			if totalSeries > s.expectedSeriesCount {
				onError(fmt.Errorf("expected to receive only %v series, but received at least %v series", s.expectedSeriesCount, totalSeries))
				return
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
				onError(err)
				return
			}

			select {
			case <-ctxDone:
				// Why do we abort if the context is done?
				// We want to make sure that this goroutine is never leaked.
				// This goroutine could be leaked if nothing is reading from the buffer, but this method is still trying to send
				// more series to a full buffer: it would block forever.
				// So, here, we try to send the series to the buffer if we can, but if the context is cancelled, then we give up.
				// This only works correctly if the context is cancelled when the query request is complete or cancelled,
				// which is true at the time of writing.
				onError(s.client.Context().Err())
				return
			case s.seriesBatchChan <- msg.StreamingSeriesChunks:
				// Batch enqueued successfully, nothing else to do for this batch.
			}
		}
	}()
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
		batch, channelOpen := <-s.seriesBatchChan

		if !channelOpen {
			// If there's an error, report it.
			select {
			case err, haveError := <-s.errorChan:
				if haveError {
					if _, ok := err.(validation.LimitError); ok {
						return nil, err
					}
					return nil, fmt.Errorf("attempted to read series at index %v from ingester chunks stream, but the stream has failed: %w", seriesIndex, err)
				}
			default:
			}

			return nil, fmt.Errorf("attempted to read series at index %v from ingester chunks stream, but the stream has already been exhausted (was expecting %v series)", seriesIndex, s.expectedSeriesCount)
		}

		s.seriesBatch = batch
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

	return series.Chunks, nil
}
