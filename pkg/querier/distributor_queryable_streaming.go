// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"errors"
	"fmt"
	"io"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/util/chunkcompat"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type streamingChunkSeries struct {
	labels            labels.Labels
	chunkIteratorFunc chunkIteratorFunc
	mint, maxt        int64
	sources           []StreamingSeriesSource
}

func (s *streamingChunkSeries) Labels() labels.Labels {
	return s.labels
}

func (s *streamingChunkSeries) Iterator(_ chunkenc.Iterator) chunkenc.Iterator {
	var rawChunks []client.Chunk

	for _, source := range s.sources {
		c, err := source.StreamReader.GetChunks(source.SeriesIndex)

		if err != nil {
			return series.NewErrIterator(err)
		}

		rawChunks = client.AccumulateChunks(rawChunks, c)
	}

	chunks, err := chunkcompat.FromChunks(s.labels, rawChunks)
	if err != nil {
		return series.NewErrIterator(err)
	}

	return s.chunkIteratorFunc(chunks, model.Time(s.mint), model.Time(s.maxt))
}

type SeriesChunksStreamReader struct {
	client              client.Ingester_QueryStreamClient
	seriesBatchChan     chan []client.QueryStreamSeriesChunks
	errorChan           chan error
	seriesBatch         []client.QueryStreamSeriesChunks
	expectedSeriesCount int
	queryLimiter        *limiter.QueryLimiter
}

func NewSeriesStreamReader(client client.Ingester_QueryStreamClient, expectedSeriesCount int, queryLimiter *limiter.QueryLimiter) *SeriesChunksStreamReader {
	return &SeriesChunksStreamReader{
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
		queryLimiter:        queryLimiter,
	}
}

// Close cleans up all resources associated with this SeriesChunksStreamReader.
// This method should only be called if StartBuffering is not called.
func (s *SeriesChunksStreamReader) Close() {
	s.client.CloseSend() //nolint:errcheck
}

// StartBuffering begins streaming series' chunks from the ingester associated with
// this SeriesChunksStreamReader. Once all series have been consumed with GetChunks, all resources
// associated with this SeriesChunksStreamReader are cleaned up.
// If an error occurs while streaming, a subsequent call to GetChunks will return an error.
// To cancel buffering, cancel the context associated with this SeriesChunksStreamReader's client.Ingester_QueryStreamClient.
func (s *SeriesChunksStreamReader) StartBuffering() {
	s.seriesBatchChan = make(chan []client.QueryStreamSeriesChunks, 1)

	// Important: to ensure that the goroutine does not become blocked and leak, the goroutine must only ever write to errorChan at most once.
	s.errorChan = make(chan error, 1)
	ctxDone := s.client.Context().Done()

	go func() {
		defer s.client.CloseSend() //nolint:errcheck
		defer close(s.seriesBatchChan)
		defer close(s.errorChan)

		totalSeries := 0

		for {
			msg, err := s.client.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if totalSeries < s.expectedSeriesCount {
						s.errorChan <- fmt.Errorf("expected to receive %v series, but got EOF after receiving %v series", s.expectedSeriesCount, totalSeries)
					}
				} else {
					s.errorChan <- err
				}

				return
			}

			totalSeries += len(msg.SeriesChunks)
			if totalSeries > s.expectedSeriesCount {
				s.errorChan <- fmt.Errorf("expected to receive only %v series, but received more than this", s.expectedSeriesCount)
				return
			}

			chunkCount := 0
			chunkBytes := 0

			for _, s := range msg.SeriesChunks {
				chunkCount += len(s.Chunks)

				for _, c := range s.Chunks {
					chunkBytes += c.Size()
				}
			}

			if err := s.queryLimiter.AddChunks(chunkCount); err != nil {
				s.errorChan <- err
				return
			}

			if err := s.queryLimiter.AddChunkBytes(chunkBytes); err != nil {
				s.errorChan <- err
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
				s.errorChan <- s.client.Context().Err()
				return
			case s.seriesBatchChan <- msg.SeriesChunks:
				// Batch sent successfully, nothing else to do for this batch.
			}
		}
	}()
}

// GetChunks returns the chunks for the series with index seriesIndex.
// This method must be called with monotonically increasing values of seriesIndex.
func (s *SeriesChunksStreamReader) GetChunks(seriesIndex uint64) ([]client.Chunk, error) {
	if len(s.seriesBatch) == 0 {
		batch, haveBatch := <-s.seriesBatchChan

		if !haveBatch {
			// If there's an error, report it.
			select {
			case err, haveError := <-s.errorChan:
				if haveError {
					return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has failed: %w", seriesIndex, err)
				}
			default:
			}

			return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has already been exhausted", seriesIndex)
		}

		// This should never happen, but it guards against misbehaving ingesters.
		// If we receive an empty batch, discard it and read the next one.
		if len(batch) == 0 {
			return s.GetChunks(seriesIndex)
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
		return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has series with index %v", seriesIndex, series.SeriesIndex)
	}

	return series.Chunks, nil
}
