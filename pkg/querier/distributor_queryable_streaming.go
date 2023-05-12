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
	var rawChunks []client.Chunk // TODO: guess a size for this?

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
}

func NewSeriesStreamer(client client.Ingester_QueryStreamClient, expectedSeriesCount int) *SeriesChunksStreamReader {
	return &SeriesChunksStreamReader{
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
	}
}

// StartBuffering begins streaming series' chunks from the ingester associated with
// this SeriesChunksStreamReader. Once all series have been consumed with GetChunks, all resources
// associated with this SeriesChunksStreamReader are cleaned up.
// If an error occurs while streaming, a subsequent call to GetChunks will return an error.
// To cancel buffering, cancel the context associated with this SeriesChunksStreamReader's client.Ingester_QueryStreamClient.
func (s *SeriesChunksStreamReader) StartBuffering() {
	s.seriesBatchChan = make(chan []client.QueryStreamSeriesChunks, 1)
	s.errorChan = make(chan error, 1)
	ctxDone := s.client.Context().Done()

	// Why does this exist?
	// We want to make sure that the goroutine below is never leaked.
	// The goroutine below could be leaked if nothing is reading from the buffer but it's still trying to send
	// more series to a full buffer: it would block forever.
	// So, here, we try to send the series to the buffer if we can, but if the context is cancelled, then we give up.
	// This only works correctly if the context is cancelled when the query request is complete (or cancelled),
	// which is true at the time of writing.
	// TODO: inline this, it's only used in one place
	writeToBufferOrAbort := func(batch []client.QueryStreamSeriesChunks) bool {
		select {
		case <-ctxDone:
			return false
		case s.seriesBatchChan <- batch:
			return true
		}
	}

	// Important: to ensure that this goroutine is not leaked, the function must only ever write to errorChan at most once.
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

			if !writeToBufferOrAbort(msg.SeriesChunks) {
				return
			}
		}
	}()
}

// GetChunks returns the chunks for the series with index seriesIndex.
// This method must be called with monotonically increasing values of seriesIndex.
func (s *SeriesChunksStreamReader) GetChunks(seriesIndex uint64) ([]client.Chunk, error) {
	if len(s.seriesBatch) == 0 {
		select {
		case err, haveError := <-s.errorChan:
			if haveError {
				return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has failed: %w", seriesIndex, err)
			}
			
			// Streaming finished successfully with no errors. Discard the error channel and try reading from seriesBatchChan again.
			s.errorChan = nil
			return s.GetChunks(seriesIndex)

		case s.seriesBatch = <-s.seriesBatchChan:
			if len(s.seriesBatch) == 0 {
				// If the context has been cancelled, report the cancellation.
				// Note that we only check this if there are no series in the buffer as the context is always cancelled
				// at the end of a successful request - so if we checked for an error even if there are series in the
				// buffer, we might incorrectly report that the context has been cancelled, when in fact the request
				// has concluded as expected.
				if err := s.client.Context().Err(); err != nil {
					return nil, err
				}

				return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has already been exhausted", seriesIndex)
			}
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
		return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has series with index %v", seriesIndex, series.SeriesIndex)
	}

	return series.Chunks, nil
}
