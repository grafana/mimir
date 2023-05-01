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
		c, err := source.Streamer.GetChunks(source.SeriesIndex)

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

type SeriesStreamer struct {
	client              client.Ingester_QueryStreamClient
	buffer              chan streamedIngesterSeries
	expectedSeriesCount int
}

func NewSeriesStreamer(client client.Ingester_QueryStreamClient, expectedSeriesCount int) *SeriesStreamer {
	return &SeriesStreamer{
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
	}
}

type streamedIngesterSeries struct {
	chunks      []client.Chunk
	seriesIndex int
	err         error
}

// Close cleans up any resources associated with this SeriesStreamer.
// This method should only be called if StartBuffering is never called.
func (s *SeriesStreamer) Close() error {
	return s.client.CloseSend()
}

// StartBuffering begins streaming series' chunks from the ingester associated with
// this SeriesStreamer. Once all series have been consumed with GetChunks, all resources
// associated with this SeriesStreamer are cleaned up.
// If an error occurs while streaming, a subsequent call to GetChunks will return an error.
// To cancel buffering, cancel the context associated with this SeriesStreamer's client.Ingester_QueryStreamClient.
func (s *SeriesStreamer) StartBuffering() {
	s.buffer = make(chan streamedIngesterSeries, 10) // TODO: what is a sensible buffer size?
	ctxDone := s.client.Context().Done()

	// Why does this exist?
	// We want to make sure that the goroutine below is never leaked.
	// The goroutine below could be leaked if nothing is reading from the buffer but it's still trying to send
	// more series to a full buffer: it would block forever.
	// So, here, we try to send the series to the buffer if we can, but if the context is cancelled, then we give up.
	// This only works correctly if the context is cancelled when the query request is complete (or cancelled),
	// which is true at the time of writing.
	writeToBufferOrAbort := func(msg streamedIngesterSeries) bool {
		select {
		case <-ctxDone:
			return false
		case s.buffer <- msg:
			return true
		}
	}

	tryToWriteErrorToBuffer := func(err error) {
		writeToBufferOrAbort(streamedIngesterSeries{err: err})
	}

	go func() {
		defer s.client.CloseSend()
		defer close(s.buffer)

		nextSeriesIndex := 0

		for {
			msg, err := s.client.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if nextSeriesIndex < s.expectedSeriesCount {
						tryToWriteErrorToBuffer(fmt.Errorf("expected to receive %v series, but got EOF after receiving %v series", s.expectedSeriesCount, nextSeriesIndex))
					}
				} else {
					tryToWriteErrorToBuffer(fmt.Errorf("expected to receive %v series, but got EOF after receiving %v series", s.expectedSeriesCount, nextSeriesIndex))
				}

				return
			}

			for _, series := range msg.SeriesChunks {
				if nextSeriesIndex >= s.expectedSeriesCount {
					tryToWriteErrorToBuffer(fmt.Errorf("expected to receive only %v series, but received more than this", s.expectedSeriesCount))
					return
				}


				if !writeToBufferOrAbort(streamedIngesterSeries{chunks: series.Chunks, seriesIndex: nextSeriesIndex}) {
					return
				}

				nextSeriesIndex++
			}
		}
	}()
}

// GetChunks returns the chunks for the series with index seriesIndex.
// This method must be called with monotonically increasing values of seriesIndex.
func (s *SeriesStreamer) GetChunks(seriesIndex int) ([]client.Chunk, error) {
	series, open := <-s.buffer

	if !open {
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

	if series.err != nil {
		return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has failed: %w", seriesIndex, series.err)
	}

	if series.seriesIndex != seriesIndex {
		return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has series with index %v", seriesIndex, series.seriesIndex)
	}

	return series.chunks, nil
}
