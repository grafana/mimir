// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
)

// The code in this file is used by the queriers to read the streaming chunks from the storegateway.

// SeriesChunksStreamReader is responsible for managing the streaming of chunks from a storegateway and buffering
// chunks in memory until they are consumed by the PromQL engine.
type SeriesChunksStreamReader struct {
	client              storegatewaypb.StoreGateway_SeriesClient
	expectedSeriesCount int
	queryLimiter        *limiter.QueryLimiter
	stats               *stats.Stats
	log                 log.Logger

	seriesChunksChan chan *storepb.StreamSeriesChunksBatch
	chunksBatch      []*storepb.StreamSeriesChunks
	errorChan        chan error
}

func NewSeriesChunksStreamReader(client storegatewaypb.StoreGateway_SeriesClient, expectedSeriesCount int, queryLimiter *limiter.QueryLimiter, stats *stats.Stats, log log.Logger) *SeriesChunksStreamReader {
	return &SeriesChunksStreamReader{
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
		queryLimiter:        queryLimiter,
		stats:               stats,
		log:                 log,
	}
}

// Close cleans up all resources associated with this SeriesChunksStreamReader.
// This method should only be called if StartBuffering is not called.
func (s *SeriesChunksStreamReader) Close() {
	if err := s.client.CloseSend(); err != nil {
		level.Warn(s.log).Log("msg", "closing storegateway client stream failed", "err", err)
	}
}

// StartBuffering begins streaming series' chunks from the storegateway associated with
// this SeriesChunksStreamReader. Once all series have been consumed with GetChunks, all resources
// associated with this SeriesChunksStreamReader are cleaned up.
// If an error occurs while streaming, a subsequent call to GetChunks will return an error.
// To cancel buffering, cancel the context associated with this SeriesChunksStreamReader's storegatewaypb.StoreGateway_SeriesClient.
func (s *SeriesChunksStreamReader) StartBuffering() {
	s.seriesChunksChan = make(chan *storepb.StreamSeriesChunksBatch, 2)

	// Important: to ensure that the goroutine does not become blocked and leak, the goroutine must only ever write to errorChan at most once.
	s.errorChan = make(chan error, 1)
	ctxDone := s.client.Context().Done()

	go func() {
		defer func() {
			if err := s.client.CloseSend(); err != nil {
				level.Warn(s.log).Log("msg", "closing storegateway client stream failed", "err", err)
			}

			close(s.seriesChunksChan)
			close(s.errorChan)
		}()

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

			c := msg.GetStreamingSeriesChunks()
			if c == nil {
				s.errorChan <- fmt.Errorf("expected to receive StreamingSeriesChunks, but got something else")
				return
			}

			totalSeries++
			if totalSeries > s.expectedSeriesCount {
				s.errorChan <- fmt.Errorf("expected to receive only %v series, but received at least %v series", s.expectedSeriesCount, totalSeries)
				return
			}

			if err := s.queryLimiter.AddChunks(len(c.Series)); err != nil {
				s.errorChan <- validation.LimitError(err.Error())
				return
			}

			chunkBytes := 0
			numChunks := 0
			for _, s := range c.Series {
				numChunks += len(s.Chunks)
				for _, ch := range s.Chunks {
					chunkBytes += ch.Size()
				}
			}
			if err := s.queryLimiter.AddChunkBytes(chunkBytes); err != nil {
				s.errorChan <- validation.LimitError(err.Error())
				return
			}

			s.stats.AddFetchedChunks(uint64(numChunks))
			s.stats.AddFetchedChunkBytes(uint64(chunkBytes))

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
			case s.seriesChunksChan <- c:
				// Batch enqueued successfully, nothing else to do for this batch.
			}
		}
	}()
}

// GetChunks returns the chunks for the series with index seriesIndex.
// This method must be called with monotonically increasing values of seriesIndex.
func (s *SeriesChunksStreamReader) GetChunks(seriesIndex uint64) ([]storepb.AggrChunk, error) {
	if len(s.chunksBatch) == 0 {
		chks, channelOpen := <-s.seriesChunksChan

		if !channelOpen {
			// If there's an error, report it.
			select {
			case err, haveError := <-s.errorChan:
				if haveError {
					if _, ok := err.(validation.LimitError); ok {
						return nil, err
					}
					return nil, errors.Wrapf(err, "attempted to read series at index %v from stream, but the stream has failed", seriesIndex)
				}
			default:
			}

			return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has already been exhausted", seriesIndex)
		}

		s.chunksBatch = chks.Series
	}

	chks := s.chunksBatch[0]
	if len(s.chunksBatch) > 1 {
		s.chunksBatch = s.chunksBatch[1:]
	} else {
		s.chunksBatch = nil
	}

	if chks.SeriesIndex != seriesIndex {
		return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has series with index %v", seriesIndex, chks.SeriesIndex)
	}

	return chks.Chunks, nil
}
