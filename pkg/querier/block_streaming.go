// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"io"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/validation"
)

// Implementation of storage.SeriesSet, based on individual responses from store client.
type blockStreamingQuerierSeriesSet struct {
	series       []*storepb.StreamingSeries
	streamReader chunkStreamReader

	// next response to process
	nextSeriesIndex int

	currSeries storage.Series
}

type chunkStreamReader interface {
	GetChunks(seriesIndex uint64) ([]storepb.AggrChunk, error)
}

func (bqss *blockStreamingQuerierSeriesSet) Next() bool {
	bqss.currSeries = nil

	if bqss.nextSeriesIndex >= len(bqss.series) {
		return false
	}

	currLabels := mimirpb.FromLabelAdaptersToLabels(bqss.series[bqss.nextSeriesIndex].Labels)
	seriesIdxStart := bqss.nextSeriesIndex // First series in this group. We might merge with more below.
	bqss.nextSeriesIndex++

	// Chunks may come in multiple responses, but as soon as the response has chunks for a new series,
	// we can stop searching. Series are sorted. See documentation for StoreClient.Series call for details.
	// The actually merging of chunks happens in the Iterator() call where chunks are fetched.
	for bqss.nextSeriesIndex < len(bqss.series) && labels.Compare(currLabels, mimirpb.FromLabelAdaptersToLabels(bqss.series[bqss.nextSeriesIndex].Labels)) == 0 {
		bqss.nextSeriesIndex++
	}

	bqss.currSeries = newBlockStreamingQuerierSeries(currLabels, seriesIdxStart, bqss.nextSeriesIndex-1, bqss.streamReader)
	return true
}

func (bqss *blockStreamingQuerierSeriesSet) At() storage.Series {
	return bqss.currSeries
}

func (bqss *blockStreamingQuerierSeriesSet) Err() error {
	return nil
}

func (bqss *blockStreamingQuerierSeriesSet) Warnings() storage.Warnings {
	return nil
}

// newBlockStreamingQuerierSeries makes a new blockQuerierSeries. Input labels must be already sorted by name.
func newBlockStreamingQuerierSeries(lbls labels.Labels, seriesIdxStart, seriesIdxEnd int, streamReader chunkStreamReader) *blockStreamingQuerierSeries {
	return &blockStreamingQuerierSeries{
		labels:         lbls,
		seriesIdxStart: seriesIdxStart,
		seriesIdxEnd:   seriesIdxEnd,
		streamReader:   streamReader,
	}
}

type blockStreamingQuerierSeries struct {
	labels                       labels.Labels
	seriesIdxStart, seriesIdxEnd int
	streamReader                 chunkStreamReader
}

func (bqs *blockStreamingQuerierSeries) Labels() labels.Labels {
	return bqs.labels
}

func (bqs *blockStreamingQuerierSeries) Iterator(reuse chunkenc.Iterator) chunkenc.Iterator {
	// Fetch the chunks from the stream.
	var allChunks []storepb.AggrChunk
	for i := bqs.seriesIdxStart; i <= bqs.seriesIdxEnd; i++ {
		chks, err := bqs.streamReader.GetChunks(uint64(i))
		if err != nil {
			return series.NewErrIterator(err)
		}
		allChunks = append(allChunks, chks...)
	}
	if len(allChunks) == 0 {
		// should not happen in practice, but we have a unit test for it
		return series.NewErrIterator(errors.New("no chunks"))
	}

	sort.Slice(allChunks, func(i, j int) bool {
		return allChunks[i].MinTime < allChunks[j].MinTime
	})

	it, err := newBlockQuerierSeriesIterator(reuse, bqs.Labels(), allChunks)
	if err != nil {
		return series.NewErrIterator(err)
	}

	return it
}

// storeGatewayStreamReader is responsible for managing the streaming of chunks from a storegateway and buffering
// chunks in memory until they are consumed by the PromQL engine.
type storeGatewayStreamReader struct {
	client              storegatewaypb.StoreGateway_SeriesClient
	expectedSeriesCount int
	queryLimiter        *limiter.QueryLimiter
	stats               *stats.Stats
	log                 log.Logger

	seriesChunksChan chan *storepb.StreamingChunksBatch
	chunksBatch      []*storepb.StreamingChunks
	errorChan        chan error
}

func newStoreGatewayStreamReader(client storegatewaypb.StoreGateway_SeriesClient, expectedSeriesCount int, queryLimiter *limiter.QueryLimiter, stats *stats.Stats, log log.Logger) *storeGatewayStreamReader {
	return &storeGatewayStreamReader{
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
		queryLimiter:        queryLimiter,
		stats:               stats,
		log:                 log,
		seriesChunksChan:    make(chan *storepb.StreamingChunksBatch, 1),
		// Important: to ensure that the goroutine does not become blocked and leak, the goroutine must only ever write to errorChan at most once.
		errorChan: make(chan error, 1),
	}
}

// Close cleans up all resources associated with this storeGatewayStreamReader.
// This method should only be called if StartBuffering is not called.
func (s *storeGatewayStreamReader) Close() {
	if err := s.client.CloseSend(); err != nil {
		level.Warn(s.log).Log("msg", "closing storegateway client stream failed", "err", err)
	}
}

// StartBuffering begins streaming series' chunks from the storegateway associated with
// this storeGatewayStreamReader. Once all series have been consumed with GetChunks, all resources
// associated with this storeGatewayStreamReader are cleaned up.
// If an error occurs while streaming, a subsequent call to GetChunks will return an error.
// To cancel buffering, cancel the context associated with this storeGatewayStreamReader's storegatewaypb.StoreGateway_SeriesClient.
func (s *storeGatewayStreamReader) StartBuffering() {
	ctxDone := s.client.Context().Done()

	go func() {
		defer func() {
			s.Close()
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

			c := msg.GetStreamingChunks()
			if c == nil {
				s.errorChan <- fmt.Errorf("expected to receive StreamingSeriesChunks, but got something else")
				return
			}

			if len(c.Series) == 0 {
				continue
			}

			totalSeries += len(c.Series)
			if totalSeries > s.expectedSeriesCount {
				s.errorChan <- fmt.Errorf("expected to receive only %v series, but received at least %v series", s.expectedSeriesCount, totalSeries)
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

			if err := s.queryLimiter.AddChunks(numChunks); err != nil {
				s.errorChan <- validation.LimitError(err.Error())
				return
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
func (s *storeGatewayStreamReader) GetChunks(seriesIndex uint64) ([]storepb.AggrChunk, error) {
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
