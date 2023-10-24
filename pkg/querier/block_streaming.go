// SPDX-License-Identifier: AGPL-3.0-only

package querier

import (
	"fmt"
	"io"
	"sort"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/opentracing/opentracing-go/ext"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/series"
	"github.com/grafana/mimir/pkg/storegateway/storegatewaypb"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
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

	currLabels := bqss.series[bqss.nextSeriesIndex].Labels
	seriesIdxStart := bqss.nextSeriesIndex // First series in this group. We might merge with more below.
	bqss.nextSeriesIndex++

	// Chunks may come in multiple responses, but as soon as the response has chunks for a new series,
	// we can stop searching. Series are sorted. See documentation for StoreClient.Series call for details.
	// The actually merging of chunks happens in the Iterator() call where chunks are fetched.
	for bqss.nextSeriesIndex < len(bqss.series) && mimirpb.CompareLabelAdapters(currLabels, bqss.series[bqss.nextSeriesIndex].Labels) == 0 {
		bqss.nextSeriesIndex++
	}

	bqss.currSeries = newBlockStreamingQuerierSeries(mimirpb.FromLabelAdaptersToLabels(currLabels), seriesIdxStart, bqss.nextSeriesIndex-1, bqss.streamReader)
	return true
}

func (bqss *blockStreamingQuerierSeriesSet) At() storage.Series {
	return bqss.currSeries
}

func (bqss *blockStreamingQuerierSeriesSet) Err() error {
	return nil
}

func (bqss *blockStreamingQuerierSeriesSet) Warnings() annotations.Annotations {
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

	chunkCountEstimateChan chan int
	seriesChunksChan       chan *storepb.StreamingChunksBatch
	chunksBatch            []*storepb.StreamingChunks
	errorChan              chan error
	err                    error
}

func newStoreGatewayStreamReader(client storegatewaypb.StoreGateway_SeriesClient, expectedSeriesCount int, queryLimiter *limiter.QueryLimiter, stats *stats.Stats, log log.Logger) *storeGatewayStreamReader {
	return &storeGatewayStreamReader{
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
		queryLimiter:        queryLimiter,
		stats:               stats,
		log:                 log,
	}
}

// Close cleans up all resources associated with this storeGatewayStreamReader.
// This method should only be called if StartBuffering is not called.
func (s *storeGatewayStreamReader) Close() {
	if err := util.CloseAndExhaust[*storepb.SeriesResponse](s.client); err != nil {
		level.Warn(s.log).Log("msg", "closing store-gateway client stream failed", "err", err)
	}
}

// StartBuffering begins streaming series' chunks from the storegateway associated with
// this storeGatewayStreamReader. Once all series have been consumed with GetChunks, all resources
// associated with this storeGatewayStreamReader are cleaned up.
// If an error occurs while streaming, a subsequent call to GetChunks will return an error.
// To cancel buffering, cancel the context associated with this storeGatewayStreamReader's storegatewaypb.StoreGateway_SeriesClient.
func (s *storeGatewayStreamReader) StartBuffering() {
	// Important: to ensure that the goroutine does not become blocked and leak, the goroutine must only ever write to errorChan at most once.
	s.errorChan = make(chan error, 1)
	s.seriesChunksChan = make(chan *storepb.StreamingChunksBatch, 1)
	s.chunkCountEstimateChan = make(chan int, 1)

	go func() {
		log, _ := spanlogger.NewWithLogger(s.client.Context(), s.log, "storeGatewayStreamReader.StartBuffering")

		defer func() {
			s.Close()
			close(s.chunkCountEstimateChan)
			close(s.seriesChunksChan)
			close(s.errorChan)
			log.Finish()
		}()

		if err := s.readStream(log); err != nil {
			s.errorChan <- err
			level.Error(log).Log("msg", "received error while streaming chunks from store-gateway", "err", err)
			ext.Error.Set(log.Span, true)
		}
	}()
}

func (s *storeGatewayStreamReader) readStream(log *spanlogger.SpanLogger) error {
	totalSeries := 0
	totalChunks := 0

	translateReceivedError := func(err error) error {
		if !errors.Is(err, io.EOF) {
			return err
		}

		if totalSeries < s.expectedSeriesCount {
			return fmt.Errorf("expected to receive %v series, but got EOF after receiving %v series", s.expectedSeriesCount, totalSeries)
		}

		level.Debug(log).Log("msg", "finished streaming", "series", totalSeries, "chunks", totalChunks)
		return nil
	}

	msg, err := s.client.Recv()
	if err != nil {
		return translateReceivedError(err)
	}

	estimate := msg.GetStreamingChunksEstimate()
	if estimate == nil {
		return fmt.Errorf("expected to receive chunks estimate, but got message of type %T", msg.Result)
	}

	level.Debug(log).Log("msg", "received estimated number of chunks", "chunks", estimate.EstimatedChunkCount)
	if err := s.sendChunksEstimate(estimate.EstimatedChunkCount); err != nil {
		return err
	}

	for {
		msg, err := s.client.Recv()
		if err != nil {
			return translateReceivedError(err)
		}

		batch := msg.GetStreamingChunks()
		if batch == nil {
			return fmt.Errorf("expected to receive streaming chunks, but got message of type %T", msg.Result)
		}

		if len(batch.Series) == 0 {
			continue
		}

		totalSeries += len(batch.Series)
		if totalSeries > s.expectedSeriesCount {
			return fmt.Errorf("expected to receive only %v series, but received at least %v series", s.expectedSeriesCount, totalSeries)
		}

		chunkBytes := 0
		numChunks := 0
		for _, s := range batch.Series {
			numChunks += len(s.Chunks)
			for _, ch := range s.Chunks {
				chunkBytes += ch.Size()
			}
		}
		totalChunks += numChunks
		if err := s.queryLimiter.AddChunks(numChunks); err != nil {
			return validation.LimitError(err.Error())
		}
		if err := s.queryLimiter.AddChunkBytes(chunkBytes); err != nil {
			return validation.LimitError(err.Error())
		}

		s.stats.AddFetchedChunks(uint64(numChunks))
		s.stats.AddFetchedChunkBytes(uint64(chunkBytes))

		if err := s.sendBatch(batch); err != nil {
			return err
		}
	}
}

func (s *storeGatewayStreamReader) sendBatch(c *storepb.StreamingChunksBatch) error {
	select {
	case <-s.client.Context().Done():
		// Why do we abort if the context is done?
		// We want to make sure that the StartBuffering goroutine is never leaked.
		// This goroutine could be leaked if nothing is reading from the buffer, but this method is still trying to send
		// more series to a full buffer: it would block forever.
		// So, here, we try to send the series to the buffer if we can, but if the context is cancelled, then we give up.
		// This only works correctly if the context is cancelled when the query request is complete or cancelled,
		// which is true at the time of writing.
		return s.client.Context().Err()
	case s.seriesChunksChan <- c:
		// Batch enqueued successfully, nothing else to do for this batch.
		return nil
	}
}

func (s *storeGatewayStreamReader) sendChunksEstimate(chunksEstimate uint64) error {
	select {
	case <-s.client.Context().Done():
		// We abort if the context is done for the same reason we do in sendBatch - to avoid leaking the StartBuffering goroutine.
		return s.client.Context().Err()
	case s.chunkCountEstimateChan <- int(chunksEstimate):
		return nil
	}
}

// GetChunks returns the chunks for the series with index seriesIndex.
// This method must be called with monotonically increasing values of seriesIndex.
func (s *storeGatewayStreamReader) GetChunks(seriesIndex uint64) (_ []storepb.AggrChunk, err error) {
	if s.err != nil {
		// Why not just return s.err?
		// GetChunks should not be called once it has previously returned an error.
		// However, if this does not hold true, this may indicate a bug somewhere else (see https://github.com/grafana/mimir-prometheus/pull/540 for an example).
		// So it's valuable to return a slightly different error to indicate that something's not quite right if GetChunks is called after it's previously returned an error.
		return nil, fmt.Errorf("attempted to read series at index %v from store-gateway chunks stream, but the stream previously failed and returned an error: %w", seriesIndex, s.err)
	}

	defer func() {
		s.err = err
	}()

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
					return nil, errors.Wrapf(err, "attempted to read series at index %v from store-gateway chunks stream, but the stream has failed", seriesIndex)
				}
			default:
			}

			return nil, fmt.Errorf("attempted to read series at index %v from store-gateway chunks stream, but the stream has already been exhausted (was expecting %v series)", seriesIndex, s.expectedSeriesCount)
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
		return nil, fmt.Errorf("attempted to read series at index %v from store-gateway chunks stream, but the stream has series with index %v", seriesIndex, chks.SeriesIndex)
	}

	return chks.Chunks, nil
}

// EstimateChunkCount returns an estimate of the number of chunks this stream reader will return.
// If the stream fails before an estimate is received from the store-gateway, this method returns 0.
// This method should only be called after calling StartBuffering. If this method is called multiple times, it may block.
func (s *storeGatewayStreamReader) EstimateChunkCount() int {
	return <-s.chunkCountEstimateChan
}
