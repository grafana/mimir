// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/loki/blob/main/pkg/util/loser/tree.go

package querier

import (
	"errors"
	"fmt"
	"io"
	"sync"

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
	seriesBatchChan     chan []streamedIngesterSeries
	seriesBatch         []streamedIngesterSeries
	expectedSeriesCount int
	seriesBufferSize    int
	batchPool           sync.Pool
}

func NewSeriesStreamer(client client.Ingester_QueryStreamClient, expectedSeriesCount int, seriesBufferSize int) *SeriesChunksStreamReader {
	return &SeriesChunksStreamReader{
		client:              client,
		expectedSeriesCount: expectedSeriesCount,
		seriesBufferSize:    seriesBufferSize,
		batchPool: sync.Pool{
			New: func() any {
				return make([]streamedIngesterSeries, 0, seriesBufferSize)
			},
		},
	}
}

type streamedIngesterSeries struct {
	chunks      []client.Chunk
	seriesIndex int
	err         error
}

// StartBuffering begins streaming series' chunks from the ingester associated with
// this SeriesChunksStreamReader. Once all series have been consumed with GetChunks, all resources
// associated with this SeriesChunksStreamReader are cleaned up.
// If an error occurs while streaming, a subsequent call to GetChunks will return an error.
// To cancel buffering, cancel the context associated with this SeriesChunksStreamReader's client.Ingester_QueryStreamClient.
func (s *SeriesChunksStreamReader) StartBuffering() {
	s.seriesBatchChan = make(chan []streamedIngesterSeries, 1)
	ctxDone := s.client.Context().Done()

	// Why does this exist?
	// We want to make sure that the goroutine below is never leaked.
	// The goroutine below could be leaked if nothing is reading from the buffer but it's still trying to send
	// more series to a full buffer: it would block forever.
	// So, here, we try to send the series to the buffer if we can, but if the context is cancelled, then we give up.
	// This only works correctly if the context is cancelled when the query request is complete (or cancelled),
	// which is true at the time of writing.
	writeToBufferOrAbort := func(batch []streamedIngesterSeries) bool {
		select {
		case <-ctxDone:
			return false
		case s.seriesBatchChan <- batch:
			return true
		}
	}

	tryToWriteErrorToBuffer := func(err error) {
		writeToBufferOrAbort([]streamedIngesterSeries{{err: err}})
	}

	go func() {
		defer s.client.CloseSend() //nolint:errcheck
		defer close(s.seriesBatchChan)

		nextSeriesIndex := 0
		currentBatch := s.batchPool.Get().([]streamedIngesterSeries)

		for {
			msg, err := s.client.Recv()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if nextSeriesIndex < s.expectedSeriesCount {
						tryToWriteErrorToBuffer(fmt.Errorf("expected to receive %v series, but got EOF after receiving %v series", s.expectedSeriesCount, nextSeriesIndex))
					}
				} else {
					tryToWriteErrorToBuffer(err)
				}

				return
			}

			for seriesIdx, series := range msg.SeriesChunks {
				if nextSeriesIndex >= s.expectedSeriesCount {
					tryToWriteErrorToBuffer(fmt.Errorf("expected to receive only %v series, but received more than this", s.expectedSeriesCount))
					return
				}

				currentBatch = append(currentBatch, streamedIngesterSeries{chunks: series.Chunks, seriesIndex: nextSeriesIndex})

				if len(currentBatch) == s.seriesBufferSize || seriesIdx == len(msg.SeriesChunks)-1 {
					if !writeToBufferOrAbort(currentBatch) {
						return
					}

					currentBatch = s.batchPool.Get().([]streamedIngesterSeries)[:0]
				}

				nextSeriesIndex++
			}
		}
	}()
}

// GetChunks returns the chunks for the series with index seriesIndex.
// This method must be called with monotonically increasing values of seriesIndex.
func (s *SeriesChunksStreamReader) GetChunks(seriesIndex int) ([]client.Chunk, error) {
	if len(s.seriesBatch) == 0 {
		s.seriesBatch = <-s.seriesBatchChan

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

	series := s.seriesBatch[0]

	if len(s.seriesBatch) > 1 {
		s.seriesBatch = s.seriesBatch[1:]
	} else {
		s.batchPool.Put(s.seriesBatch)
		s.seriesBatch = nil
	}

	if series.err != nil {
		return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has failed: %w", seriesIndex, series.err)
	}

	if series.seriesIndex != seriesIndex {
		return nil, fmt.Errorf("attempted to read series at index %v from stream, but the stream has series with index %v", seriesIndex, series.seriesIndex)
	}

	return series.chunks, nil
}

type SeriesChunksStream struct {
	StreamReader *SeriesChunksStreamReader
	Series       []labels.Labels
}

func newSeriesChunkStreamsTree(ingesters []SeriesChunksStream) *seriesChunkStreamsTree {
	nIngesters := len(ingesters)
	t := seriesChunkStreamsTree{
		nodes: make([]seriesChunkStreamsTreeNode, nIngesters*2),
	}
	for idx, s := range ingesters {
		t.nodes[idx+nIngesters].ingester = s
		t.moveNext(idx + nIngesters) // Must call Next on each item so that At() has a value.
	}
	if nIngesters > 0 {
		t.nodes[0].index = -1 // flag to be initialized on first call to Next().
	}
	return &t
}

// seriesChunkStreamsTree is a loser tree used to merge sets of series from different ingesters.
// A loser tree is a binary tree laid out such that nodes N and N+1 have parent N/2.
// We store M leaf nodes in positions M...2M-1, and M-1 internal nodes in positions 1..M-1.
// Node 0 is a special node, containing the winner of the contest.
type seriesChunkStreamsTree struct {
	nodes []seriesChunkStreamsTreeNode
}

type seriesChunkStreamsTreeNode struct {
	index           int                // This is the loser for all nodes except the 0th, where it is the winner.
	value           labels.Labels      // Value copied from the loser node, or winner for node 0.
	ingester        SeriesChunksStream // Only populated for leaf nodes.
	nextSeriesIndex int                // Only populated for leaf nodes.
}

func (t *seriesChunkStreamsTree) moveNext(index int) bool {
	n := &t.nodes[index]
	n.nextSeriesIndex++
	if n.nextSeriesIndex > len(n.ingester.Series) {
		n.value = nil
		n.index = -1
		return false
	}
	n.value = n.ingester.Series[n.nextSeriesIndex-1]
	return true
}

func (t *seriesChunkStreamsTree) Winner() (SeriesChunksStream, labels.Labels, int) {
	n := t.nodes[t.nodes[0].index]
	return n.ingester, n.value, n.nextSeriesIndex - 1
}

func (t *seriesChunkStreamsTree) Next() bool {
	if len(t.nodes) == 0 {
		return false
	}
	if t.nodes[0].index == -1 { // If tree has not been initialized yet, do that.
		t.initialize()
		return t.nodes[t.nodes[0].index].index != -1
	}
	if t.nodes[t.nodes[0].index].index == -1 { // already exhausted
		return false
	}
	t.moveNext(t.nodes[0].index)
	t.replayGames(t.nodes[0].index)
	return t.nodes[t.nodes[0].index].index != -1
}

func (t *seriesChunkStreamsTree) initialize() {
	winners := make([]int, len(t.nodes))
	// Initialize leaf nodes as winners to start.
	for i := len(t.nodes) / 2; i < len(t.nodes); i++ {
		winners[i] = i
	}
	for i := len(t.nodes) - 2; i > 0; i -= 2 {
		// At each stage the winners play each other, and we record the loser in the node.
		loser, winner := t.playGame(winners[i], winners[i+1])
		p := parent(i)
		t.nodes[p].index = loser
		t.nodes[p].value = t.nodes[loser].value
		winners[p] = winner
	}
	t.nodes[0].index = winners[1]
	t.nodes[0].value = t.nodes[winners[1]].value
}

// Starting at pos, re-consider all values up to the root.
func (t *seriesChunkStreamsTree) replayGames(pos int) {
	// At the start, pos is a leaf node, and is the winner at that level.
	n := parent(pos)
	for n != 0 {
		if t.less(t.nodes[n].value, t.nodes[pos].value) {
			loser := pos
			// Record pos as the loser here, and the old loser is the new winner.
			pos = t.nodes[n].index
			t.nodes[n].index = loser
			t.nodes[n].value = t.nodes[loser].value
		}
		n = parent(n)
	}
	// pos is now the winner; store it in node 0.
	t.nodes[0].index = pos
	t.nodes[0].value = t.nodes[pos].value
}

func (t *seriesChunkStreamsTree) playGame(a, b int) (loser, winner int) {
	if t.less(t.nodes[a].value, t.nodes[b].value) {
		return b, a
	}
	return a, b
}

func (t *seriesChunkStreamsTree) less(a, b labels.Labels) bool {
	if a == nil {
		return false
	}

	if b == nil {
		return true
	}

	return labels.Compare(a, b) < 0
}

func parent(i int) int { return i / 2 }
