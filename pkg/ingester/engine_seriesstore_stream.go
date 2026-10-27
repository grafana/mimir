// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/storage/chunk"
	schunks "github.com/grafana/mimir/pkg/storage/seriesstore/chunks"
	"github.com/grafana/mimir/pkg/storage/seriesstore/store"
)

// streamedSelector is what the store's chunk querier gives for a streamed response.
type streamedSelector interface {
	SelectStreamed(hints *storage.SelectHints, matchers ...*labels.Matcher) ([]store.StreamedSeries, error)
}

// streamingChunkQuerier is the seriesstore's chunk querier, which builds the query response from what it selected, as
// the store has it: the series are sent with their labels as adapters and their chunks without chunk iterators, which
// the generic path builds for each chunk of each series.
type streamingChunkQuerier struct {
	storage.ChunkQuerier
	selector streamedSelector
}

// wrapStreaming returns the querier with the response streaming, when it can.
func wrapStreaming(q storage.ChunkQuerier) storage.ChunkQuerier {
	if selector, ok := q.(streamedSelector); ok {
		return streamingChunkQuerier{ChunkQuerier: q, selector: selector}
	}
	return q
}

// QueryStream sends the response as sendStreamingQuerySeries and sendStreamingQueryChunks do.
func (q streamingChunkQuerier) QueryStream(_ context.Context, hints *storage.SelectHints, matchers []*labels.Matcher, batching streamBatching, send func(*client.QueryStreamResponse) error) (streamStats, error) {
	var stats streamStats
	selected, err := q.selector.SelectStreamed(hints, matchers...)
	if err != nil {
		return stats, errors.Wrap(err, "selecting series from ChunkQuerier")
	}
	stats.Series = len(selected)

	scratch := takeStreamScratch()
	defer scratch.give()
	seriesInBatch := scratch.series[:0]
	// The labels of a batch, reused once it is sent, which encodes it.
	adapters := scratch.adapters[:0]
	defer func() {
		scratch.series, scratch.adapters = held(scratch.series, seriesInBatch), held(scratch.adapters, adapters)
	}()
	for index := range selected {
		start := len(adapters)
		adapters = selected[index].AppendLabels(adapters)
		seriesInBatch = append(seriesInBatch, client.QueryStreamSeries{Labels: adapters[start:len(adapters):len(adapters)], ChunkCount: int64(len(selected[index].Chunks))})
		if len(seriesInBatch) >= batching.SeriesBatchSize {
			if err := send(&client.QueryStreamResponse{StreamingSeries: seriesInBatch}); err != nil {
				return stats, err
			}
			scratch.series, scratch.adapters = held(scratch.series, seriesInBatch), held(scratch.adapters, adapters)
			seriesInBatch, adapters = seriesInBatch[:0], adapters[:0]
		}
	}
	// Send any remaining series, and signal that there are no more.
	if err := send(&client.QueryStreamResponse{StreamingSeries: seriesInBatch, IsEndOfSeriesStream: true}); err != nil {
		return stats, err
	}

	var (
		chunksInBatch  = scratch.chunks[:0]
		batchSizeBytes = 0
		// The chunks of a batch share an array, which the next batch takes from the start once the batch is sent.
		arena = scratch.arena[:0]
		// The chunks not yet in an arena: a request for a few series doesn't allocate room for 256.
		remaining = 0
	)
	defer func() {
		scratch.chunks, scratch.arena = held(scratch.chunks, chunksInBatch), held(scratch.arena, arena)
	}()
	for index := range selected {
		remaining += len(selected[index].Chunks)
	}
	for index := range selected {
		list := selected[index].Chunks
		if cap(arena)-len(arena) < len(list) {
			// The chunks of the batch not sent yet are in the array they were put in.
			arena = make([]client.Chunk, 0, min(max(len(list), 256, 2*cap(arena)), remaining))
		}
		remaining -= len(list)
		start := len(arena)
		for _, c := range list {
			ch, samples, err := clientChunk(c.MinTime, c.MaxTime, c.Encoding, c.Data)
			if err != nil {
				return stats, err
			}
			arena = append(arena, ch)
			stats.Samples += samples
		}
		seriesChunks := client.QueryStreamSeriesChunks{SeriesIndex: uint64(index), Chunks: arena[start:len(arena):len(arena)]}
		stats.Chunks += len(seriesChunks.Chunks)
		msgSize := seriesChunks.Size()

		if (batchSizeBytes > 0 && batchSizeBytes+msgSize > batching.ChunksBatchMessageBytes) || len(chunksInBatch) >= int(batching.ChunksBatchSize) {
			// Adding this series to the batch would make it too big, flush the data and add it to new batch instead.
			if err := send(&client.QueryStreamResponse{StreamingSeriesChunks: chunksInBatch}); err != nil {
				return stats, err
			}
			scratch.chunks, scratch.arena = held(scratch.chunks, chunksInBatch), held(scratch.arena, arena)
			chunksInBatch = chunksInBatch[:0]
			batchSizeBytes = 0
			stats.Batches++
			// The batch is encoded: only the series' own chunks are still needed, at the start of the array.
			arena = arena[:copy(arena, arena[start:])]
			seriesChunks.Chunks = arena[:len(arena):len(arena)]
		}
		chunksInBatch = append(chunksInBatch, seriesChunks)
		batchSizeBytes += msgSize
	}
	// Send any remaining series.
	if batchSizeBytes != 0 {
		if err := send(&client.QueryStreamResponse{StreamingSeriesChunks: chunksInBatch}); err != nil {
			return stats, err
		}
		stats.Batches++
	}
	return stats, nil
}

// streamScratch holds the slices a response is built in, kept between responses: growing them for each of the many
// small ones was a tenth of what an ingester allocated.
type streamScratch struct {
	series   []client.QueryStreamSeries
	adapters []mimirpb.LabelAdapter
	chunks   []client.QueryStreamSeriesChunks
	arena    []client.Chunk
}

var streamScratches sync.Pool

// maxKeptStreamScratch is how many elements a kept slice holds at most: a large response's are left to the collector.
const maxKeptStreamScratch = 64 << 10

func takeStreamScratch() *streamScratch {
	if scratch, ok := streamScratches.Get().(*streamScratch); ok {
		return scratch
	}
	return &streamScratch{}
}

// held is the slice a response keeps of kept, which used is cut from or grown out of: as long as all the elements the
// response wrote, which give clears. A slice is reused from its start for each batch.
func held[T any](kept, used []T) []T {
	if unsafe.SliceData(kept) == unsafe.SliceData(used) && len(kept) > len(used) {
		return kept
	}
	return used
}

// give keeps the slices for another response. What they hold is cleared first: they point into the store's labels and
// the query's chunk buffers, of one tenant, which the buffers are reused for another's. Past it, they are empty.
func (s *streamScratch) give() {
	if cap(s.series) > maxKeptStreamScratch || cap(s.adapters) > maxKeptStreamScratch || cap(s.chunks) > maxKeptStreamScratch || cap(s.arena) > maxKeptStreamScratch {
		return
	}
	clear(s.series)
	clear(s.adapters)
	clear(s.chunks)
	clear(s.arena)
	s.series, s.adapters, s.chunks, s.arena = s.series[:0], s.adapters[:0], s.chunks[:0], s.arena[:0]
	streamScratches.Put(s)
}

// clientChunk is a chunk of the store as a response sends it, with how many samples it has.
func clientChunk(minTime, maxTime int64, encoding int32, data []byte) (client.Chunk, int, error) {
	ch := client.Chunk{StartTimestampMs: minTime, EndTimestampMs: maxTime, Data: data}
	// The store's wire encodings are three: floats, and the two histograms.
	switch encoding {
	case schunks.EncodingHistogram:
		ch.Encoding = int32(chunk.PrometheusHistogramChunk)
	case schunks.EncodingFloatHistogram:
		ch.Encoding = int32(chunk.PrometheusFloatHistogramChunk)
	default:
		ch.Encoding = int32(chunk.PrometheusXorChunk)
	}
	// The number of samples is the first two bytes of every chunk the TSDB has.
	if len(data) < 2 {
		return client.Chunk{}, 0, fmt.Errorf("chunk of the seriesstore chunk querier too short: %d bytes", len(data))
	}
	return ch, int(binary.BigEndian.Uint16(data)), nil
}
