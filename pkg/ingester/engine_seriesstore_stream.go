// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"

	"github.com/grafana/mimir/pkg/ingester/client"
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

	seriesInBatch := make([]client.QueryStreamSeries, 0, min(batching.SeriesBatchSize, len(selected)+1))
	for index := range selected {
		seriesInBatch = append(seriesInBatch, client.QueryStreamSeries{Labels: selected[index].Labels, ChunkCount: int64(len(selected[index].Chunks))})
		if len(seriesInBatch) >= batching.SeriesBatchSize {
			if err := send(&client.QueryStreamResponse{StreamingSeries: seriesInBatch}); err != nil {
				return stats, err
			}
			seriesInBatch = seriesInBatch[:0]
		}
	}
	// Send any remaining series, and signal that there are no more.
	if err := send(&client.QueryStreamResponse{StreamingSeries: seriesInBatch, IsEndOfSeriesStream: true}); err != nil {
		return stats, err
	}

	var (
		chunksInBatch  = make([]client.QueryStreamSeriesChunks, 0, min(batching.ChunksBatchSize, uint64(len(selected))))
		batchSizeBytes = 0
		// The chunks of a batch share an array, which is cut after the batch is sent.
		arena []client.Chunk
	)
	for index := range selected {
		list := selected[index].Chunks
		if cap(arena)-len(arena) < len(list) {
			arena = make([]client.Chunk, 0, max(len(list), 256))
		}
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
			chunksInBatch = chunksInBatch[:0]
			batchSizeBytes = 0
			stats.Batches++
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
