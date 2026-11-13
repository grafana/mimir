// SPDX-License-Identifier: AGPL-3.0-only
//go:build rustengine && cgo

package ingester

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"

	"github.com/grafana/mimir/pkg/ingester/client"
	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/storage/rustengine"
)

var _ streamingQuerier = rustChunkQuerier{}

func (q rustChunkQuerier) QueryStream(ctx context.Context, hints *storage.SelectHints, matchers []*labels.Matcher, batching streamBatching, send func(*client.QueryStreamResponse) error) (streamStats, error) {
	var stats streamStats
	if batching.SeriesBatchSize <= 0 || batching.ChunksBatchSize == 0 || batching.ChunksBatchMessageBytes <= 0 {
		return stats, errors.New("invalid Rust stream batching")
	}
	selected := q.selectChunks(ctx, true, hints, matchers...)
	set, ok := selected.(*rustStreamSet)
	if !ok {
		return stats, selected.Err()
	}
	frames := make([]rustSeriesFrame, 0, min(set.count, 256))
	series := make([]client.QueryStreamSeries, 0, min(set.count, uint64(batching.SeriesBatchSize)))
	totalChunks := 0
	for {
		frame, ok := set.nextRaw()
		if !ok {
			break
		}
		series = append(series, client.QueryStreamSeries{Labels: frame.labels, ChunkCount: int64(frame.count)})
		// Labels are encoded before chunks; their adapter arrays need not survive that phase.
		frame.labels = nil
		frames = append(frames, frame)
		totalChunks += frame.count
		if len(series) >= batching.SeriesBatchSize {
			if err := send(&client.QueryStreamResponse{StreamingSeries: series}); err != nil {
				return stats, err
			}
			clear(series)
			series = series[:0]
		}
	}
	if err := set.Err(); err != nil {
		return stats, err
	}
	stats.Series = len(frames)
	if err := send(&client.QueryStreamResponse{StreamingSeries: series, IsEndOfSeriesStream: true}); err != nil {
		return stats, err
	}
	clear(series)

	batch := make([]client.QueryStreamSeriesChunks, 0, min(uint64(len(frames)), batching.ChunksBatchSize, 256))
	var arena []client.Chunk
	bytes := 0
	for index, frame := range frames {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		if cap(arena)-len(arena) < frame.count {
			arena = make([]client.Chunk, 0, max(min(256, totalChunks), frame.count))
		}
		start := len(arena)
		appendChunk := func(raw rustengine.QueryChunk) error {
			ch, count, err := rustStreamChunk(set.cursor, set.encoding, raw)
			if err != nil {
				return err
			}
			arena = append(arena, ch)
			stats.Samples += count
			return nil
		}
		if err := appendChunk(frame.first); err != nil {
			return stats, err
		}
		for offset := 1; offset < frame.count; {
			page, err := set.cursor.call(rustengine.Request{Op: "cursor_chunks", Ref: frame.index, UpTo: int64(offset), Limit: 16})
			if err != nil {
				return stats, err
			}
			if len(page.Chunks) == 0 {
				return stats, errors.New("empty Rust chunk page")
			}
			for _, raw := range page.Chunks {
				if err := appendChunk(raw); err != nil {
					return stats, err
				}
			}
			offset += len(page.Chunks)
		}
		item := client.QueryStreamSeriesChunks{SeriesIndex: uint64(index), Chunks: arena[start:len(arena):len(arena)]}
		if len(item.Chunks) != frame.count {
			return stats, errors.New("Rust chunk count changed during streaming")
		}
		stats.Chunks += len(item.Chunks)
		size := item.Size()
		if bytes > 0 && bytes+size > batching.ChunksBatchMessageBytes || len(batch) >= int(batching.ChunksBatchSize) {
			if err := send(&client.QueryStreamResponse{StreamingSeriesChunks: batch}); err != nil {
				return stats, err
			}
			stats.Batches++
			clear(batch)
			batch, bytes = batch[:0], 0
			// A sent batch no longer needs its arena; keep the current series at its start.
			arena = arena[:copy(arena, arena[start:])]
			item.Chunks = arena[:len(arena):len(arena)]
		}
		batch = append(batch, item)
		bytes += size
	}
	if len(batch) > 0 {
		if err := send(&client.QueryStreamResponse{StreamingSeriesChunks: batch}); err != nil {
			return stats, err
		}
		stats.Batches++
	}
	return stats, nil
}

func rustStreamChunk(cursor *rustQueryCursor, encoding chunkenc.Encoding, raw rustengine.QueryChunk) (client.Chunk, int, error) {
	if len(raw.Data) > 0 && encoding != chunkenc.EncXOR2 {
		if len(raw.Data) < 2 {
			return client.Chunk{}, 0, fmt.Errorf("Rust XOR chunk too short: %d", len(raw.Data))
		}
		return client.Chunk{StartTimestampMs: raw.Mint, EndTimestampMs: raw.Maxt, Encoding: int32(chunk.PrometheusXorChunk), Data: raw.Data}, int(binary.BigEndian.Uint16(raw.Data)), nil
	}
	it := rustStreamChunks{cursor: cursor, encoding: encoding, page: []rustengine.QueryChunk{raw}, done: true}
	if !it.Next() {
		return client.Chunk{}, 0, it.Err()
	}
	ch, err := client.ChunkFromMeta(it.At())
	return ch, it.At().Chunk.NumSamples(), err
}
