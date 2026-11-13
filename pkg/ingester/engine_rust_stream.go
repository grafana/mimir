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
	pages := rustChunkPages{cursor: set.cursor, frames: frames}
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
			page, err := pages.read(index, offset)
			if err != nil {
				return stats, err
			}
			for _, raw := range page {
				if err := appendChunk(raw); err != nil {
					return stats, err
				}
			}
			offset += len(page)
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

type rustChunkPages struct {
	cursor *rustQueryCursor
	frames []rustSeriesFrame
	refs   []uint64
	chunks []rustengine.QueryChunk
}

func (p *rustChunkPages) read(index, offset int) ([]rustengine.QueryChunk, error) {
	if len(p.chunks) == 0 {
		var refs [16]uint64
		n := 0
		for _, frame := range p.frames[index:] {
			if frame.count > 1 {
				refs[n] = frame.index
				n++
				if n == len(refs) {
					break
				}
			}
		}
		response, err := p.cursor.call(rustengine.Request{Op: "cursor_chunks_batch", Refs: refs[:n], UpTo: int64(offset), Limit: 128})
		if err != nil {
			return nil, err
		}
		if len(response.Chunks) == 0 || len(response.Chunks) > 128 || len(response.Refs) != len(response.Chunks) {
			return nil, errors.New("invalid Rust chunk batch")
		}
		// Validate the whole page before exposing chunks to the stream.
		row, at := index, offset
		for _, ref := range response.Refs {
			for row < len(p.frames) && at >= p.frames[row].count {
				row++
				at = 1
			}
			if row >= len(p.frames) || ref != p.frames[row].index {
				return nil, errors.New("invalid Rust chunk batch order")
			}
			at++
		}
		p.refs, p.chunks = response.Refs, response.Chunks
	}
	if p.refs[0] != p.frames[index].index {
		return nil, errors.New("invalid Rust chunk batch continuation")
	}
	n := 1
	for n < len(p.refs) && p.refs[n] == p.refs[0] {
		n++
	}
	page := p.chunks[:n:n]
	p.refs, p.chunks = p.refs[n:], p.chunks[n:]
	return page, nil
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
