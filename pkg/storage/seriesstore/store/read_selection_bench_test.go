// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"runtime"
	"testing"

	promlabels "github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"
)

// Compaction invalidates these caches while dashboard variables keep asking for the same few values.
func BenchmarkLabelLookupsAfterInvalidation(b *testing.B) {
	e := labelDebugEngine(b, false)
	for _, label := range []string{"", "namespace", "asserts_assertion_name"} {
		name := label
		if name == "" {
			name = "names"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				e.store.evictEpoch.Add(1)
				q, err := e.Querier(0, 1_801_000)
				require.NoError(b, err)
				if label == "" {
					_, _, err = q.LabelNames(b.Context(), nil)
				} else {
					_, _, err = q.LabelValues(b.Context(), label, &storage.LabelHints{Limit: 10000})
				}
				require.NoError(b, err)
				require.NoError(b, q.Close())
			}
		})
	}
}

func BenchmarkStreamedColdTimeWindow(b *testing.B) {
	e, err := OpenEngine("", "synthetic", EngineOptions{Shards: 1})
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, e.Close()) })
	shard := e.store.shards[0]
	metas := make([]ChunkMeta, 360)
	for n := range metas {
		ts := int64(n) * 30000
		chunk := chunkenc.NewXORChunk()
		app, err := chunk.Appender()
		require.NoError(b, err)
		app.Append(0, ts, float64(n))
		ref, err := shard.disk.Write(chunk.Bytes(), ts)
		require.NoError(b, err)
		metas[n] = ChunkMeta{Ref: ref, MinTime: ts, MaxTime: ts, Len: uint32(len(chunk.Bytes())), Encoding: xorEncoding}
	}
	rows := benchFrozen(1000, 0)
	for n := range rows {
		rows[n].chunks = chunkListFromMetas(metas)
	}
	block, err := buildColdBlock("", 1, rows)
	require.NoError(b, err)
	shard.cold.blocks = []*coldBlock{block}
	matcher := eq("__name__", "metric_07")
	for _, tc := range []struct {
		name       string
		start, end int64
	}{
		{"narrow", 180 * 30000, 181*30000 - 1},
		{"ten_minutes", 180 * 30000, 200*30000 - 1},
		{"thirty_minutes", 180 * 30000, 240*30000 - 1},
		{"wide", 0, 360 * 30000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				q, err := e.ChunkQuerier(tc.start, tc.end)
				require.NoError(b, err)
				selected, err := q.(*engineChunkQuerier).SelectStreamed(nil, matcher)
				require.NoError(b, err)
				require.NotEmpty(b, selected)
				runtime.KeepAlive(selected)
				require.NoError(b, q.Close())
			}
		})
	}
}

// A merged block can hold hours of chunks when the query only reads a few minutes.
func BenchmarkColdSelectionTimeWindow(b *testing.B) {
	rows := benchFrozen(1000, 360)
	block, err := buildMemoryColdBlock(b.TempDir(), 1, rows)
	require.NoError(b, err)
	b.Cleanup(func() { require.NoError(b, block.close()) })
	cold := &coldState{blocks: []*coldBlock{block}}
	compiled, err := compilePromMatchers(nil, []*promlabels.Matcher{eq("__name__", "metric_07")})
	require.NoError(b, err)
	for _, tc := range []struct {
		name       string
		start, end int64
	}{
		{"narrow", 180 * 30000, 181*30000 - 1},
		{"wide", 0, 360 * 30000},
	} {
		b.Run(tc.name, func(b *testing.B) {
			selection := takeColdSelection()
			defer selection.give()
			b.ReportAllocs()
			for b.Loop() {
				selection.reset()
				new(Store).selectCold("synthetic", cold, compiled, compiled, nil, selectorID(compiled), nil, tc.start, tc.end, retainedEverything, 0, selection)
				require.NotEmpty(b, selection.candidates)
			}
		})
	}
}
