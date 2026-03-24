// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/block_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package querier

import (
	"cmp"
	"fmt"
	"slices"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func createAggrChunkWithSamples(samples ...promql.FPoint) storepb.AggrChunk {
	return createAggrChunk(samples[0].T, samples[len(samples)-1].T, samples...)
}

func createAggrChunk(minTime, maxTime int64, samples ...promql.FPoint) storepb.AggrChunk {
	// Ensure samples are sorted by timestamp.
	slices.SortFunc(samples, func(a, b promql.FPoint) int {
		return cmp.Compare(a.T, b.T)
	})

	chunk := chunkenc.NewXORChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	for _, s := range samples {
		appender.Append(0, s.T, s.F)
	}

	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: storepb.Chunk{
			Type: storepb.Chunk_XOR,
			Data: chunk.Bytes(),
		},
	}
}

func createAggrChunkWithFloatHistogramSamples(samples ...promql.HPoint) storepb.AggrChunk {
	return createAggrFloatHistogramChunk(samples[0].T, samples[len(samples)-1].T, samples...)
}

func createAggrFloatHistogramChunk(minTime, maxTime int64, samples ...promql.HPoint) storepb.AggrChunk {
	// Ensure samples are sorted by timestamp.
	slices.SortFunc(samples, func(a, b promql.HPoint) int {
		return cmp.Compare(a.T, b.T)
	})

	chunk := chunkenc.NewFloatHistogramChunk()
	appender, err := chunk.Appender()
	if err != nil {
		panic(err)
	}

	for _, s := range samples {
		_, _, appender, err = appender.AppendFloatHistogram(nil, 0, s.T, s.H, true)
		if err != nil {
			panic(err)
		}
	}

	return storepb.AggrChunk{
		MinTime: minTime,
		MaxTime: maxTime,
		Raw: storepb.Chunk{
			Type: storepb.Chunk_FloatHistogram,
			Data: chunk.Bytes(),
		},
	}
}

func TestBlockQuerierSeriesIterator_ShouldMergeOverlappingChunks(t *testing.T) {
	// Create 3 chunks with overlapping timeranges, but each sample is only in 1 chunk.
	chunk1 := createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 3, F: 3}, promql.FPoint{T: 7, F: 7})
	chunk2 := createAggrChunkWithSamples(promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 5, F: 5}, promql.FPoint{T: 9, F: 9})
	chunk3 := createAggrChunkWithSamples(promql.FPoint{T: 4, F: 4}, promql.FPoint{T: 6, F: 6}, promql.FPoint{T: 8, F: 8})

	permutations := [][]storepb.AggrChunk{
		{chunk1, chunk2, chunk3},
		{chunk1, chunk3, chunk2},
		{chunk2, chunk1, chunk3},
		{chunk2, chunk3, chunk1},
		{chunk3, chunk1, chunk2},
		{chunk3, chunk2, chunk1},
	}

	for idx, permutation := range permutations {
		t.Run(fmt.Sprintf("permutation %d", idx), func(t *testing.T) {
			it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), permutation)

			var actual []promql.FPoint
			for it.Next() != chunkenc.ValNone {
				ts, value := it.At()
				actual = append(actual, promql.FPoint{T: ts, F: value})

				require.Equal(t, ts, it.AtT())
			}

			require.NoError(t, it.Err())

			require.Equal(t, []promql.FPoint{
				{T: 1, F: 1},
				{T: 2, F: 2},
				{T: 3, F: 3},
				{T: 4, F: 4},
				{T: 5, F: 5},
				{T: 6, F: 6},
				{T: 7, F: 7},
				{T: 8, F: 8},
				{T: 9, F: 9},
			}, actual)
		})
	}
}

func TestBlockQuerierSeriesIterator_ShouldMergeNonOverlappingChunks(t *testing.T) {
	chunk1 := createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 3, F: 3})
	chunk2 := createAggrChunkWithSamples(promql.FPoint{T: 4, F: 4}, promql.FPoint{T: 5, F: 5}, promql.FPoint{T: 6, F: 6})

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2})

	var actual []promql.FPoint
	for it.Next() != chunkenc.ValNone {
		ts, value := it.At()
		actual = append(actual, promql.FPoint{T: ts, F: value})

		require.Equal(t, ts, it.AtT())
	}

	require.NoError(t, it.Err())

	require.Equal(t, []promql.FPoint{
		{T: 1, F: 1},
		{T: 2, F: 2},
		{T: 3, F: 3},
		{T: 4, F: 4},
		{T: 5, F: 5},
		{T: 6, F: 6},
	}, actual)
}

func TestBlockQuerierSeriesIterator_SeekWithNonOverlappingChunks(t *testing.T) {
	chunk1 := createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 3, F: 3})
	chunk2 := createAggrChunkWithSamples(promql.FPoint{T: 4, F: 4}, promql.FPoint{T: 5, F: 5}, promql.FPoint{T: 6, F: 6})
	chunk3 := createAggrChunkWithSamples(promql.FPoint{T: 7, F: 7}, promql.FPoint{T: 8, F: 8}, promql.FPoint{T: 9, F: 9})

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2, chunk3})

	// Seek to middle of first chunk.
	require.Equal(t, chunkenc.ValFloat, it.Seek(2))
	ts, value := it.At()
	require.Equal(t, int64(2), ts)
	require.Equal(t, 2.0, value)

	// Seek to end of first chunk.
	require.Equal(t, chunkenc.ValFloat, it.Seek(3))
	ts, value = it.At()
	require.Equal(t, int64(3), ts)
	require.Equal(t, 3.0, value)

	// Seek to the start of the second chunk.
	require.Equal(t, chunkenc.ValFloat, it.Seek(4))
	ts, value = it.At()
	require.Equal(t, int64(4), ts)
	require.Equal(t, 4.0, value)

	// Seek to the middle of the last chunk, and check all the remaining points are as we expect.
	require.Equal(t, chunkenc.ValFloat, it.Seek(8))

	var actual []promql.FPoint
	for {
		ts, value := it.At()
		actual = append(actual, promql.FPoint{T: ts, F: value})

		require.Equal(t, ts, it.AtT())

		// Keep consuming points until we've exhausted all remaining points.
		if it.Next() == chunkenc.ValNone {
			break
		}
	}

	require.NoError(t, it.Err())

	require.Equal(t, []promql.FPoint{
		{T: 8, F: 8},
		{T: 9, F: 9},
	}, actual)
}

func TestBlockQuerierSeriesIterator_SeekPastEnd(t *testing.T) {
	chunk1 := createAggrChunkWithSamples(promql.FPoint{T: 1, F: 1}, promql.FPoint{T: 2, F: 2}, promql.FPoint{T: 3, F: 3})
	chunk2 := createAggrChunkWithSamples(promql.FPoint{T: 4, F: 4}, promql.FPoint{T: 5, F: 5}, promql.FPoint{T: 6, F: 6})
	chunk3 := createAggrChunkWithSamples(promql.FPoint{T: 7, F: 7}, promql.FPoint{T: 8, F: 8}, promql.FPoint{T: 9, F: 9})

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2, chunk3})
	require.Equal(t, chunkenc.ValNone, it.Seek(10))
}
