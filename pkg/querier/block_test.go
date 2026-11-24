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

	"github.com/grafana/mimir/pkg/querier/batch"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
	"github.com/grafana/mimir/pkg/util/test"
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
			it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), permutation, batch.IteratorOptions{})

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

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2}, batch.IteratorOptions{})

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

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2, chunk3}, batch.IteratorOptions{})

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

	it := newBlockQuerierSeriesIterator(nil, labels.EmptyLabels(), []storepb.AggrChunk{chunk1, chunk2, chunk3}, batch.IteratorOptions{})
	require.Equal(t, chunkenc.ValNone, it.Seek(10))
}

func newStartTimestampTestChunk(t *testing.T, encoding chunkenc.Encoding) chunkenc.Chunk {
	t.Helper()

	encoded, err := chunkenc.NewEmptyChunk(encoding)
	require.NoError(t, err)

	app, err := encoded.Appender()
	require.NoError(t, err)

	// The sample is at 200 seconds, with a stored start timestamp of 100 seconds.
	switch encoding {
	case chunkenc.EncXOR, chunkenc.EncXOR2:
		app.Append(100_000, 200_000, 42)
	case chunkenc.EncHistogramST:
		_, _, _, err = app.AppendHistogram(nil, 100_000, 200_000, test.GenerateTestHistogram(1), true)
	case chunkenc.EncFloatHistogramST:
		_, _, _, err = app.AppendFloatHistogram(nil, 100_000, 200_000, test.GenerateTestFloatHistogram(1), true)
	default:
		t.Fatalf("unsupported test encoding: %s", encoding)
	}
	require.NoError(t, err)

	return encoded
}

// TestBlockIterator_StartTimestampsReuse mirrors Prometheus reusing an iterator across series
// within one query. The collection option stays fixed, but each series has its own start timestamp.
func TestBlockIterator_StartTimestampsReuse(t *testing.T) {
	tests := []struct {
		name                    string
		enabled                 bool
		expectedStartTimestamps [2]int64
	}{
		{
			name:                    "collection enabled",
			enabled:                 true,
			expectedStartTimestamps: [2]int64{100_000, 150_000},
		},
		{
			name:                    "collection disabled",
			enabled:                 false,
			expectedStartTimestamps: [2]int64{0, 0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			series := []struct {
				instance       string
				startTimestamp int64
				value          float64
			}{
				{
					instance:       "first",
					startTimestamp: 100_000,
					value:          42,
				},
				{
					instance:       "second",
					startTimestamp: 150_000,
					value:          84,
				},
			}
			options := batch.IteratorOptions{CollectStartTimestamps: tc.enabled}

			// Only the series data changes when the iterator is reused.
			var it chunkenc.Iterator
			for i, seriesData := range series {
				encoded, err := chunkenc.NewEmptyChunk(chunkenc.EncXOR2)
				require.NoError(t, err)

				app, err := encoded.Appender()
				require.NoError(t, err)
				app.Append(seriesData.startTimestamp, 200_000, seriesData.value)

				chunks := []storepb.AggrChunk{
					{
						MinTime: 200_000,
						MaxTime: 200_000,
						Raw: storepb.Chunk{
							Type: storepb.Chunk_XOR2,
							Data: encoded.Bytes(),
						},
					},
				}
				lbls := labels.FromStrings("__name__", "counter", "instance", seriesData.instance)
				next := newBlockQuerierSeriesIterator(it, lbls, chunks, options)
				if it != nil {
					require.Same(t, it, next)
				}
				it = next

				require.Equal(t, chunkenc.ValFloat, it.Seek(200_000))
				timestamp, value := it.At()
				require.Equal(t, int64(200_000), timestamp)
				require.Equal(t, seriesData.value, value)
				require.Equal(t, tc.expectedStartTimestamps[i], it.AtST())
				require.Equal(t, chunkenc.ValNone, it.Next())
				require.NoError(t, it.Err())
			}
		})
	}
}
