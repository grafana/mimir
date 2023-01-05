// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/iterators/chunk_merge_iterator_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package iterators

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestChunkMergeIterator(t *testing.T) {
	for i, tc := range []struct {
		chunks     []chunk.Chunk
		mint, maxt int64
	}{
		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 100,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 100,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 200,
		},

		{
			chunks: []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
				mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusXorChunk),
			},
			maxt: 200,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			iter := NewChunkMergeIterator(tc.chunks, 0, 0)
			for i := tc.mint; i < tc.maxt; i++ {
				require.Equal(t, chunkenc.ValFloat, iter.Next())
				ts, s := iter.At()
				assert.Equal(t, i, ts)
				assert.Equal(t, float64(i), s)
				assert.NoError(t, iter.Err())
			}
			assert.Equal(t, chunkenc.ValNone, iter.Next())
		})
	}
}

func TestChunkMergeIteratorSeek(t *testing.T) {
	iter := NewChunkMergeIterator([]chunk.Chunk{
		mkChunk(t, 0, 100, 1*time.Millisecond, chunk.PrometheusXorChunk),
		mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusXorChunk),
		mkChunk(t, 100, 200, 1*time.Millisecond, chunk.PrometheusXorChunk),
	}, 0, 0)

	for i := int64(0); i < 10; i += 20 {
		require.Equal(t, chunkenc.ValFloat, iter.Seek(i))
		ts, s := iter.At()
		assert.Equal(t, i, ts)
		assert.Equal(t, float64(i), s)
		assert.NoError(t, iter.Err())

		for j := i + 1; j < 200; j++ {
			require.Equal(t, chunkenc.ValFloat, iter.Next())
			ts, s := iter.At()
			assert.Equal(t, j, ts)
			assert.Equal(t, float64(j), s)
			assert.NoError(t, iter.Err())
		}
		assert.Equal(t, chunkenc.ValNone, iter.Next())
	}
}

func mkChunk(t require.TestingT, mint, maxt model.Time, step time.Duration, encoding chunk.Encoding) chunk.Chunk {
	metric := labels.FromStrings(model.MetricNameLabel, "foo")
	pc, err := chunk.NewForEncoding(encoding)
	require.NoError(t, err)
	for i := mint; i.Before(maxt); i = i.Add(step) {
		npc, err := pc.Add(model.SamplePair{
			Timestamp: i,
			Value:     model.SampleValue(float64(i)),
		})
		require.NoError(t, err)
		require.Nil(t, npc)
	}
	return chunk.NewChunk(metric, pc, mint, maxt)
}
