// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/iterators/chunk_merge_iterator_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package iterators

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/test"
)

var (
	generateTestHistogram      = test.GenerateTestHistogram
	generateTestFloatHistogram = test.GenerateTestFloatHistogram
)

type chunkbound struct {
	mint, maxt model.Time
}

type chunkencoding struct {
	enc          chunk.Encoding
	assertSample func(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType)
}

func TestChunkMergeIterator(t *testing.T) {
	for i, tc := range []struct {
		chunkbounds []chunkbound
		mint, maxt  int64
	}{
		{
			chunkbounds: []chunkbound{{0, 100}},
			maxt:        100,
		},
		{
			chunkbounds: []chunkbound{{0, 100}, {0, 100}},
			maxt:        100,
		},
		{
			chunkbounds: []chunkbound{{0, 100}, {50, 150}, {100, 200}},
			maxt:        200,
		},
		{
			chunkbounds: []chunkbound{{0, 100}, {100, 200}},
			maxt:        200,
		},
	} {
		for _, encoding := range []chunkencoding{
			{enc: chunk.PrometheusXorChunk, assertSample: test.RequireIteratorIthFloat},
			{enc: chunk.PrometheusHistogramChunk, assertSample: test.RequireIteratorIthHistogram},
			{enc: chunk.PrometheusFloatHistogramChunk, assertSample: test.RequireIteratorIthFloatHistogram}} {

			t.Run(strconv.Itoa(i), func(t *testing.T) {
				chunks := []chunk.Chunk{}
				for _, bounds := range tc.chunkbounds {
					chunks = append(chunks, mkChunk(t, bounds.mint, bounds.maxt, 1*time.Millisecond, encoding.enc))
				}
				iter := NewChunkMergeIterator(chunks, 0, 0)
				for i := tc.mint; i < tc.maxt; i++ {
					encoding.assertSample(t, i, iter, iter.Next())
				}
				assert.Equal(t, chunkenc.ValNone, iter.Next())
			})
		}
	}
}

func TestChunkMergeIteratorMixed(t *testing.T) {
	iter := NewChunkMergeIterator([]chunk.Chunk{
		mkChunk(t, 0, 75, 1*time.Millisecond, chunk.PrometheusXorChunk),
		mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
		mkChunk(t, 125, 200, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
	}, 0, 0)

	assertSample := func(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
		if i < 50 {
			test.RequireIteratorIthFloat(t, i, iter, valueType)
			return
		}
		if i < 125 {
			test.RequireIteratorIthHistogram(t, i, iter, valueType)
			return
		}
		test.RequireIteratorIthFloatHistogram(t, i, iter, valueType)
	}

	for i := int64(0); i < 200; i++ {
		assertSample(t, i, iter, iter.Next())
	}
}

func TestChunkMergeIteratorSeek(t *testing.T) {
	for _, encoding := range []chunkencoding{
		{enc: chunk.PrometheusXorChunk, assertSample: test.RequireIteratorIthFloat},
		{enc: chunk.PrometheusHistogramChunk, assertSample: test.RequireIteratorIthHistogram},
		{enc: chunk.PrometheusFloatHistogramChunk, assertSample: test.RequireIteratorIthFloatHistogram}} {
		t.Run(fmt.Sprintf("%v", encoding.enc), func(t *testing.T) {
			chunks := []chunk.Chunk{
				mkChunk(t, 0, 100, 1*time.Millisecond, encoding.enc),
				mkChunk(t, 50, 150, 1*time.Millisecond, encoding.enc),
				mkChunk(t, 100, 200, 1*time.Millisecond, encoding.enc),
			}
			mint := int64(0)
			maxt := int64(200)

			for i := mint; i < maxt; i += 20 {
				iter := NewChunkMergeIterator(chunks, 0, 0)
				valueType := iter.Seek(i)
				encoding.assertSample(t, i, iter, valueType)

				for j := i + 1; j < maxt; j++ {
					valueType := iter.Next()
					encoding.assertSample(t, j, iter, valueType)
				}
				assert.Equal(t, chunkenc.ValNone, iter.Next())
			}
		})
	}
}

func TestChunkMergeIteratorSeekMixed(t *testing.T) {
	chunks := []chunk.Chunk{
		mkChunk(t, 0, 75, 1*time.Millisecond, chunk.PrometheusXorChunk),
		mkChunk(t, 50, 150, 1*time.Millisecond, chunk.PrometheusHistogramChunk),
		mkChunk(t, 125, 200, 1*time.Millisecond, chunk.PrometheusFloatHistogramChunk),
	}
	mint := int64(0)
	maxt := int64(200)

	assertSample := func(t *testing.T, i int64, iter chunkenc.Iterator, valueType chunkenc.ValueType) {
		if i < 50 {
			test.RequireIteratorIthFloat(t, i, iter, valueType)
			return
		}
		if i < 125 {
			test.RequireIteratorIthHistogram(t, i, iter, valueType)
			return
		}
		test.RequireIteratorIthFloatHistogram(t, i, iter, valueType)
	}

	for i := mint; i < maxt; i += 10 {
		iter := NewChunkMergeIterator(chunks, 0, 0)
		assertSample(t, i, iter, iter.Seek(i))

		for j := i + 1; j < maxt; j++ {
			assertSample(t, j, iter, iter.Next())
		}
	}
}

func mkChunk(t require.TestingT, mint, maxt model.Time, step time.Duration, encoding chunk.Encoding) chunk.Chunk {
	var addPair func(pc chunk.EncodedChunk, ts model.Time) (chunk.EncodedChunk, error)
	switch encoding {
	case chunk.PrometheusXorChunk:
		addPair = func(pc chunk.EncodedChunk, ts model.Time) (chunk.EncodedChunk, error) {
			return pc.Add(model.SamplePair{
				Timestamp: ts,
				Value:     model.SampleValue(float64(ts)),
			})
		}
	case chunk.PrometheusHistogramChunk:
		addPair = func(pc chunk.EncodedChunk, ts model.Time) (chunk.EncodedChunk, error) {
			return pc.AddHistogram(int64(ts), generateTestHistogram(int(ts)))
		}
	case chunk.PrometheusFloatHistogramChunk:
		addPair = func(pc chunk.EncodedChunk, ts model.Time) (chunk.EncodedChunk, error) {
			return pc.AddFloatHistogram(int64(ts), generateTestFloatHistogram(int(ts)))
		}
	default:
		panic(fmt.Sprintf("mkChunk - unhandled encoding: %v", encoding))
	}
	metric := labels.FromStrings(model.MetricNameLabel, "foo")
	pc, err := chunk.NewForEncoding(encoding)
	require.NoError(t, err)
	for i := mint; i.Before(maxt); i = i.Add(step) {
		npc, err := addPair(pc, i)
		require.NoError(t, err)
		require.Nil(t, npc)
	}
	return chunk.NewChunk(metric, pc, mint, maxt)
}
