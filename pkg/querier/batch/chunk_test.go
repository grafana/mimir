// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/chunk_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/test"
)

const (
	step = 1 * time.Second
)

func TestChunkIter(t *testing.T) {
	testChunkIter(t, chunk.PrometheusXorChunk)
	testChunkIter(t, chunk.PrometheusHistogramChunk)
	testChunkIter(t, chunk.PrometheusFloatHistogramChunk)
}

func testChunkIter(t *testing.T, encoding chunk.Encoding) {
	chunk := mkGenericChunk(t, 0, 100, encoding)
	iter := &chunkIterator{}

	iter.reset(chunk)
	testIter(t, 100, newIteratorAdapter(iter), encoding)

	iter.reset(chunk)
	testSeek(t, 100, newIteratorAdapter(iter), encoding)
}

func mkChunk(t require.TestingT, from model.Time, points int, encoding chunk.Encoding) chunk.Chunk {
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
			return pc.AddHistogram(int64(ts), tsdb.GenerateTestHistogram(int(ts)))
		}
	case chunk.PrometheusFloatHistogramChunk:
		addPair = func(pc chunk.EncodedChunk, ts model.Time) (chunk.EncodedChunk, error) {
			return pc.AddFloatHistogram(int64(ts), tsdb.GenerateTestFloatHistogram(int(ts)))
		}
	default:
		panic(fmt.Sprintf("mkChunk - unhandled encoding: %v", encoding))
	}
	metric := labels.FromStrings(model.MetricNameLabel, "foo")
	pc, err := chunk.NewForEncoding(encoding)
	require.NoError(t, err)
	ts := from
	for i := 0; i < points; i++ {
		npc, err := addPair(pc, ts)
		require.NoError(t, err)
		require.Nil(t, npc)
		ts = ts.Add(step)
	}
	ts = ts.Add(-step) // undo the add that we did just before exiting the loop
	return chunk.NewChunk(metric, pc, from, ts)
}

func mkGenericChunk(t require.TestingT, from model.Time, points int, encoding chunk.Encoding) GenericChunk {
	ck := mkChunk(t, from, points, encoding)
	return NewGenericChunk(int64(ck.From), int64(ck.Through), ck.Data.NewIterator)
}

func testIter(t require.TestingT, points int, iter chunkenc.Iterator, encoding chunk.Encoding) {
	ets := model.TimeFromUnix(0)
	var testPoint func(i int)
	switch encoding {
	case chunk.PrometheusXorChunk:
		testPoint = func(i int) {
			require.Equal(t, chunkenc.ValFloat, iter.Next(), strconv.Itoa(i))
			ts, v := iter.At()
			require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
			require.EqualValues(t, float64(ets), v, strconv.Itoa(i))
			ets = ets.Add(step)
		}
	case chunk.PrometheusHistogramChunk:
		testPoint = func(i int) {
			require.Equal(t, chunkenc.ValHistogram, iter.Next(), strconv.Itoa(i))
			ts, h := iter.AtHistogram()
			require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
			test.RequireHistogramEqual(t, tsdb.GenerateTestHistogram(int(ets)), h, strconv.Itoa(i))
			ets = ets.Add(step)
		}
	case chunk.PrometheusFloatHistogramChunk:
		testPoint = func(i int) {
			require.Equal(t, chunkenc.ValFloatHistogram, iter.Next(), strconv.Itoa(i))
			ts, fh := iter.AtFloatHistogram()
			require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
			test.RequireFloatHistogramEqual(t, tsdb.GenerateTestFloatHistogram(int(ets)), fh, strconv.Itoa(i))
			ets = ets.Add(step)
		}
	default:
		panic(fmt.Sprintf("testIter - unhandled encoding: %v", encoding))
	}
	for i := 0; i < points; i++ {
		testPoint(i)
	}
	require.Equal(t, chunkenc.ValNone, iter.Next())
}

func testSeek(t require.TestingT, points int, iter chunkenc.Iterator, encoding chunk.Encoding) {
	var testSeekPoint func(ets int64, valType chunkenc.ValueType)
	switch encoding {
	case chunk.PrometheusXorChunk:
		testSeekPoint = func(ets int64, valType chunkenc.ValueType) {
			require.Equal(t, chunkenc.ValFloat, valType)
			ts, v := iter.At()
			require.EqualValues(t, ets, ts)
			require.EqualValues(t, float64(ets), v)
			require.NoError(t, iter.Err())
		}
	case chunk.PrometheusHistogramChunk:
		testSeekPoint = func(ets int64, valType chunkenc.ValueType) {
			require.Equal(t, chunkenc.ValHistogram, valType)
			ts, h := iter.AtHistogram()
			require.EqualValues(t, ets, ts)
			test.RequireHistogramEqual(t, tsdb.GenerateTestHistogram(int(ets)), h)
			require.NoError(t, iter.Err())
		}
	case chunk.PrometheusFloatHistogramChunk:
		testSeekPoint = func(ets int64, valType chunkenc.ValueType) {
			require.Equal(t, chunkenc.ValFloatHistogram, valType)
			ts, fh := iter.AtFloatHistogram()
			require.EqualValues(t, ets, ts)
			test.RequireFloatHistogramEqual(t, tsdb.GenerateTestFloatHistogram(int(ets)), fh)
			require.NoError(t, iter.Err())
		}
	default:
		panic(fmt.Sprintf("testSeek - unhandled encoding: %v", encoding))
	}

	for i := 0; i < points; i += points / 10 {
		ets := int64(i * int(step/time.Millisecond))
		testSeekPoint(ets, iter.Seek(ets))

		for j := i + 1; j < i+points/10; j++ {
			ets := int64(j * int(step/time.Millisecond))
			testSeekPoint(ets, iter.Next())
		}
	}
}

func TestSeek(t *testing.T) {
	var it mockIterator
	c := chunkIterator{
		chunk: GenericChunk{
			MaxTime: chunk.BatchSize,
		},
		it: &it,
	}

	for i := 0; i < chunk.BatchSize-1; i++ {
		require.Equal(t, chunkenc.ValFloat, c.Seek(int64(i), 1), i)
	}
	require.Equal(t, 1, it.seeks)

	require.Equal(t, chunkenc.ValFloat, c.Seek(int64(chunk.BatchSize), 1))
	require.Equal(t, 2, it.seeks)
}

type mockIterator struct {
	seeks int
}

func (i *mockIterator) Scan() chunkenc.ValueType {
	return chunkenc.ValFloat
}

func (i *mockIterator) FindAtOrAfter(model.Time) chunkenc.ValueType {
	i.seeks++
	return chunkenc.ValFloat
}

func (i *mockIterator) Value() model.SamplePair {
	return model.SamplePair{}
}

func (i *mockIterator) AtHistogram() (int64, *histogram.Histogram) {
	return 0, &histogram.Histogram{}
}

func (i *mockIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	return 0, &histogram.FloatHistogram{}
}

func (i *mockIterator) Timestamp() int64 {
	return 0
}

func (i *mockIterator) Batch(size int, valueType chunkenc.ValueType) chunk.Batch {
	batch := chunk.Batch{
		Length:    chunk.BatchSize,
		ValueType: chunkenc.ValFloat,
	}
	for i := 0; i < chunk.BatchSize; i++ {
		batch.Timestamps[i] = int64(i)
	}
	return batch
}

func (i *mockIterator) Err() error {
	return nil
}
