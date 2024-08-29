// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/chunk_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/zeropool"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/test"
)

const (
	step = 1 * time.Second
)

func TestChunkIter(t *testing.T) {
	for _, encoding := range []chunk.Encoding{chunk.PrometheusXorChunk, chunk.PrometheusHistogramChunk, chunk.PrometheusFloatHistogramChunk} {
		t.Run(encoding.String(), func(t *testing.T) { testChunkIter(t, encoding) })
	}
}

func testChunkIter(t *testing.T, encoding chunk.Encoding) {
	chunk := mkGenericChunk(t, 0, 100, encoding)
	iter := &chunkIterator{}

	iter.reset(chunk)
	testIter(t, 100, newIteratorAdapter(nil, iter, labels.EmptyLabels()), encoding)

	iter.reset(chunk)
	testSeek(t, 100, newIteratorAdapter(nil, iter, labels.EmptyLabels()), encoding)
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
			return pc.AddHistogram(int64(ts), test.GenerateTestHistogram(int(ts)))
		}
	case chunk.PrometheusFloatHistogramChunk:
		addPair = func(pc chunk.EncodedChunk, ts model.Time) (chunk.EncodedChunk, error) {
			return pc.AddFloatHistogram(int64(ts), test.GenerateTestFloatHistogram(int(ts)))
		}
	default:
		t.Errorf("mkChunk - unhandled encoding: %v", encoding)
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
	nextExpectedTS := model.TimeFromUnix(0)
	var assertPoint func(i int)
	switch encoding {
	case chunk.PrometheusXorChunk:
		assertPoint = func(i int) {
			require.Equal(t, chunkenc.ValFloat, iter.Next(), strconv.Itoa(i))
			ts, v := iter.At()
			require.EqualValues(t, int64(nextExpectedTS), ts, strconv.Itoa(i))
			require.EqualValues(t, float64(nextExpectedTS), v, strconv.Itoa(i))
			nextExpectedTS = nextExpectedTS.Add(step)
		}
	case chunk.PrometheusHistogramChunk:
		assertPoint = func(i int) {
			require.Equal(t, chunkenc.ValHistogram, iter.Next(), strconv.Itoa(i))
			ts, h := iter.AtHistogram(nil)
			require.EqualValues(t, int64(nextExpectedTS), ts, strconv.Itoa(i))
			test.RequireHistogramEqual(t, test.GenerateTestHistogram(int(nextExpectedTS)), h, strconv.Itoa(i))
			nextExpectedTS = nextExpectedTS.Add(step)
		}
	case chunk.PrometheusFloatHistogramChunk:
		assertPoint = func(i int) {
			require.Equal(t, chunkenc.ValFloatHistogram, iter.Next(), strconv.Itoa(i))
			ts, fh := iter.AtFloatHistogram(nil)
			require.EqualValues(t, int64(nextExpectedTS), ts, strconv.Itoa(i))
			test.RequireFloatHistogramEqual(t, test.GenerateTestFloatHistogram(int(nextExpectedTS)), fh, strconv.Itoa(i))
			nextExpectedTS = nextExpectedTS.Add(step)
		}
	default:
		t.Errorf("testIter - unhandled encoding: %v", encoding)
	}
	for i := 0; i < points; i++ {
		assertPoint(i)
	}
	require.Equal(t, chunkenc.ValNone, iter.Next())
}

func testSeek(t require.TestingT, points int, iter chunkenc.Iterator, encoding chunk.Encoding) {
	var assertPoint func(expectedTS int64, valType chunkenc.ValueType)
	switch encoding {
	case chunk.PrometheusXorChunk:
		assertPoint = func(expectedTS int64, valType chunkenc.ValueType) {
			require.Equal(t, chunkenc.ValFloat, valType)
			ts, v := iter.At()
			require.EqualValues(t, expectedTS, ts)
			require.EqualValues(t, float64(expectedTS), v)
			require.NoError(t, iter.Err())
		}
	case chunk.PrometheusHistogramChunk:
		assertPoint = func(expectedTS int64, valType chunkenc.ValueType) {
			require.Equal(t, chunkenc.ValHistogram, valType)
			ts, h := iter.AtHistogram(nil)
			require.EqualValues(t, expectedTS, ts)
			test.RequireHistogramEqual(t, test.GenerateTestHistogram(int(expectedTS)), h)
			require.NoError(t, iter.Err())
		}
	case chunk.PrometheusFloatHistogramChunk:
		assertPoint = func(expectedTS int64, valType chunkenc.ValueType) {
			require.Equal(t, chunkenc.ValFloatHistogram, valType)
			ts, fh := iter.AtFloatHistogram(nil)
			require.EqualValues(t, expectedTS, ts)
			test.RequireFloatHistogramEqual(t, test.GenerateTestFloatHistogram(int(expectedTS)), fh)
			require.NoError(t, iter.Err())
		}
	default:
		t.Errorf("testSeek - unhandled encoding: %v", encoding)
	}

	for i := 0; i < points; i += points / 10 {
		expectedTS := int64(i * int(step/time.Millisecond))
		assertPoint(expectedTS, iter.Seek(expectedTS))

		for j := i + 1; j < i+points/10 && j < points; j++ {
			expectedTS := int64(j * int(step/time.Millisecond))
			assertPoint(expectedTS, iter.Next())
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

func (i *mockIterator) AtHistogram(*histogram.Histogram) (int64, *histogram.Histogram) {
	return 0, &histogram.Histogram{}
}

func (i *mockIterator) AtFloatHistogram(*histogram.FloatHistogram) (int64, *histogram.FloatHistogram) {
	return 0, &histogram.FloatHistogram{}
}

func (i *mockIterator) Timestamp() int64 {
	return 0
}

func (i *mockIterator) Batch(_ int, valueType chunkenc.ValueType, _ *zeropool.Pool[*histogram.Histogram], _ *zeropool.Pool[*histogram.FloatHistogram]) chunk.Batch {
	batch := chunk.Batch{
		Length:    chunk.BatchSize,
		ValueType: valueType,
	}
	for i := 0; i < chunk.BatchSize; i++ {
		batch.Timestamps[i] = int64(i)
	}
	return batch
}

func (i *mockIterator) Err() error {
	return nil
}

func TestChunkIterator_SeekBeforeCurrentBatch(t *testing.T) {
	chunkTimestamps := []int64{50, 60, 70, 80, 90, 100}

	ch := chunkenc.NewXORChunk()
	app, err := ch.Appender()
	require.NoError(t, err)
	for _, ts := range chunkTimestamps {
		app.Append(ts, float64(ts))
	}

	genericChunk := NewGenericChunk(chunkTimestamps[0], chunkTimestamps[len(chunkTimestamps)-1], func(reuse chunk.Iterator) chunk.Iterator {
		chk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
		require.NoError(t, err)
		require.NoError(t, chk.UnmarshalFromBuf(ch.Bytes()))

		// We should never need to reset this iterator, as the Seek call below should be satisfiable by the initial batch.
		return &chunkIteratorThatForbidsFindAtOrAfter{chk.NewIterator(reuse)}
	})

	it := &chunkIterator{}
	it.reset(genericChunk)

	require.Equal(t, chunkenc.ValFloat, it.Next(2))
	require.Equal(t, int64(50), it.AtTime())

	require.Equal(t, chunkenc.ValFloat, it.Seek(45, 1))
	require.Equal(t, int64(50), it.AtTime())
}

type chunkIteratorThatForbidsFindAtOrAfter struct {
	chunk.Iterator
}

func (it *chunkIteratorThatForbidsFindAtOrAfter) FindAtOrAfter(model.Time) chunkenc.ValueType {
	panic("FindAtOrAfter should never be called")
}
