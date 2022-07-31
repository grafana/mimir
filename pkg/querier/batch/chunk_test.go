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
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

const (
	step = 1 * time.Second
)

func TestChunkIter(t *testing.T) {
	chunk := mkGenericChunk(t, 0, 100, chunk.PrometheusXorChunk)
	iter := &chunkIterator{}

	iter.reset(chunk)
	testIter(t, 100, newIteratorAdapter(iter))

	iter.reset(chunk)
	testSeek(t, 100, newIteratorAdapter(iter))
}

func mkChunk(t require.TestingT, from model.Time, points int, enc chunk.Encoding) chunk.Chunk {
	metric := labels.Labels{
		{Name: model.MetricNameLabel, Value: "foo"},
	}
	pc, err := chunk.NewForEncoding(enc)
	require.NoError(t, err)
	ts := from
	for i := 0; i < points; i++ {
		npc, err := pc.Add(model.SamplePair{
			Timestamp: ts,
			Value:     model.SampleValue(float64(ts)),
		})
		require.NoError(t, err)
		require.Nil(t, npc)
		ts = ts.Add(step)
	}
	ts = ts.Add(-step) // undo the add that we did just before exiting the loop
	return chunk.NewChunk(metric, pc, from, ts)
}

func mkGenericChunk(t require.TestingT, from model.Time, points int, enc chunk.Encoding) GenericChunk {
	ck := mkChunk(t, from, points, enc)
	return NewGenericChunk(int64(ck.From), int64(ck.Through), ck.Data.NewIterator)
}

func testIter(t require.TestingT, points int, iter chunkenc.Iterator) {
	ets := model.TimeFromUnix(0)
	for i := 0; i < points; i++ {
		require.True(t, iter.Next() == chunkenc.ValFloat, strconv.Itoa(i))
		ts, v := iter.At()
		require.EqualValues(t, int64(ets), ts, strconv.Itoa(i))
		require.EqualValues(t, float64(ets), v, strconv.Itoa(i))
		ets = ets.Add(step)
	}
	require.False(t, iter.Next() == chunkenc.ValFloat)
}

func testSeek(t require.TestingT, points int, iter chunkenc.Iterator) {
	for i := 0; i < points; i += points / 10 {
		ets := int64(i * int(step/time.Millisecond))

		require.True(t, iter.Seek(ets) == chunkenc.ValFloat)
		ts, v := iter.At()
		require.EqualValues(t, ets, ts)
		require.EqualValues(t, v, float64(ets))
		require.NoError(t, iter.Err())

		for j := i + 1; j < i+points/10; j++ {
			ets := int64(j * int(step/time.Millisecond))
			require.True(t, iter.Next() == chunkenc.ValFloat)
			ts, v := iter.At()
			require.EqualValues(t, ets, ts)
			require.EqualValues(t, float64(ets), v)
			require.NoError(t, iter.Err())
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
		require.True(t, c.Seek(int64(i), 1) == chunkenc.ValFloat)
	}
	require.Equal(t, 1, it.seeks)

	require.True(t, c.Seek(int64(chunk.BatchSize), 1) == chunkenc.ValFloat)
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

func (i *mockIterator) Batch(size int, valueType chunkenc.ValueType) chunk.Batch {
	batch := chunk.Batch{
		Length: chunk.BatchSize,
	}
	for i := 0; i < chunk.BatchSize; i++ {
		batch.Timestamps[i] = int64(i)
	}
	return batch
}

func (i *mockIterator) Err() error {
	return nil
}
