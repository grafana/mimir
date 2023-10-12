// SPDX-License-Identifier: AGPL-3.0-only

package iterators

import (
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestChunkIteratorAtFloatHistogramAfterAtHistogram(t *testing.T) {
	c := mkChunk(t, 0, 50, 1*time.Millisecond, chunk.PrometheusHistogramChunk)
	it := chunkIterator{Chunk: c, it: c.Data.NewIterator(nil)}
	require.Equal(t, chunkenc.ValHistogram, it.Next())
	// populate cache with histogram
	_, h := it.AtHistogram()
	require.NotNil(t, h)
	// read float histogram
	_, fh := it.AtFloatHistogram()
	require.NotNil(t, fh)
}

func TestChunkIteratorCaching(t *testing.T) {
	testCases := map[string]struct {
		encoding     chunk.Encoding
		expectedType chunkenc.ValueType
		verifySample func(t *testing.T, i int64, iter *chunkIterator)
	}{
		"float chunk": {
			encoding:     chunk.PrometheusXorChunk,
			expectedType: chunkenc.ValFloat,
			verifySample: func(t *testing.T, i int64, iter *chunkIterator) {
				ts, v := iter.At()
				require.Equal(t, i, ts)
				require.Equal(t, float64(i), v)
			},
		},
		"histogram chunk": {
			encoding:     chunk.PrometheusHistogramChunk,
			expectedType: chunkenc.ValHistogram,
			verifySample: func(t *testing.T, i int64, iter *chunkIterator) {
				ts, h := iter.AtHistogram()
				require.Equal(t, i, ts)
				test.RequireHistogramEqual(t, test.GenerateTestHistogram(int(i)), h)
				// auto convert
				ts2, fh := iter.AtFloatHistogram()
				require.Equal(t, i, ts2)
				test.RequireFloatHistogramEqual(t, test.GenerateTestHistogram(int(i)).ToFloat(), fh)
			},
		},
		"float histogram chunk": {
			encoding:     chunk.PrometheusFloatHistogramChunk,
			expectedType: chunkenc.ValFloatHistogram,
			verifySample: func(t *testing.T, i int64, iter *chunkIterator) {
				ts, fh := iter.AtFloatHistogram()
				require.Equal(t, i, ts)
				test.RequireFloatHistogramEqual(t, test.GenerateTestFloatHistogram(int(i)), fh)
			},
		},
	}
	for name, data := range testCases {
		t.Run(name, func(t *testing.T) {
			callCount := 0
			c := mkChunk(t, 0, 50, 1*time.Millisecond, data.encoding)
			it := &chunkIterator{Chunk: c, it: countAtChunkIterator{it: c.Data.NewIterator(nil), callcount: &callCount}}
			require.Equal(t, data.expectedType, it.Next())
			require.Equal(t, 0, callCount)
			data.verifySample(t, 0, it)
			require.Equal(t, 1, callCount) // first At* calls underlying iterator
			data.verifySample(t, 0, it)
			require.Equal(t, 1, callCount) // second At* uses cache
			require.Equal(t, int64(0), it.AtT())
			require.Equal(t, 1, callCount) // AtT after At* uses cache

			require.Equal(t, data.expectedType, it.Next())
			require.Equal(t, int64(1), it.AtT())
			require.Equal(t, 2, callCount) // AtT after Next calls underlying iterator
			require.Equal(t, int64(1), it.AtT())
			require.Equal(t, 2, callCount)
			data.verifySample(t, 1, it)
			require.Equal(t, 2, callCount) // At* after AtT uses cache

			require.Equal(t, data.expectedType, it.Seek(20))
			require.Equal(t, int64(20), it.AtT())
			require.Equal(t, 3, callCount) // AtT after Seek calls underlying iterator
			require.Equal(t, int64(20), it.AtT())
			require.Equal(t, 3, callCount)

			require.Equal(t, data.expectedType, it.Seek(30))
			data.verifySample(t, 30, it)
			require.Equal(t, 4, callCount) // At* after Seek calls underlying iterator
			data.verifySample(t, 30, it)
			require.Equal(t, 4, callCount)
		})
	}
}

type countAtChunkIterator struct {
	it        chunk.Iterator
	callcount *int
}

func (i countAtChunkIterator) Value() model.SamplePair {
	*i.callcount++
	return i.it.Value()
}

func (i countAtChunkIterator) AtHistogram() (int64, *histogram.Histogram) {
	*i.callcount++
	return i.it.AtHistogram()
}

func (i countAtChunkIterator) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	*i.callcount++
	return i.it.AtFloatHistogram()
}

func (i countAtChunkIterator) Batch(size int, valueType chunkenc.ValueType) chunk.Batch {
	return i.it.Batch(size, valueType)
}

func (i countAtChunkIterator) Err() error {
	return i.it.Err()
}

func (i countAtChunkIterator) FindAtOrAfter(t model.Time) chunkenc.ValueType {
	return i.it.FindAtOrAfter(t)
}

func (i countAtChunkIterator) Scan() chunkenc.ValueType {
	return i.it.Scan()
}

func (i countAtChunkIterator) Timestamp() int64 {
	*i.callcount++
	return i.it.Timestamp()
}

func TestChunkIterator_ScanShortcut(t *testing.T) {
	encChk, err := chunk.NewForEncoding(chunk.PrometheusXorChunk)
	require.NoError(t, err)

	for i := 0; i < 120; i++ {
		overflow, err := encChk.Add(model.SamplePair{
			Timestamp: model.Time(i),
			Value:     model.SampleValue(i),
		})
		require.NoError(t, err)
		require.Nil(t, overflow)
	}

	chk := chunk.NewChunk(labels.FromStrings(labels.MetricName, "foobar"), encChk, 0, 119)

	it := chunkIterator{Chunk: chk, it: chk.Data.NewIterator(nil)}

	// Seek past what's in the chunk; triggers the shortcut in seek and returns chunkenc.ValNone.
	valType := it.Seek(120)
	require.Equal(t, chunkenc.ValNone, valType)

	// The iterator is exhausted so it returns chunkenc.ValNone.
	valType = it.Next()
	require.Equal(t, chunkenc.ValNone, valType)

	// Likewise for seeking.
	valType = it.Seek(100)
	require.Equal(t, chunkenc.ValNone, valType)
}
