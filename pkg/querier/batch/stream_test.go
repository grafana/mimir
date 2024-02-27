// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/stream_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"fmt"
	"strconv"
	"testing"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/zeropool"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/test"
)

func mkFloatBatch(from int64) chunk.Batch {
	return mkGenericFloatBatch(from, chunk.BatchSize)
}

func mkHistogramBatch(from int64) chunk.Batch {
	return mkGenericHistogramBatch(from, chunk.BatchSize)
}

func mkGenericFloatBatch(from int64, size int) chunk.Batch {
	batch := chunk.Batch{ValueType: chunkenc.ValFloat}
	for i := 0; i < size; i++ {
		batch.Timestamps[i] = from + int64(i)
		batch.Values[i] = float64(from + int64(i))
	}
	batch.Length = size
	return batch
}

func mkGenericHistogramBatch(from int64, size int) chunk.Batch {
	batch := chunk.Batch{ValueType: chunkenc.ValHistogram}
	for i := 0; i < size; i++ {
		batch.Timestamps[i] = from + int64(i)
		batch.PointerValues[i] = unsafe.Pointer(test.GenerateTestHistogram(int(from) + i))
	}
	batch.Length = size
	return batch
}

func requireBatchEqual(t *testing.T, b, o chunk.Batch) {
	require.Equal(t, b.ValueType, o.ValueType)
	require.Equal(t, b.Length, o.Length)
	for i := 0; i < b.Length; i++ {
		switch b.ValueType {
		case chunkenc.ValFloat:
			require.Equal(t, b.Values[i], o.Values[i], fmt.Sprintf("at idx %v", i))
		case chunkenc.ValHistogram:
			bh := (*histogram.Histogram)(b.PointerValues[i])
			oh := (*histogram.Histogram)(o.PointerValues[i])
			require.Equal(t, *bh, *oh, fmt.Sprintf("at idx %v", i))
		case chunkenc.ValFloatHistogram:
			bh := (*histogram.FloatHistogram)(b.PointerValues[i])
			oh := (*histogram.FloatHistogram)(o.PointerValues[i])
			require.Equal(t, *bh, *oh, fmt.Sprintf("at idx %v", i))
		}
	}
}

func TestBatchStream_Empty(t *testing.T) {
	hPool := zeropool.New(func() *histogram.Histogram { return &histogram.Histogram{} })
	s := newBatchStream(1, &hPool, nil)
	b1 := mkHistogramBatch(0)
	b2 := mkHistogramBatch(chunk.BatchSize)
	s.batches = []chunk.Batch{b1, b2}

	require.Len(t, s.batches, 2)
	require.Equal(t, b1, s.batches[0])
	require.Equal(t, b2, s.batches[1])

	s.empty()
	require.Len(t, s.batches, 0)
}

func TestBatchStream_RemoveFirst(t *testing.T) {
	hPool := zeropool.New(func() *histogram.Histogram { return &histogram.Histogram{} })
	s := newBatchStream(1, &hPool, nil)
	b1 := mkHistogramBatch(0)
	b2 := mkHistogramBatch(chunk.BatchSize)
	s.batches = []chunk.Batch{b1, b2}

	require.Len(t, s.batches, 2)
	require.Equal(t, b1, s.batches[0])
	require.Equal(t, b2, s.batches[1])

	s.removeFirst()
	require.Len(t, s.batches, 1)
	require.Equal(t, b2, s.batches[0])

	for i := 0; i < b1.Length; i++ {
		h := hPool.Get()
		found := false
		for j := 0; j < b1.Length && !found; j++ {
			currH := (*histogram.Histogram)(b1.PointerValues[j])
			found = currH == h
		}
		require.True(t, found)
	}
}

func TestBatchStream_Merge(t *testing.T) {
	for i, tc := range []struct {
		testcase string
		batches  []chunk.Batch
		batch    chunk.Batch
		output   []chunk.Batch
	}{
		{
			testcase: "One float input",
			batches:  []chunk.Batch{mkFloatBatch(0)},
			output:   []chunk.Batch{mkFloatBatch(0)},
		},

		{
			testcase: "Equal float inputs",
			batches:  []chunk.Batch{mkFloatBatch(0)},
			batch:    mkFloatBatch(0),
			output:   []chunk.Batch{mkFloatBatch(0)},
		},

		{
			testcase: "Non overlapping float inputs",
			batches:  []chunk.Batch{mkFloatBatch(0)},
			batch:    mkFloatBatch(chunk.BatchSize),
			output:   []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize)},
		},

		{
			testcase: "Overlapping and overhanging float inputs 0-11, 12-23 vs 6-17",
			batches:  []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize)},
			batch:    mkFloatBatch(chunk.BatchSize / 2),
			output:   []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize)},
		},

		{
			testcase: "Overlapping and overhanging float inputs 6-17, 18-29, 30-41 vs 36-47",
			batches:  []chunk.Batch{mkFloatBatch(chunk.BatchSize / 2), mkFloatBatch(3 * chunk.BatchSize / 2), mkFloatBatch(5 * chunk.BatchSize / 2)},
			batch:    mkFloatBatch(3 * chunk.BatchSize),
			output:   []chunk.Batch{mkFloatBatch(chunk.BatchSize / 2), mkFloatBatch(3 * chunk.BatchSize / 2), mkFloatBatch(5 * chunk.BatchSize / 2), mkGenericFloatBatch(7*chunk.BatchSize/2, chunk.BatchSize/2)},
		},

		{
			testcase: "Equal histograms",
			batches:  []chunk.Batch{mkHistogramBatch(0)},
			output:   []chunk.Batch{mkHistogramBatch(0)},
		},

		{
			testcase: "Histogram preferred over float",
			batches:  []chunk.Batch{mkHistogramBatch(0), mkFloatBatch(chunk.BatchSize)},
			batch:    mkHistogramBatch(chunk.BatchSize),
			output:   []chunk.Batch{mkHistogramBatch(0), mkHistogramBatch(chunk.BatchSize)},
		},

		{
			testcase: "Non overlapping histograms and floats",
			batches:  []chunk.Batch{mkFloatBatch(0)},
			batch:    mkHistogramBatch(chunk.BatchSize),
			output:   []chunk.Batch{mkFloatBatch(0), mkHistogramBatch(chunk.BatchSize)},
		},

		{
			testcase: "Histogram splits floats 0-12 float vs histogram at 6",
			batches:  []chunk.Batch{mkFloatBatch(0)},
			batch:    mkGenericHistogramBatch(chunk.BatchSize/2, 1),
			output:   []chunk.Batch{mkGenericFloatBatch(0, 6), mkGenericHistogramBatch(chunk.BatchSize/2, 1), mkGenericFloatBatch(7, 5)},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			s := newBatchStream(len(tc.batches), nil, nil)
			s.batches = tc.batches

			s.merge(tc.batch, chunk.BatchSize)
			require.Equal(t, len(tc.output), len(s.batches))
			for i, batch := range tc.output {
				other := s.batches[i]
				requireBatchEqual(t, batch, other)
				require.Equal(t, 0, batch.Index)
			}
		})
	}
}
