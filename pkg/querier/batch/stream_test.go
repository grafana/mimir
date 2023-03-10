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
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestStream(t *testing.T) {
	for i, tc := range []struct {
		testcase       string
		input1, input2 []chunk.Batch
		output         batchStream
	}{
		{
			testcase: "One float input",
			input1:   []chunk.Batch{mkFloatBatch(0)},
			output:   []chunk.Batch{mkFloatBatch(0)},
		},

		{
			testcase: "Equal float inputs",
			input1:   []chunk.Batch{mkFloatBatch(0)},
			input2:   []chunk.Batch{mkFloatBatch(0)},
			output:   []chunk.Batch{mkFloatBatch(0)},
		},

		{
			testcase: "Non overlapping float inputs",
			input1:   []chunk.Batch{mkFloatBatch(0)},
			input2:   []chunk.Batch{mkFloatBatch(chunk.BatchSize)},
			output:   []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize)},
		},

		{
			testcase: "Overlapping and overhanging float inputs 0-11, 12-23 vs 6-17, 24-35",
			input1:   []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize)},
			input2:   []chunk.Batch{mkFloatBatch(chunk.BatchSize / 2), mkFloatBatch(2 * chunk.BatchSize)},
			output:   []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize), mkFloatBatch(2 * chunk.BatchSize)},
		},

		{
			testcase: "Overlapping and overhanging float inputs 6-17, 18-29, 30-41 vs 0-11, 12-23, 24-35, 36-47",
			input1:   []chunk.Batch{mkFloatBatch(chunk.BatchSize / 2), mkFloatBatch(3 * chunk.BatchSize / 2), mkFloatBatch(5 * chunk.BatchSize / 2)},
			input2:   []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize), mkFloatBatch(3 * chunk.BatchSize)},
			output:   []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize), mkFloatBatch(2 * chunk.BatchSize), mkFloatBatch(3 * chunk.BatchSize)},
		},

		{
			testcase: "Equal histograms",
			input1:   []chunk.Batch{mkHistogramBatch(0)},
			output:   []chunk.Batch{mkHistogramBatch(0)},
		},

		{
			testcase: "Histogram preferred over float",
			input1:   []chunk.Batch{mkHistogramBatch(0), mkFloatBatch(chunk.BatchSize)},
			input2:   []chunk.Batch{mkFloatBatch(0), mkHistogramBatch(chunk.BatchSize)},
			output:   []chunk.Batch{mkHistogramBatch(0), mkHistogramBatch(chunk.BatchSize)},
		},

		{
			testcase: "Non overlapping histograms and floats",
			input1:   []chunk.Batch{mkFloatBatch(0)},
			input2:   []chunk.Batch{mkHistogramBatch(chunk.BatchSize)},
			output:   []chunk.Batch{mkFloatBatch(0), mkHistogramBatch(chunk.BatchSize)},
		},

		{
			testcase: "Histogram splits floats 0-12 float vs histogram at 6",
			input1:   []chunk.Batch{mkFloatBatch(0)},
			input2:   []chunk.Batch{mkGenericHistogramBatch(chunk.BatchSize/2, 1)},
			output:   []chunk.Batch{mkGenericFloatBatch(0, 6), mkGenericHistogramBatch(chunk.BatchSize/2, 1), mkGenericFloatBatch(7, 5)},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			result := make(batchStream, len(tc.input1)+len(tc.input2))
			result = mergeStreams(tc.input1, tc.input2, result, chunk.BatchSize)
			require.Equal(t, len(tc.output), len(result))
			for i, batch := range tc.output {
				other := result[i]
				requireBatchEqual(t, batch, other)
			}
		})
	}
}

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
