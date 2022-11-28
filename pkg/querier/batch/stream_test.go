// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/stream_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
)

func TestStream(t *testing.T) {
	for i, tc := range []struct {
		testcase       string
		input1, input2 []chunk.Batch
		output         batchStream
	}{
		{
			testcase: "One float input",
			input1:   []chunk.Batch{mkBatch(0)},
			output:   []chunk.Batch{mkBatch(0)},
		},

		{
			testcase: "Equal float inputs",
			input1:   []chunk.Batch{mkBatch(0)},
			input2:   []chunk.Batch{mkBatch(0)},
			output:   []chunk.Batch{mkBatch(0)},
		},

		{
			testcase: "Non overlapping float inputs",
			input1:   []chunk.Batch{mkBatch(0)},
			input2:   []chunk.Batch{mkBatch(chunk.BatchSize)},
			output:   []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize)},
		},

		{
			testcase: "Overlapping and overhanging float inputs 0-11, 12-23 vs 6-17, 24-35",
			input1:   []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize)},
			input2:   []chunk.Batch{mkBatch(chunk.BatchSize / 2), mkBatch(2 * chunk.BatchSize)},
			output:   []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize), mkBatch(2 * chunk.BatchSize)},
		},

		{
			testcase: "Overlapping and overhanging float inputs 6-17, 18-29, 30-41 vs 0-11, 12-23, 24-35, 36-47",
			input1:   []chunk.Batch{mkBatch(chunk.BatchSize / 2), mkBatch(3 * chunk.BatchSize / 2), mkBatch(5 * chunk.BatchSize / 2)},
			input2:   []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize), mkBatch(3 * chunk.BatchSize)},
			output:   []chunk.Batch{mkBatch(0), mkBatch(chunk.BatchSize), mkBatch(2 * chunk.BatchSize), mkBatch(3 * chunk.BatchSize)},
		},

		{
			testcase: "Equal histograms",
			input1:   []chunk.Batch{mkHistogramBatch(0)},
			output:   []chunk.Batch{mkHistogramBatch(0)},
		},

		{
			testcase: "Histogram preferred over float",
			input1:   []chunk.Batch{mkHistogramBatch(0), mkBatch(chunk.BatchSize)},
			input2:   []chunk.Batch{mkBatch(0), mkHistogramBatch(chunk.BatchSize)},
			output:   []chunk.Batch{mkHistogramBatch(0), mkHistogramBatch(chunk.BatchSize)},
		},

		{
			testcase: "Non overlapping histograms and floats",
			input1:   []chunk.Batch{mkBatch(0)},
			input2:   []chunk.Batch{mkHistogramBatch(chunk.BatchSize)},
			output:   []chunk.Batch{mkBatch(0), mkHistogramBatch(chunk.BatchSize)},
		},

		{
			testcase: "Histogram splits floats 0-12 float vs histogram at 6",
			input1:   []chunk.Batch{mkBatch(0)},
			input2:   []chunk.Batch{mkGenericHistogramBatch(chunk.BatchSize/2, 1)},
			output:   []chunk.Batch{mkGenericFloatBatch(0, 6), mkGenericHistogramBatch(chunk.BatchSize/2, 1), mkGenericFloatBatch(7, 5)},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			result := make(batchStream, len(tc.input1)+len(tc.input2))
			result = mergeStreams(tc.input1, tc.input2, result, chunk.BatchSize)
			require.Equal(t, batchStream(tc.output), result, tc.testcase)
		})
	}
}

func mkBatch(from int64) chunk.Batch {
	return mkGenericFloatBatch(from, chunk.BatchSize)
}

func mkHistogramBatch(from int64) chunk.Batch {
	return mkGenericHistogramBatch(from, chunk.BatchSize)
}

func mkGenericFloatBatch(from int64, size int) chunk.Batch {
	result := createBatch(chunkenc.ValFloat)
	for i := 0; i < size; i++ {
		result.Timestamps[i] = from + int64(i)
		result.SampleValues[i] = float64(from + int64(i))
	}
	result.Length = size
	return result
}

func mkGenericHistogramBatch(from int64, size int) chunk.Batch {
	result := createBatch(chunkenc.ValHistogram)
	for i := 0; i < size; i++ {
		result.Timestamps[i] = from + int64(i)
		result.HistogramValues[i] = &histogram.Histogram{ // yes, this doesn't make much sense with the calculated Count
			Schema:        3,
			Count:         uint64(from + int64(i)),
			Sum:           2.7,
			ZeroThreshold: 0.1,
			ZeroCount:     42,
			PositiveSpans: []histogram.Span{
				{Offset: 0, Length: 4},
				{Offset: 10, Length: 3},
			},
			PositiveBuckets: []int64{1, 2, -2, 1, -1, 0, 0},
		}
	}
	result.Length = size
	return result
}
