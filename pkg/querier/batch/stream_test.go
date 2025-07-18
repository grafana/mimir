// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/batch/stream_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package batch

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"unsafe"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/chunk"
	"github.com/grafana/mimir/pkg/util/test"
)

func TestBatchStream_Merge(t *testing.T) {
	for i, tc := range []struct {
		testcase   string
		batches    []chunk.Batch
		newBatches []chunk.Batch
		output     []chunk.Batch
	}{
		{
			testcase: "One float input",
			batches:  []chunk.Batch{mkFloatBatch(0)},
			output:   []chunk.Batch{mkFloatBatch(0)},
		},

		{
			testcase:   "Equal float inputs",
			batches:    []chunk.Batch{mkFloatBatch(0)},
			newBatches: []chunk.Batch{mkFloatBatch(0)},
			output:     []chunk.Batch{mkFloatBatch(0)},
		},

		{
			testcase:   "Non overlapping float inputs",
			batches:    []chunk.Batch{mkFloatBatch(0)},
			newBatches: []chunk.Batch{mkFloatBatch(chunk.BatchSize)},
			output:     []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize)},
		},

		{
			testcase:   "Overlapping and overhanging float inputs 0-11, 12-23 vs 6-17, 24-35",
			batches:    []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize)},
			newBatches: []chunk.Batch{mkFloatBatch(chunk.BatchSize / 2), mkFloatBatch(2 * chunk.BatchSize)},
			output:     []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize), mkFloatBatch(2 * chunk.BatchSize)},
		},

		{
			testcase:   "Overlapping and overhanging float inputs 6-17, 18-29, 30-41 vs 0-11, 12-23, 24-35, 36-47",
			batches:    []chunk.Batch{mkFloatBatch(chunk.BatchSize / 2), mkFloatBatch(3 * chunk.BatchSize / 2), mkFloatBatch(5 * chunk.BatchSize / 2)},
			newBatches: []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize), mkFloatBatch(3 * chunk.BatchSize)},
			output:     []chunk.Batch{mkFloatBatch(0), mkFloatBatch(chunk.BatchSize), mkFloatBatch(2 * chunk.BatchSize), mkFloatBatch(3 * chunk.BatchSize)},
		},

		{
			testcase: "Equal histograms",
			batches:  []chunk.Batch{mkHistogramBatch(0)},
			output:   []chunk.Batch{mkHistogramBatch(0)},
		},

		{
			testcase:   "Histogram preferred over float",
			batches:    []chunk.Batch{mkHistogramBatch(0), mkFloatBatch(chunk.BatchSize)},
			newBatches: []chunk.Batch{mkFloatBatch(0), mkHistogramBatch(chunk.BatchSize)},
			output:     []chunk.Batch{mkHistogramBatch(0), mkHistogramBatch(chunk.BatchSize)},
		},

		{
			testcase:   "Non overlapping histograms and floats",
			batches:    []chunk.Batch{mkFloatBatch(0)},
			newBatches: []chunk.Batch{mkHistogramBatch(chunk.BatchSize)},
			output:     []chunk.Batch{mkFloatBatch(0), mkHistogramBatch(chunk.BatchSize)},
		},

		{
			testcase:   "Histogram splits floats 0-12 float vs histogram at 6",
			batches:    []chunk.Batch{mkFloatBatch(0)},
			newBatches: []chunk.Batch{mkGenericHistogramBatch(chunk.BatchSize/2, 1)},
			output:     []chunk.Batch{mkGenericFloatBatch(0, 6), mkGenericHistogramBatch(chunk.BatchSize/2, 1), mkGenericFloatBatch(7, 5)},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			s := newBatchStream(len(tc.batches), nil, nil)
			s.batches = tc.batches

			for i := range tc.newBatches {
				s.merge(&tc.newBatches[i], chunk.BatchSize, 0)
			}

			require.Equal(t, len(tc.output), len(s.batches))
			for i, batch := range tc.output {
				other := s.batches[i]
				requireBatchEqual(t, batch, other)
				require.Equal(t, 0, batch.Index)
			}
		})
	}
}

func TestBatchStream_Empty(t *testing.T) {
	s := newBatchStream(1, nil, nil)
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
	s := newBatchStream(1, nil, nil)
	b1 := mkHistogramBatch(0)
	b2 := mkHistogramBatch(chunk.BatchSize)
	s.batches = []chunk.Batch{b1, b2}

	require.Len(t, s.batches, 2)
	require.Equal(t, b1, s.batches[0])
	require.Equal(t, b2, s.batches[1])

	s.removeFirst()
	require.Len(t, s.batches, 1)
	require.Equal(t, b2, s.batches[0])
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

func TestDecideTimestampConflict_DeterministicBehavior(t *testing.T) {
	testCases := []struct {
		name               string
		aType              chunkenc.ValueType
		bType              chunkenc.ValueType
		aBatch             *chunk.Batch
		bBatch             *chunk.Batch
		expectedPickA      bool
		expectedPickEither bool
	}{
		{
			name:  "Float vs Float - higher value wins",
			aType: chunkenc.ValFloat,
			bType: chunkenc.ValFloat,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = 10.0
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = 5.0
				return &b
			}(),
			expectedPickA: true,
		},
		{
			name:  "Histogram vs Histogram - higher count wins",
			aType: chunkenc.ValHistogram,
			bType: chunkenc.ValHistogram,
			aBatch: func() *chunk.Batch {
				b := mkGenericHistogramBatch(100, 1)
				h := (*histogram.Histogram)(b.PointerValues[0])
				h.Count = 100
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericHistogramBatch(100, 1)
				h := (*histogram.Histogram)(b.PointerValues[0])
				h.Count = 50
				return &b
			}(),
			expectedPickA: true,
		},
		{
			name:  "FloatHistogram vs FloatHistogram - higher count wins",
			aType: chunkenc.ValFloatHistogram,
			bType: chunkenc.ValFloatHistogram,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatHistogramBatch(100, 1)
				h := (*histogram.FloatHistogram)(b.PointerValues[0])
				h.Count = 75
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatHistogramBatch(100, 1)
				h := (*histogram.FloatHistogram)(b.PointerValues[0])
				h.Count = 90
				return &b
			}(),
			expectedPickA: false,
		},
		{
			name:          "Histogram vs Float - histogram wins",
			aType:         chunkenc.ValHistogram,
			bType:         chunkenc.ValFloat,
			aBatch:        func() *chunk.Batch { b := mkGenericHistogramBatch(100, 1); return &b }(),
			bBatch:        func() *chunk.Batch { b := mkGenericFloatBatch(100, 1); return &b }(),
			expectedPickA: true,
		},
		{
			name:          "FloatHistogram vs Float - float histogram wins",
			aType:         chunkenc.ValFloatHistogram,
			bType:         chunkenc.ValFloat,
			aBatch:        func() *chunk.Batch { b := mkGenericFloatHistogramBatch(100, 1); return &b }(),
			bBatch:        func() *chunk.Batch { b := mkGenericFloatBatch(100, 1); return &b }(),
			expectedPickA: true,
		},
		{
			name:          "FloatHistogram vs Histogram - float histogram wins",
			aType:         chunkenc.ValFloatHistogram,
			bType:         chunkenc.ValHistogram,
			aBatch:        func() *chunk.Batch { b := mkGenericFloatHistogramBatch(100, 1); return &b }(),
			bBatch:        func() *chunk.Batch { b := mkGenericHistogramBatch(100, 1); return &b }(),
			expectedPickA: true,
		},
		{
			name:  "Histogram vs Histogram - equal count, higher sum wins",
			aType: chunkenc.ValHistogram,
			bType: chunkenc.ValHistogram,
			aBatch: func() *chunk.Batch {
				b := mkGenericHistogramBatch(100, 1)
				h := (*histogram.Histogram)(b.PointerValues[0])
				h.Count = 50
				h.Sum = 200.0
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericHistogramBatch(100, 1)
				h := (*histogram.Histogram)(b.PointerValues[0])
				h.Count = 50
				h.Sum = 100.0
				return &b
			}(),
			expectedPickA: true,
		},
		{
			name:  "FloatHistogram vs FloatHistogram - equal count, higher sum wins",
			aType: chunkenc.ValFloatHistogram,
			bType: chunkenc.ValFloatHistogram,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatHistogramBatch(100, 1)
				h := (*histogram.FloatHistogram)(b.PointerValues[0])
				h.Count = 50.0
				h.Sum = 150.0
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatHistogramBatch(100, 1)
				h := (*histogram.FloatHistogram)(b.PointerValues[0])
				h.Count = 50.0
				h.Sum = 180.0
				return &b
			}(),
			expectedPickA: false,
		},
		{
			name:  "Float vs Float - positive infinity vs finite",
			aType: chunkenc.ValFloat,
			bType: chunkenc.ValFloat,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = math.Inf(1)
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = 100.0
				return &b
			}(),
			expectedPickA: true,
		},
		{
			name:  "Float vs Float - finite vs positive infinity",
			aType: chunkenc.ValFloat,
			bType: chunkenc.ValFloat,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = 100.0
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = math.Inf(1)
				return &b
			}(),
			expectedPickA: false,
		},
		{
			name:  "Float vs Float - negative infinity vs finite",
			aType: chunkenc.ValFloat,
			bType: chunkenc.ValFloat,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = math.Inf(-1)
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = -100.0
				return &b
			}(),
			expectedPickA: false,
		},
		{
			name:  "Float vs Float - finite vs negative infinity",
			aType: chunkenc.ValFloat,
			bType: chunkenc.ValFloat,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = -100.0
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = math.Inf(-1)
				return &b
			}(),
			expectedPickA: true,
		},
		{
			name:  "Float vs Float - positive vs negative infinity",
			aType: chunkenc.ValFloat,
			bType: chunkenc.ValFloat,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = math.Inf(1)
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = math.Inf(-1)
				return &b
			}(),
			expectedPickA: true,
		},
		{
			name:  "Float vs Float - very large vs very small",
			aType: chunkenc.ValFloat,
			bType: chunkenc.ValFloat,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = 1e308
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = 1e-308
				return &b
			}(),
			expectedPickA: true,
		},
		{
			name:  "Float vs Float - zero vs negative zero",
			aType: chunkenc.ValFloat,
			bType: chunkenc.ValFloat,
			aBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = 0.0
				return &b
			}(),
			bBatch: func() *chunk.Batch {
				b := mkGenericFloatBatch(100, 1)
				b.Values[0] = math.Copysign(0, -1)
				return &b
			}(),
			// In go -0 and 0 are the same number, so we end up picking the left-hand side always.
			// We don't bother picking one of them because it's complicated, so this test case is mosty for documentation.
			expectedPickEither: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := decideTimestampConflict(tc.aType, tc.bType, tc.aBatch, tc.bBatch)
			require.Equal(t, tc.expectedPickA, result)

			// Test with reversed order to ensure deterministic behavior
			resultReversed := decideTimestampConflict(tc.bType, tc.aType, tc.bBatch, tc.aBatch)
			if !tc.expectedPickEither {
				require.Equal(t, !tc.expectedPickA, resultReversed)
			}
		})
	}
}

func TestBatchStream_MergeWithTimestampConflicts(t *testing.T) {
	t.Run("same timestamp different float values", func(t *testing.T) {
		s := newBatchStream(2, nil, nil)

		// Create batch with value 10.0 at timestamp 100
		leftBatch := mkGenericFloatBatch(100, 1)
		leftBatch.Values[0] = 10.0
		s.batches = []chunk.Batch{leftBatch}

		// Create batch with value 5.0 at same timestamp 100
		rightBatch := mkGenericFloatBatch(100, 1)
		rightBatch.Values[0] = 5.0

		s.merge(&rightBatch, chunk.BatchSize, 0)

		require.Equal(t, 1, len(s.batches))
		require.Equal(t, 1, s.batches[0].Length)
		require.Equal(t, int64(100), s.batches[0].Timestamps[0])
		require.Equal(t, 10.0, s.batches[0].Values[0]) // Higher value should win
	})

	t.Run("multiple timestamp conflicts", func(t *testing.T) {
		s := newBatchStream(3, nil, nil)

		// Create batch with values at timestamps 100, 200, 300
		leftBatch := mkGenericFloatBatch(100, 3)
		leftBatch.Values[0] = 10.0 // ts 100
		leftBatch.Values[1] = 20.0 // ts 101
		leftBatch.Values[2] = 30.0 // ts 102
		s.batches = []chunk.Batch{leftBatch}

		// Create batch with conflicting values at same timestamps
		rightBatch := mkGenericFloatBatch(100, 3)
		rightBatch.Values[0] = 15.0 // ts 100 - higher, should win
		rightBatch.Values[1] = 15.0 // ts 101 - lower, should lose
		rightBatch.Values[2] = 25.0 // ts 102 - lower, should lose

		s.merge(&rightBatch, chunk.BatchSize, 0)

		require.Equal(t, 1, len(s.batches))
		require.Equal(t, 3, s.batches[0].Length)
		require.Equal(t, 15.0, s.batches[0].Values[0]) // Right wins (15.0 > 10.0)
		require.Equal(t, 20.0, s.batches[0].Values[1]) // Left wins (20.0 > 15.0)
		require.Equal(t, 30.0, s.batches[0].Values[2]) // Left wins (30.0 > 25.0)
	})

	t.Run("empty batch merge", func(t *testing.T) {
		s := newBatchStream(2, nil, nil)

		// Start with empty stream
		require.Equal(t, 0, s.len())

		// Merge with empty batch
		emptyBatch := chunk.Batch{ValueType: chunkenc.ValFloat, Length: 0}
		s.merge(&emptyBatch, chunk.BatchSize, 0)
		// Empty batches might still get added, but they should not contain data
		if s.len() > 0 {
			require.Equal(t, 0, s.batches[0].Length)
		}

		// Merge non-empty batch with empty stream
		batch := mkGenericFloatBatch(100, 2)
		s.merge(&batch, chunk.BatchSize, 0)
		require.Equal(t, 1, s.len())
		require.Equal(t, 2, s.batches[0].Length)

		// Merge empty batch with non-empty stream
		s.merge(&emptyBatch, chunk.BatchSize, 0)
		require.Equal(t, 1, s.len())
		require.Equal(t, 2, s.batches[0].Length)
	})

	t.Run("nan handling", func(t *testing.T) {
		s := newBatchStream(2, nil, nil)

		// NaN vs finite value
		leftBatch := mkGenericFloatBatch(100, 1)
		leftBatch.Values[0] = math.NaN()
		s.batches = []chunk.Batch{leftBatch}

		rightBatch := mkGenericFloatBatch(100, 1)
		rightBatch.Values[0] = 5.0
		s.merge(&rightBatch, chunk.BatchSize, 0)

		require.Equal(t, 1, s.len())
		require.Equal(t, 1, s.batches[0].Length)

		// Result should be deterministic, even with NaN
		// NaN > 5.0 should be false, so right side should win
		require.Equal(t, 5.0, s.batches[0].Values[0])
	})

	t.Run("histogram with zero count", func(t *testing.T) {
		s := newBatchStream(2, nil, nil)

		// Create histogram with zero count
		leftBatch := mkGenericHistogramBatch(100, 1)
		leftHist := (*histogram.Histogram)(leftBatch.PointerValues[0])
		leftHist.Count = 0
		s.batches = []chunk.Batch{leftBatch}

		// Create histogram with non-zero count
		rightBatch := mkGenericHistogramBatch(100, 1)
		rightHist := (*histogram.Histogram)(rightBatch.PointerValues[0])
		rightHist.Count = 10
		s.merge(&rightBatch, chunk.BatchSize, 0)

		require.Equal(t, 1, s.len())
		require.Equal(t, 1, s.batches[0].Length)

		// Right histogram should win (higher count)
		resultHist := (*histogram.Histogram)(s.batches[0].PointerValues[0])
		require.Equal(t, uint64(10), resultHist.Count)
	})

	t.Run("float histogram with zero count", func(t *testing.T) {
		s := newBatchStream(2, nil, nil)

		// Create float histogram with zero count
		leftBatch := mkGenericFloatHistogramBatch(100, 1)
		leftHist := (*histogram.FloatHistogram)(leftBatch.PointerValues[0])
		leftHist.Count = 0.0
		s.batches = []chunk.Batch{leftBatch}

		// Create float histogram with non-zero count
		rightBatch := mkGenericFloatHistogramBatch(100, 1)
		rightHist := (*histogram.FloatHistogram)(rightBatch.PointerValues[0])
		rightHist.Count = 10.5
		s.merge(&rightBatch, chunk.BatchSize, 0)

		require.Equal(t, 1, s.len())
		require.Equal(t, 1, s.batches[0].Length)

		// Right histogram should win (higher count)
		resultHist := (*histogram.FloatHistogram)(s.batches[0].PointerValues[0])
		require.Equal(t, 10.5, resultHist.Count)
	})

	t.Run("mixed sample types at boundary timestamps", func(t *testing.T) {
		s := newBatchStream(3, nil, nil)

		// Start with float at timestamp 100
		floatBatch := mkGenericFloatBatch(100, 1)
		s.batches = []chunk.Batch{floatBatch}

		// Merge histogram at same timestamp (histogram should win)
		histBatch := mkGenericHistogramBatch(100, 1)
		s.merge(&histBatch, chunk.BatchSize, 0)

		require.Equal(t, 1, s.len())
		require.Equal(t, 1, s.batches[0].Length)
		require.Equal(t, chunkenc.ValHistogram, s.batches[0].ValueType)

		// Merge float histogram at same timestamp (float histogram should win)
		fhistBatch := mkGenericFloatHistogramBatch(100, 1)
		s.merge(&fhistBatch, chunk.BatchSize, 0)

		require.Equal(t, 1, s.len())
		require.Equal(t, 1, s.batches[0].Length)
		require.Equal(t, chunkenc.ValFloatHistogram, s.batches[0].ValueType)
	})
}

func mkGenericFloatHistogramBatch(from int64, size int) chunk.Batch {
	batch := chunk.Batch{ValueType: chunkenc.ValFloatHistogram}
	for i := 0; i < size; i++ {
		batch.Timestamps[i] = from + int64(i)
		h := test.GenerateTestFloatHistogram(int(from) + i)
		batch.PointerValues[i] = unsafe.Pointer(h)
	}
	batch.Length = size
	return batch
}
