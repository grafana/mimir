// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

func TestMaxExpectedSeriesPerResultConstantIsPowerOfTwo(t *testing.T) {
	// Although not strictly required (as the code should handle MaxExpectedSeriesPerResult not being a power of two correctly),
	// it is best that we keep it as one for now.
	require.True(t, pool.IsPowerOfTwo(MaxExpectedSeriesPerResult), "MaxExpectedSeriesPerResult must be a power of two")
}

func TestEnsureFPointSliceCapacityIsPowerOfTwo(t *testing.T) {
	testCases := map[string]struct {
		input            []promql.FPoint
		expectedCapacity int
	}{
		"empty slice": {
			input:            nil,
			expectedCapacity: 0,
		},
		"slice with length 0 and capacity 0": {
			input:            make([]promql.FPoint, 0),
			expectedCapacity: 0,
		},
		"slice with length 0 and capacity 1": {
			input:            make([]promql.FPoint, 0, 1),
			expectedCapacity: 1,
		},
		"slice with length 1 and capacity 1": {
			input:            make([]promql.FPoint, 1),
			expectedCapacity: 1,
		},
		"slice with length 0 and capacity 2": {
			input:            make([]promql.FPoint, 0, 2),
			expectedCapacity: 2,
		},
		"slice with length 1 and capacity 2": {
			input:            make([]promql.FPoint, 1, 2),
			expectedCapacity: 2,
		},
		"slice with length 2 and capacity 2": {
			input:            make([]promql.FPoint, 2),
			expectedCapacity: 2,
		},
		"slice with length 2 and capacity 3": {
			input:            make([]promql.FPoint, 2, 3),
			expectedCapacity: 4,
		},
		"slice with length 3 and capacity 3": {
			input:            make([]promql.FPoint, 3),
			expectedCapacity: 4,
		},
		"slice with length 4 and capacity 4": {
			input:            make([]promql.FPoint, 4),
			expectedCapacity: 4,
		},
		"slice with length 5 and capacity 5": {
			input:            make([]promql.FPoint, 5),
			expectedCapacity: 8,
		},
		"slice with length 6 and capacity 6": {
			input:            make([]promql.FPoint, 6),
			expectedCapacity: 8,
		},
		"slice with length 7 and capacity 7": {
			input:            make([]promql.FPoint, 7),
			expectedCapacity: 8,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			for idx := range testCase.input {
				testCase.input[idx].T = int64(idx)
				testCase.input[idx].F = float64(idx * 10)
			}

			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(testCase.input))*FPointSize, limiter.FPointSlices)
			require.NoError(t, err)

			output, err := EnsureFPointSliceCapacityIsPowerOfTwo(testCase.input, memoryConsumptionTracker)
			require.NoError(t, err)
			require.Len(t, output, len(testCase.input), "output length should be the same as the provided slice")
			require.Equal(t, testCase.expectedCapacity, cap(output))
			require.Equal(t, testCase.input, output, "output should contain the same elements as the provided slice")

			require.Equal(t, uint64(testCase.expectedCapacity)*FPointSize, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

			if cap(testCase.input) == testCase.expectedCapacity {
				require.Equal(t, uint64(testCase.expectedCapacity)*FPointSize, memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes(), "should not allocate a new slice if the provided slice already has a capacity that is a power of two")
			} else {
				require.Equal(t, uint64(testCase.expectedCapacity+cap(testCase.input))*FPointSize, memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes())
			}
		})
	}
}

func TestEnsureHPointSliceCapacityIsPowerOfTwo(t *testing.T) {
	testCases := map[string]struct {
		input            []promql.HPoint
		expectedCapacity int
	}{
		"empty slice": {
			input:            nil,
			expectedCapacity: 0,
		},
		"slice with length 0 and capacity 0": {
			input:            make([]promql.HPoint, 0),
			expectedCapacity: 0,
		},
		"slice with length 0 and capacity 1": {
			input:            make([]promql.HPoint, 0, 1),
			expectedCapacity: 1,
		},
		"slice with length 1 and capacity 1": {
			input:            make([]promql.HPoint, 1),
			expectedCapacity: 1,
		},
		"slice with length 0 and capacity 2": {
			input:            make([]promql.HPoint, 0, 2),
			expectedCapacity: 2,
		},
		"slice with length 1 and capacity 2": {
			input:            make([]promql.HPoint, 1, 2),
			expectedCapacity: 2,
		},
		"slice with length 2 and capacity 2": {
			input:            make([]promql.HPoint, 2),
			expectedCapacity: 2,
		},
		"slice with length 2 and capacity 3": {
			input:            make([]promql.HPoint, 2, 3),
			expectedCapacity: 4,
		},
		"slice with length 3 and capacity 3": {
			input:            make([]promql.HPoint, 3),
			expectedCapacity: 4,
		},
		"slice with length 4 and capacity 4": {
			input:            make([]promql.HPoint, 4),
			expectedCapacity: 4,
		},
		"slice with length 5 and capacity 5": {
			input:            make([]promql.HPoint, 5),
			expectedCapacity: 8,
		},
		"slice with length 6 and capacity 6": {
			input:            make([]promql.HPoint, 6),
			expectedCapacity: 8,
		},
		"slice with length 7 and capacity 7": {
			input:            make([]promql.HPoint, 7),
			expectedCapacity: 8,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			for idx := range testCase.input {
				testCase.input[idx].T = int64(idx)
				testCase.input[idx].H = &histogram.FloatHistogram{Count: float64(idx * 10)}
			}

			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(testCase.input))*HPointSize, limiter.HPointSlices)
			require.NoError(t, err)

			output, err := EnsureHPointSliceCapacityIsPowerOfTwo(testCase.input, memoryConsumptionTracker)
			require.NoError(t, err)
			require.Len(t, output, len(testCase.input), "output length should be the same as the provided slice")
			require.Equal(t, testCase.expectedCapacity, cap(output))
			require.Equal(t, testCase.input, output, "output should contain the same elements as the provided slice")

			require.Equal(t, uint64(testCase.expectedCapacity)*HPointSize, memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

			if cap(testCase.input) == testCase.expectedCapacity {
				require.Equal(t, uint64(testCase.expectedCapacity)*HPointSize, memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes(), "should not allocate a new slice if the provided slice already has a capacity that is a power of two")
			} else {
				require.Equal(t, uint64(testCase.expectedCapacity+cap(testCase.input))*HPointSize, memoryConsumptionTracker.PeakEstimatedMemoryConsumptionBytes())
			}
		})
	}
}
