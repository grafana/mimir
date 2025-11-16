// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// Most of the functionality of the binary operation operator is tested through the test scripts in
// pkg/streamingpromql/testdata.
//
// The merging behaviour has many edge cases, so it's easier to test it directly from Go.
//
// Most of the edge cases are already covered by TestMergeSeries, so we focus on the logic
// unique to OneToOneVectorVectorBinaryOperation: converting conflicts to user-friendly error messages.
func TestOneToOneVectorVectorBinaryOperation_SeriesMerging(t *testing.T) {
	testCases := map[string]struct {
		input                []types.InstantVectorSeriesData
		sourceSeriesIndices  []int
		sourceSeriesMetadata []types.SeriesMetadata

		expectedOutput types.InstantVectorSeriesData
		expectedError  string
	}{
		"no input series": {
			input:          []types.InstantVectorSeriesData{},
			expectedOutput: types.InstantVectorSeriesData{},
		},
		"single float only input series": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 3, F: 30},
					},
				},
			},
			sourceSeriesIndices: []int{0},
			sourceSeriesMetadata: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "a")},
			},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
				},
			},
		},
		"single histogram only input series": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
					},
				},
			},
			sourceSeriesIndices: []int{0},
			sourceSeriesMetadata: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "a")},
			},
			expectedOutput: types.InstantVectorSeriesData{
				Histograms: []promql.HPoint{
					{T: 1, H: &histogram.FloatHistogram{Count: 10, Sum: 100}},
					{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
				},
			},
		},
		"two float only input series with no overlap, series in time order": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 3, F: 30},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 4, F: 40},
						{T: 5, F: 50},
						{T: 6, F: 60},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			sourceSeriesMetadata: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "a")},
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "b")},
			},
			expectedOutput: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
					{T: 4, F: 40},
					{T: 5, F: 50},
					{T: 6, F: 60},
				},
			},
		},
		"float only input series with conflict": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
						{T: 3, F: 30},
						{T: 5, F: 50},
					},
				},
			},
			sourceSeriesIndices: []int{3, 2},
			sourceSeriesMetadata: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "a")},
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "b")},
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "c")},
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "d")},
			},
			expectedError: `found duplicate series for the match group {env="test"} on the right side of the operation at timestamp 1970-01-01T00:00:00.002Z: {__name__="right_side", env="test", pod="d"} and {__name__="right_side", env="test", pod="c"}`,
		},
		"histogram only input series with conflict": {
			input: []types.InstantVectorSeriesData{
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 20, Sum: 200}},
						{T: 3, H: &histogram.FloatHistogram{Count: 30, Sum: 300}},
						{T: 5, H: &histogram.FloatHistogram{Count: 50, Sum: 500}},
					},
				},
			},
			sourceSeriesIndices: []int{3, 2},
			sourceSeriesMetadata: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "a")},
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "b")},
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "c")},
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "d")},
			},
			expectedError: `found duplicate series for the match group {env="test"} on the right side of the operation at timestamp 1970-01-01T00:00:00.002Z: {__name__="right_side", env="test", pod="d"} and {__name__="right_side", env="test", pod="c"}`,
		},
		"mixed float and histogram input series, series in conflict on different types": {
			input: []types.InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
					},
				},
				{
					Histograms: []promql.HPoint{
						{T: 2, H: &histogram.FloatHistogram{Count: 2, Sum: 2}},
					},
				},
			},
			sourceSeriesIndices: []int{0, 1},
			sourceSeriesMetadata: []types.SeriesMetadata{
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "a")},
				{Labels: labels.FromStrings("__name__", "right_side", "env", "test", "pod", "b")},
			},
			expectedError: `found both float and histogram samples for the match group {env="test"} on the right side of the operation at timestamp 1970-01-01T00:00:00.002Z`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
			o := &OneToOneVectorVectorBinaryOperation{
				// Simulate an expression with "on (env)".
				// This is used to generate error messages.
				VectorMatching: parser.VectorMatching{
					On:             true,
					MatchingLabels: []string{"env"},
				},
				MemoryConsumptionTracker: memoryConsumptionTracker,
			}
			for _, s := range testCase.input {
				// Count the memory for the given floats + histograms
				require.NoError(t, memoryConsumptionTracker.IncreaseMemoryConsumption(types.FPointSize*uint64(len(s.Floats)), limiter.FPointSlices))
				require.NoError(t, memoryConsumptionTracker.IncreaseMemoryConsumption(types.HPointSize*uint64(len(s.Histograms)), limiter.HPointSlices))
			}

			result, err := o.mergeSingleSide(testCase.input, testCase.sourceSeriesIndices, testCase.sourceSeriesMetadata, "right")

			if testCase.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedOutput, result)
			} else {
				require.EqualError(t, err, testCase.expectedError)
			}
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_Sorting(t *testing.T) {
	testCases := map[string]struct {
		series []*oneToOneBinaryOperationOutputSeries

		expectedOrderFavouringLeftSide  []int
		expectedOrderFavouringRightSide []int
	}{
		"no output series": {
			series: []*oneToOneBinaryOperationOutputSeries{},

			expectedOrderFavouringLeftSide:  []int{},
			expectedOrderFavouringRightSide: []int{},
		},
		"single output series": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{4},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0},
			expectedOrderFavouringRightSide: []int{0},
		},
		"two output series, both with one input series, read from both sides in same order and already sorted correctly": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 1},
			expectedOrderFavouringRightSide: []int{0, 1},
		},
		"two output series, both with one input series, read from both sides in same order but sorted incorrectly": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: []int{1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{1, 0},
			expectedOrderFavouringRightSide: []int{1, 0},
		},
		"two output series, both with one input series, read from both sides in different order": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 1},
			expectedOrderFavouringRightSide: []int{1, 0},
		},
		"two output series, both with multiple input series": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{1, 2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{0, 3}},
				},
				{
					leftSeriesIndices: []int{0, 3},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1, 2}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 1},
			expectedOrderFavouringRightSide: []int{1, 0},
		},
		"multiple output series, both with one input series, read from both sides in same order and already sorted correctly": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: []int{3},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{3}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 1, 2},
			expectedOrderFavouringRightSide: []int{0, 1, 2},
		},
		"multiple output series, both with one input series, read from both sides in same order but sorted incorrectly": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: []int{3},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{3}},
				},
				{
					leftSeriesIndices: []int{1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{2, 0, 1},
			expectedOrderFavouringRightSide: []int{2, 0, 1},
		},
		"multiple output series, both with one input series, read from both sides in different order": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: []int{3},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{3}},
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 2, 1},
			expectedOrderFavouringRightSide: []int{2, 0, 1},
		},
		"multiple output series, with multiple input series each": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{4, 5, 10},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2, 20}},
				},
				{
					leftSeriesIndices: []int{2, 4, 15},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{3, 5, 50}},
				},
				{
					leftSeriesIndices: []int{3, 1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1, 40}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{2, 0, 1},
			expectedOrderFavouringRightSide: []int{0, 2, 1},
		},
		"multiple output series which depend on the same input series": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: []int{1},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
				},
			},

			expectedOrderFavouringLeftSide:  []int{1, 0, 3, 2},
			expectedOrderFavouringRightSide: []int{1, 3, 0, 2},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.Len(t, testCase.expectedOrderFavouringLeftSide, len(testCase.series), "invalid test case: should have same number of input and output series for order favouring left side")
			require.Len(t, testCase.expectedOrderFavouringRightSide, len(testCase.series), "invalid test case: should have same number of input and output series for order favouring right side")

			metadata := make([]types.SeriesMetadata, len(testCase.series))
			for i := range testCase.series {
				metadata[i] = types.SeriesMetadata{Labels: labels.FromStrings("series", strconv.Itoa(i))}
			}

			test := func(t *testing.T, series []*oneToOneBinaryOperationOutputSeries, metadata []types.SeriesMetadata, sorter sort.Interface, expectedOrder []int) {
				expectedSeriesOrder := make([]*oneToOneBinaryOperationOutputSeries, len(series))
				expectedMetadataOrder := make([]types.SeriesMetadata, len(metadata))

				for outputIndex, inputIndex := range expectedOrder {
					expectedSeriesOrder[outputIndex] = series[inputIndex]
					expectedMetadataOrder[outputIndex] = metadata[inputIndex]
				}

				sort.Sort(sorter)

				require.Equal(t, expectedSeriesOrder, series)
				require.Equal(t, expectedMetadataOrder, metadata)
			}

			t.Run("sorting favouring left side", func(t *testing.T) {
				series := slices.Clone(testCase.series)
				metadata := slices.Clone(metadata)
				sorter := newFavourLeftSideSorter(metadata, series)
				test(t, series, metadata, sorter, testCase.expectedOrderFavouringLeftSide)
			})

			t.Run("sorting favouring right side", func(t *testing.T) {
				series := slices.Clone(testCase.series)
				metadata := slices.Clone(metadata)
				sorter := newFavourRightSideSorter(metadata, series)
				test(t, series, metadata, sorter, testCase.expectedOrderFavouringRightSide)
			})
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_ClosesInnerOperatorsAsSoonAsPossible(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeries                        []labels.Labels
		expectLeftSideClosedAfterOutputSeriesIndex  int
		expectRightSideClosedAfterOutputSeriesIndex int
	}{
		"no series on left": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries:                        []labels.Labels{},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"no series on right": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeries:                        []labels.Labels{},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"reach end of both sides at the same time": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "2", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: 1,
		},
		"no more matches with unmatched series still to read on both sides": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  0,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"no more matches with unmatched series still to read on left side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  0,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"no more matches with unmatched series still to read on right side": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  0,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"no matches": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "3", "series", "right-1"),
				labels.FromStrings("group", "4", "series", "right-2"),
				labels.FromStrings("group", "5", "series", "right-3"),
			},

			expectedOutputSeries:                        []labels.Labels{},
			expectLeftSideClosedAfterOutputSeriesIndex:  -1,
			expectRightSideClosedAfterOutputSeriesIndex: -1,
		},
		"right side exhausted before left": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "2", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: 0,
		},
		"left side exhausted before right": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "3", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "2", "series", "right-1"),
				labels.FromStrings("group", "3", "series", "right-3"),
				labels.FromStrings("group", "3", "series", "right-4"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1"),
				labels.FromStrings("group", "2"),
				labels.FromStrings("group", "3"),
			},
			expectLeftSideClosedAfterOutputSeriesIndex:  1,
			expectRightSideClosedAfterOutputSeriesIndex: 2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.expectLeftSideClosedAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectLeftSideClosedAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectLeftSideClosedAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			if testCase.expectRightSideClosedAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectRightSideClosedAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectRightSideClosedAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}}
			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.ADD, false, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if len(testCase.expectedOutputSeries) == 0 {
				require.Empty(t, outputSeries)
			} else {
				require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			}

			if testCase.expectLeftSideClosedAfterOutputSeriesIndex == -1 {
				require.True(t, left.Finalized, "left side should be finalized after SeriesMetadata, but it is not")
				require.True(t, left.Closed, "left side should be closed after SeriesMetadata, but it is not")
			} else {
				require.False(t, left.Finalized, "left side should not be finalized after SeriesMetadata, but it is")
				require.False(t, left.Closed, "left side should not be closed after SeriesMetadata, but it is")
			}

			if testCase.expectRightSideClosedAfterOutputSeriesIndex == -1 {
				require.True(t, right.Finalized, "right side should be finalized after SeriesMetadata, but it is not")
				require.True(t, right.Closed, "right side should be closed after SeriesMetadata, but it is not")
			} else {
				require.False(t, right.Finalized, "right side should not be finalized after SeriesMetadata, but it is")
				require.False(t, right.Closed, "right side should not be closed after SeriesMetadata, but it is")
			}

			for outputSeriesIdx := range outputSeries {
				_, err := o.NextSeries(ctx)
				require.NoErrorf(t, err, "got error while reading series at index %v", outputSeriesIdx)

				if outputSeriesIdx >= testCase.expectLeftSideClosedAfterOutputSeriesIndex {
					require.Truef(t, left.Finalized, "left side should be finalized after output series at index %v, but it is not", outputSeriesIdx)
					require.Truef(t, left.Closed, "left side should be closed after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, left.Finalized, "left side should not be finalized after output series at index %v, but it is", outputSeriesIdx)
					require.Falsef(t, left.Closed, "left side should not be closed after output series at index %v, but it is", outputSeriesIdx)
				}

				if outputSeriesIdx >= testCase.expectRightSideClosedAfterOutputSeriesIndex {
					require.Truef(t, right.Finalized, "right side should be finalized after output series at index %v, but it is not", outputSeriesIdx)
					require.Truef(t, right.Closed, "right side should be closed after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, right.Finalized, "right side should not be finalized after output series at index %v, but it is", outputSeriesIdx)
					require.Falsef(t, right.Closed, "right side should not be closed after output series at index %v, but it is", outputSeriesIdx)
				}
			}

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err)

			o.Close()
			// Make sure we've returned everything to their pools.
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_ReleasesIntermediateStateIfClosedEarly(t *testing.T) {
	for _, closeAfterFirstSeries := range []bool{true, false} {
		t.Run(fmt.Sprintf("close after first series=%v", closeAfterFirstSeries), func(t *testing.T) {
			leftSeries := []labels.Labels{
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_1"),
				labels.FromStrings("group", "1", model.MetricNameLabel, "left_2"),
			}

			rightSeries := []labels.Labels{
				labels.FromStrings("group", "1"),
			}

			step1 := timestamp.Time(0)
			step2 := step1.Add(time.Minute)
			timeRange := types.NewRangeQueryTimeRange(step1, step2, time.Minute)

			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

			var err error
			left1Data := types.InstantVectorSeriesData{}
			left1Data.Floats, err = types.FPointSlicePool.Get(1, memoryConsumptionTracker)
			require.NoError(t, err)
			left1Data.Floats = append(left1Data.Floats, promql.FPoint{T: timestamp.FromTime(step1), F: 10})

			left2Data := types.InstantVectorSeriesData{} // This series doesn't need any data.

			rightData := types.InstantVectorSeriesData{}
			rightData.Floats, err = types.FPointSlicePool.Get(2, memoryConsumptionTracker)
			require.NoError(t, err)
			rightData.Floats = append(rightData.Floats, promql.FPoint{T: timestamp.FromTime(step1), F: 5})
			rightData.Floats = append(rightData.Floats, promql.FPoint{T: timestamp.FromTime(step2), F: 7})

			left := &operators.TestOperator{Series: leftSeries, Data: []types.InstantVectorSeriesData{left1Data, left2Data}, MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: rightSeries, Data: []types.InstantVectorSeriesData{rightData}, MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: false}
			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.LTE, false, memoryConsumptionTracker, annotations.New(), posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			metadata, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			require.Equal(t, testutils.LabelsToSeriesMetadata(leftSeries), metadata)
			types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)

			// Read the first series.
			d, err := o.NextSeries(ctx)
			require.NoError(t, err)
			types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

			if !closeAfterFirstSeries {
				d, err = o.NextSeries(ctx)
				require.NoError(t, err)
				types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
			}

			// Close the operator and verify that the intermediate state is released.
			o.Close()
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
		})
	}
}
