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
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(context.Background())
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

func TestOneToOneVectorVectorBinaryOperation_CallsFinishedReadingOnInnerOperatorsAsSoonAsPossible(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeries                                       []labels.Labels
		expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex  int
		expectRightSideFinishedReadingCalledAfterOutputSeriesIndex int
	}{
		"no series on left": {
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{},
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  -1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: -1,
		},
		"no series on right": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeries: []labels.Labels{},
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  -1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: -1,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 1,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  0,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 0,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  0,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 0,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  0,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 0,
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

			expectedOutputSeries: []labels.Labels{},
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  -1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: -1,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 0,
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
			expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex:  1,
			expectRightSideFinishedReadingCalledAfterOutputSeriesIndex: 2,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			if testCase.expectRightSideFinishedReadingCalledAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectRightSideFinishedReadingCalledAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectRightSideFinishedReadingCalledAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}}
			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.ADD, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if len(testCase.expectedOutputSeries) == 0 {
				require.Empty(t, outputSeries)
			} else {
				require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			}

			if testCase.expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex == -1 {
				require.True(t, left.FinishedReadingCalled, "left side should have FinishedReading called after SeriesMetadata, but it is not")
			} else {
				require.False(t, left.FinishedReadingCalled, "left side should not have FinishedReading called after SeriesMetadata, but it is")
			}

			if testCase.expectRightSideFinishedReadingCalledAfterOutputSeriesIndex == -1 {
				require.True(t, right.FinishedReadingCalled, "right side should have FinishedReading called after SeriesMetadata, but it is not")
			} else {
				require.False(t, right.FinishedReadingCalled, "right side should not have FinishedReading called after SeriesMetadata, but it is")
			}

			require.False(t, left.Closed, "left side should not be closed after SeriesMetadata, but it is")
			require.False(t, right.Closed, "right side should not be closed after SeriesMetadata, but it is")

			for outputSeriesIdx := range outputSeries {
				_, err := o.NextSeries(ctx)
				require.NoErrorf(t, err, "got error while reading series at index %v", outputSeriesIdx)

				if outputSeriesIdx >= testCase.expectLeftSideFinishedReadingCalledAfterOutputSeriesIndex {
					require.Truef(t, left.FinishedReadingCalled, "left side should have FinishedReading called after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, left.FinishedReadingCalled, "left side should not have FinishedReading called after output series at index %v, but it is", outputSeriesIdx)
				}

				if outputSeriesIdx >= testCase.expectRightSideFinishedReadingCalledAfterOutputSeriesIndex {
					require.Truef(t, right.FinishedReadingCalled, "right side should have FinishedReading called after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, right.FinishedReadingCalled, "right side should not have FinishedReading called after output series at index %v, but it is", outputSeriesIdx)
				}
			}

			require.False(t, left.Closed, "left side should not be closed after reading all output series, but it is")
			require.False(t, right.Closed, "right side should not be closed after reading all output series, but it is")

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err)

			require.NoError(t, o.FinishedReading(ctx))
			require.True(t, left.FinishedReadingCalled, "left side should have FinishedReading called after calling FinishedReading, but it is not")
			require.True(t, right.FinishedReadingCalled, "right side should have FinishedReading called after calling FinishedReading, but it is not")
			// Make sure we've returned everything to their pools.
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

			o.Close()
			require.True(t, left.Closed, "left side should be closed after closing operator, but it isn't")
			require.True(t, right.Closed, "right side should be closed after closing operator, but it isn't")
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_FillModifiers_OutputSeries(t *testing.T) {
	// Tests the output series and labels produced with fill modifiers set, including the asymmetry
	// between fill directions: a filled-right series takes its labels from the left series (as a real
	// match would), while a filled-left series takes only the matching labels and no metric name.
	fillZero := 0.0

	testCases := map[string]struct {
		vectorMatching parser.VectorMatching
		op             parser.ItemType
		returnBool     bool
		leftSeries     []labels.Labels
		rightSeries    []labels.Labels

		expectedOutputSeries []labels.Labels
	}{
		"fill both sides, partial overlap, arithmetic": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "label", "c"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "right", "label", "d"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("label", "a"),
				labels.FromStrings("label", "c"),
				labels.FromStrings("label", "d"),
			},
		},
		"fill_right only keeps unmatched left groups": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{RHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "label", "c"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "right", "label", "d"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("label", "a"),
				labels.FromStrings("label", "c"),
			},
		},
		"fill_left only keeps unmatched right groups": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "label", "c"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "right", "label", "d"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("label", "a"),
				labels.FromStrings("label", "d"),
			},
		},
		"no overlap with fill both sides": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "label", "b"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("label", "a"),
				labels.FromStrings("label", "b"),
			},
		},
		"complete overlap with fill has no extra series": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "label", "a"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("label", "a"),
			},
		},
		"comparison filter retains name for matched and filled-right groups but not filled-left": {
			// left != fill_left(0)/fill(0) right, comparison filter (no bool): matched and filled-right
			// output series keep the left metric name; the filled-left output series has no metric name.
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero}},
			op:             parser.NEQ,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_metric", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "left_metric", "label", "c"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "d"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_metric", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "left_metric", "label", "c"),
				labels.FromStrings("label", "d"),
			},
		},
		"on matching with fill both sides": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, On: true, MatchingLabels: []string{"job", "instance"}, FillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "job", "foo", "instance", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "job", "bar", "instance", "c"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "job", "foo", "instance", "a"),
				labels.FromStrings(model.MetricNameLabel, "right", "job", "foo", "instance", "d"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("job", "foo", "instance", "a"),
				labels.FromStrings("job", "bar", "instance", "c"),
				labels.FromStrings("job", "foo", "instance", "d"),
			},
		},
		"ignoring matching with fill both sides": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, On: false, MatchingLabels: []string{"job"}, FillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "job", "foo", "instance", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "job", "bar", "instance", "c"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "job", "foo", "instance", "a"),
				labels.FromStrings(model.MetricNameLabel, "right", "job", "foo", "instance", "d"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("instance", "a"),
				labels.FromStrings("instance", "c"),
				labels.FromStrings("instance", "d"),
			},
		},
		"left side empty with fill_left keeps right groups": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero}},
			op:             parser.ADD,
			leftSeries:     []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "right", "label", "b"),
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("label", "a"),
				labels.FromStrings("label", "b"),
			},
		},
		"right side empty with fill_right keeps left groups": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{RHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "label", "b"),
			},
			rightSeries: []labels.Labels{},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("label", "a"),
				labels.FromStrings("label", "b"),
			},
		},
		"right side empty with fill_left produces no series": {
			vectorMatching: parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero}},
			op:             parser.ADD,
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
			},
			rightSeries:          []labels.Labels{},
			expectedOutputSeries: []labels.Labels{},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, testCase.vectorMatching, testCase.op, testCase.returnBool, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if len(testCase.expectedOutputSeries) == 0 {
				require.Empty(t, outputSeries)
			} else {
				require.ElementsMatch(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			}

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)
			require.NoError(t, o.FinishedReading(ctx))
			o.Close()
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_FillModifiers_EvaluationAndPooling(t *testing.T) {
	// Evaluates a one-to-one fill expression over a range query, verifying the produced data and that
	// all pooled memory is released once reading is complete.
	fillZero := 0.0

	step1 := timestamp.Time(0)
	step2 := step1.Add(5 * time.Minute)
	step3 := step2.Add(5 * time.Minute)
	timeRange := types.NewRangeQueryTimeRange(step1, step3, 5*time.Minute)

	t0 := timestamp.FromTime(step1)
	t1 := timestamp.FromTime(step2)
	t2 := timestamp.FromTime(step3)

	testCases := map[string]struct {
		fillValues parser.VectorMatchFillValues

		expected map[string][]promql.FPoint
	}{
		"fill both sides": {
			fillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero},
			expected: map[string][]promql.FPoint{
				// Matched group "a": left + right at every step.
				`{label="a"}`: {{T: t0, F: 11}, {T: t1, F: 22}, {T: t2, F: 33}},
				// Left-only group "b": right filled with 0.
				`{label="b"}`: {{T: t0, F: 100}, {T: t1, F: 200}, {T: t2, F: 300}},
				// Right-only group "c": left filled with 0.
				`{label="c"}`: {{T: t0, F: 1000}, {T: t1, F: 2000}, {T: t2, F: 3000}},
			},
		},
		"fill_right only": {
			fillValues: parser.VectorMatchFillValues{RHS: &fillZero},
			expected: map[string][]promql.FPoint{
				`{label="a"}`: {{T: t0, F: 11}, {T: t1, F: 22}, {T: t2, F: 33}},
				`{label="b"}`: {{T: t0, F: 100}, {T: t1, F: 200}, {T: t2, F: 300}},
			},
		},
		"fill_left only": {
			fillValues: parser.VectorMatchFillValues{LHS: &fillZero},
			expected: map[string][]promql.FPoint{
				`{label="a"}`: {{T: t0, F: 11}, {T: t1, F: 22}, {T: t2, F: 33}},
				`{label="c"}`: {{T: t0, F: 1000}, {T: t1, F: 2000}, {T: t2, F: 3000}},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

			makeData := func(f0, f1, f2 float64) types.InstantVectorSeriesData {
				floats, err := types.FPointSlicePool.Get(3, memoryConsumptionTracker)
				require.NoError(t, err)
				floats = append(floats, promql.FPoint{T: t0, F: f0}, promql.FPoint{T: t1, F: f1}, promql.FPoint{T: t2, F: f2})
				return types.InstantVectorSeriesData{Floats: floats}
			}

			leftSeries := []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "left", "label", "b"),
			}
			rightSeries := []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right", "label", "a"),
				labels.FromStrings(model.MetricNameLabel, "right", "label", "c"),
			}

			leftData := []types.InstantVectorSeriesData{makeData(1, 2, 3), makeData(100, 200, 300)}
			rightData := []types.InstantVectorSeriesData{makeData(10, 20, 30), makeData(1000, 2000, 3000)}

			left := &operators.TestOperator{Series: leftSeries, Data: leftData, MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: rightSeries, Data: rightData, MemoryConsumptionTracker: memoryConsumptionTracker}

			vectorMatching := parser.VectorMatching{Card: parser.CardOneToOne, FillValues: testCase.fillValues}
			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.ADD, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			metadata, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			actual := map[string][]promql.FPoint{}
			for range metadata {
				d, err := o.NextSeries(ctx)
				require.NoError(t, err)
				idx := len(actual)
				actual[metadata[idx].Labels.String()] = slices.Clone(d.Floats)
				require.Empty(t, d.Histograms)
				types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
			}

			require.Equal(t, testCase.expected, actual)

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err)

			types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
			require.NoError(t, o.FinishedReading(ctx))

			// Release legitimately-dropped input series. In a real query the source operator's
			// FinishedReading does this; the TestOperator leaves it to the test.
			left.ReleaseUnreadData(memoryConsumptionTracker)
			right.ReleaseUnreadData(memoryConsumptionTracker)

			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "all pooled memory should be released")
			o.Close()
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_FillModifiers_IntermittentPresenceAndPooling(t *testing.T) {
	// Tests per-timestep fill within a matched group whose left and right series are present at
	// different timesteps, verifying the values produced and that all pooled memory is released.
	fillZero := 0.0

	step1 := timestamp.Time(0)
	timeRange := types.NewRangeQueryTimeRange(step1, step1.Add(4*5*time.Minute), 5*time.Minute)

	ts := make([]int64, 5)
	for i := range ts {
		ts[i] = timestamp.FromTime(step1.Add(time.Duration(i) * 5 * time.Minute))
	}

	// A single matched group "a" whose left samples are at even steps and right samples at odd steps,
	// so no step has both sides present. This forces every emitted point through the fill path.
	//   left  : t0=1  t2=3  t4=5
	//   right : t1=200 t3=400
	leftPoints := []promql.FPoint{{T: ts[0], F: 1}, {T: ts[2], F: 3}, {T: ts[4], F: 5}}
	rightPoints := []promql.FPoint{{T: ts[1], F: 200}, {T: ts[3], F: 400}}

	testCases := map[string]struct {
		fillValues parser.VectorMatchFillValues
		expected   []promql.FPoint
	}{
		"fill both sides: output at every step where either side is present": {
			fillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero},
			expected: []promql.FPoint{
				{T: ts[0], F: 1},   // left + 0
				{T: ts[1], F: 200}, // 0 + right
				{T: ts[2], F: 3},   // left + 0
				{T: ts[3], F: 400}, // 0 + right
				{T: ts[4], F: 5},   // left + 0
			},
		},
		"fill_right: output only where the left side is present": {
			fillValues: parser.VectorMatchFillValues{RHS: &fillZero},
			expected: []promql.FPoint{
				{T: ts[0], F: 1},
				{T: ts[2], F: 3},
				{T: ts[4], F: 5},
			},
		},
		"fill_left: output only where the right side is present": {
			fillValues: parser.VectorMatchFillValues{LHS: &fillZero},
			expected: []promql.FPoint{
				{T: ts[1], F: 200},
				{T: ts[3], F: 400},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

			makeData := func(points []promql.FPoint) types.InstantVectorSeriesData {
				floats, err := types.FPointSlicePool.Get(len(points), memoryConsumptionTracker)
				require.NoError(t, err)
				floats = append(floats, points...)
				return types.InstantVectorSeriesData{Floats: floats}
			}

			leftSeries := []labels.Labels{labels.FromStrings(model.MetricNameLabel, "left", "label", "a")}
			rightSeries := []labels.Labels{labels.FromStrings(model.MetricNameLabel, "right", "label", "a")}

			leftData := []types.InstantVectorSeriesData{makeData(leftPoints)}
			rightData := []types.InstantVectorSeriesData{makeData(rightPoints)}

			left := &operators.TestOperator{Series: leftSeries, Data: leftData, MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: rightSeries, Data: rightData, MemoryConsumptionTracker: memoryConsumptionTracker}

			vectorMatching := parser.VectorMatching{Card: parser.CardOneToOne, FillValues: testCase.fillValues}
			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.ADD, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			metadata, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			require.Len(t, metadata, 1)
			require.Equal(t, `{label="a"}`, metadata[0].Labels.String())

			d, err := o.NextSeries(ctx)
			require.NoError(t, err)
			require.Empty(t, d.Histograms)
			require.Equal(t, testCase.expected, d.Floats)
			types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err)

			types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
			require.NoError(t, o.FinishedReading(ctx))

			left.ReleaseUnreadData(memoryConsumptionTracker)
			right.ReleaseUnreadData(memoryConsumptionTracker)

			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "all pooled memory should be released")
			o.Close()
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_PassesWithoutDerivedMatchersToRHS(t *testing.T) {
	// Verifies that exclude-style matchers are forwarded to the RHS via explicit
	// exclude hints (set by an up-to-date query-frontend). When hints are nil
	// (old query-frontend plans), no matchers are generated to avoid incorrect
	// filtering of labels synthesized by label_replace/label_join.
	testCases := map[string]struct {
		vectorMatching       parser.VectorMatching
		hints                *Hints
		leftSeries           []labels.Labels
		rightSeries          []labels.Labels
		expectedRHSMatchers  types.Matchers
		expectedOutputSeries []labels.Labels
	}{
		"exclude hints: RHS receives matchers for non-excluded LHS labels": {
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"foo"}},
			hints:          &Hints{Exclude: []string{"foo"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "bar", "region", "us-east"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "x", "region", "us-east"),
				labels.FromStrings("env", "staging", "foo", "y", "region", "us-east"), // filtered by env hint
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "region", Value: "us-east"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
		},
		"exclude hints with multiple LHS series: RHS receives matchers from common non-excluded labels": {
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"foo"}},
			hints:          &Hints{Exclude: []string{"foo"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "bar", "region", "us-east"),
				labels.FromStrings("env", "prod", "foo", "baz", "region", "eu-west"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "x", "region", "us-east"),
				labels.FromStrings("env", "prod", "foo", "y", "region", "eu-west"),
				labels.FromStrings("env", "staging", "foo", "z", "region", "us-east"), // filtered by env matcher
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "region", Value: "eu-west|us-east"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
				labels.FromStrings("env", "prod", "region", "eu-west"),
			},
		},
		"exclude hints with heterogeneous LHS labels: absent label matched with empty string": {
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{}},
			hints:          &Hints{Exclude: []string{}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
				labels.FromStrings("env", "prod"), // no region label
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
				labels.FromStrings("env", "prod"),
				labels.FromStrings("env", "staging"), // filtered by env matcher
			},
			// region is absent from one LHS series, so the matcher includes the empty
			// string to also match RHS series without a region label.
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "region", Value: "|us-east"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
				labels.FromStrings("env", "prod"),
			},
		},
		"exclude hints with multiple excluded labels": {
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"foo", "bar"}},
			hints:          &Hints{Exclude: []string{"bar", "foo"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "a", "bar", "b", "region", "us-east"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "x", "bar", "y", "region", "us-east"),
				labels.FromStrings("env", "dev", "foo", "x", "bar", "y", "region", "us-east"), // filtered by env matcher
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "region", Value: "us-east"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
		},
		"nil hints with !On matching: RHS receives nil matchers (no fallback)": {
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"foo"}},
			hints:          nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "bar", "region", "us-east"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "x", "region", "us-east"),
				labels.FromStrings("env", "staging", "foo", "y", "region", "eu-west"),
			},
			expectedRHSMatchers: nil,
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
		},
		"on matching with hints: RHS receives include-derived matchers": {
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"env"}},
			hints:          &Hints{Include: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
				labels.FromStrings("env", "staging", "region", "us-east"), // filtered by env hint
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
		},
		"on matching without hints: RHS receives nil matchers": {
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"env"}},
			hints:          nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
			expectedRHSMatchers: nil,
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{Series: testCase.leftSeries, MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, testCase.vectorMatching, parser.ADD, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, testCase.hints, log.NewNopLogger())
			require.NoError(t, err)

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			require.Equal(t, testCase.expectedRHSMatchers, right.MatchersProvided, "matchers passed to RHS")
			require.ElementsMatch(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)
			require.NoError(t, o.FinishedReading(ctx))
			o.Close()
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_DropsParentMatchersWhenHintsProduceNoMatchers(t *testing.T) {
	// When hints are non-nil but BuildMatchers returns nil (e.g., all labels are excluded),
	// parent matchers must still be dropped. Parent matchers may refer to labels that don't
	// exist on the RHS of this binary operation.
	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	// Both sides have "cluster" so parent matchers don't filter out the LHS.
	// The RHS intentionally does NOT have "cluster" — this is the scenario where
	// forwarding parent matchers to the RHS would be wrong.
	leftSeries := []labels.Labels{
		labels.FromStrings("env", "prod", "cluster", "us-east"),
	}
	rightSeries := []labels.Labels{
		labels.FromStrings("env", "prod"),
	}

	left := &operators.TestOperator{Series: leftSeries, MemoryConsumptionTracker: memoryConsumptionTracker}
	right := &operators.TestOperator{Series: rightSeries, Data: make([]types.InstantVectorSeriesData, len(rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

	// Exclude hints that exclude all non-__name__ LHS labels: BuildMatchers will return nil
	// because all label names present on the LHS are excluded.
	hints := &Hints{Exclude: []string{"cluster", "env"}}
	vectorMatching := parser.VectorMatching{On: false, MatchingLabels: []string{"cluster", "env"}}

	o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.ADD, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, hints, log.NewNopLogger())
	require.NoError(t, err)

	// Pass non-nil parent matchers that refer to a label ("cluster") not present on the RHS.
	parentMatchers := types.Matchers{
		{Type: labels.MatchRegexp, Name: "cluster", Value: "us-east"},
	}
	outputSeries, err := o.SeriesMetadata(ctx, parentMatchers)
	require.NoError(t, err)

	// Parent matchers must be dropped, not forwarded to RHS.
	require.Nil(t, right.MatchersProvided, "parent matchers should be dropped when hints are set but produce no matchers")

	types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)
	require.NoError(t, o.FinishedReading(ctx))
	o.Close()
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
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

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
			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.LTE, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
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

			// Call FinishedReading on the operator and verify that the intermediate state is released.
			require.NoError(t, o.FinishedReading(ctx))
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
			o.Close()
		})
	}
}
