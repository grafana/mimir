// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Most of the functionality of the binary operation operator is tested through the test scripts in
// pkg/streamingpromql/testdata.
//
// The merging behaviour has many edge cases, so it's easier to test it directly from Go.
//
// Most of the edge cases are already covered by TestMergeSeries, so we focus on the logic
// unique to OneToOneVectorVectorBinaryOperation: converting conflicts to user-friendly error messages.
func TestVectorVectorBinaryOperation_SeriesMerging(t *testing.T) {
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
			memoryConsumptionTracker := limiting.NewMemoryConsumptionTracker(0, nil)
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
				require.NoError(t, memoryConsumptionTracker.IncreaseMemoryConsumption(types.FPointSize*uint64(len(s.Floats))+types.HPointSize*uint64(len(s.Histograms))))
			}

			result, err := o.mergeOneSide(testCase.input, testCase.sourceSeriesIndices, testCase.sourceSeriesMetadata, "right")

			if testCase.expectedError == "" {
				require.NoError(t, err)
				require.Equal(t, testCase.expectedOutput, result)
			} else {
				require.EqualError(t, err, testCase.expectedError)
			}
		})
	}
}

func TestVectorVectorBinaryOperation_Sorting(t *testing.T) {
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
					leftSeriesIndices:  []int{4},
					rightSeriesIndices: []int{1},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0},
			expectedOrderFavouringRightSide: []int{0},
		},
		"two output series, both with one input series, read from both sides in same order and already sorted correctly": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{1},
					rightSeriesIndices: []int{1},
				},
				{
					leftSeriesIndices:  []int{2},
					rightSeriesIndices: []int{2},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 1},
			expectedOrderFavouringRightSide: []int{0, 1},
		},
		"two output series, both with one input series, read from both sides in same order but sorted incorrectly": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{2},
					rightSeriesIndices: []int{2},
				},
				{
					leftSeriesIndices:  []int{1},
					rightSeriesIndices: []int{1},
				},
			},

			expectedOrderFavouringLeftSide:  []int{1, 0},
			expectedOrderFavouringRightSide: []int{1, 0},
		},
		"two output series, both with one input series, read from both sides in different order": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{1},
					rightSeriesIndices: []int{2},
				},
				{
					leftSeriesIndices:  []int{2},
					rightSeriesIndices: []int{1},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 1},
			expectedOrderFavouringRightSide: []int{1, 0},
		},
		"two output series, both with multiple input series": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{1, 2},
					rightSeriesIndices: []int{0, 3},
				},
				{
					leftSeriesIndices:  []int{0, 3},
					rightSeriesIndices: []int{1, 2},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 1},
			expectedOrderFavouringRightSide: []int{1, 0},
		},
		"multiple output series, both with one input series, read from both sides in same order and already sorted correctly": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{1},
					rightSeriesIndices: []int{1},
				},
				{
					leftSeriesIndices:  []int{2},
					rightSeriesIndices: []int{2},
				},
				{
					leftSeriesIndices:  []int{3},
					rightSeriesIndices: []int{3},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 1, 2},
			expectedOrderFavouringRightSide: []int{0, 1, 2},
		},
		"multiple output series, both with one input series, read from both sides in same order but sorted incorrectly": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{2},
					rightSeriesIndices: []int{2},
				},
				{
					leftSeriesIndices:  []int{3},
					rightSeriesIndices: []int{3},
				},
				{
					leftSeriesIndices:  []int{1},
					rightSeriesIndices: []int{1},
				},
			},

			expectedOrderFavouringLeftSide:  []int{2, 0, 1},
			expectedOrderFavouringRightSide: []int{2, 0, 1},
		},
		"multiple output series, both with one input series, read from both sides in different order": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{1},
					rightSeriesIndices: []int{2},
				},
				{
					leftSeriesIndices:  []int{3},
					rightSeriesIndices: []int{3},
				},
				{
					leftSeriesIndices:  []int{2},
					rightSeriesIndices: []int{1},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0, 2, 1},
			expectedOrderFavouringRightSide: []int{2, 0, 1},
		},
		"multiple output series, with multiple input series each": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{4, 5, 10},
					rightSeriesIndices: []int{2, 20},
				},
				{
					leftSeriesIndices:  []int{2, 4, 15},
					rightSeriesIndices: []int{3, 5, 50},
				},
				{
					leftSeriesIndices:  []int{3, 1},
					rightSeriesIndices: []int{1, 40},
				},
			},

			expectedOrderFavouringLeftSide:  []int{2, 0, 1},
			expectedOrderFavouringRightSide: []int{0, 2, 1},
		},
		"multiple output series which depend on the same input series": {
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{1},
					rightSeriesIndices: []int{2},
				},
				{
					leftSeriesIndices:  []int{1},
					rightSeriesIndices: []int{1},
				},
				{
					leftSeriesIndices:  []int{2},
					rightSeriesIndices: []int{2},
				},
				{
					leftSeriesIndices:  []int{2},
					rightSeriesIndices: []int{1},
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
