// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"context"
	"slices"
	"sort"
	"strconv"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

// Most of the functionality of the binary operation operator is tested through the test scripts in
// pkg/streamingpromql/testdata.
//
// The merging behaviour has many edge cases, so it's easier to test it here.
func TestBinaryOperation_SeriesMerging(t *testing.T) {
	testCases := map[string]struct {
		input                []InstantVectorSeriesData
		sourceSeriesIndices  []int
		sourceSeriesMetadata []SeriesMetadata

		expectedOutput InstantVectorSeriesData
		expectedError  string
	}{
		"no input series": {
			input:          []InstantVectorSeriesData{},
			expectedOutput: InstantVectorSeriesData{},
		},
		"single input series": {
			input: []InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 3, F: 30},
					},
				},
			},
			expectedOutput: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
				},
			},
		},
		"two input series with no overlap, series in time order": {
			input: []InstantVectorSeriesData{
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
			expectedOutput: InstantVectorSeriesData{
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
		"two input series with no overlap, series not in time order": {
			input: []InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 4, F: 40},
						{T: 5, F: 50},
						{T: 6, F: 60},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 3, F: 30},
					},
				},
			},
			expectedOutput: InstantVectorSeriesData{
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
		"three input series with no overlap": {
			input: []InstantVectorSeriesData{
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
				{
					Floats: []promql.FPoint{
						{T: 7, F: 70},
						{T: 8, F: 80},
						{T: 9, F: 90},
					},
				},
			},
			expectedOutput: InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{T: 1, F: 10},
					{T: 2, F: 20},
					{T: 3, F: 30},
					{T: 4, F: 40},
					{T: 5, F: 50},
					{T: 6, F: 60},
					{T: 7, F: 70},
					{T: 8, F: 80},
					{T: 9, F: 90},
				},
			},
		},
		"two input series with overlap": {
			input: []InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 3, F: 30},
						{T: 5, F: 50},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
						{T: 4, F: 40},
						{T: 6, F: 60},
					},
				},
			},
			expectedOutput: InstantVectorSeriesData{
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
		"three input series with overlap": {
			input: []InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 4, F: 40},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
						{T: 5, F: 50},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 3, F: 30},
						{T: 6, F: 60},
					},
				},
			},
			expectedOutput: InstantVectorSeriesData{
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
		"input series with conflict": {
			input: []InstantVectorSeriesData{
				{
					Floats: []promql.FPoint{
						{T: 1, F: 10},
						{T: 2, F: 20},
						{T: 5, F: 50},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 6, F: 60},
					},
				},
				{
					Floats: []promql.FPoint{
						{T: 2, F: 20},
						{T: 4, F: 40},
					},
				},
			},
			sourceSeriesIndices: []int{6, 9, 4},
			sourceSeriesMetadata: []SeriesMetadata{
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "a")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "b")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "c")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "d")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "e")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "f")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "g")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "h")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "i")},
				{labels.FromStrings("__name__", "right_side", "env", "test", "pod", "j")},
			},
			expectedError: `found duplicate series for the match group {env="test"} on the right side of the operation at timestamp 1970-01-01T00:00:00.002Z: {__name__="right_side", env="test", pod="g"} and {__name__="right_side", env="test", pod="j"}`,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			o := &BinaryOperation{
				// Simulate an expression with "on (env)".
				// This is used to generate error messages.
				VectorMatching: parser.VectorMatching{
					On:             true,
					MatchingLabels: []string{"env"},
				},
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

func TestBinaryOperation_Sorting(t *testing.T) {
	testCases := map[string]struct {
		series []*binaryOperationOutputSeries

		expectedOrderFavouringLeftSide  []int
		expectedOrderFavouringRightSide []int
	}{
		"no output series": {
			series: []*binaryOperationOutputSeries{},

			expectedOrderFavouringLeftSide:  []int{},
			expectedOrderFavouringRightSide: []int{},
		},
		"single output series": {
			series: []*binaryOperationOutputSeries{
				{
					leftSeriesIndices:  []int{4},
					rightSeriesIndices: []int{1},
				},
			},

			expectedOrderFavouringLeftSide:  []int{0},
			expectedOrderFavouringRightSide: []int{0},
		},
		"two output series, both with one input series, read from both sides in same order and already sorted correctly": {
			series: []*binaryOperationOutputSeries{
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
			series: []*binaryOperationOutputSeries{
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
			series: []*binaryOperationOutputSeries{
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
			series: []*binaryOperationOutputSeries{
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
			series: []*binaryOperationOutputSeries{
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
			series: []*binaryOperationOutputSeries{
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
			series: []*binaryOperationOutputSeries{
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
			series: []*binaryOperationOutputSeries{
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
			series: []*binaryOperationOutputSeries{
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

			metadata := make([]SeriesMetadata, len(testCase.series))
			for i := range testCase.series {
				metadata[i] = SeriesMetadata{labels.FromStrings("series", strconv.Itoa(i))}
			}

			test := func(t *testing.T, series []*binaryOperationOutputSeries, metadata []SeriesMetadata, sorter sort.Interface, expectedOrder []int) {
				expectedSeriesOrder := make([]*binaryOperationOutputSeries, len(series))
				expectedMetadataOrder := make([]SeriesMetadata, len(metadata))

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

func TestBinaryOperationSeriesBuffer(t *testing.T) {
	series0Data := InstantVectorSeriesData{Floats: []promql.FPoint{{T: 0, F: 0}}}
	series2Data := InstantVectorSeriesData{Floats: []promql.FPoint{{T: 0, F: 2}}}
	series3Data := InstantVectorSeriesData{Floats: []promql.FPoint{{T: 0, F: 3}}}
	series4Data := InstantVectorSeriesData{Floats: []promql.FPoint{{T: 0, F: 4}}}
	series5Data := InstantVectorSeriesData{Floats: []promql.FPoint{{T: 0, F: 5}}}
	series6Data := InstantVectorSeriesData{Floats: []promql.FPoint{{T: 0, F: 6}}}

	inner := &testOperator{
		series: []labels.Labels{
			labels.FromStrings("series", "0"),
			labels.FromStrings("series", "1"),
			labels.FromStrings("series", "2"),
			labels.FromStrings("series", "3"),
			labels.FromStrings("series", "4"),
			labels.FromStrings("series", "5"),
			labels.FromStrings("series", "6"),
		},
		data: []InstantVectorSeriesData{
			series0Data,
			{Floats: []promql.FPoint{{T: 0, F: 1}}},
			series2Data,
			series3Data,
			series4Data,
			series5Data,
			series6Data,
		},
	}

	seriesUsed := []bool{true, false, true, true, true}
	buffer := newBinaryOperationSeriesBuffer(inner, seriesUsed)
	ctx := context.Background()

	// Read first series.
	series, err := buffer.getSeries(ctx, []int{0})
	require.NoError(t, err)
	require.Equal(t, []InstantVectorSeriesData{series0Data}, series)
	require.Empty(t, buffer.buffer) // Should not buffer series that was immediately returned.

	// Read next desired series, skipping over series that won't be used.
	series, err = buffer.getSeries(ctx, []int{2})
	require.NoError(t, err)
	require.Equal(t, []InstantVectorSeriesData{series2Data}, series)
	require.Empty(t, buffer.buffer) // Should not buffer series at index 1 that won't be used.

	// Read another desired series, skipping over a series that will be used later.
	series, err = buffer.getSeries(ctx, []int{4})
	require.NoError(t, err)
	require.Equal(t, []InstantVectorSeriesData{series4Data}, series)
	require.Len(t, buffer.buffer, 1) // Should only have buffered a single series (index 3).

	// Read the series we just read past from the buffer.
	series, err = buffer.getSeries(ctx, []int{3})
	require.NoError(t, err)
	require.Equal(t, []InstantVectorSeriesData{series3Data}, series)
	require.Empty(t, buffer.buffer) // Series that has been returned should be removed from buffer once it's returned.

	// Read multiple series.
	series, err = buffer.getSeries(ctx, []int{5, 6})
	require.NoError(t, err)
	require.Equal(t, []InstantVectorSeriesData{series5Data, series6Data}, series)
}
