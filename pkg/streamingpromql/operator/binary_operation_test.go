// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
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
