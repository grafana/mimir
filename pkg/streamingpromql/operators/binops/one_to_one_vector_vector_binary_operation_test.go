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
	// Every output series of one name-retaining fill-left split group shares one right side.
	siblingGroupRightSide := &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{0}}
	carrierGroupRightSide := &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{0}}

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
		"fill-missing-right series mixed with a normal series": {
			// input[0]: normal            -> latestLeftSeries=2, latestRightSeries=2
			// input[1]: fill-missing-right -> latestLeftSeries=1, latestRightSeries=-1
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: []int{1},
					rightSide:         nil,
					fillMissingRight:  true,
				},
			},

			// Favour left (left asc, tie right asc): input[1](L=1) < input[0](L=2).
			expectedOrderFavouringLeftSide: []int{1, 0},
			// Favour right (right asc, tie left asc): input[1](R=-1) < input[0](R=2).
			expectedOrderFavouringRightSide: []int{1, 0},
		},
		"fill-missing-left series mixed with a normal series": {
			// input[0]: normal           -> latestLeftSeries=2, latestRightSeries=2
			// input[1]: fill-missing-left -> latestLeftSeries=-1, latestRightSeries=1
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: nil,
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
					fillMissingLeft:   true,
				},
			},

			// Favour left (left asc, tie right asc): input[1](L=-1) < input[0](L=2).
			expectedOrderFavouringLeftSide: []int{1, 0},
			// Favour right (right asc, tie left asc): input[1](R=1) < input[0](R=2).
			expectedOrderFavouringRightSide: []int{1, 0},
		},
		"mix of one fill-missing-left, one fill-missing-right, and one normal series": {
			// input[0]: normal            -> latestLeftSeries=2, latestRightSeries=2
			// input[1]: fill-missing-left  -> latestLeftSeries=-1, latestRightSeries=3
			// input[2]: fill-missing-right -> latestLeftSeries=3, latestRightSeries=-1
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
				{
					leftSeriesIndices: nil,
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{3}},
					fillMissingLeft:   true,
				},
				{
					leftSeriesIndices: []int{3},
					rightSide:         nil,
					fillMissingRight:  true,
				},
			},

			// Favour left (left asc): L values input[0]=2, input[1]=-1, input[2]=3 -> -1, 2, 3.
			expectedOrderFavouringLeftSide: []int{1, 0, 2},
			// Favour right (right asc): R values input[0]=2, input[1]=3, input[2]=-1 -> -1, 2, 3.
			expectedOrderFavouringRightSide: []int{2, 0, 1},
		},
		"two fill-missing-right series mixed with a normal series, tie-break on left index": {
			// Both fill-missing-right series have latestRightSeries=-1, so the favour-right
			// sorter must tie-break on the left index.
			// input[0]: fill-missing-right -> latestLeftSeries=3, latestRightSeries=-1
			// input[1]: fill-missing-right -> latestLeftSeries=1, latestRightSeries=-1
			// input[2]: normal             -> latestLeftSeries=2, latestRightSeries=2
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: []int{3},
					rightSide:         nil,
					fillMissingRight:  true,
				},
				{
					leftSeriesIndices: []int{1},
					rightSide:         nil,
					fillMissingRight:  true,
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
			},

			// Favour left (left asc): L values input[0]=3, input[1]=1, input[2]=2 -> 1, 2, 3.
			expectedOrderFavouringLeftSide: []int{1, 2, 0},
			// Favour right (right asc, tie left asc): R values input[0]=-1, input[1]=-1, input[2]=2.
			// input[0] and input[1] tie on R=-1, tie-break on left: input[1](L=1) < input[0](L=3).
			expectedOrderFavouringRightSide: []int{1, 0, 2},
		},
		"two fill-missing-left series mixed with a normal series, tie-break on right index": {
			// Both fill-missing-left series have latestLeftSeries=-1, so the favour-left
			// sorter must tie-break on the right index.
			// input[0]: fill-missing-left -> latestLeftSeries=-1, latestRightSeries=3
			// input[1]: fill-missing-left -> latestLeftSeries=-1, latestRightSeries=1
			// input[2]: normal            -> latestLeftSeries=2, latestRightSeries=2
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices: nil,
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{3}},
					fillMissingLeft:   true,
				},
				{
					leftSeriesIndices: nil,
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{1}},
					fillMissingLeft:   true,
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         &oneToOneBinaryOperationRightSide{rightSeriesIndices: []int{2}},
				},
			},

			// Favour left (left asc, tie right asc): L values input[0]=-1, input[1]=-1, input[2]=2.
			// input[0] and input[1] tie on L=-1, tie-break on right: input[1](R=1) < input[0](R=3).
			expectedOrderFavouringLeftSide: []int{1, 0, 2},
			// Favour right (right asc): R values input[0]=3, input[1]=1, input[2]=2 -> 1, 2, 3.
			expectedOrderFavouringRightSide: []int{1, 2, 0},
		},
		"name-dropped sibling of a split group sorts after every matched series of the group": {
			// A split group with two matched output series and one name-dropped sibling. All three
			// share one right side, and the sibling reports the group's highest left series index. So
			// the sibling ties with input[2] on both sides, and only the final tie-break puts it last.
			// The operator must read the sibling last. The last matched read of the group computes the
			// group's fill-left points.
			// input[0]: sibling -> latestLeftSeries=1, latestRightSeries=0
			// input[1]: matched -> latestLeftSeries=0, latestRightSeries=0
			// input[2]: matched -> latestLeftSeries=1, latestRightSeries=0
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					rightSide:                  siblingGroupRightSide,
					splitHolder:                &oneToOneBinaryOperationSplitHolder{},
					fillLeftCarrier:            true,
					nameDropped:                true,
					groupLatestLeftSeriesIndex: 1,
				},
				{
					leftSeriesIndices: []int{0},
					rightSide:         siblingGroupRightSide,
					splitHolder:       &oneToOneBinaryOperationSplitHolder{},
				},
				{
					leftSeriesIndices: []int{1},
					rightSide:         siblingGroupRightSide,
					splitHolder:       &oneToOneBinaryOperationSplitHolder{},
				},
			},

			expectedOrderFavouringLeftSide:  []int{1, 2, 0},
			expectedOrderFavouringRightSide: []int{1, 2, 0},
		},
		"fill-left carrier that is a matched series sorts after every other matched series of the group": {
			// A split group whose fill-left carrier is one of its matched output series. The carrier
			// reads left series 0, but it reports the group's highest left series index (2). So it ties
			// with input[2] on both sides, and only the final tie-break puts it last.
			// input[0]: carrier -> latestLeftSeries=2, latestRightSeries=0
			// input[1]: matched -> latestLeftSeries=1, latestRightSeries=0
			// input[2]: matched -> latestLeftSeries=2, latestRightSeries=0
			series: []*oneToOneBinaryOperationOutputSeries{
				{
					leftSeriesIndices:          []int{0},
					rightSide:                  carrierGroupRightSide,
					splitHolder:                &oneToOneBinaryOperationSplitHolder{},
					fillLeftCarrier:            true,
					groupLatestLeftSeriesIndex: 2,
				},
				{
					leftSeriesIndices: []int{1},
					rightSide:         carrierGroupRightSide,
					splitHolder:       &oneToOneBinaryOperationSplitHolder{},
				},
				{
					leftSeriesIndices: []int{2},
					rightSide:         carrierGroupRightSide,
					splitHolder:       &oneToOneBinaryOperationSplitHolder{},
				},
			},

			expectedOrderFavouringLeftSide:  []int{1, 2, 0},
			expectedOrderFavouringRightSide: []int{1, 2, 0},
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
			// left != fill(0) right, comparison filter (no bool) with ignoring() matching:
			//   - matched group "a": keeps the left metric name at both-present steps, but a kept
			//     left-filled step drops the name. So it splits into two output series: left_metric{a}
			//     (name-retaining) and {a} (name-dropped fill-left half).
			//   - unmatched-left group "c": filled-right, keeps the left metric name.
			//   - unmatched-right group "d": filled-left, has no metric name.
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
				labels.FromStrings("label", "a"),
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

func TestOneToOneVectorVectorBinaryOperation_FillModifiers_SeriesUsed(t *testing.T) {
	// This test checks the leftSeriesUsed and rightSeriesUsed slices that computeOutputSeries returns
	// with fill modifiers set. It also checks the memory optimization for the left side.
	//
	// With fill_right, every left series makes output. So computeOutputSeries leaves leftSeriesUsed
	// nil. InstantVectorOperatorBuffer reads a nil "used" slice as "the operator needs all series".
	// This removes the need to get and fill an all-true slice.
	//
	// With fill_left, the operator uses every right series. All rightSeriesUsed entries are true,
	// because unmatched right groups still make output. rightSeriesUsed keeps its explicit form.
	// The collision path in addUnmatchedRightGroupsWithFilledLeftSides can leave a right series
	// unused. So the nil optimization does not apply to the right side.
	//
	// The test also checks that the last-used index equals the final input index for a full side.
	fillZero := 0.0

	// The left and right sides overlap only in part. "a" matches. The other series do not. This makes
	// unmatched groups on both sides. Without a fill modifier the operator prunes those unmatched
	// series. So the all-used result comes directly from the fill logic.
	leftSeries := []labels.Labels{
		labels.FromStrings(model.MetricNameLabel, "left", "label", "a"),
		labels.FromStrings(model.MetricNameLabel, "left", "label", "b"),
		labels.FromStrings(model.MetricNameLabel, "left", "label", "c"),
	}
	rightSeries := []labels.Labels{
		labels.FromStrings(model.MetricNameLabel, "right", "label", "a"),
		labels.FromStrings(model.MetricNameLabel, "right", "label", "d"),
		labels.FromStrings(model.MetricNameLabel, "right", "label", "e"),
	}

	testCases := map[string]struct {
		vectorMatching parser.VectorMatching

		// expectLeftUsedNil is true when leftSeriesUsed must be nil. This is the "all left used" optimization.
		expectLeftUsedNil bool
		// The test checks expectAllLeftUsed only when expectLeftUsedNil is false.
		expectAllLeftUsed  bool
		expectAllRightUsed bool
	}{
		"fill_left marks every right series used, but not every left series": {
			vectorMatching:     parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero}},
			expectLeftUsedNil:  false,
			expectAllLeftUsed:  false,
			expectAllRightUsed: true,
		},
		"fill_right leaves leftSeriesUsed nil, but not every right series is used": {
			vectorMatching:     parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{RHS: &fillZero}},
			expectLeftUsedNil:  true,
			expectAllRightUsed: false,
		},
		"fill both leaves leftSeriesUsed nil and marks every right series used": {
			vectorMatching:     parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero, RHS: &fillZero}},
			expectLeftUsedNil:  true,
			expectAllRightUsed: true,
		},
		"no fill prunes unmatched series on both sides": {
			vectorMatching:     parser.VectorMatching{Card: parser.CardOneToOne},
			expectLeftUsedNil:  false,
			expectAllLeftUsed:  false,
			expectAllRightUsed: false,
		},
	}

	allTrue := func(s []bool) bool {
		for _, v := range s {
			if !v {
				return false
			}
		}
		return true
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{Series: leftSeries, Data: make([]types.InstantVectorSeriesData, len(leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: rightSeries, Data: make([]types.InstantVectorSeriesData, len(rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, testCase.vectorMatching, parser.ADD, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			// Fill leftMetadata and rightMetadata. Then call the internal computeOutputSeries directly.
			// This lets the test read the leftSeriesUsed and rightSeriesUsed slices that it returns.
			o.leftMetadata, err = left.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			o.rightMetadata, err = right.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			_, _, leftSeriesUsed, lastLeftSeriesUsedIndex, rightSeriesUsed, lastRightSeriesUsedIndex, err := o.computeOutputSeries()
			require.NoError(t, err)

			require.Len(t, rightSeriesUsed, len(rightSeries))
			require.Equal(t, testCase.expectAllRightUsed, allTrue(rightSeriesUsed), "rightSeriesUsed=%v", rightSeriesUsed)
			if testCase.expectAllRightUsed {
				require.Equal(t, len(rightSeries)-1, lastRightSeriesUsedIndex)
			}

			if testCase.expectLeftUsedNil {
				// A nil leftSeriesUsed means the operator needs all left series. The buffer needs this.
				require.Nil(t, leftSeriesUsed)
				require.Equal(t, len(leftSeries)-1, lastLeftSeriesUsedIndex)
			} else {
				require.Len(t, leftSeriesUsed, len(leftSeries))
				require.Equal(t, testCase.expectAllLeftUsed, allTrue(leftSeriesUsed), "leftSeriesUsed=%v", leftSeriesUsed)
				if testCase.expectAllLeftUsed {
					require.Equal(t, len(leftSeries)-1, lastLeftSeriesUsedIndex)
				}
			}

			require.NoError(t, o.FinishedReading(ctx))
			o.Close()
		})
	}
}

func TestOneToOneVectorVectorBinaryOperation_FillLeft_CollisionLeavesRightSeriesUnused(t *testing.T) {
	// This test drives the filled-labels collision path in addUnmatchedRightGroupsWithFilledLeftSides.
	// This path is the reason the "all right series used with fill_left" optimization does not apply
	// to rightSeriesUsed. A later change that sets rightSeriesUsed to nil ("all used") with fill_left
	// makes this test fail, because one right series stays unused here.
	//
	// The test uses on(__name__) matching, a name-retaining operator, and fill_left. The operator is
	// NEQ without the bool modifier, so it acts as a filter and keeps the name. The two unmatched
	// right series differ only in __name__. The group key keeps only __name__, so the two series go
	// to different match groups. The filled labels drop __name__, so both groups fill to empty
	// labels. The second group collides with the output series that the first group made. The
	// operator skips the second group, so it never marks the second right series as used.
	fillZero := 0.0
	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	// The left side has no series. So every right group is unmatched and takes the fill-left path.
	leftSeries := []labels.Labels{}
	rightSeries := []labels.Labels{
		labels.FromStrings(model.MetricNameLabel, "right_a"),
		labels.FromStrings(model.MetricNameLabel, "right_b"),
	}

	vectorMatching := parser.VectorMatching{
		Card:           parser.CardOneToOne,
		On:             true,
		MatchingLabels: []string{model.MetricNameLabel},
		FillValues:     parser.VectorMatchFillValues{LHS: &fillZero},
	}

	left := &operators.TestOperator{Series: leftSeries, Data: make([]types.InstantVectorSeriesData, len(leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
	right := &operators.TestOperator{Series: rightSeries, Data: make([]types.InstantVectorSeriesData, len(rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

	// parser.NEQ without the bool modifier keeps the metric name. The operator needs this to reach
	// the collision branch and not the "this indicates a bug" error.
	o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.NEQ, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
	require.NoError(t, err)

	o.leftMetadata, err = left.SeriesMetadata(ctx, nil)
	require.NoError(t, err)
	o.rightMetadata, err = right.SeriesMetadata(ctx, nil)
	require.NoError(t, err)

	allMetadata, _, _, _, rightSeriesUsed, _, err := o.computeOutputSeries()
	require.NoError(t, err)

	// The operator makes only one output series, because both right groups fill to the same empty
	// labels. The two right groups collided. So one right series stays unused. This invariant makes
	// the nil optimization unsafe for the right side with fill_left.
	require.Len(t, allMetadata, 1)
	require.Equal(t, labels.EmptyLabels(), allMetadata[0].Labels)

	require.Len(t, rightSeriesUsed, len(rightSeries))
	usedCount := 0
	for _, used := range rightSeriesUsed {
		if used {
			usedCount++
		}
	}
	require.Equal(t, 1, usedCount, "expected exactly one right series to be used, rightSeriesUsed=%v", rightSeriesUsed)

	require.NoError(t, o.FinishedReading(ctx))
	o.Close()
}

func TestOneToOneVectorVectorBinaryOperation_FillRight_CollisionKeepsMatchedSeries(t *testing.T) {
	// This test drives the filled-labels collision path in addUnmatchedLeftSeriesWithFilledRightSides.
	//
	// The test uses on(__name__) matching, a name-retaining operator (NEQ without the bool modifier,
	// so it acts as a filter and keeps the name), and fill_right. The group key keeps only __name__,
	// so left series with different names go to different match groups. The filled labels drop
	// __name__ (on(...) keeps only the matching labels, which is empty after removing __name__), so
	// every left series produces empty output labels.
	//
	// One left series ("matched") has a right group of the same name, so it becomes a real matched
	// output series. The other left series ("unmatched") has no right group and takes the fill-right
	// path. Its filled labels collide with the matched series' empty labels. The operator must keep the
	// matched series with its real right side and skip the unmatched left series. It must not merge the
	// unmatched left index into the matched series, which would evaluate it against the wrong right
	// side. It must not turn the matched series into a filled-right series.
	//
	// The test runs both iteration orders to confirm a matched series always wins the collision.
	fillZero := 0.0

	testCases := map[string]struct {
		leftSeries []labels.Labels
	}{
		"matched left series first": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "matched"),
				labels.FromStrings(model.MetricNameLabel, "unmatched"),
			},
		},
		"unmatched left series first": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "unmatched"),
				labels.FromStrings(model.MetricNameLabel, "matched"),
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

			rightSeries := []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "matched"),
			}

			vectorMatching := parser.VectorMatching{
				Card:           parser.CardOneToOne,
				On:             true,
				MatchingLabels: []string{model.MetricNameLabel},
				FillValues:     parser.VectorMatchFillValues{RHS: &fillZero},
			}

			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: rightSeries, Data: make([]types.InstantVectorSeriesData, len(rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

			// parser.NEQ without the bool modifier keeps the metric name. The operator needs this to
			// reach the collision branch and not the "this indicates a bug" error.
			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.NEQ, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			o.leftMetadata, err = left.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			o.rightMetadata, err = right.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			allMetadata, allSeries, _, _, _, _, err := o.computeOutputSeries()
			require.NoError(t, err)

			// Both left groups fill to the same empty labels, so there is exactly one output series.
			require.Len(t, allMetadata, 1)
			require.Equal(t, labels.EmptyLabels(), allMetadata[0].Labels)
			require.Len(t, allSeries, 1)

			// The surviving output series must be the matched one. It keeps its real right side and the
			// operator does not treat it as a filled-right series. Only the matched left series index is
			// present. The operator skipped the unmatched left series and did not merge it in.
			matchedLeftIndex := slices.IndexFunc(testCase.leftSeries, func(l labels.Labels) bool {
				return l.Get(model.MetricNameLabel) == "matched"
			})
			require.False(t, allSeries[0].fillMissingRight, "output series must keep its real right side, not be treated as filled-right")
			require.NotNil(t, allSeries[0].rightSide, "output series must have a real right side")
			require.Equal(t, []int{matchedLeftIndex}, allSeries[0].leftSeriesIndices, "only the matched left series must be attached to the output series")

			require.NoError(t, o.FinishedReading(ctx))
			o.Close()
		})
	}
}

// expectedSplitGroup describes the output series that computeOutputSeries must build for one
// name-retaining fill-left split group.
type expectedSplitGroup struct {
	// matchedSeries maps the labels of each matched output series of the group to the left series
	// indices that output series reads.
	matchedSeries map[string][]int

	// fillLeftCarrier is the labels of the output series that emits the group's fill-left points. It
	// is empty when the group has no carrier. The group has no carrier when the collision path clears
	// the split.
	fillLeftCarrier string

	// carrierIsSibling is true when the carrier is an extra name-dropped output series, and false when
	// the carrier is one of matchedSeries.
	carrierIsSibling bool

	// latestLeftSeriesIndex is the highest left series index of the group. The carrier must report it.
	latestLeftSeriesIndex int

	// rightSideOutputSeriesCount is the number of output series that count against the group's right
	// side. A name-dropped sibling must not count.
	rightSideOutputSeriesCount int
}

func TestOneToOneVectorVectorBinaryOperation_FillLeft_NameRetainingSplitOutputSeries(t *testing.T) {
	// This test checks the output series that computeOutputSeries builds for a name-retaining
	// fill-left split group. It checks which output series exist. It checks which left series each
	// output series reads. It checks which output series carries the group's fill-left points. It
	// also checks how the groups share their split holder and right side.
	//
	// The operator is != without the bool modifier, so it retains __name__. Matching is the default
	// and ignores nothing. That combination makes the operator split a match group.
	fillZero := 0.0

	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedGroups []expectedSplitGroup
	}{
		"one left series in the group": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_a", "label", "x"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "x"),
			},
			expectedGroups: []expectedSplitGroup{
				{
					matchedSeries:              map[string][]int{`{__name__="left_a", label="x"}`: {0}},
					fillLeftCarrier:            `{label="x"}`,
					carrierIsSibling:           true,
					latestLeftSeriesIndex:      0,
					rightSideOutputSeriesCount: 1,
				},
			},
		},
		"two differently named left series in the group": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_a", "label", "x"),
				labels.FromStrings(model.MetricNameLabel, "left_b", "label", "x"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "x"),
			},
			expectedGroups: []expectedSplitGroup{
				{
					matchedSeries: map[string][]int{
						`{__name__="left_a", label="x"}`: {0},
						`{__name__="left_b", label="x"}`: {1},
					},
					fillLeftCarrier:            `{label="x"}`,
					carrierIsSibling:           true,
					latestLeftSeriesIndex:      1,
					rightSideOutputSeriesCount: 2,
				},
			},
		},
		"three differently named left series in the group": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_a", "label", "x"),
				labels.FromStrings(model.MetricNameLabel, "left_b", "label", "x"),
				labels.FromStrings(model.MetricNameLabel, "left_c", "label", "x"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "x"),
			},
			expectedGroups: []expectedSplitGroup{
				{
					matchedSeries: map[string][]int{
						`{__name__="left_a", label="x"}`: {0},
						`{__name__="left_b", label="x"}`: {1},
						`{__name__="left_c", label="x"}`: {2},
					},
					fillLeftCarrier:            `{label="x"}`,
					carrierIsSibling:           true,
					latestLeftSeriesIndex:      2,
					rightSideOutputSeriesCount: 3,
				},
			},
		},
		"two split groups stay independent": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_a", "label", "x"),
				labels.FromStrings(model.MetricNameLabel, "left_b", "label", "x"),
				labels.FromStrings(model.MetricNameLabel, "left_c", "label", "y"),
				labels.FromStrings(model.MetricNameLabel, "left_d", "label", "y"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "x"),
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "y"),
			},
			expectedGroups: []expectedSplitGroup{
				{
					matchedSeries: map[string][]int{
						`{__name__="left_a", label="x"}`: {0},
						`{__name__="left_b", label="x"}`: {1},
					},
					fillLeftCarrier:            `{label="x"}`,
					carrierIsSibling:           true,
					latestLeftSeriesIndex:      1,
					rightSideOutputSeriesCount: 2,
				},
				{
					matchedSeries: map[string][]int{
						`{__name__="left_c", label="y"}`: {2},
						`{__name__="left_d", label="y"}`: {3},
					},
					fillLeftCarrier:            `{label="y"}`,
					carrierIsSibling:           true,
					latestLeftSeriesIndex:      3,
					rightSideOutputSeriesCount: 2,
				},
			},
		},
		"single left series with no metric name clears the split": {
			// The group's only left series has no __name__, so the matched output series already carries
			// the group's match key labels. The group needs no split at all.
			leftSeries: []labels.Labels{
				labels.FromStrings("label", "x"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "x"),
			},
			expectedGroups: []expectedSplitGroup{
				{
					matchedSeries:              map[string][]int{`{label="x"}`: {0}},
					fillLeftCarrier:            "",
					latestLeftSeriesIndex:      0,
					rightSideOutputSeriesCount: 1,
				},
			},
		},
		"left series with no metric name next to a named left series makes the matched series the carrier": {
			// The group holds a named left series and an unnamed one. The matched output series of the
			// unnamed one already carries the group's match key labels. So it becomes the carrier, and
			// the group gets no extra name-dropped sibling. The split stays in place, so the named left
			// series keeps its fill-left points out of its own output series.
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_a", "label", "x"),
				labels.FromStrings("label", "x"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "x"),
			},
			expectedGroups: []expectedSplitGroup{
				{
					matchedSeries: map[string][]int{
						`{__name__="left_a", label="x"}`: {0},
						`{label="x"}`:                    {1},
					},
					fillLeftCarrier:            `{label="x"}`,
					carrierIsSibling:           false,
					latestLeftSeriesIndex:      1,
					rightSideOutputSeriesCount: 2,
				},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}

			vectorMatching := parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero}}
			o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.NEQ, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
			require.NoError(t, err)

			o.leftMetadata, err = left.SeriesMetadata(ctx, nil)
			require.NoError(t, err)
			o.rightMetadata, err = right.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			allMetadata, allSeries, leftSeriesUsed, _, rightSeriesUsed, _, err := o.computeOutputSeries()
			require.NoError(t, err)
			require.Len(t, allSeries, len(allMetadata))

			seriesByLabels := make(map[string]*oneToOneBinaryOperationOutputSeries, len(allSeries))
			expectedLabels := []string{}

			for i, m := range allMetadata {
				seriesByLabels[m.Labels.String()] = allSeries[i]
			}

			for _, group := range testCase.expectedGroups {
				for l := range group.matchedSeries {
					expectedLabels = append(expectedLabels, l)
				}

				if group.carrierIsSibling {
					expectedLabels = append(expectedLabels, group.fillLeftCarrier)
				}
			}

			actualLabels := make([]string, 0, len(seriesByLabels))
			for l := range seriesByLabels {
				actualLabels = append(actualLabels, l)
			}

			require.ElementsMatch(t, expectedLabels, actualLabels, "unexpected set of output series")

			holders := map[*oneToOneBinaryOperationSplitHolder]struct{}{}

			for groupIndex, group := range testCase.expectedGroups {
				t.Run(fmt.Sprintf("group %v", groupIndex), func(t *testing.T) {
					var (
						groupHolder    *oneToOneBinaryOperationSplitHolder
						groupRightSide *oneToOneBinaryOperationRightSide
					)

					for l, expectedLeftSeriesIndices := range group.matchedSeries {
						series := seriesByLabels[l]
						require.NotNilf(t, series, "expected an output series with labels %v", l)
						require.Equalf(t, expectedLeftSeriesIndices, series.leftSeriesIndices, "unexpected left series indices for %v", l)
						require.Falsef(t, series.nameDropped, "%v is a matched output series, so it must not be name-dropped", l)
						require.NotNilf(t, series.rightSide, "expected %v to have a right side", l)

						if groupRightSide == nil {
							groupRightSide = series.rightSide
						}

						require.Samef(t, groupRightSide, series.rightSide, "every matched output series of the group must share one right side, but %v does not", l)

						if group.fillLeftCarrier == "" {
							// The collision path cleared the split for the whole group.
							require.Nilf(t, series.splitHolder, "expected the split to be cleared for %v", l)
							require.Falsef(t, series.fillLeftCarrier, "expected %v not to carry the group's fill-left points", l)
							continue
						}

						require.NotNilf(t, series.splitHolder, "expected %v to have a split holder", l)

						if groupHolder == nil {
							groupHolder = series.splitHolder
						}

						require.Samef(t, groupHolder, series.splitHolder, "every output series of the group must share one split holder, but %v does not", l)
					}

					require.NotNil(t, groupRightSide)
					require.Equal(t, group.rightSideOutputSeriesCount, groupRightSide.outputSeriesCount, "unexpected number of output series counted against the group's right side")

					if group.fillLeftCarrier == "" {
						return
					}

					carrier := seriesByLabels[group.fillLeftCarrier]
					require.NotNilf(t, carrier, "expected a fill-left carrier with labels %v", group.fillLeftCarrier)
					require.True(t, carrier.fillLeftCarrier, "expected the carrier to be marked as such")
					require.Same(t, groupHolder, carrier.splitHolder, "the carrier must share the group's split holder")
					require.Same(t, groupRightSide, carrier.rightSide, "the carrier must share the group's right side")
					require.Equal(t, group.latestLeftSeriesIndex, carrier.groupLatestLeftSeriesIndex)
					require.Equal(t, group.latestLeftSeriesIndex, carrier.latestLeftSeries(), "the carrier must report the group's highest left series index")

					require.Equal(t, group.carrierIsSibling, carrier.nameDropped)
					if group.carrierIsSibling {
						require.Empty(t, carrier.leftSeriesIndices, "the name-dropped sibling must read no left series of its own")
					}

					// Each group must have its own holder. The map key is a pointer, so the lookup
					// compares identity and not the holder's contents.
					_, alreadySeen := holders[groupHolder]
					require.False(t, alreadySeen, "split groups must not share a split holder")
					holders[groupHolder] = struct{}{}
				})
			}

			// The operator must read the carrier after every other matched output series of its group.
			// That order must hold, no matter which sorter sortSeries picks.
			//
			// computeOutputSeries builds its result from a map, so it returns the output series in Go
			// map iteration order. That order changes on every run. sort.Sort is not stable, so the
			// result of the sort depends on that input order. The test therefore checks every input
			// order. The assertion then does not depend on the order of one run.
			assertCarrierSortedLast := func(t *testing.T, newSorter func([]types.SeriesMetadata, []*oneToOneBinaryOperationOutputSeries) sort.Interface) {
				metadata := make([]types.SeriesMetadata, len(allMetadata))
				series := make([]*oneToOneBinaryOperationOutputSeries, len(allSeries))
				positions := make(map[string]int, len(allMetadata))

				forEachPermutation(len(allMetadata), func(permutation []int) {
					for i, from := range permutation {
						metadata[i] = allMetadata[from]
						series[i] = allSeries[from]
					}

					sort.Sort(newSorter(metadata, series))

					clear(positions)
					for i, m := range metadata {
						positions[m.Labels.String()] = i
					}

					for _, group := range testCase.expectedGroups {
						if group.fillLeftCarrier == "" {
							continue
						}

						for l := range group.matchedSeries {
							if l == group.fillLeftCarrier {
								continue
							}

							require.Lessf(t, positions[l], positions[group.fillLeftCarrier], "with input order %v, expected the fill-left carrier %v to be sorted after the matched output series %v", permutation, group.fillLeftCarrier, l)
						}
					}
				})
			}

			t.Run("sorting favouring left side", func(t *testing.T) {
				assertCarrierSortedLast(t, func(m []types.SeriesMetadata, s []*oneToOneBinaryOperationOutputSeries) sort.Interface {
					return newFavourLeftSideSorter(m, s)
				})
			})

			t.Run("sorting favouring right side", func(t *testing.T) {
				assertCarrierSortedLast(t, func(m []types.SeriesMetadata, s []*oneToOneBinaryOperationOutputSeries) sort.Interface {
					return newFavourRightSideSorter(m, s)
				})
			})

			types.SeriesMetadataSlicePool.Put(&allMetadata, memoryConsumptionTracker)
			types.BoolSlicePool.Put(&leftSeriesUsed, memoryConsumptionTracker)
			types.BoolSlicePool.Put(&rightSeriesUsed, memoryConsumptionTracker)

			require.NoError(t, o.FinishedReading(ctx))
			o.Close()
		})
	}
}

// forEachPermutation calls f once for every permutation of the indices 0 to n-1.
//
// The slice that forEachPermutation passes to f is valid only during that call.
func forEachPermutation(n int, f func(permutation []int)) {
	permutation := make([]int, n)
	for i := range permutation {
		permutation[i] = i
	}

	var permute func(firstIndexToFix int)

	permute = func(firstIndexToFix int) {
		if firstIndexToFix == n {
			f(permutation)
			return
		}

		for i := firstIndexToFix; i < n; i++ {
			permutation[firstIndexToFix], permutation[i] = permutation[i], permutation[firstIndexToFix]
			permute(firstIndexToFix + 1)
			permutation[firstIndexToFix], permutation[i] = permutation[i], permutation[firstIndexToFix]
		}
	}

	permute(0)
}

func TestOneToOneVectorVectorBinaryOperation_FillLeft_NameRetainingSplitReadOrder(t *testing.T) {
	// The .test files in testdata/ours/binary_operators_fill.test already cover the output values and
	// labels of a name-retaining fill-left split. Pedantic mode there covers pool release.
	//
	// This test pins the read order that the .test files cannot check. The name-dropped output series
	// of a split group holds no result of its own. The last matched read of the group computes the
	// group's fill-left points. So the operator must always return the name-dropped series after every
	// matched series of its group. It must also report an out-of-order read as a bug.
	fillZero := 0.0

	step1 := timestamp.Time(0)
	step2 := step1.Add(5 * time.Minute)
	step3 := step2.Add(5 * time.Minute)
	timeRange := types.NewRangeQueryTimeRange(step1, step3, 5*time.Minute)

	t0 := timestamp.FromTime(step1)
	t1 := timestamp.FromTime(step2)
	t2 := timestamp.FromTime(step3)

	ctx := context.Background()
	memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

	makeData := func(points ...promql.FPoint) types.InstantVectorSeriesData {
		floats, err := types.FPointSlicePool.Get(len(points), memoryConsumptionTracker)
		require.NoError(t, err)
		floats = append(floats, points...)
		return types.InstantVectorSeriesData{Floats: floats}
	}

	// Left present at steps 0 and 2. Right present at steps 1 and 2.
	//   step 1: left missing -> fill 0 -> 0 != 10 -> kept, no __name__ (name-dropped series).
	//   step 2: both present -> 300 != 20 -> kept, keeps __name__ (matched series).
	newOperator := func() (*OneToOneVectorVectorBinaryOperation, []types.SeriesMetadata) {
		leftSeries := []labels.Labels{labels.FromStrings(model.MetricNameLabel, "left_metric", "label", "a")}
		rightSeries := []labels.Labels{labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "a")}

		leftData := []types.InstantVectorSeriesData{makeData(promql.FPoint{T: t0, F: 100}, promql.FPoint{T: t2, F: 300})}
		rightData := []types.InstantVectorSeriesData{makeData(promql.FPoint{T: t1, F: 10}, promql.FPoint{T: t2, F: 20})}

		left := &operators.TestOperator{Series: leftSeries, Data: leftData, MemoryConsumptionTracker: memoryConsumptionTracker}
		right := &operators.TestOperator{Series: rightSeries, Data: rightData, MemoryConsumptionTracker: memoryConsumptionTracker}

		vectorMatching := parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero}}
		o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.NEQ, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
		require.NoError(t, err)

		metadata, err := o.SeriesMetadata(ctx, nil)
		require.NoError(t, err)
		require.Len(t, metadata, 2)

		return o, metadata
	}

	t.Run("the operator returns the name-dropped series last", func(t *testing.T) {
		o, metadata := newOperator()

		require.True(t, metadata[0].Labels.Has(model.MetricNameLabel), "expected the matched series to be returned first")
		require.False(t, metadata[1].Labels.Has(model.MetricNameLabel), "expected the name-dropped series to be returned last")

		actual := map[string][]promql.FPoint{}

		for i := range metadata {
			d, err := o.NextSeries(ctx)
			require.NoError(t, err)
			actual[metadata[i].Labels.String()] = slices.Clone(d.Floats)
			types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
		}

		expected := map[string][]promql.FPoint{
			// Matched series: only the both-present step keeps the left metric name.
			`{__name__="left_metric", label="a"}`: {{T: t2, F: 300}},
			// Name-dropped series: the kept left-filled step drops the metric name.
			`{label="a"}`: {{T: t1, F: 0}},
		}
		require.Equal(t, expected, actual)

		types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
		require.NoError(t, o.FinishedReading(ctx))
		o.Close()
	})

	t.Run("an out-of-order read of the name-dropped series is reported as a bug", func(t *testing.T) {
		o, metadata := newOperator()

		// b.remainingSeries aligns with metadata, so reorder both together to put the name-dropped
		// ({label="a"}) series first.
		metadata[0], metadata[1] = metadata[1], metadata[0]
		o.remainingSeries[0], o.remainingSeries[1] = o.remainingSeries[1], o.remainingSeries[0]

		_, err := o.NextSeries(ctx)
		require.EqualError(t, err, "read the name-dropped fill-left series of a match group before the group was evaluated for operator !=; this indicates a bug in the query engine")

		types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
		require.NoError(t, o.FinishedReading(ctx))
		o.Close()
	})

	// A match group that holds a left series with no __name__ next to a named left series has no
	// name-dropped sibling. The matched output series of the unnamed left series already carries the
	// group's match key labels. So it carries the group's fill-left points as well.
	//
	// Left series left_metric is present at step 0 only. The unnamed left series is present at step 1
	// only. The right side is present at every step.
	//   step 0: left_metric present -> 100 != 1 -> kept, keeps __name__.
	//   step 1: the unnamed left series is present -> 200 != 2 -> kept, labels {label="a"}.
	//   step 2: no left series of the group is present -> fill 0 -> 0 != 3 -> kept, labels {label="a"}.
	newOperatorWithCarrier := func() (*OneToOneVectorVectorBinaryOperation, []types.SeriesMetadata) {
		leftSeries := []labels.Labels{
			labels.FromStrings(model.MetricNameLabel, "left_metric", "label", "a"),
			labels.FromStrings("label", "a"),
		}
		rightSeries := []labels.Labels{labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "a")}

		leftData := []types.InstantVectorSeriesData{
			makeData(promql.FPoint{T: t0, F: 100}),
			makeData(promql.FPoint{T: t1, F: 200}),
		}
		rightData := []types.InstantVectorSeriesData{makeData(promql.FPoint{T: t0, F: 1}, promql.FPoint{T: t1, F: 2}, promql.FPoint{T: t2, F: 3})}

		left := &operators.TestOperator{Series: leftSeries, Data: leftData, MemoryConsumptionTracker: memoryConsumptionTracker}
		right := &operators.TestOperator{Series: rightSeries, Data: rightData, MemoryConsumptionTracker: memoryConsumptionTracker}

		vectorMatching := parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero}}
		o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.NEQ, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
		require.NoError(t, err)

		metadata, err := o.SeriesMetadata(ctx, nil)
		require.NoError(t, err)
		require.Len(t, metadata, 2)

		return o, metadata
	}

	t.Run("the operator returns the fill-left carrier last and merges the group's fill-left points into it", func(t *testing.T) {
		o, metadata := newOperatorWithCarrier()

		require.True(t, metadata[0].Labels.Has(model.MetricNameLabel), "expected the named matched series to be returned first")
		require.False(t, metadata[1].Labels.Has(model.MetricNameLabel), "expected the fill-left carrier to be returned last")

		actual := map[string][]promql.FPoint{}

		for i := range metadata {
			d, err := o.NextSeries(ctx)
			require.NoError(t, err)
			actual[metadata[i].Labels.String()] = slices.Clone(d.Floats)
			types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
		}

		expected := map[string][]promql.FPoint{
			// The named matched series holds only its own point. It must not hold any left-filled point.
			`{__name__="left_metric", label="a"}`: {{T: t0, F: 100}},
			// The carrier holds its own point and the group's one left-filled point.
			`{label="a"}`: {{T: t1, F: 200}, {T: t2, F: 0}},
		}
		require.Equal(t, expected, actual)

		types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
		require.NoError(t, o.FinishedReading(ctx))
		o.Close()
	})

	t.Run("an out-of-order read of the fill-left carrier is reported as a bug", func(t *testing.T) {
		o, metadata := newOperatorWithCarrier()

		// b.remainingSeries aligns with metadata, so reorder both together to put the carrier
		// ({label="a"}) series first.
		metadata[0], metadata[1] = metadata[1], metadata[0]
		o.remainingSeries[0], o.remainingSeries[1] = o.remainingSeries[1], o.remainingSeries[0]

		_, err := o.NextSeries(ctx)
		require.EqualError(t, err, "read the fill-left carrier of a match group before the group was evaluated for operator !=; this indicates a bug in the query engine")

		types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)
		require.NoError(t, o.FinishedReading(ctx))
		o.Close()
	})
}

func TestOneToOneVectorVectorBinaryOperation_FillLeft_ReleasesSplitGroupDataIfStoppedEarly(t *testing.T) {
	// The last matched read of a name-retaining fill-left split group stores the group's fill-left
	// points in the group's split holder. Only the group's name-dropped sibling takes them. A consumer
	// that stops before it reads the sibling must not leak that pooled data.
	//
	// This test stops after each possible number of reads and checks that FinishedReading returns
	// everything to the pools. init_test.go turns on slice mangling. So this test also catches a slice
	// that the operator still references after it returns the slice to a pool.
	fillZero := 0.0

	step1 := timestamp.Time(0)
	step2 := step1.Add(5 * time.Minute)
	step3 := step2.Add(5 * time.Minute)
	timeRange := types.NewRangeQueryTimeRange(step1, step3, 5*time.Minute)

	t0 := timestamp.FromTime(step1)
	t1 := timestamp.FromTime(step2)
	t2 := timestamp.FromTime(step3)

	testCases := map[string]struct {
		leftSeries []labels.Labels
		leftPoints [][]promql.FPoint

		// expectedOutputSeriesCount is the number of output series. That is the number of matched
		// output series of the group plus its one name-dropped sibling.
		expectedOutputSeriesCount int
	}{
		"one left series in the group": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_a", "label", "x"),
			},
			leftPoints: [][]promql.FPoint{
				{{T: t0, F: 100}},
			},
			expectedOutputSeriesCount: 2,
		},
		"two differently named left series in the group": {
			leftSeries: []labels.Labels{
				labels.FromStrings(model.MetricNameLabel, "left_a", "label", "x"),
				labels.FromStrings(model.MetricNameLabel, "left_b", "label", "x"),
			},
			leftPoints: [][]promql.FPoint{
				{{T: t0, F: 100}},
				{{T: t1, F: 200}},
			},
			expectedOutputSeriesCount: 3,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			for seriesToRead := 0; seriesToRead <= testCase.expectedOutputSeriesCount; seriesToRead++ {
				t.Run(fmt.Sprintf("stop after reading %v output series", seriesToRead), func(t *testing.T) {
					ctx := context.Background()
					memoryConsumptionTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)

					makeData := func(points []promql.FPoint) types.InstantVectorSeriesData {
						floats, err := types.FPointSlicePool.Get(len(points), memoryConsumptionTracker)
						require.NoError(t, err)
						floats = append(floats, points...)
						return types.InstantVectorSeriesData{Floats: floats}
					}

					leftData := make([]types.InstantVectorSeriesData, 0, len(testCase.leftPoints))
					for _, points := range testCase.leftPoints {
						leftData = append(leftData, makeData(points))
					}

					// The right side has a sample at every step. The last step has no left sample at
					// all, so the group always has fill-left points for the sibling to take.
					rightSeries := []labels.Labels{labels.FromStrings(model.MetricNameLabel, "right_metric", "label", "x")}
					rightData := []types.InstantVectorSeriesData{makeData([]promql.FPoint{{T: t0, F: 1}, {T: t1, F: 2}, {T: t2, F: 3}})}

					left := &operators.TestOperator{Series: testCase.leftSeries, Data: leftData, MemoryConsumptionTracker: memoryConsumptionTracker}
					right := &operators.TestOperator{Series: rightSeries, Data: rightData, MemoryConsumptionTracker: memoryConsumptionTracker}

					// parser.NEQ without the bool modifier retains __name__. That is what makes the
					// operator split the match group.
					vectorMatching := parser.VectorMatching{Card: parser.CardOneToOne, FillValues: parser.VectorMatchFillValues{LHS: &fillZero}}
					o, err := NewOneToOneVectorVectorBinaryOperation(left, right, vectorMatching, parser.NEQ, false, memoryConsumptionTracker, posrange.PositionRange{}, timeRange, nil, log.NewNopLogger())
					require.NoError(t, err)

					metadata, err := o.SeriesMetadata(ctx, nil)
					require.NoError(t, err)
					require.Len(t, metadata, testCase.expectedOutputSeriesCount)
					types.SeriesMetadataSlicePool.Put(&metadata, memoryConsumptionTracker)

					for i := 0; i < seriesToRead; i++ {
						d, err := o.NextSeries(ctx)
						require.NoErrorf(t, err, "got error while reading output series at index %v", i)
						types.PutInstantVectorSeriesData(d, memoryConsumptionTracker)
					}

					require.NoError(t, o.FinishedReading(ctx))

					// TestOperator keeps the data of any series that the operator never read. The test
					// must release that data, so do it before the check below.
					left.ReleaseUnreadData(memoryConsumptionTracker)
					right.ReleaseUnreadData(memoryConsumptionTracker)

					require.Equalf(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes(), "expected all pooled data to be released, but have %v", memoryConsumptionTracker.DescribeCurrentMemoryConsumption())
					o.Close()
				})
			}
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
