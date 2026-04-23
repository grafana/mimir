// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

func TestAndUnlessBinaryOperation_PassesHintMatchersToRHS(t *testing.T) {
	// Test that matchers derived from LHS series label values are passed to the RHS SeriesMetadata
	// call to narrow what the RHS fetches.
	//
	// For on(...) matching, the matching labels are known from VectorMatching and used directly.
	// For ignoring(...) matching, the effective matching labels are computed as the intersection
	// of label names present in all LHS series, excluding the ignored labels.
	testCases := map[string]struct {
		isUnless       bool
		hints          *Hints
		vectorMatching parser.VectorMatching

		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedRHSMatchers  types.Matchers
		expectedOutputSeries []labels.Labels
	}{
		"and op, on(...): RHS receives matcher derived from LHS label values, filtering non-matching RHS series": {
			isUnless:       false,
			hints:          nil,
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "prod", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
				labels.FromStrings("env", "staging", "series", "right-2"), // filtered out: "staging" not in LHS env values
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "prod", "series", "left-2"),
			},
		},
		"unless op, on(...): RHS receives matchers for all LHS env values; RHS series not in LHS env values are filtered out": {
			isUnless:       true,
			hints:          nil,
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
				labels.FromStrings("env", "dev", "series", "right-2"), // filtered out: "dev" not in LHS env values
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod|staging"},
			},
			// the SeriesMetadata for unless always returns all LHS series; filtering happens later
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"),
			},
		},
		"and op, on() with empty matching labels: RHS receives nil matchers": {
			isUnless:       false,
			hints:          nil,
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: nil},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
			},
			expectedRHSMatchers: nil,
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
			},
		},
		"and op, ignoring(...): RHS receives matchers for labels common to all LHS series, excluding ignored labels": {
			isUnless:       false,
			hints:          nil,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"series"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
				labels.FromStrings("env", "staging", "series", "right-2"),
				labels.FromStrings("env", "dev", "series", "right-3"), // filtered out: "dev" not in LHS env values
			},
			// "series" is excluded (it's in the ignoring set), "env" is common to all LHS series
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod|staging"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"),
			},
		},
		"and op, ignoring(...): labels not common to all LHS series are excluded from matchers to avoid over-narrowing": {
			isUnless:       false,
			hints:          nil,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"series"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"), // no "region" label
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east", "series", "right-1"),
				labels.FromStrings("env", "staging", "series", "right-2"),
				labels.FromStrings("env", "dev", "series", "right-3"), // filtered out
			},
			// "region" is absent from some LHS series, so only "env" (present in all) is used.
			// Using "region" would incorrectly filter out right-2, a valid match for left-2.
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod|staging"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"),
			},
		},
		"and op, ignoring() with no labels to ignore: RHS receives matchers for all LHS labels, filtering non-matching RHS series": {
			isUnless:       false,
			hints:          nil,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: nil},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),    // matches all LHS labels → kept
				labels.FromStrings("env", "prod", "series", "right-1"),   // series differs → filtered out
				labels.FromStrings("env", "staging", "series", "left-1"), // env differs → filtered out
			},
			// All LHS label names (env, series) become hints since ignoring() excludes nothing
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "series", Value: "left-1"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
			},
		},
		"and op, ignoring(...) with heterogeneous LHS and no common non-ignored labels: RHS receives nil matchers": {
			isUnless:       false,
			hints:          nil,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"series"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("region", "us-east", "series", "left-2"), // no "env", different label set
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
				labels.FromStrings("region", "us-east", "series", "right-2"),
			},
			// no labels common to all LHS series (excluding ignored "series"), so no matchers
			expectedRHSMatchers: nil,
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("region", "us-east", "series", "left-2"),
			},
		},
		"planner hints are used as fallback when VectorMatching yields no hints (on() with empty labels)": {
			isUnless:       false,
			hints:          &Hints{Include: []string{"env"}},
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: nil},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
				labels.FromStrings("env", "staging", "series", "right-2"), // filtered by planner hint
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
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
			o := NewAndUnlessBinaryOperation(left, right, testCase.vectorMatching, memoryConsumptionTracker, testCase.isUnless, timeRange, posrange.PositionRange{}, testCase.hints, log.NewNopLogger())

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			require.Equal(t, testCase.expectedRHSMatchers, right.MatchersProvided, "matchers passed to RHS")

			require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)
			require.NoError(t, o.Finalize(ctx))
			o.Close()
		})
	}
}

func TestAndUnlessBinaryOperation_FinalizesInnerOperatorsAsSoonAsPossible(t *testing.T) {
	testCases := map[string]struct {
		isUnless    bool
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedOutputSeries                           []labels.Labels
		expectLeftSideFinalizedAfterOutputSeriesIndex  int
		expectRightSideFinalizedAfterOutputSeriesIndex int
	}{
		"and: reach end of both sides at the same time": {
			isUnless: false,
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
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  2,
			expectRightSideFinalizedAfterOutputSeriesIndex: 2,
		},
		"unless: reach end of both sides at the same time": {
			isUnless: true,
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
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  2,
			expectRightSideFinalizedAfterOutputSeriesIndex: 2,
		},
		"and: no more matches with unmatched series still to read on both sides": {
			isUnless: false,
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
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  1,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"unless: no more matches with unmatched series still to read on both sides": {
			isUnless: true,
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
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  2,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"and: no more matches with unmatched series still to read on left side": {
			isUnless: false,
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
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  1,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"unless: no more matches with unmatched series still to read on left side": {
			isUnless: true,
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
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  2,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"and: no more matches with unmatched series still to read on right side": {
			isUnless: false,
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
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  1,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"unless: no more matches with unmatched series still to read on right side": {
			isUnless: true,
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
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  1,
			expectRightSideFinalizedAfterOutputSeriesIndex: 0,
		},
		"and: some series do not match anything on the right": {
			isUnless: false,
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "1", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  2,
			expectRightSideFinalizedAfterOutputSeriesIndex: 2,
		},
		"and: no series match": {
			isUnless: false,
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "3", "series", "right-1"),
			},

			expectedOutputSeries:                           []labels.Labels{},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  -1,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
		"unless: no series match": {
			isUnless: true,
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "3", "series", "right-1"),
			},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  1,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
		"and: no series on left": {
			isUnless:   false,
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries:                           []labels.Labels{},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  -1,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
		"unless: no series on left": {
			isUnless:   true,
			leftSeries: []labels.Labels{},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "2", "series", "right-2"),
				labels.FromStrings("group", "3", "series", "right-3"),
			},

			expectedOutputSeries:                           []labels.Labels{},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  -1,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
		"and: no series on right": {
			isUnless: false,
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeries:                           []labels.Labels{},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  -1,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
		"unless: no series on right": {
			isUnless: true,
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			rightSeries: []labels.Labels{},

			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "2", "series", "left-2"),
				labels.FromStrings("group", "3", "series", "left-3"),
			},
			expectLeftSideFinalizedAfterOutputSeriesIndex:  2,
			expectRightSideFinalizedAfterOutputSeriesIndex: -1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.expectLeftSideFinalizedAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectLeftSideFinalizedAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectLeftSideFinalizedAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			if testCase.expectRightSideFinalizedAfterOutputSeriesIndex >= len(testCase.expectedOutputSeries) {
				require.Failf(t, "invalid test case", "expectRightSideFinalizedAfterOutputSeriesIndex %v is beyond end of expected output series %v", testCase.expectRightSideFinalizedAfterOutputSeriesIndex, testCase.expectedOutputSeries)
			}

			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
			left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}}
			o := NewAndUnlessBinaryOperation(left, right, vectorMatching, memoryConsumptionTracker, testCase.isUnless, timeRange, posrange.PositionRange{}, nil, log.NewNopLogger())

			outputSeries, err := o.SeriesMetadata(ctx, nil)
			require.NoError(t, err)

			if len(testCase.expectedOutputSeries) == 0 {
				require.Empty(t, outputSeries)
			} else {
				require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedOutputSeries), outputSeries)
			}

			if testCase.expectLeftSideFinalizedAfterOutputSeriesIndex == -1 {
				require.True(t, left.Finalized, "left side should be finalized after SeriesMetadata, but it is not")
			} else {
				require.False(t, left.Finalized, "left side should not be finalized after SeriesMetadata, but it is")
			}

			if testCase.expectRightSideFinalizedAfterOutputSeriesIndex == -1 {
				require.True(t, right.Finalized, "right side should be finalized after SeriesMetadata, but it is not")
			} else {
				require.False(t, right.Finalized, "right side should not be finalized after SeriesMetadata, but it is")
			}

			require.False(t, left.Closed, "left side should not be closed after SeriesMetadata, but it is")
			require.False(t, right.Closed, "right side should not be closed after SeriesMetadata, but it is")

			for outputSeriesIdx := range outputSeries {
				_, err := o.NextSeries(ctx)
				require.NoErrorf(t, err, "got error while reading series at index %v", outputSeriesIdx)

				if outputSeriesIdx >= testCase.expectLeftSideFinalizedAfterOutputSeriesIndex {
					require.Truef(t, left.Finalized, "left side should be finalized after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, left.Finalized, "left side should not be finalized after output series at index %v, but it is", outputSeriesIdx)
				}

				if outputSeriesIdx >= testCase.expectRightSideFinalizedAfterOutputSeriesIndex {
					require.Truef(t, right.Finalized, "right side should be finalized after output series at index %v, but it is not", outputSeriesIdx)
				} else {
					require.Falsef(t, right.Finalized, "right side should not be finalized after output series at index %v, but it is", outputSeriesIdx)
				}
			}

			require.False(t, left.Closed, "left side should not be closed after reading all output series, but it is")
			require.False(t, right.Closed, "right side should not be closed after reading all output series, but it is")

			types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

			_, err = o.NextSeries(ctx)
			require.Equal(t, types.EOS, err)

			require.NoError(t, o.Finalize(ctx))
			require.True(t, left.Finalized, "left side should be finalized after calling Finalize, but it is not")
			require.True(t, right.Finalized, "right side should be finalized after calling Finalize, but it is not")
			// Make sure we've returned everything to their pools.
			require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())

			o.Close()
			require.True(t, left.Closed, "left side should be closed after closing operator, but it isn't")
			require.True(t, right.Closed, "right side should be closed after closing operator, but it isn't")
		})
	}
}

func TestAndUnlessBinaryOperation_ReleasesIntermediateStateIfClosedEarly(t *testing.T) {
	testCases := map[string]struct {
		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedAndOutputSeries    []labels.Labels
		expectedUnlessOutputSeries []labels.Labels
	}{
		"closed after only reading series for current output group": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "2", "series", "right-3"),
			},

			expectedAndOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded presence for group="1", but nothing for group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectedUnlessOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded presence for group="1", but nothing for group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
		},
		"closed after reading series for multiple output groups": {
			leftSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"),
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("group", "2", "series", "right-1"),
				labels.FromStrings("group", "1", "series", "right-2"),
				labels.FromStrings("group", "1", "series", "right-3"),
				labels.FromStrings("group", "2", "series", "right-4"),
			},

			expectedAndOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded presence for group="1" and part of group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
			},
			expectedUnlessOutputSeries: []labels.Labels{
				labels.FromStrings("group", "1", "series", "left-1"), // When we read this series, we'll have loaded presence for group="1" and part of group="2".
				labels.FromStrings("group", "1", "series", "left-2"),
				labels.FromStrings("group", "2", "series", "left-3"),
				labels.FromStrings("group", "3", "series", "left-4"),
			},
		},
	}

	for name, isUnless := range map[string]bool{"and": false, "unless": true} {
		t.Run(name, func(t *testing.T) {
			for name, testCase := range testCases {
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					timeRange := types.NewInstantQueryTimeRange(time.Now())
					memoryConsumptionTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")
					left := &operators.TestOperator{Series: testCase.leftSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.leftSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
					right := &operators.TestOperator{Series: testCase.rightSeries, Data: make([]types.InstantVectorSeriesData, len(testCase.rightSeries)), MemoryConsumptionTracker: memoryConsumptionTracker}
					vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"group"}}
					o := NewAndUnlessBinaryOperation(left, right, vectorMatching, memoryConsumptionTracker, isUnless, timeRange, posrange.PositionRange{}, nil, log.NewNopLogger())

					outputSeries, err := o.SeriesMetadata(ctx, nil)
					require.NoError(t, err)

					if isUnless {
						require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedUnlessOutputSeries), outputSeries)
					} else {
						require.Equal(t, testutils.LabelsToSeriesMetadata(testCase.expectedAndOutputSeries), outputSeries)
					}
					types.SeriesMetadataSlicePool.Put(&outputSeries, memoryConsumptionTracker)

					// Read the first output series to trigger the loading of some intermediate state for at least one of the output groups.
					_, err = o.NextSeries(ctx)
					require.NoError(t, err)

					// Finalize the operator and confirm that we've returned everything to their pools.
					require.NoError(t, o.Finalize(ctx))
					require.Equal(t, uint64(0), memoryConsumptionTracker.CurrentEstimatedMemoryConsumptionBytes())
					o.Close()
				})
			}
		})
	}
}

// BenchmarkAndUnlessDerivedHints measures the cost of SeriesMetadata for and/unless operations
// and validates that the optimisation actually narrows the RHS series fetched.
//
// The scenario has LHS series covering 2 out of 10 env values, and a large RHS spread
// across all 10 env values. The optimised paths (on/ignoring) should derive matchers that
// filter the RHS to ~20% of total, which is visible in the rhs_series_fetched metric.
func BenchmarkAndUnlessDerivedHints(b *testing.B) {
	const totalEnvValues = 10
	const lhsEnvValues = 2 // LHS only uses env-0 and env-1

	makeLHS := func(count int) []labels.Labels {
		out := make([]labels.Labels, count)
		for i := range count {
			out[i] = labels.FromStrings(
				"__name__", "http_requests_total",
				"env", fmt.Sprintf("env-%d", i%lhsEnvValues),
				"handler", fmt.Sprintf("/api/v%d", i%5),
				"pod", fmt.Sprintf("pod-%d", i),
			)
		}
		return out
	}

	makeRHS := func(count int) []labels.Labels {
		out := make([]labels.Labels, count)
		for i := range count {
			out[i] = labels.FromStrings(
				"__name__", "up",
				"env", fmt.Sprintf("env-%d", i%totalEnvValues),
				"handler", fmt.Sprintf("/api/v%d", i%5),
				"pod", fmt.Sprintf("pod-%d", i),
			)
		}
		return out
	}

	for _, rhsCount := range []int{1_000, 10_000} {
		lhsSeries := makeLHS(100)
		rhsSeries := makeRHS(rhsCount)

		// on(env): matching labels are static — derived directly from VectorMatching.
		// Narrows RHS to lhsEnvValues/totalEnvValues of total series.
		b.Run(fmt.Sprintf("on(env)/rhs=%d", rhsCount), func(b *testing.B) {
			vm := parser.VectorMatching{On: true, MatchingLabels: []string{"env"}}
			runAndUnlessBench(b, lhsSeries, rhsSeries, vm)
		})

		// ignoring(handler): matching labels computed at runtime as the intersection of
		// non-ignored label names across all LHS series.
		// env and pod are common to all LHS series, so both become RHS matchers.
		b.Run(fmt.Sprintf("ignoring(handler)/rhs=%d", rhsCount), func(b *testing.B) {
			vm := parser.VectorMatching{On: false, MatchingLabels: []string{"handler"}}
			runAndUnlessBench(b, lhsSeries, rhsSeries, vm)
		})

		// Baseline: on() with no matching labels produces no hints, so the full RHS is fetched.
		// rhs_series_fetched should equal rhsCount, confirming no narrowing occurs.
		b.Run(fmt.Sprintf("baseline_no_hints/rhs=%d", rhsCount), func(b *testing.B) {
			vm := parser.VectorMatching{On: true, MatchingLabels: nil}
			runAndUnlessBench(b, lhsSeries, rhsSeries, vm)
		})
	}
}

// runAndUnlessBench is a helper that benchmarks a single SeriesMetadata call for the given
// VectorMatching configuration. It reports rhs_series_fetched — the number of RHS series that
// survived the derived matcher filter — so callers can confirm the optimisation is active.
func runAndUnlessBench(b *testing.B, lhsSeries, rhsSeries []labels.Labels, vm parser.VectorMatching) {
	b.Helper()

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	// Pre-allocate backing arrays to amortise slice allocation cost across iterations.
	lhsBuf := make([]labels.Labels, len(lhsSeries))
	rhsBuf := make([]labels.Labels, len(rhsSeries))

	var rhsSeriesFetched int
	for b.Loop() {
		// TestOperator.SeriesMetadata filters t.Series in-place, so each iteration needs
		// a fresh copy of the slice (the underlying labels.Labels values are immutable).
		copy(lhsBuf, lhsSeries)
		copy(rhsBuf, rhsSeries)

		memTracker := limiter.NewUnlimitedMemoryConsumptionTracker(ctx)
		left := &operators.TestOperator{
			Series:                   lhsBuf,
			Data:                     make([]types.InstantVectorSeriesData, len(lhsBuf)),
			MemoryConsumptionTracker: memTracker,
		}
		right := &operators.TestOperator{
			Series:                   rhsBuf,
			Data:                     make([]types.InstantVectorSeriesData, len(rhsBuf)),
			MemoryConsumptionTracker: memTracker,
		}

		o := NewAndUnlessBinaryOperation(left, right, vm, memTracker, false, timeRange, posrange.PositionRange{}, nil, log.NewNopLogger())
		outputSeries, err := o.SeriesMetadata(ctx, nil)
		if err != nil {
			b.Fatal(err)
		}

		rhsSeriesFetched = len(right.Series)

		types.SeriesMetadataSlicePool.Put(&outputSeries, memTracker)
		if err := o.Finalize(ctx); err != nil {
			b.Fatal(err)
		}
		o.Close()
	}

	b.ReportMetric(float64(rhsSeriesFetched), "rhs_series_fetched")
}
