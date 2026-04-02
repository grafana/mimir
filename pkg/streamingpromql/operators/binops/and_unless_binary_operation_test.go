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
	// Test that when hints are provided, matchers derived from the LHS series' label values are
	// passed to the RHS SeriesMetadata call to narrow what the RHS fetches. When hints are nil,
	// the RHS receives nil matchers (which is what happened before).
	testCases := map[string]struct {
		isUnless bool
		hints    *Hints

		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedRHSMatchers  types.Matchers
		expectedOutputSeries []labels.Labels
	}{
		"and op with hints: RHS receives matcher derived from LHS label values, filtering non-matching RHS series": {
			isUnless: false,
			hints:    &Hints{Include: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "prod", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
				labels.FromStrings("env", "staging", "series", "right-2"), // these labels will be filtered out by hint matcher because they're not in RHS
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "prod", "series", "left-2"),
			},
		},
		"unless op with hints: RHS receives matchers for all LHS env values; RHS series not in LHS env values are filtered out": {
			isUnless: true,
			hints:    &Hints{Include: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
				labels.FromStrings("env", "dev", "series", "right-2"), // filtered out by hint matcher like above
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod|staging"},
			},
			// the SeriesMetadata for unless should always returns all of the LHS series, filtering happens in a different part of the pipeline
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"),
			},
		},
		"and op with no hints: RHS receives nil matchers": {
			isUnless: false,
			hints:    nil,
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
		"unless op with no hints: RHS receives nil matchers": {
			isUnless: true,
			hints:    nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				labels.FromStrings("env", "staging", "series", "left-2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "right-1"),
			},
			expectedRHSMatchers: nil,
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "series", "left-1"),
				// the SeriesMetadata for unless should always returns all of the LHS series, filtering happens in a different part of the pipeline
				labels.FromStrings("env", "staging", "series", "left-2"),
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
			vectorMatching := parser.VectorMatching{On: true, MatchingLabels: []string{"env"}}
			o := NewAndUnlessBinaryOperation(left, right, vectorMatching, memoryConsumptionTracker, testCase.isUnless, timeRange, posrange.PositionRange{}, testCase.hints, log.NewNopLogger())

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

func TestAndUnlessBinaryOperation_PassesWithoutDerivedMatchersToRHS(t *testing.T) {
	// When hints are nil but the operator uses without or default (no on/without) matching,
	// matchers derived from LHS label values should be passed to the RHS to narrow what it
	// fetches. When on-matching is used without hints, nil matchers are passed.
	testCases := map[string]struct {
		isUnless       bool
		vectorMatching parser.VectorMatching

		leftSeries  []labels.Labels
		rightSeries []labels.Labels

		expectedRHSMatchers  types.Matchers
		expectedOutputSeries []labels.Labels
	}{
		"and op with without(foo) matching: RHS receives matchers for non-excluded labels": {
			isUnless:       false,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"foo"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "bar", "region", "us-east"),
				labels.FromStrings("env", "prod", "foo", "baz", "region", "eu-west"),
			},
			rightSeries: []labels.Labels{
				// Matched by LHS group {env="prod", region="us-east"}.
				labels.FromStrings("env", "prod", "foo", "x", "region", "us-east"),
				// Matched by LHS group {env="prod", region="eu-west"}.
				labels.FromStrings("env", "prod", "foo", "y", "region", "eu-west"),
				// Filtered out by derived env matcher (env="staging" not in LHS).
				labels.FromStrings("env", "staging", "foo", "z", "region", "us-east"),
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "region", Value: "eu-west|us-east"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "bar", "region", "us-east"),
				labels.FromStrings("env", "prod", "foo", "baz", "region", "eu-west"),
			},
		},
		"unless op with without(foo) matching: RHS receives matchers for non-excluded labels": {
			isUnless:       true,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"foo"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "bar", "region", "us-east"),
				labels.FromStrings("env", "staging", "foo", "baz", "region", "eu-west"),
			},
			rightSeries: []labels.Labels{
				// Matches LHS group {env="prod", region="us-east"}, so this LHS series is suppressed by unless.
				labels.FromStrings("env", "prod", "foo", "x", "region", "us-east"),
				// Filtered out by derived env matcher (env="dev" not in LHS).
				labels.FromStrings("env", "dev", "foo", "y", "region", "us-east"),
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod|staging"},
				{Type: labels.MatchRegexp, Name: "region", Value: "eu-west|us-east"},
			},
			// unless returns all LHS series; filtering of values happens in NextSeries
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "foo", "bar", "region", "us-east"),
				labels.FromStrings("env", "staging", "foo", "baz", "region", "eu-west"),
			},
		},
		"and op with default matching (no on/without): RHS receives matchers for all non-__name__ labels": {
			isUnless:       false,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
				labels.FromStrings("env", "staging", "region", "us-east"), // filtered by hint matcher
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
				{Type: labels.MatchRegexp, Name: "region", Value: "us-east"},
			},
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
		},
		"and op with on matching and no hints: RHS receives nil matchers": {
			isUnless:       false,
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"env"}},
			leftSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
			},
			expectedRHSMatchers: nil,
			expectedOutputSeries: []labels.Labels{
				labels.FromStrings("env", "prod", "region", "us-east"),
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
			o := NewAndUnlessBinaryOperation(left, right, testCase.vectorMatching, memoryConsumptionTracker, testCase.isUnless, timeRange, posrange.PositionRange{}, nil, log.NewNopLogger())

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

// BenchmarkAndUnlessBinaryOperation_WithoutDerivedRHSFiltering measures the benefit of
// building RHS matchers from LHS label values when using without/default matching (no hints).
//
// Setup: LHS has lhsEnvs distinct env values; RHS has rhsEnvsTotal distinct env values,
// so (rhsEnvsTotal - lhsEnvs) / rhsEnvsTotal of RHS series should be filtered away.
// The benchmark reports rhs-series/op so the effect of filtering is directly visible.
func BenchmarkAndUnlessBinaryOperation_WithoutDerivedRHSFiltering(b *testing.B) {
	const (
		lhsEnvs      = 10
		rhsEnvsTotal = 100 // 90 % of RHS envs have no LHS match
	)

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	// LHS: one series per env-0 … env-9, with kind="a" to distinguish from RHS.
	lhsSeries := make([]labels.Labels, lhsEnvs)
	for i := range lhsEnvs {
		lhsSeries[i] = labels.FromStrings("env", fmt.Sprintf("env-%d", i), "kind", "a")
	}

	// RHS: one series per env-0 … env-99, with kind="b".
	// Only env-0 … env-9 have a matching LHS group under without(kind) semantics.
	allRHSSeries := make([]labels.Labels, rhsEnvsTotal)
	for i := range rhsEnvsTotal {
		allRHSSeries[i] = labels.FromStrings("env", fmt.Sprintf("env-%d", i), "kind", "b")
	}

	run := func(b *testing.B, vectorMatching parser.VectorMatching, isUnless bool) {
		b.Helper()
		b.ReportAllocs()

		var totalRHSSeries int

		for b.Loop() {
			// Fresh operators are required each iteration because TestOperator mutates its
			// Series slice in-place when matchers are applied to it.
			memTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

			left := &operators.TestOperator{
				Series:                   append([]labels.Labels(nil), lhsSeries...),
				MemoryConsumptionTracker: memTracker,
			}
			right := &operators.TestOperator{
				Series:                   append([]labels.Labels(nil), allRHSSeries...),
				Data:                     make([]types.InstantVectorSeriesData, len(allRHSSeries)),
				MemoryConsumptionTracker: memTracker,
			}

			o := NewAndUnlessBinaryOperation(
				left, right, vectorMatching, memTracker, isUnless,
				timeRange, posrange.PositionRange{}, nil, log.NewNopLogger(),
			)

			if _, err := o.SeriesMetadata(ctx, nil); err != nil {
				b.Fatal(err)
			}

			// right.Series has been filtered in-place by any matchers passed to it,
			// so its length reflects how many RHS series were actually fetched.
			totalRHSSeries += len(right.Series)

			if err := o.Finalize(ctx); err != nil {
				b.Fatal(err)
			}
			o.Close()
		}

		b.ReportMetric(float64(totalRHSSeries)/float64(b.N), "rhs-series/op")
	}

	// without(kind): the new optimization derives env-value matchers from LHS, so only
	// the 10 matching RHS series are fetched instead of all 100.
	b.Run("and/without_derived_filtering", func(b *testing.B) {
		run(b, parser.VectorMatching{On: false, MatchingLabels: []string{"kind"}}, false)
	})
	b.Run("unless/without_derived_filtering", func(b *testing.B) {
		run(b, parser.VectorMatching{On: false, MatchingLabels: []string{"kind"}}, true)
	})

	// on(env) without hints: no matchers are derived, so all 100 RHS series are fetched.
	// This serves as the baseline showing the cost without the optimization.
	b.Run("and/no_filtering", func(b *testing.B) {
		run(b, parser.VectorMatching{On: true, MatchingLabels: []string{"env"}}, false)
	})
	b.Run("unless/no_filtering", func(b *testing.B) {
		run(b, parser.VectorMatching{On: true, MatchingLabels: []string{"env"}}, true)
	})
}
