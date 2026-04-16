// SPDX-License-Identifier: AGPL-3.0-only

package binops

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// TestOneToOneVectorVectorBinaryOperation_MatcherPropagation verifies that when a parent
// binary operation passes matchers to a 1:1 binary operation, only the matchers relevant
// to the operation's matching labels are forwarded to child operators.
//
// Without the fix, if a parent passes matchers for labels outside the 'on' set (e.g., region=us
// for an 'on(zone)' operation), those matchers would incorrectly filter the RHS, causing series
// with a different region but the same zone to be discarded.
func TestOneToOneVectorVectorBinaryOperation_MatcherPropagation(t *testing.T) {
	testCases := map[string]struct {
		vectorMatching parser.VectorMatching
		parentMatchers types.Matchers
		leftSeries     []labels.Labels
		rightSeries    []labels.Labels
		expectedLHS    types.Matchers
		expectedRHS    types.Matchers
		expectedSeries int
	}{
		"on(zone): region matcher is filtered out before reaching children": {
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
				{Type: labels.MatchEqual, Name: "zone", Value: "1"},
			},
			// LHS matches zone=1, RHS has different region but same zone.
			// Without the fix, region=us would be forwarded to RHS, filtering out b{region=eu}.
			leftSeries:  []labels.Labels{labels.FromStrings("region", "us", "zone", "1")},
			rightSeries: []labels.Labels{labels.FromStrings("region", "eu", "zone", "1")},
			// Only zone=1 should reach both sides.
			expectedLHS:    types.Matchers{{Type: labels.MatchEqual, Name: "zone", Value: "1"}},
			expectedRHS:    types.Matchers{{Type: labels.MatchEqual, Name: "zone", Value: "1"}},
			expectedSeries: 1, // they match on zone=1 even though region differs
		},
		"on(): no matchers reach children": {
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{}},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			leftSeries:     []labels.Labels{labels.FromStrings("region", "us")},
			rightSeries:    []labels.Labels{labels.FromStrings("region", "eu")},
			expectedLHS:    nil,
			expectedRHS:    nil,
			expectedSeries: 1, // on() matches everything to everything
		},
		"without(region): region matcher is filtered out on LHS; RHS receives regexp matcher built from LHS series": {
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"region"}},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
				{Type: labels.MatchEqual, Name: "zone", Value: "1"},
			},
			leftSeries:  []labels.Labels{labels.FromStrings("region", "us", "zone", "1")},
			rightSeries: []labels.Labels{labels.FromStrings("region", "eu", "zone", "1")},
			// LHS receives the filtered parent matchers (region excluded, zone kept).
			expectedLHS: types.Matchers{{Type: labels.MatchEqual, Name: "zone", Value: "1"}},
			// RHS receives regexp matchers built from LHS series for all non-excluded labels.
			// The LHS has zone=1, so RHS gets zone=~"1".
			expectedRHS:    types.Matchers{{Type: labels.MatchRegexp, Name: "zone", Value: "1"}},
			expectedSeries: 1,
		},
		"no parent matchers: both sides receive nil": {
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			parentMatchers: nil,
			leftSeries:     []labels.Labels{labels.FromStrings("zone", "1")},
			rightSeries:    []labels.Labels{labels.FromStrings("zone", "1")},
			expectedLHS:    nil,
			expectedRHS:    nil,
			expectedSeries: 1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

			left := &operators.TestOperator{
				Series:                   tc.leftSeries,
				Data:                     make([]types.InstantVectorSeriesData, len(tc.leftSeries)),
				MemoryConsumptionTracker: memTracker,
			}
			right := &operators.TestOperator{
				Series:                   tc.rightSeries,
				Data:                     make([]types.InstantVectorSeriesData, len(tc.rightSeries)),
				MemoryConsumptionTracker: memTracker,
			}

			op, err := NewOneToOneVectorVectorBinaryOperation(
				left, right, tc.vectorMatching, parser.ADD, false,
				memTracker, annotations.New(), posrange.PositionRange{}, timeRange, nil, log.NewNopLogger(),
			)
			require.NoError(t, err)

			outputSeries, err := op.SeriesMetadata(ctx, tc.parentMatchers)
			require.NoError(t, err)

			require.Equal(t, tc.expectedLHS, left.MatchersProvided, "LHS should receive filtered matchers")
			require.Equal(t, tc.expectedRHS, right.MatchersProvided, "RHS should receive filtered matchers")
			require.Len(t, outputSeries, tc.expectedSeries, "unexpected number of output series")

			require.NoError(t, op.Finalize(ctx))
			op.Close()
		})
	}
}

// TestGroupedVectorVectorBinaryOperation_MatcherPropagation verifies that matchers are
// filtered to the operation's matching labels before being forwarded to child operators
// in a grouped (many-to-one or one-to-many) binary operation.
func TestGroupedVectorVectorBinaryOperation_MatcherPropagation(t *testing.T) {
	testCases := map[string]struct {
		vectorMatching parser.VectorMatching
		parentMatchers types.Matchers
		leftSeries     []labels.Labels
		rightSeries    []labels.Labels
		expectedLHS    types.Matchers
		expectedRHS    types.Matchers
		expectedSeries int
	}{
		"on(zone) group_left: region matcher filtered out before reaching children": {
			vectorMatching: parser.VectorMatching{
				On:             true,
				MatchingLabels: []string{"zone"},
				Card:           parser.CardManyToOne,
			},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
				{Type: labels.MatchEqual, Name: "zone", Value: "1"},
			},
			// Multiple left series with zone=1, one right series with different region but same zone.
			// Without the fix, region=us would filter out the right series.
			leftSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "1"),
				labels.FromStrings("region", "us", "zone", "2"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("region", "eu", "zone", "1"),
			},
			expectedLHS: types.Matchers{{Type: labels.MatchEqual, Name: "zone", Value: "1"}},
			expectedRHS: types.Matchers{{Type: labels.MatchEqual, Name: "zone", Value: "1"}},
			// left[zone=1] matches right[zone=1]; left[zone=2] has no match.
			expectedSeries: 1,
		},
		"no parent matchers: both sides receive nil": {
			vectorMatching: parser.VectorMatching{
				On:             true,
				MatchingLabels: []string{"zone"},
				Card:           parser.CardManyToOne,
			},
			parentMatchers: nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("zone", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("zone", "1"),
			},
			expectedLHS:    nil,
			expectedRHS:    nil,
			expectedSeries: 1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

			left := &operators.TestOperator{
				Series:                   tc.leftSeries,
				Data:                     make([]types.InstantVectorSeriesData, len(tc.leftSeries)),
				MemoryConsumptionTracker: memTracker,
			}
			right := &operators.TestOperator{
				Series:                   tc.rightSeries,
				Data:                     make([]types.InstantVectorSeriesData, len(tc.rightSeries)),
				MemoryConsumptionTracker: memTracker,
			}

			op, err := NewGroupedVectorVectorBinaryOperation(
				left, right, tc.vectorMatching, parser.ADD, false,
				memTracker, annotations.New(), posrange.PositionRange{}, timeRange, nil, log.NewNopLogger(),
			)
			require.NoError(t, err)

			outputSeries, err := op.SeriesMetadata(ctx, tc.parentMatchers)
			require.NoError(t, err)

			require.Equal(t, tc.expectedLHS, left.MatchersProvided, "LHS should receive filtered matchers")
			require.Equal(t, tc.expectedRHS, right.MatchersProvided, "RHS should receive filtered matchers")
			require.Len(t, outputSeries, tc.expectedSeries, "unexpected number of output series")

			require.NoError(t, op.Finalize(ctx))
			op.Close()
		})
	}
}

// TestAndUnlessBinaryOperation_MatcherPropagation verifies 3 behaviors:
//
//  1. Parent matchers are never forwarded to the RHS of an 'and'/'unless'
//     operation. The RHS acts only as a presence filter; passing parent matchers to it
//     can cause wrong results when those matchers cover labels outside the matching set.
//     When no hints are set and 'on' matching is used, the RHS receives nil.
//
//  2. (Correctness) An 'and on(zone)' correctly matches LHS and RHS series that share the
//     same zone value even when other labels (e.g., region) differ, because the RHS is not
//     narrowed by the parent's region matchers.
//
//  3. (Optimization) When hints are set by the optimizer, the RHS receives LHS-derived
//     matchers for the hinted labels rather than nil. This narrows the RHS fetch without
//     incorrectly filtering it, because any RHS series whose hinted-label values don't
//     appear in the LHS cannot affect the result.
func TestAndUnlessBinaryOperation_MatcherPropagation(t *testing.T) {
	testCases := map[string]struct {
		isUnless       bool
		hints          *Hints
		vectorMatching parser.VectorMatching
		parentMatchers types.Matchers
		leftSeries     []labels.Labels
		rightSeries    []labels.Labels
		// expectedLHSMatchers: what the LHS should receive (full parent matchers, unchanged).
		expectedLHSMatchers types.Matchers
		// expectedRHSMatchers: what the RHS should receive. Nil when no hints are set and
		// 'on' matching is used (parent matchers must not be forwarded). Non-nil when hints
		// drive LHS-derived matchers to the RHS.
		expectedRHSMatchers types.Matchers
		expectedSeries      int
	}{
		"and on(zone): parent region=us matcher does not filter RHS": {
			isUnless:       false,
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			// LHS filtered by region=us (it has region=us → kept).
			leftSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "1"),
			},
			// RHS has a different region. Without the fix, region=us would be forwarded to
			// the RHS, filtering out b{region=eu,zone=1} and causing a{region=us,zone=1} to
			// have no match in the 'and', resulting in empty output.
			rightSeries: []labels.Labels{
				labels.FromStrings("region", "eu", "zone", "1"),
			},
			expectedLHSMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			expectedRHSMatchers: nil,
			// a{region=us,zone=1} matches b{region=eu,zone=1} on zone=1 → output is a{region=us,zone=1}
			expectedSeries: 1,
		},
		"unless on(zone): parent region=us matcher does not filter RHS": {
			isUnless:       true,
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			leftSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "1"),
			},
			// RHS has a different region. Without the fix, region=us would filter out
			// b{region=eu,zone=1}, so the 'unless' would incorrectly keep a{region=us,zone=1}
			// (since b appears absent). With the fix, b is kept and will filter out a at data
			// level (verified by the e2e test in binary_operators.test).
			rightSeries: []labels.Labels{
				labels.FromStrings("region", "eu", "zone", "1"),
			},
			expectedLHSMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			expectedRHSMatchers: nil,
			// 'unless' metadata always returns all LHS series; data-level filtering happens
			// in NextSeries. The key check here is that RHS receives nil (not region=us).
			expectedSeries: 1,
		},
		"and: nil parent matchers propagate as nil to both sides": {
			isUnless:            false,
			vectorMatching:      parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			parentMatchers:      nil,
			leftSeries:          []labels.Labels{labels.FromStrings("zone", "1")},
			rightSeries:         []labels.Labels{labels.FromStrings("zone", "1")},
			expectedLHSMatchers: nil,
			expectedRHSMatchers: nil,
			expectedSeries:      1,
		},
		"and with hints: RHS receives LHS-derived zone matchers, not parent region matchers": {
			isUnless:       false,
			hints:          &Hints{Include: []string{"zone"}},
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			leftSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "1"),
			},
			// RHS has a different region. With hints, the RHS should receive zone=~"1"
			// (derived from LHS zone values), not the parent's region=us.
			rightSeries: []labels.Labels{
				labels.FromStrings("region", "eu", "zone", "1"),
			},
			expectedLHSMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "zone", Value: "1"},
			},
			expectedSeries: 1,
		},
		"and without(zone): RHS receives matchers built from LHS non-excluded labels at runtime": {
			// without(zone) means match on all labels except zone. buildMatchersForWithout
			// collects region from the LHS and passes region=~"us" to the RHS so it can
			// narrow its series fetch without any plan-time hints.
			isUnless:       false,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"zone"}},
			parentMatchers: nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "2"),
				labels.FromStrings("region", "eu", "zone", "1"),
			},
			expectedLHSMatchers: nil,
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "region", Value: "us"},
			},
			// a{region=us,zone=1} matches b{region=us,zone=2} on region (excluding zone).
			expectedSeries: 1,
		},
		"unless without(zone): RHS receives matchers built from LHS non-excluded labels at runtime": {
			isUnless:       true,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"zone"}},
			parentMatchers: nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "1"),
			},
			// b{region=us,zone=2} matches a{region=us,zone=1} on region → a is suppressed.
			rightSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "2"),
			},
			expectedLHSMatchers: nil,
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "region", Value: "us"},
			},
			// Metadata phase still returns all LHS series; data-level suppression happens in NextSeries.
			expectedSeries: 1,
		},
		"and without(zone) all labels excluded: RHS receives nil when no non-excluded labels remain": {
			// LHS has only zone (plus __name__). After excluding zone, buildMatchersForWithout
			// finds no non-excluded labels and returns nil, so RHS is not narrowed.
			isUnless:       false,
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"zone"}},
			parentMatchers: nil,
			leftSeries: []labels.Labels{
				labels.FromStrings("zone", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("zone", "1"),
			},
			expectedLHSMatchers: nil,
			expectedRHSMatchers: nil,
			expectedSeries:      1,
		},
		"unless with hints: RHS receives LHS-derived zone matchers, not parent region matchers": {
			isUnless:       true,
			hints:          &Hints{Include: []string{"zone"}},
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			leftSeries: []labels.Labels{
				labels.FromStrings("region", "us", "zone", "1"),
			},
			rightSeries: []labels.Labels{
				labels.FromStrings("region", "eu", "zone", "1"),
			},
			expectedLHSMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
			},
			expectedRHSMatchers: types.Matchers{
				{Type: labels.MatchRegexp, Name: "zone", Value: "1"},
			},
			// 'unless' metadata always returns all LHS series; data-level filtering happens
			// in NextSeries. The key check here is that RHS receives the hint-derived zone
			// matcher (not the parent's region=us), so b{region=eu,zone=1} is still fetched.
			expectedSeries: 1,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			memTracker := limiter.NewMemoryConsumptionTracker(ctx, 0, nil, "")

			left := &operators.TestOperator{
				Series:                   tc.leftSeries,
				Data:                     make([]types.InstantVectorSeriesData, len(tc.leftSeries)),
				MemoryConsumptionTracker: memTracker,
			}
			right := &operators.TestOperator{
				Series:                   tc.rightSeries,
				Data:                     make([]types.InstantVectorSeriesData, len(tc.rightSeries)),
				MemoryConsumptionTracker: memTracker,
			}

			op := NewAndUnlessBinaryOperation(
				left, right, tc.vectorMatching, memTracker, tc.isUnless, timeRange, posrange.PositionRange{}, tc.hints, log.NewNopLogger(),
			)

			outputSeries, err := op.SeriesMetadata(ctx, tc.parentMatchers)
			require.NoError(t, err)

			// LHS receives the full parent matchers unchanged.
			require.Equal(t, tc.expectedLHSMatchers, left.MatchersProvided, "LHS should receive full parent matchers")

			// RHS must never receive parent matchers directly. When no hints are set and 'on'
			// matching is used it receives nil; when hints are set it receives LHS-derived matchers.
			require.Equal(t, tc.expectedRHSMatchers, right.MatchersProvided, "RHS should receive expected matchers")

			require.Len(t, outputSeries, tc.expectedSeries, "unexpected number of output series")

			require.NoError(t, op.Finalize(ctx))
			op.Close()
		})
	}
}
