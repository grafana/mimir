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
// to the operation's matching labels are forwarded to child operators (Bug 1 fix).
//
// Without the fix, if a parent passes matchers for labels outside the 'on' set (e.g., region=us
// for an 'on(zone)' operation), those matchers would incorrectly filter the RHS, causing series
// with a different region but the same zone to be discarded.
func TestOneToOneVectorVectorBinaryOperation_MatcherPropagation(t *testing.T) {
	testCases := map[string]struct {
		vectorMatching  parser.VectorMatching
		parentMatchers  types.Matchers
		leftSeries      []labels.Labels
		rightSeries     []labels.Labels
		expectedLHS     types.Matchers
		expectedRHS     types.Matchers
		expectedSeries  int
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
		"without(region): region matcher is filtered out, zone passes through": {
			vectorMatching: parser.VectorMatching{On: false, MatchingLabels: []string{"region"}},
			parentMatchers: types.Matchers{
				{Type: labels.MatchEqual, Name: "region", Value: "us"},
				{Type: labels.MatchEqual, Name: "zone", Value: "1"},
			},
			leftSeries:  []labels.Labels{labels.FromStrings("region", "us", "zone", "1")},
			rightSeries: []labels.Labels{labels.FromStrings("region", "eu", "zone", "1")},
			// region is excluded label in without, so region matcher is dropped.
			expectedLHS:    types.Matchers{{Type: labels.MatchEqual, Name: "zone", Value: "1"}},
			expectedRHS:    types.Matchers{{Type: labels.MatchEqual, Name: "zone", Value: "1"}},
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
// in a grouped (many-to-one or one-to-many) binary operation (Bug 1 fix).
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
				memTracker, annotations.New(), posrange.PositionRange{}, timeRange,
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

// TestAndUnlessBinaryOperation_MatcherPropagation verifies two behaviors introduced by the
// bug fixes:
//
//  1. (Bug 2 fix) The RHS of an 'and'/'unless' operation always receives nil matchers,
//     regardless of what the parent passes. The RHS acts only as a presence filter; passing
//     parent matchers to it can cause wrong results when those matchers cover labels outside
//     the operation's matching set.
//
//  2. (Correctness) An 'and on(zone)' correctly matches LHS and RHS series that share the
//     same zone value even when other labels (e.g., region) differ, because the RHS is not
//     narrowed by the parent's region matchers.
func TestAndUnlessBinaryOperation_MatcherPropagation(t *testing.T) {
	testCases := map[string]struct {
		isUnless       bool
		vectorMatching parser.VectorMatching
		parentMatchers types.Matchers
		leftSeries     []labels.Labels
		rightSeries    []labels.Labels
		// expectedLHSMatchers: what the LHS should receive (full parent matchers, unchanged).
		expectedLHSMatchers types.Matchers
		// RHS always receives nil (Bug 2 fix).
		expectedSeries int
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
			// 'unless' metadata always returns all LHS series; data-level filtering happens
			// in NextSeries. The key check here is that RHS receives nil (not region=us).
			expectedSeries: 1,
		},
		"and: nil parent matchers propagate as nil to both sides": {
			isUnless:       false,
			vectorMatching: parser.VectorMatching{On: true, MatchingLabels: []string{"zone"}},
			parentMatchers: nil,
			leftSeries:     []labels.Labels{labels.FromStrings("zone", "1")},
			rightSeries:    []labels.Labels{labels.FromStrings("zone", "1")},
			expectedLHSMatchers: nil,
			expectedSeries:      1,
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
				left, right, tc.vectorMatching, memTracker, tc.isUnless, timeRange, posrange.PositionRange{},
			)

			outputSeries, err := op.SeriesMetadata(ctx, tc.parentMatchers)
			require.NoError(t, err)

			// LHS receives the full parent matchers unchanged.
			require.Equal(t, tc.expectedLHSMatchers, left.MatchersProvided, "LHS should receive full parent matchers")

			// RHS always receives nil (Bug 2 fix): the RHS is a presence filter and must not
			// be narrowed by parent matchers that cover labels outside the matching set.
			require.Nil(t, right.MatchersProvided, "RHS should always receive nil matchers")

			require.Len(t, outputSeries, tc.expectedSeries, "unexpected number of output series")

			require.NoError(t, op.Finalize(ctx))
			op.Close()
		})
	}
}
