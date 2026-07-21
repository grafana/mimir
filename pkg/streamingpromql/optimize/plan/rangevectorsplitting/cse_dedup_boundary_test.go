// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// These tests show how enabling range vector splitting reorganizes the query plan around common-subexpression
// elimination, and where that changes how much data is buffered.
//
// Common-subexpression elimination inserts a Duplicate node at the point a shared subexpression is buffered so it can
// be fed to multiple consumers. The result type of that Duplicate's inner node determines how much is buffered per
// series (see MaterializeDuplicate in commonsubexpressionelimination/node.go):
//
//   - an instant vector (rate(...) result) -> InstantVectorDuplicationBuffer, ~1 value per series.
//   - a range vector (raw MatrixSelector)  -> RangeVectorDuplicationBuffer, the full range of raw samples per series.
//
// Enabling range vector splitting wraps each branch's rate(...) in its own SplitFunctionCall. Splitting works by
// subdividing the range of a range-vector function into sub-intervals, evaluating the function over each, and combining
// the results, so its inner node must be a MatrixSelector: only a range vector has a [range] to partition (an instant
// vector does not). That is why trySplitFunction requires the function's child to be a SplitNode whose RangeParams.Range
// exceeds the split interval - i.e. a MatrixSelector - and why the SplitFunctionCall sits directly above rate, on top of
// the MatrixSelector.
//
// When subset-selector elimination has merged two overlapping selectors, the shared point can no longer be the rate
// result, so the Duplicate is pushed below rate onto that raw MatrixSelector - the buffer switches from instant vector
// to range vector even though split_range_vectors ends up 0 when the split falls back to unsplit at materialize time.

const (
	// {a="1"} is a strict subset of the bare selector, so subset-selector elimination merges the two into one
	// MatrixSelector shared via a Duplicate, with a DuplicateFilter selecting the subset branch. The 3h range exceeds
	// the 2h split interval so splitting applies (see trySplitFunction, "too_short_interval").
	subsetSelectorQuery = `rate(foo{a="1"}[3h]) / rate(foo[3h])`

	// Both operands are the identical subexpression, so plain CSE (no subset-selector elimination) shares them.
	identicalSubexpressionQuery = `rate(foo[3h]) + rate(foo[3h])`
)

// TestRangeVectorSplitting_SubsetSelectorElimination_MovesDuplicationBoundaryToRawSamples shows that with
// subset-selector elimination, enabling splitting pushes the CSE Duplicate down from the rate result onto the raw
// MatrixSelector, so the duplication buffer holds the full range of raw samples per series instead of one value per
// series.
func TestRangeVectorSplitting_SubsetSelectorElimination_MovesDuplicationBoundaryToRawSamples(t *testing.T) {
	t.Run("splitting off - Duplicate buffers the instant vector rate result", func(t *testing.T) {
		plan := buildInstantQueryPlan(t, subsetSelectorQuery, false)

		// The Duplicate (the shared, buffered subexpression) sits directly above the rate(...) result.
		expectedPlan := `
			- BinaryExpression: LHS / RHS, hints exclude ()
				- LHS: DuplicateFilter: {a="1"}, subset index: 0
					- ref#1 Duplicate
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[3h0m0s], subsets: {a="1"}
				- RHS: ref#1 Duplicate ...
		`
		require.Equal(t, testutils.TrimIndent(expectedPlan), plan.String())
		require.Equal(t, parser.ValueTypeVector, duplicatedResultType(t, plan.Root),
			"the buffered subexpression is the rate(...) instant vector -> InstantVectorDuplicationBuffer")
	})

	t.Run("splitting on - Duplicate buffers the raw range vector", func(t *testing.T) {
		plan := buildInstantQueryPlan(t, subsetSelectorQuery, true)

		// SplitFunctionCall now wraps each branch's rate(...), so the Duplicate drops below rate onto the raw
		// MatrixSelector - the buffered subexpression is now a range vector of raw samples.
		expectedPlan := `
			- BinaryExpression: LHS / RHS, hints exclude ()
				- LHS: SplitFunctionCall
					- FunctionCall: rate(...)
						- DuplicateFilter: {a="1"}, subset index: 0
							- ref#1 Duplicate
								- MatrixSelector: {__name__="foo"}[3h0m0s], subsets: {a="1"}
				- RHS: SplitFunctionCall
					- FunctionCall: rate(...)
						- ref#1 Duplicate ...
		`
		require.Equal(t, testutils.TrimIndent(expectedPlan), plan.String())
		require.Equal(t, parser.ValueTypeMatrix, duplicatedResultType(t, plan.Root),
			"the buffered subexpression drops to the raw MatrixSelector range vector -> RangeVectorDuplicationBuffer")
	})
}

// TestRangeVectorSplitting_IdenticalSubexpression_DuplicationBoundaryStaysAtFunctionResult is the control case:
// plain CSE (identical operands, no subset-selector elimination) shares the rate result directly, so the Duplicate
// stays above the SplitFunctionCall and keeps buffering an instant vector even with splitting enabled. This isolates
// subset-selector elimination as the trigger for the range-vector buffering.
func TestRangeVectorSplitting_IdenticalSubexpression_DuplicationBoundaryStaysAtFunctionResult(t *testing.T) {
	t.Run("splitting off", func(t *testing.T) {
		plan := buildInstantQueryPlan(t, identicalSubexpressionQuery, false)

		expectedPlan := `
			- BinaryExpression: LHS + RHS, hints exclude ()
				- LHS: ref#1 Duplicate
					- FunctionCall: rate(...)
						- MatrixSelector: {__name__="foo"}[3h0m0s]
				- RHS: ref#1 Duplicate ...
		`
		require.Equal(t, testutils.TrimIndent(expectedPlan), plan.String())
		require.Equal(t, parser.ValueTypeVector, duplicatedResultType(t, plan.Root))
	})

	t.Run("splitting on", func(t *testing.T) {
		plan := buildInstantQueryPlan(t, identicalSubexpressionQuery, true)

		// SplitFunctionCall is inserted, but the Duplicate stays above it at the rate(...) instant vector: the shared
		// subexpression is still the rate result, so the buffer type is unchanged.
		expectedPlan := `
			- BinaryExpression: LHS + RHS, hints exclude ()
				- LHS: ref#1 Duplicate
					- SplitFunctionCall
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[3h0m0s]
				- RHS: ref#1 Duplicate ...
		`
		require.Equal(t, testutils.TrimIndent(expectedPlan), plan.String())
		require.Equal(t, parser.ValueTypeVector, duplicatedResultType(t, plan.Root),
			"without subset-selector elimination the Duplicate stays at the rate(...) instant vector")
	})
}

func buildInstantQueryPlan(t *testing.T, expr string, splittingEnabled bool) *planning.QueryPlan {
	t.Helper()

	// NewTestEngineOpts already enables common-subexpression, subset-selector, and range-vector CSE; splitting is the
	// only variable we change between the "off" and "on" plans.
	opts := streamingpromql.NewTestEngineOpts()
	opts.RangeVectorSplitting.Enabled = splittingEnabled
	opts.RangeVectorSplitting.SplitInterval = 2 * time.Hour

	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewStaticQueryPlanVersionProvider(planning.MaximumSupportedQueryPlanVersion))
	require.NoError(t, err)

	ctx := user.InjectOrgID(context.Background(), "anonymous")
	timeRange := types.NewInstantQueryTimeRange(time.Unix(1000000, 0))
	plan, err := planner.NewQueryPlan(ctx, expr, timeRange, streamingpromql.DefaultLookbackDelta, false, streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)

	return plan
}

// duplicatedResultType returns the result type of the inner node of the single CSE Duplicate in the plan. This is the
// type MaterializeDuplicate switches on to pick an instant-vector vs range-vector duplication buffer.
func duplicatedResultType(t *testing.T, root planning.Node) parser.ValueType {
	t.Helper()

	// CSE produces a DAG: the shared Duplicate is reached via every consumer branch, so dedupe by pointer identity.
	duplicates := map[*commonsubexpressionelimination.Duplicate]struct{}{}
	collectDuplicates(root, duplicates)
	require.Len(t, duplicates, 1, "expected exactly one CSE Duplicate node in the plan")

	var duplicate *commonsubexpressionelimination.Duplicate
	for d := range duplicates {
		duplicate = d
	}

	resultType, err := duplicate.Inner.ResultType()
	require.NoError(t, err)
	return resultType
}

func collectDuplicates(n planning.Node, found map[*commonsubexpressionelimination.Duplicate]struct{}) {
	if d, ok := n.(*commonsubexpressionelimination.Duplicate); ok {
		found[d] = struct{}{}
	}
	for child := range planning.ChildrenIter(n) {
		collectDuplicates(child, found)
	}
}
