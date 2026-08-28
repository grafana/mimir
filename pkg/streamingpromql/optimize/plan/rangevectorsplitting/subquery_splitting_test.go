// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting_test

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestSubquery_IsSplittable(t *testing.T) {
	planner, err := streamingpromql.NewQueryPlanner(defaultSplittingOpts(), streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	testCases := map[string]struct {
		expr       string
		splittable bool
	}{
		"plain selector nested inside the subquery": {
			expr:       `sum_over_time(test_metric[5h:1h])`,
			splittable: true,
		},
		"step-invariant expression with no selector nested inside the subquery": {
			expr:       `sum_over_time(vector(1)[5h:1h])`,
			splittable: true,
		},
		"smoothed matrix selector nested inside the subquery": {
			expr:       `sum_over_time(rate(test_metric[3m] smoothed)[5h:1h])`,
			splittable: false,
		},
		"smoothed vector selector nested inside the subquery": {
			expr:       `sum_over_time((test_metric smoothed)[5h:1h])`,
			splittable: false,
		},
		"anchored selector nested inside the subquery": {
			expr:       `sum_over_time(rate(test_metric[3m] anchored)[5h:1h])`,
			splittable: false,
		},
		"positive offset selector nested inside the subquery": {
			expr:       `sum_over_time(rate(test_metric[3m] offset 10m)[5h:1h])`,
			splittable: true,
		},
		"negative offset selector nested inside the subquery": {
			expr:       `sum_over_time(rate(test_metric[3m] offset -10m)[5h:1h])`,
			splittable: false,
		},
		"@ modifier selector nested inside the subquery": {
			expr:       `sum_over_time((test_metric @ 100)[5h:1h])`,
			splittable: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			plan, err := planner.NewQueryPlan(t.Context(), tc.expr, types.NewInstantQueryTimeRange(timestamp.Time(0).Add(24*time.Hour)),
				streamingpromql.DefaultLookbackDelta, false, &streamingpromql.NoopPlanningObserver{})
			require.NoError(t, err)

			require.Equal(t, tc.splittable, strings.Contains(plan.String(), "SplitFunctionCall"), "plan:\n%s", plan.String())
		})
	}
}

// TestQuerySplitting_InsertDuplicatesAcrossSplitBlocks checks that insertDuplicatesAcrossSplitBlocks (see commonsubexpressionelimination/optimization_pass.go)
// wraps exactly the nodes it needs to in a Duplicate node. core.Subquery or core.StepInvariantExpression nested
// below a split target's own child, at any depth, but not the split target's own child itself.
func TestQuerySplitting_InsertDuplicatesAcrossSplitBlocks(t *testing.T) {
	planner, err := streamingpromql.NewQueryPlanner(defaultSplittingOpts(), streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	testCases := map[string]struct {
		expr         string
		expectedPlan string
	}{
		"no nested subquery or step-invariant expression: nothing is wrapped": {
			expr: `sum_over_time(max_over_time(test_metric[10m])[5h:1h])`,
			expectedPlan: `
				- SplitFunctionCall
					- FunctionCall: sum_over_time(...)
						- Subquery: [5h0m0s:1h0m0s]
							- FunctionCall: max_over_time(...)
								- MatrixSelector: {__name__="test_metric"}[10m0s]
			`,
		},
		"subquery nested one level below the split target: only the nested subquery's own child is wrapped": {
			expr: `count_over_time(sum_over_time(min_over_time(test_metric[2h])[20h:2h])[5h:12h])`,
			expectedPlan: `
				- SplitFunctionCall
					- FunctionCall: count_over_time(...)
						- Subquery: [5h0m0s:12h0m0s]
							- FunctionCall: sum_over_time(...)
								- Subquery: [20h0m0s:2h0m0s]
									- Duplicate
										- FunctionCall: min_over_time(...)
											- MatrixSelector: {__name__="test_metric"}[2h0m0s]
			`,
		},
		"binary expression with a constant nested below the split target: the whole expression is wrapped": {
			expr: `count_over_time(sum_over_time((test_metric / 2)[3h:1h])[5h:12h])`,
			expectedPlan: `
				- SplitFunctionCall
					- FunctionCall: count_over_time(...)
						- Subquery: [5h0m0s:12h0m0s]
							- FunctionCall: sum_over_time(...)
								- Subquery: [3h0m0s:1h0m0s]
									- Duplicate
										- DeduplicateAndMerge
											- BinaryExpression: LHS / RHS
												- LHS: VectorSelector: {__name__="test_metric"}
												- RHS: NumberLiteral: 2
			`,
		},
		"subquery nested two levels below the split target: every nested level's own child is wrapped": {
			expr: `count_over_time(sum_over_time(avg_over_time(min_over_time(test_metric[1h])[3h:30m])[10h:1h])[5h:12h])`,
			expectedPlan: `
				- SplitFunctionCall
					- FunctionCall: count_over_time(...)
						- Subquery: [5h0m0s:12h0m0s]
							- FunctionCall: sum_over_time(...)
								- Subquery: [10h0m0s:1h0m0s]
									- Duplicate
										- FunctionCall: avg_over_time(...)
											- Subquery: [3h0m0s:30m0s]
												- Duplicate
													- FunctionCall: min_over_time(...)
														- MatrixSelector: {__name__="test_metric"}[1h0m0s]
			`,
		},
		"step-invariant expression nested below the split target: its child is wrapped": {
			expr: `count_over_time(vector(1)[5h:3h])`,
			expectedPlan: `
				- DeduplicateAndMerge
					- SplitFunctionCall
						- FunctionCall: count_over_time(...)
							- Subquery: [5h0m0s:3h0m0s]
								- StepInvariantExpression
									- Duplicate
										- FunctionCall: vector(...)
											- NumberLiteral: 1
			`,
		},
		"step-invariant expression (vector(1)) as one operand of a binary expression: only that operand is wrapped": {
			expr: `sum_over_time((vector(1) + on() test_metric)[5h:1h])`,
			expectedPlan: `
				- SplitFunctionCall
					- FunctionCall: sum_over_time(...)
						- Subquery: [5h0m0s:1h0m0s]
							- BinaryExpression: LHS + on () RHS
								- LHS: StepInvariantExpression
									- Duplicate
										- FunctionCall: vector(...)
											- NumberLiteral: 1
								- RHS: VectorSelector: {__name__="test_metric"}
			`,
		},
		"nested subquery shared by subset selector elimination: its child is still wrapped exactly once": {
			expr: `count_over_time((sum_over_time(max_over_time(dedupe_filter_metric{a="1"}[1h])[5h:1h]) / ignoring(a) min_over_time(max_over_time(dedupe_filter_metric[1h])[5h:1h]))[10h:12h])`,
			expectedPlan: `
				- SplitFunctionCall
					- FunctionCall: count_over_time(...)
						- Subquery: [10h0m0s:12h0m0s]
							- BinaryExpression: LHS / ignoring (a) RHS, hints exclude (a)
								- LHS: FunctionCall: sum_over_time(...)
									- DuplicateFilter: {a="1"}, subset index: 0
										- ref#1 Duplicate
											- Subquery: [5h0m0s:1h0m0s]
												- Duplicate
													- FunctionCall: max_over_time(...)
														- MatrixSelector: {__name__="dedupe_filter_metric"}[1h0m0s], subsets: {a="1"} ({__name__="dedupe_filter_metric", a="1"})
								- RHS: FunctionCall: min_over_time(...)
									- ref#1 Duplicate ...
			`,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			plan, err := planner.NewQueryPlan(t.Context(), tc.expr, types.NewInstantQueryTimeRange(timestamp.Time(0).Add(24*time.Hour)),
				streamingpromql.DefaultLookbackDelta, false, &streamingpromql.NoopPlanningObserver{})
			require.NoError(t, err)

			require.Equal(t, testutils.TrimIndent(tc.expectedPlan), plan.String())
		})
	}
}

// TestQuerySplitting_MinimumRequiredPlanVersion verifies that a SplitFunctionCall reports QueryPlanV18 when it
// wraps a plain selector and QueryPlanV21 when it wraps a subquery, in each case regardless of whether CSE has
// inserted a Duplicate node between the SplitFunctionCall and what it wraps.
func TestQuerySplitting_MinimumRequiredPlanVersion(t *testing.T) {
	planner, err := streamingpromql.NewQueryPlanner(defaultSplittingOpts(), streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	testCases := map[string]struct {
		expr            string
		expectedPlan    string
		expectedVersion planning.QueryPlanVersion
	}{
		"selector, no CSE duplication": {
			expr: `sum_over_time(test_metric[5h])`,
			expectedPlan: `
				- SplitFunctionCall
					- FunctionCall: sum_over_time(...)
						- MatrixSelector: {__name__="test_metric"}[5h0m0s]
			`,
			expectedVersion: planning.QueryPlanV18,
		},
		"selector, CSE inserts Duplicate below SplitFunctionCall": {
			expr: `sum_over_time(test_metric[5h]) / count_over_time(test_metric[5h])`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints exclude ()
					- LHS: SplitFunctionCall
						- FunctionCall: sum_over_time(...)
							- ref#1 Duplicate
								- MatrixSelector: {__name__="test_metric"}[5h0m0s]
					- RHS: SplitFunctionCall
						- FunctionCall: count_over_time(...)
							- ref#1 Duplicate ...
			`,
			expectedVersion: planning.QueryPlanV18,
		},
		"subquery, no CSE duplication": {
			expr: `sum_over_time(max_over_time(test_metric[10m])[5h:1h])`,
			expectedPlan: `
				- SplitFunctionCall
					- FunctionCall: sum_over_time(...)
						- Subquery: [5h0m0s:1h0m0s]
							- FunctionCall: max_over_time(...)
								- MatrixSelector: {__name__="test_metric"}[10m0s]
			`,
			expectedVersion: planning.QueryPlanV21,
		},
		"subquery, CSE inserts Duplicate below SplitFunctionCall": {
			expr: `sum_over_time(max_over_time(test_metric[10m])[5h:1h]) / count_over_time(max_over_time(test_metric[10m])[5h:1h])`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints exclude ()
					- LHS: SplitFunctionCall
						- FunctionCall: sum_over_time(...)
							- ref#1 Duplicate
								- Subquery: [5h0m0s:1h0m0s]
									- FunctionCall: max_over_time(...)
										- MatrixSelector: {__name__="test_metric"}[10m0s]
					- RHS: SplitFunctionCall
						- FunctionCall: count_over_time(...)
							- ref#1 Duplicate ...
			`,
			expectedVersion: planning.QueryPlanV21,
		},
		"subquery, CSE inserts Duplicate above SplitFunctionCall": {
			expr: `sum_over_time(max_over_time(test_metric[10m])[5h:1h]) + sum_over_time(max_over_time(test_metric[10m])[5h:1h])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS, hints exclude ()
					- LHS: ref#1 Duplicate
						- SplitFunctionCall
							- FunctionCall: sum_over_time(...)
								- Subquery: [5h0m0s:1h0m0s]
									- FunctionCall: max_over_time(...)
										- MatrixSelector: {__name__="test_metric"}[10m0s]
					- RHS: ref#1 Duplicate ...
			`,
			expectedVersion: planning.QueryPlanV21,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			plan, err := planner.NewQueryPlan(t.Context(), tc.expr, types.NewInstantQueryTimeRange(timestamp.Time(0).Add(6*time.Hour)),
				streamingpromql.DefaultLookbackDelta, false, &streamingpromql.NoopPlanningObserver{})
			require.NoError(t, err)

			require.Equal(t, testutils.TrimIndent(tc.expectedPlan), plan.String())
			require.Equal(t, tc.expectedVersion, plan.Version)
		})
	}
}
