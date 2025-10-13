// SPDX-License-Identifier: AGPL-3.0-only

package plan_test

import (
	"context"
	"io"
	"io/fs"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"
)

func TestEliminateDeduplicateAndMergeOptimizationPassPlan(t *testing.T) {
	testCases := map[string]struct {
		expr            string
		expectedPlan    string
		nodesEliminated int // Number of DeduplicateAndMerge nodes expected to be eliminated
	}{
		"function where selector has exact name matcher - should eliminate DeduplicateAndMerge": {
			expr: `rate(foo[5m])`,
			expectedPlan: `
				- FunctionCall: rate(...)
					- MatrixSelector: {__name__="foo"}[5m0s]
			`,
			nodesEliminated: 1,
		},
		"function where selector has no name matcher - should keep DeduplicateAndMerge": {
			expr: `rate({job="test"}[5m])`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: rate(...)
						- MatrixSelector: {job="test"}[5m0s]
			`,
			nodesEliminated: 0,
		},
		"function where selector has regex name matcher - should keep DeduplicateAndMerge": {
			expr: `rate({__name__=~"(foo|bar)"}[5m])`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: rate(...)
						- MatrixSelector: {__name__=~"(foo|bar)"}[5m0s]
			`,
			nodesEliminated: 0,
		},
		"nested functions where selector has exact name matcher - should eliminate all DeduplicateAndMerge": {
			expr: `abs(rate(foo[5m]))`,
			expectedPlan: `	
				- FunctionCall: abs(...)
					- FunctionCall: rate(...)
						- MatrixSelector: {__name__="foo"}[5m0s]
			`,
			nodesEliminated: 2,
		},
		"nested functions where selector has no name matcher - should keep only inner DeduplicateAndMerge": {
			expr: `abs(rate({job="test"}[5m]))`,
			expectedPlan: `
				- FunctionCall: abs(...)
					- DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {job="test"}[5m0s]
			`,
			nodesEliminated: 1,
		},
		"nested functions where selector has regex name matcher - should keep only inner DeduplicateAndMerge": {
			expr: `abs(rate({__name__=~"(foo|bar)"}[5m]))`,
			expectedPlan: `
				- FunctionCall: abs(...)
					- DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__=~"(foo|bar)"}[5m0s]
			`,
			nodesEliminated: 1,
		},
		"deeply nested functions where selector has no name matcher - should keep only innermost DeduplicateAndMerge": {
			expr: `abs(ceil(rate({job="test"}[5m])))`,
			expectedPlan: `
					- FunctionCall: abs(...)
						- FunctionCall: ceil(...)
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {job="test"}[5m0s]
				`,
			nodesEliminated: 2,
		},

		"unary negation with exact name matcher - should eliminate DeduplicateAndMerge": {
			expr: `-foo`,
			expectedPlan: `
					- UnaryExpression: -
						- VectorSelector: {__name__="foo"}
				`,
			nodesEliminated: 1,
		},
		"unary negation without exact name matcher - should keep DeduplicateAndMerge": {
			expr: `-{job="test"}`,
			expectedPlan: `
					- DeduplicateAndMerge
						- UnaryExpression: -
							- VectorSelector: {job="test"}
				`,
			nodesEliminated: 0,
		},
		"unary negation with regex name matcher - should keep DeduplicateAndMerge": {
			expr: `-{__name__=~"(foo|bar)"}`,
			expectedPlan: `
					- DeduplicateAndMerge
						- UnaryExpression: -
							- VectorSelector: {__name__=~"(foo|bar)"}
				`,
			nodesEliminated: 0,
		},
		"nested unary after function - keep only inner DeduplicateAndMerge": {
			expr: `-(rate({job="test"}[5m]))`,
			expectedPlan: `
					- UnaryExpression: -
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {job="test"}[5m0s]
				`,
			nodesEliminated: 1,
		},
		"label_replace - should always keep DeduplicateAndMerge": {
			expr: `label_replace(foo, "dst", "$1", "src", "(.*)")`,
			expectedPlan: `
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: VectorSelector: {__name__="foo"}
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "src"
								- param 4: StringLiteral: "(.*)"
					`,
			nodesEliminated: 0,
		},
		"label_join - should always keep DeduplicateAndMerge": {
			expr: `label_join(foo, "dst", ",", "a", "b")`,
			expectedPlan: `
						- DeduplicateAndMerge
							- FunctionCall: label_join(...)
								- param 0: VectorSelector: {__name__="foo"}
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: ","
								- param 3: StringLiteral: "a"
								- param 4: StringLiteral: "b"
					`,
			nodesEliminated: 0,
		},
		"label_replace where DeduplicateAndMerge is inside - should keep DeduplicateAndMerge around label_replace": {
			expr: `label_replace(rate({job="test"}[5m]), "dst", "$1", "src", "(.*)")`,
			expectedPlan: `
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {job="test"}[5m0s]
								- param 1: StringLiteral: "dst"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "src"
								- param 4: StringLiteral: "(.*)"
					`,
			nodesEliminated: 0,
		},
		"function call enclosing label_replace - should keep DeduplicateAndMerge around label_replace and enclosing function call": {
			expr: `abs(label_replace(rate(foo[5m]), "dst", "$1", "src", "(.*)"))`,
			expectedPlan: `
					- DeduplicateAndMerge
						- FunctionCall: abs(...)
							- DeduplicateAndMerge
								- FunctionCall: label_replace(...)
									- param 0: FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
									- param 1: StringLiteral: "dst"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "src"
									- param 4: StringLiteral: "(.*)"
					`,
			nodesEliminated: 1,
		},
		"nested function calls enclosing label_replace - should keep DeduplicateAndMerge closest to function enclosing label_replace": {
			expr: `abs(ceil(label_replace(rate(foo[5m]), "dst", "$1", "src", "(.*)")))`,
			expectedPlan: `
						- FunctionCall: abs(...)
							- DeduplicateAndMerge
								- FunctionCall: ceil(...)
									- DeduplicateAndMerge
										- FunctionCall: label_replace(...)
											- param 0: FunctionCall: rate(...)
												- MatrixSelector: {__name__="foo"}[5m0s]
											- param 1: StringLiteral: "dst"
											- param 2: StringLiteral: "$1"
											- param 3: StringLiteral: "src"
											- param 4: StringLiteral: "(.*)"
					`,
			nodesEliminated: 2,
		},
		"function which does not drop name after label_replace - should not introduce DeduplicateAndMerge to function enclosing label_replace": {
			expr: `sort(label_replace(rate(foo[5m]), "dst", "$1", "src", "(.*)"))`,
			expectedPlan: `
								- FunctionCall: sort(...)
									- DeduplicateAndMerge
										- FunctionCall: label_replace(...)
											- param 0: FunctionCall: rate(...)
												- MatrixSelector: {__name__="foo"}[5m0s]
											- param 1: StringLiteral: "dst"
											- param 2: StringLiteral: "$1"
											- param 3: StringLiteral: "src"
											- param 4: StringLiteral: "(.*)"
					`,
			nodesEliminated: 1,
		},
		"nested function calls, function in between doesn't drop __name__- should keep DeduplicateAndMerge closest to node which drops name after label_replace": {
			expr: `abs(sort(label_replace(rate(foo[5m]), "dst", "$1", "src", "(.*)")))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: abs(...)
						- FunctionCall: sort(...)
							- DeduplicateAndMerge
								- FunctionCall: label_replace(...)
									- param 0: FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
									- param 1: StringLiteral: "dst"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "src"
									- param 4: StringLiteral: "(.*)"
				`,
			nodesEliminated: 1,
		},

		// Test cases to confirm we're skipping optimization when expression has a binary operation.
		"or operator - should keep DeduplicateAndMerge": {
			expr: `foo or bar`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: VectorSelector: {__name__="foo"}
						- RHS: VectorSelector: {__name__="bar"}
				`,
			nodesEliminated: 0,
		},
		"or operator with functions - should keep DeduplicateAndMerge": {
			expr: `rate(foo[5m]) or rate(bar[5m])`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
						- RHS: DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminated: 0,
		},
		"vector-scalar comparison with bool modifier should keep DeduplicateAndMerge": {
			expr: `foo == bool 6`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS == bool RHS
						- LHS: VectorSelector: {__name__="foo"}
						- RHS: NumberLiteral: 6
				`,
			nodesEliminated: 0,
		},
		"vector-scalar arithmetic with exact name matcher should keep DeduplicateAndMerge": {
			expr: `foo * 2`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS * RHS
						- LHS: VectorSelector: {__name__="foo"}
						- RHS: NumberLiteral: 2
				`,
			nodesEliminated: 0,
		},
		"binary operation with nested rate functions - should skip optimization entirely": {
			expr: `rate(foo[5m]) + rate(bar[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminated: 0,
		},
		"binary operation with aggregations - should skip optimization entirely": {
			expr: `sum(rate(foo[5m])) / sum(rate(bar[5m]))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: AggregateExpression: sum
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminated: 0,
		},
		"binary operation and expression - should skip optimization entirely": {
			expr: `rate(foo[5m]) and rate(bar[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS and RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="bar"}[5m0s]
				`,
			nodesEliminated: 0,
		},
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := streamingpromql.NewTestEngineOpts()

			// First, create a plan without optimization to count original nodes
			plannerNoOpt, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
			require.NoError(t, err)
			planBefore, err := plannerNoOpt.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			nodesBefore := countDeduplicateAndMergeNodes(planBefore.Root)

			// Then, create a plan with optimization
			plannerWithOpt, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
			require.NoError(t, err)
			plannerWithOpt.RegisterQueryPlanOptimizationPass(plan.NewEliminateDeduplicateAndMergeOptimizationPass())
			planAfter, err := plannerWithOpt.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			nodesAfter := countDeduplicateAndMergeNodes(planAfter.Root)

			// Check the plan structure
			actual := planAfter.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual, "Query: %s", testCase.expr)

			// Check the number of nodes eliminated
			actualEliminated := nodesBefore - nodesAfter
			require.Equal(t, testCase.nodesEliminated, actualEliminated,
				"Query: %s\nExpected to eliminate %d nodes, but eliminated %d (before: %d, after: %d)",
				testCase.expr, testCase.nodesEliminated, actualEliminated, nodesBefore, nodesAfter)
		})
	}
}

// countDeduplicateAndMergeNodes recursively counts DeduplicateAndMerge nodes in a plan
func countDeduplicateAndMergeNodes(node planning.Node) int {
	count := 0
	if _, ok := node.(*core.DeduplicateAndMerge); ok {
		count = 1
	}
	for _, child := range node.Children() {
		count += countDeduplicateAndMergeNodes(child)
	}
	return count
}

func TestEliminateDeduplicateAndMergeOptimizationPassCorrectness(t *testing.T) {
	testCases := map[string]struct {
		data           string
		expr           string
		expectedResult string
		expectedError  string
	}{
		"series with different labelset after name drop": {
			data: `
				load 1m
					a{env="prod"} 0+1x10
					a{env="test"} 0+2x10
			`,
			expr: `abs(rate(a[5m]))`,
			expectedResult: `
				{env="prod"} => 0.016666666666666666 @[600000]
				{env="test"} => 0.03333333333333333 @[600000]
			`,
		},
		"series with same labelset after name drop, but datapoints at different timestamps - should be merged": {
			data: `
				load 1m
					a{env="prod"} _ 0 1 2 _ _ _ _ _ _
					b{env="prod"} _ _ _ _ _ 0 1 2 3 4
			`,
			expr: `abs(rate({__name__=~"(a|b)"}[5m]))`,
			expectedResult: `
				{env="prod"} => 0.016666666666666666 @[600000]
			`,
		},
		"series with same labelset after name drop, but datapoints at same timestamp - should error": {
			data: `
				load 1m
					a{env="prod"} 0 1 2 3 4 5 6 7 8 9
					b{env="prod"} 0 1 2 3 4 5 6 7 8 9
			`,
			expr:          `abs(rate({__name__=~"(a|b)"}[5m]))`,
			expectedError: "vector cannot contain metrics with the same labelset",
		},
		"nested function calls over series with different labelset after name drop": {
			data: `
				load 1m
					a{env="prod"} 0+1x10
					a{env="test"} 0+2x10
			`,
			expr: `abs(rate(a[5m]))`,
			expectedResult: `
				{env="prod"} => 0.016666666666666666 @[600000]
				{env="test"} => 0.03333333333333333 @[600000]
			`,
		},
		"label_replace with exact name matcher, no conflicts": {
			data: `
				load 1m
					a{env="prod", src="value1"} 0+1x10
					a{env="test", src="value2"} 0+2x10
			`,
			expr: `abs(label_replace(rate(a[5m]), "dst", "$1", "src", "(.*)"))`,
			expectedResult: `
				{dst="value1", env="prod", src="value1"} => 0.016666666666666666 @[600000]
				{dst="value2", env="test", src="value2"} => 0.03333333333333333 @[600000]
			`,
		},
		"label_replace that creates duplicate labelsets - should error": {
			data: `
				load 1m
					a{env="prod", src="value"} 0 1 2 3 4 5 6 7 8 9
					b{env="prod", src="value"} 0 1 2 3 4 5 6 7 8 9
			`,
			expr:          `abs(label_replace(rate({__name__=~"(a|b)"}[5m]), "dst", "same", "src", "(.*)"))`,
			expectedError: "vector cannot contain metrics with the same labelset",
		},
		"label_replace where series will be merged": {
			// This tests that label_replace can create duplicate labelsets that get merged successfully.
			// Step by step:
			// 1. Initial: two series a{env="prod"} at timestamps 1-3m and a{env="test"} at timestamps 5-9m (same __name__, different env, non-overlapping timestamps)
			// 2. label_replace overwrites env label: both become a{env="merged"} (now identical labelsets)
			// 3. Since labelsets are identical and timestamps don't overlap, the series merge into one: a{env="merged"}
			data: `
				load 1m
					a{env="prod"} _ 0 1 2 _ _ _ _ _ _
					a{env="test"} _ _ _ _ _ 0 1 2 3 4
			`,
			expr: `label_replace(a, "env", "merged", "env", "(.*)")`,
			expectedResult: `
				{__name__="a", env="merged"} => 4 @[600000]
			`,
		},
		"label_replace re-introduces __name__ after rate drops it": {
			data: `
				load 1m
					a{env="prod"} 0+1x10
					a{env="test"} 0+2x10
			`,
			expr: `label_replace(rate(a[5m]), "__name__", "my_metric", "", "")`,
			expectedResult: `
				{__name__="my_metric", env="prod"} => 0.016666666666666666 @[600000]
				{__name__="my_metric", env="test"} => 0.03333333333333333 @[600000]
			`,
		},
		"label_replace re-introduces __name__, then name-dropping operation merges series": {
			// This tests the full cycle: drop name -> re-introduce name with label change -> drop name again -> merge.
			// Step by step:
			// 1. Initial: a{env="prod"} at timestamps 1-3m and a{env="test"} at timestamps 5-9m (different env, non-overlapping)
			// 2. rate() drops __name__: {env="prod"} and {env="test"} (still two separate series)
			// 3. label_replace re-introduces __name__ AND changes env: {__name__="my_metric", env="my_metric"} for both
			// 4. abs() drops __name__ again: {env="my_metric"} for both (now identical labelsets!)
			// 5. Since labelsets are identical and timestamps don't overlap, series merge into one: {env="my_metric"}
			data: `
				load 1m
					a{env="prod"} _ 0 1 2 _ _ _ _ _ _
					a{env="test"} _ _ _ _ _ 0 1 2 3 4
			`,
			expr: `abs(label_replace(label_replace(rate(a[5m]), "__name__", "my_metric", "", ""), "env", "my_metric", "", ""))`,
			expectedResult: `
				{env="my_metric"} => 0.016666666666666666 @[600000]
			`,
		},
		"label_replace re-introduces __name__, then name-dropping operation causes error": {
			// This tests the error case: drop name -> re-introduce name with label change -> drop name again -> error.
			// Step by step:
			// 1. Initial: a{env="prod"} and a{env="test"} both at all timestamps (different env, overlapping data)
			// 2. rate() drops __name__: {env="prod"} and {env="test"} (still two separate series)
			// 3. label_replace re-introduces __name__ AND changes env: {__name__="my_metric", env="my_metric"} for both
			// 4. abs() drops __name__ again: {env="my_metric"} for both (now identical labelsets!)
			// 5. Since labelsets are identical but timestamps overlap, this should error
			data: `
				load 1m
					a{env="prod"} 0 1 2 3 4 5 6 7 8 9
					a{env="test"} 0 1 2 3 4 5 6 7 8 9
			`,
			expr:          `abs(label_replace(label_replace(rate(a[5m]), "__name__", "my_metric", "", ""), "env", "my_metric", "", ""))`,
			expectedError: "vector cannot contain metrics with the same labelset",
		},
	}

	ctx := context.Background()
	end := timestamp.Time(0).Add(10 * time.Minute)

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			storage := promqltest.LoadedStorage(t, testCase.data)
			t.Cleanup(func() { require.NoError(t, storage.Close()) })

			runTest := func(t *testing.T, withOptimization bool) {
				opts := streamingpromql.NewTestEngineOpts()
				// Disable delayed name removal, since EliminateDeduplicateAndMergeOptimizationPass is enabled only when delayed name removal is disabled.
				opts.CommonOpts.EnableDelayedNameRemoval = false
				planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
				require.NoError(t, err)

				if withOptimization {
					planner.RegisterQueryPlanOptimizationPass(plan.NewEliminateDeduplicateAndMergeOptimizationPass())
				}

				engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), planner)
				require.NoError(t, err)

				q, err := engine.NewInstantQuery(ctx, storage, nil, testCase.expr, end)
				require.NoError(t, err)
				defer q.Close()

				result := q.Exec(ctx)

				if testCase.expectedError != "" {
					require.Error(t, result.Err)
					require.Contains(t, result.Err.Error(), testCase.expectedError)
				} else {
					require.NoError(t, result.Err)
					require.Equal(t, testutils.TrimIndent(testCase.expectedResult), result.String())
				}
			}

			t.Run("without optimization", func(t *testing.T) {
				runTest(t, false)
			})

			t.Run("with optimization", func(t *testing.T) {
				runTest(t, true)
			})
		})
	}
}

// Test runs upstream and our test cases with delayed name removal disabled, ensuring that the optimization pass doesn't cause regressions.
// It's needed because in TestUpstreamTestCases and TestOurTestCase delayed name removal is enabled, but the optimization pass requires it to be disabled.
func TestEliminateDeduplicateAndMergeOptimizationDoesNotRegress(t *testing.T) {
	runTestCasesWithDelayedNameRemovalDisabled(t, "upstream/*.test")
	runTestCasesWithDelayedNameRemovalDisabled(t, "ours*/*.test")
}

func runTestCasesWithDelayedNameRemovalDisabled(t *testing.T, globPattern string) {

	types.EnableManglingReturnedSlices = true
	parser.ExperimentalDurationExpr = true
	parser.EnableExperimentalFunctions = true

	testdataFS := os.DirFS("../../testdata")
	testFiles, err := fs.Glob(testdataFS, globPattern)
	require.NoError(t, err)

	for _, testFile := range testFiles {
		t.Run(testFile, func(t *testing.T) {
			if strings.Contains(testFile, "name_label_dropping") {
				t.Skip("name_label_dropping tests require delayed name removal to be enabled, but optimization pass requires it to be disabled")
			}

			f, err := testdataFS.Open(testFile)
			require.NoError(t, err)
			defer f.Close()

			b, err := io.ReadAll(f)
			require.NoError(t, err)

			testScript := string(b)
			opts := streamingpromql.NewTestEngineOpts()
			opts.CommonOpts.EnableDelayedNameRemoval = false
			planner, err := streamingpromql.NewQueryPlanner(opts)
			require.NoError(t, err)
			engine, err := streamingpromql.NewEngine(opts, streamingpromql.NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), planner)
			require.NoError(t, err)
			promqltest.RunTest(t, testScript, engine)
		})
	}
}
