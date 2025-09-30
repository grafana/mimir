// SPDX-License-Identifier: AGPL-3.0-only

package plan_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestEliminateDeduplicateAndMergeOptimizationPass(t *testing.T) {
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
