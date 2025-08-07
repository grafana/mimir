// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/engineopts"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr                        string
		rangeQuery                  bool
		expectedPlan                string
		expectUnchanged             bool
		expectedDuplicateNodes      int // Check that we don't do unnecessary work introducing a duplicate node multiple times when starting from different selectors (eg. when handling (a+b) + (a+b)).
		expectedSelectorsEliminated int
		expectedSelectorsInspected  int
	}{
		"single vector selector": {
			expr:                       `foo`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 1,
		},
		"single matrix selector": {
			expr:                       `foo[5m]`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 1,
		},
		"single subquery": {
			expr:                       `foo[5m:10s]`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 1,
		},
		"duplicated numeric literal": {
			expr:            `1 + 1`,
			expectUnchanged: true,
		},
		"duplicated string literal": {
			expr:                       `label_join(foo, "abc", "-") + label_join(bar, "def", ",")`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 2,
		},
		"vector selector duplicated twice": {
			expr: `foo + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"vector selector duplicated twice with other selector": {
			expr: `foo + foo + bar`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- RHS: ref#1 Duplicate ...
					- RHS: VectorSelector: {__name__="bar"}
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  3,
		},
		"vector selector duplicated three times": {
			expr: `foo + foo + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- RHS: ref#1 Duplicate ...
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  3,
		},
		"vector selector duplicated many times": {
			expr: `foo + foo + foo + bar + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: BinaryExpression: LHS + RHS
							- LHS: BinaryExpression: LHS + RHS
								- LHS: ref#1 Duplicate
									- VectorSelector: {__name__="foo"}
								- RHS: ref#1 Duplicate ...
							- RHS: ref#1 Duplicate ...
						- RHS: VectorSelector: {__name__="bar"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 3,
			expectedSelectorsInspected:  5,
		},
		"duplicated vector selector with different aggregations": {
			expr: `max(foo) - min(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS
					- LHS: AggregateExpression: max
						- ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
					- RHS: AggregateExpression: min
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicated vector selector with same aggregations": {
			expr: `max(foo) + max(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- AggregateExpression: max
							- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"multiple levels of duplication: vector selector and aggregation": {
			expr: `a + sum(a) + sum(a)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- VectorSelector: {__name__="a"}
						- RHS: ref#2 Duplicate
							- AggregateExpression: sum
								- ref#1 Duplicate ...
					- RHS: ref#2 Duplicate ...
			`,
			expectedDuplicateNodes:      2,
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  3,
		},
		"multiple levels of duplication: vector selector and binary operation": {
			expr: `(a - a) + (a - a)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#2 Duplicate
						- BinaryExpression: LHS - RHS
							- LHS: ref#1 Duplicate
								- VectorSelector: {__name__="a"}
							- RHS: ref#1 Duplicate ...
					- RHS: ref#2 Duplicate ...
			`,
			expectedDuplicateNodes:      2,
			expectedSelectorsEliminated: 3,
			expectedSelectorsInspected:  4,
		},
		"multiple levels of duplication: multiple vector selectors and binary operation": {
			expr: `(a - a) + (a - a) + (a * b) + (a * b)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: BinaryExpression: LHS + RHS
							- LHS: ref#2 Duplicate
								- BinaryExpression: LHS - RHS
									- LHS: ref#1 Duplicate
										- VectorSelector: {__name__="a"}
									- RHS: ref#1 Duplicate ...
							- RHS: ref#2 Duplicate ...
						- RHS: ref#3 Duplicate
							- BinaryExpression: LHS * RHS
								- LHS: ref#1 Duplicate ...
								- RHS: VectorSelector: {__name__="b"}
					- RHS: ref#3 Duplicate ...
			`,
			expectedDuplicateNodes:      3,
			expectedSelectorsEliminated: 6, // 5 instances of 'a', and one instance of 'b'
			expectedSelectorsInspected:  8,
		},
		"duplicated binary operation with different vector selectors": {
			expr: `(a - b) + (a - b)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- BinaryExpression: LHS - RHS
							- LHS: VectorSelector: {__name__="a"}
							- RHS: VectorSelector: {__name__="b"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  4,
		},
		"duplicated binary operation with different matrix selectors": {
			expr: `(rate(a[5m]) - rate(b[5m])) + (rate(a[5m]) - rate(b[5m]))`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- BinaryExpression: LHS - RHS
							- LHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="a"}[5m0s]
							- RHS: FunctionCall: rate(...)
								- MatrixSelector: {__name__="b"}[5m0s]
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  4,
		},
		"same selector used for both vector and matrix selector": {
			expr:                       `foo + rate(foo[5m])`,
			expectUnchanged:            true,
			expectedSelectorsInspected: 2,
		},
		"duplicate matrix selectors with different outer function in instant query": {
			expr:       `rate(foo[5m]) + increase(foo[5m])`,
			rangeQuery: false,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: rate(...)
						- ref#1 Duplicate
							- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: FunctionCall: increase(...)
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate matrix selectors with different outer function in range query": {
			// We do not want to deduplicate matrix selectors in range queries.
			expr:                       `rate(foo[5m]) + increase(foo[5m])`,
			rangeQuery:                 true,
			expectUnchanged:            true,
			expectedSelectorsInspected: 2,
		},
		"duplicate matrix selectors, some with different outer function in instant query": {
			expr: `rate(foo[5m]) + increase(foo[5m]) + rate(foo[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#2 Duplicate
							- FunctionCall: rate(...)
								- ref#1 Duplicate
									- MatrixSelector: {__name__="foo"}[5m0s]
						- RHS: FunctionCall: increase(...)
							- ref#1 Duplicate ...
					- RHS: ref#2 Duplicate ...
			`,
			rangeQuery:                  false,
			expectedDuplicateNodes:      2,
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  3,
		},
		"duplicate matrix selectors, some with different outer function in range query": {
			// We do not want to deduplicate matrix selectors themselves in range queries, but do want to deduplicate duplicate functions over matrix selectors.
			expr: `rate(foo[5m]) + increase(foo[5m]) + rate(foo[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="foo"}[5m0s]
						- RHS: FunctionCall: increase(...)
							- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: ref#1 Duplicate ...
			`,
			rangeQuery:                  true,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1, // We only eliminate one 'foo' selector in the duplicate rate(...) expression.
			expectedSelectorsInspected:  3,
		},
		"duplicate matrix selectors with same outer function": {
			expr: `rate(foo[5m]) + rate(foo[5m])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[5m0s]
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate subqueries with different outer function in instant query": {
			expr: `rate(foo[5m:]) + increase(foo[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: rate(...)
						- ref#1 Duplicate
							- Subquery: [5m0s:1m0s]
								- VectorSelector: {__name__="foo"}
					- RHS: FunctionCall: increase(...)
						- ref#1 Duplicate ...
			`,
			rangeQuery:                  false,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate subqueries with different outer function in range query": {
			// We do not want to deduplicate subqueries directly in range queries, but do want to deduplicate their contents if they are the same.
			expr: `rate(foo[5m:]) + increase(foo[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: rate(...)
						- Subquery: [5m0s:1m0s]
							- ref#1 Duplicate
								- VectorSelector: {__name__="foo"}
					- RHS: FunctionCall: increase(...)
						- Subquery: [5m0s:1m0s]
							- ref#1 Duplicate ...
			`,
			rangeQuery:                  true,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate subqueries with different outer function and multiple child selectors in instant query": {
			expr: `rate((a - b)[5m:]) + increase((a - b)[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: rate(...)
						- ref#1 Duplicate
							- Subquery: [5m0s:1m0s]
								- BinaryExpression: LHS - RHS
									- LHS: VectorSelector: {__name__="a"}
									- RHS: VectorSelector: {__name__="b"}
					- RHS: FunctionCall: increase(...)
						- ref#1 Duplicate ...
			`,
			rangeQuery:                  false,
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  4,
		},
		"duplicate subqueries with different outer function and multiple child selectors in range query": {
			expr: `rate((a - b)[5m:]) + increase((a - b)[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: rate(...)
						- Subquery: [5m0s:1m0s]
							- ref#1 Duplicate
								- BinaryExpression: LHS - RHS
									- LHS: VectorSelector: {__name__="a"}
									- RHS: VectorSelector: {__name__="b"}
					- RHS: FunctionCall: increase(...)
						- Subquery: [5m0s:1m0s]
							- ref#1 Duplicate ...
			`,
			rangeQuery:                  true,
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
			expectedSelectorsInspected:  4,
		},
		"duplicate subqueries with same outer function": {
			expr: `rate(foo[5m:]) + rate(foo[5m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- FunctionCall: rate(...)
							- Subquery: [5m0s:1m0s]
								- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate nested subqueries": {
			expr: `max_over_time(rate(foo[5m:])[10m:]) + max_over_time(rate(foo[5m:])[10m:])`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- FunctionCall: max_over_time(...)
							- Subquery: [10m0s:1m0s]
								- FunctionCall: rate(...)
									- Subquery: [5m0s:1m0s]
										- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate subqueries with different ranges": {
			expr:                       `max_over_time(rate(foo[5m:])[10m:]) + max_over_time(rate(foo[5m:])[7m:])`,
			expectUnchanged:            true, // We don't support deduplicating common expressions that are evaluated over different ranges.
			expectedSelectorsInspected: 2,
		},
		"duplicate selectors, both with timestamp()": {
			expr: `timestamp(foo) + timestamp(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- FunctionCall: timestamp(...)
							- VectorSelector: {__name__="foo"}, return sample timestamps
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate selectors, one with timestamp()": {
			expr:                       `timestamp(foo) + foo`,
			expectUnchanged:            true, // In the future, we might be able to deduplicate these, but for now, treat them as unique expressions.
			expectedSelectorsInspected: 2,
		},
		"duplicate selectors, one with timestamp() over an intermediate expression": {
			expr: `timestamp(abs(foo)) + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: timestamp(...)
						- FunctionCall: abs(...)
							- ref#1 Duplicate
								- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate operation with different children": {
			expr: `topk(3, foo) + topk(5, foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: topk
						- expression: ref#1 Duplicate
							- VectorSelector: {__name__="foo"}
						- parameter: NumberLiteral: 3
					- RHS: AggregateExpression: topk
						- expression: ref#1 Duplicate ...
						- parameter: NumberLiteral: 5
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate expression with multiple children": {
			expr: `topk(5, foo) + topk(5, foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- AggregateExpression: topk
							- expression: VectorSelector: {__name__="foo"}
							- parameter: NumberLiteral: 5
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate expression where 'skip histogram decoding' applies to one expression": {
			expr: `histogram_count(some_metric) * histogram_quantile(0.5, some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: FunctionCall: histogram_count(...)
						- ref#1 Duplicate
							- VectorSelector: {__name__="some_metric"}
					- RHS: FunctionCall: histogram_quantile(...)
						- param 0: NumberLiteral: 0.5
						- param 1: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
		"duplicate expression where 'skip histogram decoding' applies to both expressions": {
			expr: `histogram_count(some_metric) * histogram_sum(some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: FunctionCall: histogram_count(...)
						- ref#1 Duplicate
							- VectorSelector: {__name__="some_metric"}
					- RHS: FunctionCall: histogram_sum(...)
						- ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
			expectedSelectorsInspected:  2,
		},
	}

	ctx := context.Background()
	observer := streamingpromql.NoopPlanningObserver{}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := engineopts.NewTestEngineOpts()
			plannerWithoutOptimizationPass := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
			plannerWithoutOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})

			reg := prometheus.NewPedanticRegistry()
			plannerWithOptimizationPass := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
			plannerWithOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})
			plannerWithOptimizationPass.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, reg))

			var timeRange types.QueryTimeRange

			if testCase.rangeQuery {
				timeRange = types.NewRangeQueryTimeRange(time.Now(), time.Now().Add(-time.Hour), time.Minute)
			} else {
				timeRange = types.NewInstantQueryTimeRange(time.Now())
			}

			if testCase.expectUnchanged {
				p, err := plannerWithoutOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
				require.NoError(t, err)
				testCase.expectedPlan = p.String()
			}

			p, err := plannerWithOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			requireDuplicateNodeCount(t, reg, testCase.expectedDuplicateNodes)
			requireSelectorCounts(t, reg, testCase.expectedSelectorsInspected, testCase.expectedSelectorsEliminated)
		})
	}
}

func requireDuplicateNodeCount(t *testing.T, g prometheus.Gatherer, expected int) {
	const metricName = "cortex_mimir_query_engine_common_subexpression_elimination_duplication_nodes_introduced"

	expectedMetrics := fmt.Sprintf(`# HELP %v Number of duplication nodes introduced by the common subexpression elimination optimization pass.
# TYPE %v counter
%v %v
`, metricName, metricName, metricName, expected)

	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(expectedMetrics), metricName))
}

func requireSelectorCounts(t *testing.T, g prometheus.Gatherer, expectedInspected int, expectedEliminated int) {
	const inspectedMetricName = "cortex_mimir_query_engine_common_subexpression_elimination_selectors_inspected"
	const eliminatedMetricName = "cortex_mimir_query_engine_common_subexpression_elimination_selectors_eliminated"

	expectedMetrics := fmt.Sprintf(`# HELP %[1]v Number of selectors inspected by the common subexpression elimination optimization pass, before elimination.
# TYPE %[1]v counter
%[1]v %[2]v
# HELP %[3]v Number of selectors eliminated by the common subexpression elimination optimization pass.
# TYPE %[3]v counter
%[3]v %v
`, inspectedMetricName, expectedInspected, eliminatedMetricName, expectedEliminated)

	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(expectedMetrics), inspectedMetricName, eliminatedMetricName))
}
