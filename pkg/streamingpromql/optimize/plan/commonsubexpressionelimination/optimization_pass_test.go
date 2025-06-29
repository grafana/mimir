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
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr                        string
		expectedPlan                string
		expectUnchanged             bool
		expectedDuplicateNodes      int // Check that we don't do unnecessary work introducing a duplicate node multiple times when starting from different selectors (eg. when handling (a+b) + (a+b)).
		expectedSelectorsEliminated int
	}{
		"single vector selector": {
			expr:            `foo`,
			expectUnchanged: true,
		},
		"single matrix selector": {
			expr:            `foo[5m]`,
			expectUnchanged: true,
		},
		"single subquery": {
			expr:            `foo[5m:10s]`,
			expectUnchanged: true,
		},
		"duplicated numeric literal": {
			expr:            `1 + 1`,
			expectUnchanged: true,
		},
		"duplicated string literal": {
			expr:            `label_join(foo, "abc", "-") + label_join(bar, "def", ",")`,
			expectUnchanged: true,
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
		},
		"same selector used for both vector and matrix selector": {
			expr:            `foo + rate(foo[5m])`,
			expectUnchanged: true,
		},
		"duplicate matrix selectors with different outer function": {
			// We do not want to deduplicate matrix selectors.
			expr:            `rate(foo[5m]) + increase(foo[5m])`,
			expectUnchanged: true,
		},
		"duplicate matrix selectors, some with different outer function": {
			// We do not want to deduplicate matrix selectors themselves, but do want to deduplicate duplicate functions over matrix selectors.
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1, // We only eliminate one 'foo' selector in the duplicate rate(...) expression.
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
		},
		"duplicate subqueries with different outer function": {
			// We do not want to deduplicate subqueries directly, but do want to deduplicate their contents if they are the same.
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
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
		},
		"duplicate subqueries with different outer function and multiple child selectors": {
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
			expectedDuplicateNodes:      1, // This test ensures that we don't do unnecessary work when traversing up from both the a and b selectors.
			expectedSelectorsEliminated: 2,
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
		},
		"duplicate subqueries with different ranges": {
			expr:            `max_over_time(rate(foo[5m:])[10m:]) + max_over_time(rate(foo[5m:])[7m:])`,
			expectUnchanged: true, // We don't support deduplicating common expressions that are evaluated over different ranges.
		},
		"duplicate selectors, both with timestamp()": {
			expr: `timestamp(foo) + timestamp(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- FunctionCall: timestamp(...)
							- VectorSelector: {__name__="foo"} (return sample timestamps)
					- RHS: ref#1 Duplicate ...
			`,
			expectedDuplicateNodes:      1,
			expectedSelectorsEliminated: 1,
		},
		"duplicate selectors, one with timestamp()": {
			expr:            `timestamp(foo) + foo`,
			expectUnchanged: true, // In the future, we might be able to deduplicate these, but for now, treat them as unique expressions.
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
		},
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := streamingpromql.NewTestEngineOpts()
			plannerWithoutOptimizationPass := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
			plannerWithoutOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})

			reg := prometheus.NewPedanticRegistry()
			plannerWithOptimizationPass := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
			plannerWithOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})
			plannerWithOptimizationPass.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(reg))

			if testCase.expectUnchanged {
				p, err := plannerWithoutOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
				require.NoError(t, err)
				testCase.expectedPlan = p.String()
			}

			p, err := plannerWithOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, trimIndent(testCase.expectedPlan), actual)

			requireDuplicateNodeCount(t, reg, testCase.expectedDuplicateNodes)
			requireSelectorsEliminatedCount(t, reg, testCase.expectedSelectorsEliminated)
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

func requireSelectorsEliminatedCount(t *testing.T, g prometheus.Gatherer, expected int) {
	const metricName = "cortex_mimir_query_engine_common_subexpression_elimination_selectors_eliminated"

	expectedMetrics := fmt.Sprintf(`# HELP %v Number of selectors eliminated by the common subexpression elimination optimization pass.
# TYPE %v counter
%v %v
`, metricName, metricName, metricName, expected)

	require.NoError(t, testutil.GatherAndCompare(g, strings.NewReader(expectedMetrics), metricName))
}

func trimIndent(s string) string {
	lines := strings.Split(s, "\n")

	// Remove leading empty lines
	for len(lines) > 0 && isEmpty(lines[0]) {
		lines = lines[1:]
	}

	// Remove trailing empty lines
	for len(lines) > 0 && isEmpty(lines[len(lines)-1]) {
		lines = lines[:len(lines)-1]
	}

	if len(lines) == 0 {
		return ""
	}

	// Identify the indentation applied to the first line, and remove it from all lines.
	indentation := ""
	for _, char := range lines[0] {
		if char != '\t' {
			break
		}
		indentation += string(char)
	}

	for i, line := range lines {
		lines[i] = strings.TrimPrefix(line, indentation)
	}

	return strings.Join(lines, "\n")
}

func isEmpty(s string) bool {
	return strings.TrimSpace(s) == ""
}
