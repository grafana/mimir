// SPDX-License-Identifier: AGPL-3.0-only

package plan_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestEliminateCommonSubexpressions(t *testing.T) {
	testCases := map[string]struct {
		expr            string
		expectedPlan    string
		expectUnchanged bool
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
		"vector selector duplicated twice": {
			expr: `foo + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- VectorSelector: {__name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
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
			// TODO: verify that we don't inject the duplicate multiple times (need to detect this in applyDeduplication and short-circuit)
		},
	}

	opts := streamingpromql.NewTestEngineOpts()
	plannerWithoutOptimizationPass := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
	plannerWithoutOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})

	plannerWithOptimizationPass := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
	plannerWithOptimizationPass.RegisterASTOptimizationPass(&ast.CollapseConstants{})
	plannerWithOptimizationPass.RegisterQueryPlanOptimizationPass(&plan.EliminateCommonSubexpressions{})

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.expectUnchanged {
				p, err := plannerWithoutOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
				require.NoError(t, err)
				testCase.expectedPlan = p.String()
			}

			p, err := plannerWithOptimizationPass.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, trimIndent(testCase.expectedPlan), actual)
		})
	}
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
