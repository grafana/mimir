// SPDX-License-Identifier: AGPL-3.0-only

// Different test package name to break import cycle
package plan_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestNarrowSelectorsOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr         string
		expectedPlan string
	}{
		"raw vector selector": {
			expr: `some_metric`,
			expectedPlan: `
				- VectorSelector: {__name__="some_metric"}
			`,
		},
		"binary expression raw vector selectors": {
			expr: `some_metric + some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression on raw vector selectors": {
			expr: `some_metric + on (cluster) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + on (cluster) RHS, hints (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"nested binary expression on raw vector selectors": {
			expr: `some_metric + (some_other_metric / on (cluster) some_third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS / on (cluster) RHS, hints (cluster)
						- LHS: VectorSelector: {__name__="some_other_metric"}
						- RHS: VectorSelector: {__name__="some_third_metric"}
			`,
		},
		"binary expression aggregation LHS raw vector selector RHS": {
			expr: `sum by (region) (some_metric) / some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression aggregation LHS aggregation RHS": {
			expr: `sum by (region) (some_metric) / sum(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression aggregation LHS aggregation RHS aggregation": {
			expr: `sum by (region) (some_metric) / sum by (region) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression multiple aggregation LHS aggregation RHS": {
			expr: `sum by (region, env) (some_metric) / sum(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region, env)
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression multiple aggregation LHS aggregation RHS aggregation": {
			expr: `sum by (region, env) (some_metric) / sum by (region, env) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region, env)
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression multiple aggregation LHS aggregation RHS aggregation different labels": {
			expr: `sum by (region, env) (some_metric) / sum by (region, cluster) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region, env)
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (region, cluster)
						- VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression on multiple aggregation LHS aggregation RHS aggregation": {
			expr: `sum by (region, env) (some_metric) / on(region) sum(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / on (region) RHS, hints (region)
					- LHS: AggregateExpression: sum by (region, env)
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression aggregation LHS aggregation and nested binary expression RHS": {
			expr: `sum by (region) (some_metric) / (sum(some_other_metric) + sum(some_third_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS
						- LHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_other_metric"}
						- RHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_third_metric"}
			`,
		},
		"binary expression aggregation LHS aggregation and nested binary expression RHS aggregation": {
			expr: `sum by (region) (some_metric) / (sum by (cluster) (some_other_metric) + sum(some_third_metric))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: BinaryExpression: LHS + RHS, hints (cluster)
						- LHS: AggregateExpression: sum by (cluster)
							- VectorSelector: {__name__="some_other_metric"}
						- RHS: AggregateExpression: sum
							- VectorSelector: {__name__="some_third_metric"}
			`,
		},
		"binary expression LHS aggregation RHS aggregation": {
			expr: `sum(some_metric) / sum by (cluster) (some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum
						- VectorSelector: {__name__="some_metric"}
					- RHS: AggregateExpression: sum by (cluster)
						- VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"binary expression with no selectors": {
			expr: `vector(1) + vector(0)`,
			expectedPlan: `
				- StepInvariantExpression: [step invariant]
					- BinaryExpression: LHS + RHS
						- LHS: FunctionCall: vector(...)
							- NumberLiteral: 1
						- RHS: FunctionCall: vector(...)
							- NumberLiteral: 0
			`,
		},
		"binary expression on with no selectors": {
			expr: `vector(1) + on (region) vector(0)`,
			expectedPlan: `
				- StepInvariantExpression: [step invariant]
					- BinaryExpression: LHS + on (region) RHS
						- LHS: FunctionCall: vector(...)
							- NumberLiteral: 1
						- RHS: FunctionCall: vector(...)
							- NumberLiteral: 0
			`,
		},
		// Make sure we don't modify query plans that have been rewritten to be sharded
		"binary expression that has been sharded": {
			expr: `sum by (container) (__embedded_queries__{__queries__="something"}) / sum by (container) (__embedded_queries__{__queries__="something else"})`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: AggregateExpression: sum by (container)
						- VectorSelector: {__queries__="something", __name__="__embedded_queries__"}
					- RHS: AggregateExpression: sum by (container)
						- VectorSelector: {__queries__="something else", __name__="__embedded_queries__"}
			`,
		},
		// Make sure we don't modify query plans that don't have a binary expression
		"aggregation and function call": {
			expr: `sum by (route) (rate(some_metric[5m]))`,
			expectedPlan: `
				- AggregateExpression: sum by (route)
					- DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="some_metric"}[5m0s]
			`,
		},
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
	require.NoError(t, err)
	planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, nil, opts.Logger))
	planner.RegisterQueryPlanOptimizationPass(plan.NewNarrowSelectorsOptimizationPass(opts.Logger))

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
}
