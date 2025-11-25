package plan_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	optimizetestutils "github.com/grafana/mimir/pkg/streamingpromql/optimize/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestEagerLoadMiddlewareSelectorsOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr         string
		expectedPlan string
	}{
		"expression without sharding or subqueries": {
			expr: `foo`,
			expectedPlan: `
				- VectorSelector: {__name__="foo"}
			`,
		},
		"expression with aggregation eligible for sharding": {
			expr: `sum(foo)`,
			expectedPlan: `
				- AggregateExpression: sum
					- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}, eager load
			`,
		},
		"expression with multiple aggregations eligible for sharding ": {
			expr: `sum(foo) + sum(bar)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}, eager load
					- RHS: AggregateExpression: sum
						- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(bar{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(bar{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}, eager load
			`,
		},
		"expression with subquery eligible for spin-off": {
			expr: `max_over_time(sum(foo)[5h:30s])`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: max_over_time(...)
						- MatrixSelector: {__query__="sum(foo)", __range__="5h0m0s", __step__="30s", __name__="__subquery_spinoff__"}[5h0m0s], eager load
			`,
		},
		"expression with subquery and other selector": {
			expr: `max_over_time(sum(foo)[5h:30s]) + foo`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: max_over_time(...)
							- MatrixSelector: {__query__="sum(foo)", __range__="5h0m0s", __step__="30s", __name__="__subquery_spinoff__"}[5h0m0s], eager load
					- RHS: VectorSelector: {__query__="foo", __name__="__downstream_query__"}, eager load
			`,
		},
		"expression with multiple selectors, none eligible for sharding or subquery spin-off": {
			expr: `foo + bar + baz`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: VectorSelector: {__name__="foo"}
						- RHS: VectorSelector: {__name__="bar"}
					- RHS: VectorSelector: {__name__="baz"}
			`,
		},
	}

	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	observer := streamingpromql.NoopPlanningObserver{}
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	planner.RegisterQueryPlanOptimizationPass(&plan.EagerLoadMiddlewareSelectorsOptimizationPass{})

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ast, err := parser.ParseExpr(testCase.expr)
			require.NoError(t, err)
			ast, err = optimizetestutils.RewriteForQuerySharding(ctx, ast)
			require.NoError(t, err)
			ast, err = optimizetestutils.RewriteForSubquerySpinoff(ctx, ast)
			require.NoError(t, err)

			actual, err := planner.NewQueryPlan(ctx, ast.String(), timeRange, observer)
			require.NoError(t, err)
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual.String())
		})
	}
}
