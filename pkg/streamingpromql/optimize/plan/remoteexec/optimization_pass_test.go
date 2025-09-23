// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr         string
		expectedPlan string
	}{
		"string expression": {
			expr: `"the string"`,
			expectedPlan: `
				- StringLiteral: "the string"
			`,
		},
		"scalar expression without selectors": {
			expr: `123`,
			expectedPlan: `
				- NumberLiteral: 123
			`,
		},
		"scalar expression with selector": {
			expr: `scalar(my_metric)`,
			expectedPlan: `
				- RemoteExecution
					- FunctionCall: scalar(...)
						- VectorSelector: {__name__="my_metric"}
			`,
		},
		"range vector expression": {
			expr: `my_metric[5m]`,
			expectedPlan: `
				- RemoteExecution
					- MatrixSelector: {__name__="my_metric"}[5m0s]
			`,
		},
		"unshardable instant vector expression": {
			expr: "my_metric",
			expectedPlan: `
				- RemoteExecution
					- VectorSelector: {__name__="my_metric"}
			`,
		},
		"unshardable instant vector expression with a function call with multiple arguments": {
			expr: "clamp(my_metric, 0, 1)",
			expectedPlan: `
				- RemoteExecution
					- DeduplicateAndMerge
						- FunctionCall: clamp(...)
							- param 0: VectorSelector: {__name__="my_metric"}
							- param 1: NumberLiteral: 0
							- param 2: NumberLiteral: 1
			`,
		},
		"unshardable instant vector expression with an aggregation with multiple arguments": {
			expr: "topk(5, my_metric)",
			expectedPlan: `
				- RemoteExecution
					- AggregateExpression: topk
						- expression: VectorSelector: {__name__="my_metric"}
						- parameter: NumberLiteral: 5
			`,
		},
		"shardable instant vector expression": {
			expr: `sum(my_metric)`,
			// Query-frontends use a query engine instance when evaluating sharded queries, and again when evaluating the individual legs.
			// For now, we don't want to apply remote execution to the sharded queries, only to the individual legs.
			// So we need to leave the query plan unchanged and allow it to fall through to the special
			// sharding Queryable used in the query-frontend, which will then make the requests to queriers itself.
			expectedPlan: `
				- AggregateExpression: sum
					- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(my_metric{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(my_metric{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
			`,
		},
		"subquery with spin-off": {
			expr: `sum_over_time(sum(my_metric)[6h:5m])`,
			// Similar to the sharding case above, we only want to apply remote execution to the individual legs, not the
			// overall query containing a subquery.
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: sum_over_time(...)
						- MatrixSelector: {__query__="sum(my_metric)", __range__="6h0m0s", __step__="5m0s", __name__="__subquery_spinoff__"}[6h0m0s]
			`,
		},
	}

	ctx := context.Background()
	observer := streamingpromql.NoopPlanningObserver{}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := streamingpromql.NewTestEngineOpts()
			planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
			require.NoError(t, err)
			planner.RegisterQueryPlanOptimizationPass(remoteexec.NewOptimizationPass())

			timeRange := types.NewInstantQueryTimeRange(time.Now())

			// Rewrite the query in a shardable form, if we can.
			testCase.expr, err = rewriteForQuerySharding(ctx, testCase.expr)
			require.NoError(t, err)

			// And do the same for queries eligible for subquery spin-off.
			testCase.expr, err = rewriteForSubquerySpinoff(ctx, testCase.expr)
			require.NoError(t, err)

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
}

func rewriteForQuerySharding(ctx context.Context, expr string) (string, error) {
	const maxShards = 2
	stats := astmapper.NewMapperStats()
	squasher := astmapper.EmbeddedQueriesSquasher
	summer := astmapper.NewQueryShardSummer(maxShards, false, squasher, log.NewNopLogger(), stats)
	ast, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	shardedQuery, err := summer.Map(ctx, ast)
	if err != nil {
		return "", err
	}

	return shardedQuery.String(), nil
}

func rewriteForSubquerySpinoff(ctx context.Context, expr string) (string, error) {
	stats := astmapper.NewSubquerySpinOffMapperStats()
	defaultStepFunc := func(rangeMillis int64) int64 { return 1000 }
	mapper := astmapper.NewSubquerySpinOffMapper(defaultStepFunc, log.NewNopLogger(), stats)
	ast, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	rewrittenQuery, err := mapper.Map(ctx, ast)
	if err != nil {
		return "", err
	}

	if stats.SpunOffSubqueries() == 0 {
		return expr, nil
	}

	return rewrittenQuery.String(), nil
}
