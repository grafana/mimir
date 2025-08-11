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
			// For now, we don't support using MQE's remote execution with shardable queries.
			// So we need to leave the query plan unchanged and allow it to fall through to the special
			// sharding Queryable used in the query-frontend, which will then make the requests to
			// queriers itself.
			expectedPlan: `
				- AggregateExpression: sum
					- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(my_metric{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(my_metric{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
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
	summer, err := astmapper.NewQueryShardSummer(maxShards, astmapper.VectorSquasher, log.NewNopLogger(), stats)
	if err != nil {
		return "", err
	}

	mapper := astmapper.NewSharding(summer)
	ast, err := parser.ParseExpr(expr)
	if err != nil {
		return "", err
	}

	shardedQuery, err := mapper.Map(ctx, ast)
	if err != nil {
		return "", err
	}

	if stats.GetShardedQueries() == 0 {
		// If no part of the query was shardable, then the query-frontend will use the original expression as-is.
		return expr, nil
	}

	return shardedQuery.String(), nil
}
