// SPDX-License-Identifier: AGPL-3.0-only

package remoteexec_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/remoteexec"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr                                string
		expectedPlan                        string
		expectedPlanWithMultiNodeRemoteExec string
		expectedPlanWithMiddlewareSharding  string
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
				- RemoteExecutionConsumer: node 0
					- RemoteExecutionGroup
						- node 0: FunctionCall: scalar(...)
							- VectorSelector: {__name__="my_metric"}
			`,
		},
		"range vector expression": {
			expr: `my_metric[5m]`,
			expectedPlan: `
				- RemoteExecutionConsumer: node 0
					- RemoteExecutionGroup
						- node 0: MatrixSelector: {__name__="my_metric"}[5m0s]
			`,
		},
		"unshardable instant vector expression": {
			expr: "my_metric",
			expectedPlan: `
				- RemoteExecutionConsumer: node 0
					- RemoteExecutionGroup
						- node 0: VectorSelector: {__name__="my_metric"}
			`,
		},
		"unshardable instant vector expression with a function call with multiple arguments": {
			expr: "clamp(my_metric, 0, 1)",
			expectedPlan: `
				- RemoteExecutionConsumer: node 0
					- RemoteExecutionGroup
						- node 0: DeduplicateAndMerge
							- FunctionCall: clamp(...)
								- param 0: VectorSelector: {__name__="my_metric"}
								- param 1: NumberLiteral: 0
								- param 2: NumberLiteral: 1
			`,
		},
		"unshardable instant vector expression with an aggregation with multiple arguments": {
			expr: "topk(5, my_metric)",
			expectedPlan: `
				- RemoteExecutionConsumer: node 0
					- RemoteExecutionGroup
						- node 0: AggregateExpression: topk
							- expression: VectorSelector: {__name__="my_metric"}
							- parameter: NumberLiteral: 5
			`,
		},
		"shardable instant vector expression": {
			expr: `sum(my_metric)`,
			expectedPlan: `
				- AggregateExpression: sum
					- FunctionCall: __sharded_concat__(...)
						- param 0: RemoteExecutionConsumer: node 0
							- RemoteExecutionGroup: eager load
								- node 0: AggregateExpression: sum
									- VectorSelector: {__query_shard__="1_of_2", __name__="my_metric"}
						- param 1: RemoteExecutionConsumer: node 0
							- RemoteExecutionGroup: eager load
								- node 0: AggregateExpression: sum
									- VectorSelector: {__query_shard__="2_of_2", __name__="my_metric"}
			`,

			// Query-frontends use a query engine instance when evaluating sharded queries, and again when evaluating the individual legs.
			// If we're using middleware sharding, we don't want to apply remote execution to the sharded queries, only to the individual legs.
			// So we need to leave the query plan unchanged and allow it to fall through to the special
			// sharding Queryable used in the query-frontend, which will then make the requests to queriers itself.
			expectedPlanWithMiddlewareSharding: `
				- AggregateExpression: sum
					- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(my_metric{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(my_metric{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
			`,
		},
		"multiple shardable expressions": {
			expr: `sum(my_metric) + count(my_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- VectorSelector: {__query_shard__="1_of_2", __name__="my_metric"}
							- param 1: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- VectorSelector: {__query_shard__="2_of_2", __name__="my_metric"}
					- RHS: AggregateExpression: sum
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: count
										- VectorSelector: {__query_shard__="1_of_2", __name__="my_other_metric"}
							- param 1: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: count
										- VectorSelector: {__query_shard__="2_of_2", __name__="my_other_metric"}
			`,

			expectedPlanWithMiddlewareSharding: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(my_metric{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(my_metric{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
					- RHS: AggregateExpression: sum
						- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"count(my_other_metric{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"count(my_other_metric{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
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
		"expression with common sharded subexpression": {
			expr: `sum(foo) + sum(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- AggregateExpression: sum
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- VectorSelector: {__query_shard__="1_of_2", __name__="foo"}
								- param 1: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- VectorSelector: {__query_shard__="2_of_2", __name__="foo"}
					- RHS: ref#1 Duplicate ...
			`,
			expectedPlanWithMiddlewareSharding: `
				- BinaryExpression: LHS + RHS
					- LHS: ref#1 Duplicate
						- AggregateExpression: sum
							- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
					- RHS: ref#1 Duplicate ...
			`,
		},
		"expression with common subexpression that is not sharded": {
			expr: `foo + foo`,
			expectedPlan: `
				- RemoteExecutionConsumer: node 0
					- RemoteExecutionGroup
						- node 0: BinaryExpression: LHS + RHS
							- LHS: ref#1 Duplicate
								- VectorSelector: {__name__="foo"}
							- RHS: ref#1 Duplicate ...
			`,
		},
		"expression with common instant vector selector but aggregation is different": {
			expr: `sum(foo) + max(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- ref#1 Duplicate
											- VectorSelector: {__query_shard__="1_of_2", __name__="foo"}
							- param 1: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- ref#2 Duplicate
											- VectorSelector: {__query_shard__="2_of_2", __name__="foo"}
					- RHS: AggregateExpression: max
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: max
										- ref#1 Duplicate ...
							- param 1: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: max
										- ref#2 Duplicate ...
			`,
			expectedPlanWithMultiNodeRemoteExec: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 0
								- ref#3 RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- ref#1 Duplicate
											- VectorSelector: {__query_shard__="1_of_2", __name__="foo"}
									- node 1: AggregateExpression: max
										- ref#1 Duplicate ...
							- param 1: RemoteExecutionConsumer: node 0
								- ref#4 RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- ref#2 Duplicate
											- VectorSelector: {__query_shard__="2_of_2", __name__="foo"}
									- node 1: AggregateExpression: max
										- ref#2 Duplicate ...
					- RHS: AggregateExpression: max
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 1
								- ref#3 RemoteExecutionGroup ...
							- param 1: RemoteExecutionConsumer: node 1
								- ref#4 RemoteExecutionGroup ...
			`,
			expectedPlanWithMiddlewareSharding: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
					- RHS: AggregateExpression: max
						- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"max(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"max(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
			`,
		},
		"expression with common instant vector selectors and some common aggregations": {
			expr: `sum(foo) + max(foo) + sum(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#3 Duplicate
							- AggregateExpression: sum
								- FunctionCall: __sharded_concat__(...)
									- param 0: RemoteExecutionConsumer: node 0
										- RemoteExecutionGroup: eager load
											- node 0: AggregateExpression: sum
												- ref#1 Duplicate
													- VectorSelector: {__query_shard__="1_of_2", __name__="foo"}
									- param 1: RemoteExecutionConsumer: node 0
										- RemoteExecutionGroup: eager load
											- node 0: AggregateExpression: sum
												- ref#2 Duplicate
													- VectorSelector: {__query_shard__="2_of_2", __name__="foo"}
						- RHS: AggregateExpression: max
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: max
											- ref#1 Duplicate ...
								- param 1: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: max
											- ref#2 Duplicate ...
					- RHS: ref#3 Duplicate ...
			`,
			expectedPlanWithMultiNodeRemoteExec: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#5 Duplicate
							- AggregateExpression: sum
								- FunctionCall: __sharded_concat__(...)
									- param 0: RemoteExecutionConsumer: node 0
										- ref#3 RemoteExecutionGroup: eager load
											- node 0: AggregateExpression: sum
												- ref#1 Duplicate
													- VectorSelector: {__query_shard__="1_of_2", __name__="foo"}
											- node 1: AggregateExpression: max
												- ref#1 Duplicate ...
									- param 1: RemoteExecutionConsumer: node 0
										- ref#4 RemoteExecutionGroup: eager load
											- node 0: AggregateExpression: sum
												- ref#2 Duplicate
													- VectorSelector: {__query_shard__="2_of_2", __name__="foo"}
											- node 1: AggregateExpression: max
												- ref#2 Duplicate ...
						- RHS: AggregateExpression: max
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 1
									- ref#3 RemoteExecutionGroup ...
								- param 1: RemoteExecutionConsumer: node 1
									- ref#4 RemoteExecutionGroup ...
					- RHS: ref#5 Duplicate ...
			`,
			expectedPlanWithMiddlewareSharding: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: ref#1 Duplicate
							- AggregateExpression: sum
								- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
						- RHS: AggregateExpression: max
							- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"max(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"max(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
					- RHS: ref#1 Duplicate ...
			`,
		},
		"expression with multiple common instant vector selectors": {
			expr: `(sum(foo) + max(foo)) * (sum(bar) - min(bar))`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: AggregateExpression: sum
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- ref#1 Duplicate
												- VectorSelector: {__query_shard__="1_of_2", __name__="foo"}
								- param 1: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- ref#2 Duplicate
												- VectorSelector: {__query_shard__="2_of_2", __name__="foo"}
						- RHS: AggregateExpression: max
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: max
											- ref#1 Duplicate ...
								- param 1: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: max
											- ref#2 Duplicate ...
					- RHS: BinaryExpression: LHS - RHS
						- LHS: AggregateExpression: sum
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- ref#3 Duplicate
												- VectorSelector: {__query_shard__="1_of_2", __name__="bar"}
								- param 1: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- ref#4 Duplicate
												- VectorSelector: {__query_shard__="2_of_2", __name__="bar"}
						- RHS: AggregateExpression: min
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: min
											- ref#3 Duplicate ...
								- param 1: RemoteExecutionConsumer: node 0
									- RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: min
											- ref#4 Duplicate ...
			`,
			expectedPlanWithMultiNodeRemoteExec: `
				- BinaryExpression: LHS * RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: AggregateExpression: sum
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 0
									- ref#3 RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- ref#1 Duplicate
												- VectorSelector: {__query_shard__="1_of_2", __name__="foo"}
										- node 1: AggregateExpression: max
											- ref#1 Duplicate ...
								- param 1: RemoteExecutionConsumer: node 0
									- ref#4 RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- ref#2 Duplicate
												- VectorSelector: {__query_shard__="2_of_2", __name__="foo"}
										- node 1: AggregateExpression: max
											- ref#2 Duplicate ...
						- RHS: AggregateExpression: max
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 1
									- ref#3 RemoteExecutionGroup ...
								- param 1: RemoteExecutionConsumer: node 1
									- ref#4 RemoteExecutionGroup ...
					- RHS: BinaryExpression: LHS - RHS
						- LHS: AggregateExpression: sum
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 0
									- ref#7 RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- ref#5 Duplicate
												- VectorSelector: {__query_shard__="1_of_2", __name__="bar"}
										- node 1: AggregateExpression: min
											- ref#5 Duplicate ...
								- param 1: RemoteExecutionConsumer: node 0
									- ref#8 RemoteExecutionGroup: eager load
										- node 0: AggregateExpression: sum
											- ref#6 Duplicate
												- VectorSelector: {__query_shard__="2_of_2", __name__="bar"}
										- node 1: AggregateExpression: min
											- ref#6 Duplicate ...
						- RHS: AggregateExpression: min
							- FunctionCall: __sharded_concat__(...)
								- param 0: RemoteExecutionConsumer: node 1
									- ref#7 RemoteExecutionGroup ...
								- param 1: RemoteExecutionConsumer: node 1
									- ref#8 RemoteExecutionGroup ...
			`,
			expectedPlanWithMiddlewareSharding: `
				- BinaryExpression: LHS * RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: AggregateExpression: sum
							- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
						- RHS: AggregateExpression: max
							- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"max(foo{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"max(foo{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
					- RHS: BinaryExpression: LHS - RHS
						- LHS: AggregateExpression: sum
							- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(bar{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"sum(bar{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
						- RHS: AggregateExpression: min
							- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"min(bar{__query_shard__=\\\"1_of_2\\\"})\"},{\"Expr\":\"min(bar{__query_shard__=\\\"2_of_2\\\"})\"}]}", __name__="__embedded_queries__"}
			`,
		},
		"expression with common range vector selector but aggregation is different": {
			expr: `sum(rate(foo[5m])) + max(rate(foo[5m]))`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- ref#1 Duplicate
											- DeduplicateAndMerge
												- FunctionCall: rate(...)
													- MatrixSelector: {__query_shard__="1_of_2", __name__="foo"}[5m0s]
							- param 1: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- ref#2 Duplicate
											- DeduplicateAndMerge
												- FunctionCall: rate(...)
													- MatrixSelector: {__query_shard__="2_of_2", __name__="foo"}[5m0s]
					- RHS: AggregateExpression: max
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: max
										- ref#1 Duplicate ...
							- param 1: RemoteExecutionConsumer: node 0
								- RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: max
										- ref#2 Duplicate ...
			`,
			expectedPlanWithMultiNodeRemoteExec: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 0
								- ref#3 RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- ref#1 Duplicate
											- DeduplicateAndMerge
												- FunctionCall: rate(...)
													- MatrixSelector: {__query_shard__="1_of_2", __name__="foo"}[5m0s]
									- node 1: AggregateExpression: max
										- ref#1 Duplicate ...
							- param 1: RemoteExecutionConsumer: node 0
								- ref#4 RemoteExecutionGroup: eager load
									- node 0: AggregateExpression: sum
										- ref#2 Duplicate
											- DeduplicateAndMerge
												- FunctionCall: rate(...)
													- MatrixSelector: {__query_shard__="2_of_2", __name__="foo"}[5m0s]
									- node 1: AggregateExpression: max
										- ref#2 Duplicate ...
					- RHS: AggregateExpression: max
						- FunctionCall: __sharded_concat__(...)
							- param 0: RemoteExecutionConsumer: node 1
								- ref#3 RemoteExecutionGroup ...
							- param 1: RemoteExecutionConsumer: node 1
								- ref#4 RemoteExecutionGroup ...
			`,
			expectedPlanWithMiddlewareSharding: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: sum
						- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"sum(rate(foo{__query_shard__=\\\"1_of_2\\\"}[5m]))\"},{\"Expr\":\"sum(rate(foo{__query_shard__=\\\"2_of_2\\\"}[5m]))\"}]}", __name__="__embedded_queries__"}
					- RHS: AggregateExpression: max
						- VectorSelector: {__queries__="{\"Concat\":[{\"Expr\":\"max(rate(foo{__query_shard__=\\\"1_of_2\\\"}[5m]))\"},{\"Expr\":\"max(rate(foo{__query_shard__=\\\"2_of_2\\\"}[5m]))\"}]}", __name__="__embedded_queries__"}
			`,
		},
	}

	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	observer := streamingpromql.NoopPlanningObserver{}
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	runTestCase := func(t *testing.T, expr string, expected string, enableMiddlewareSharding bool, enableMultiNodeRemoteExec bool, supportedQueryPlanVersion planning.QueryPlanVersion) {
		require.False(t, enableMiddlewareSharding && enableMultiNodeRemoteExec, "invalid test case: cannot enable both middleware sharding and multi-node remote execution")

		opts := streamingpromql.NewTestEngineOpts()
		planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewStaticQueryPlanVersionProvider(supportedQueryPlanVersion))
		require.NoError(t, err)
		planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, opts.CommonOpts.Reg, opts.Logger))
		planner.RegisterQueryPlanOptimizationPass(remoteexec.NewOptimizationPass(enableMultiNodeRemoteExec))

		if enableMiddlewareSharding {
			// Rewrite the query in a shardable form.
			expr, err = rewriteForQuerySharding(ctx, expr)
			require.NoError(t, err)
		} else {
			planner.RegisterASTOptimizationPass(sharding.NewOptimizationPass(&mockLimits{}, 0, nil, opts.Logger))
		}

		// And do the same for queries eligible for subquery spin-off.
		expr, err = rewriteForSubquerySpinoff(ctx, expr)
		require.NoError(t, err)

		p, err := planner.NewQueryPlan(ctx, expr, timeRange, false, observer)
		require.NoError(t, err)
		actual := p.String()
		require.Equal(t, testutils.TrimIndent(expected), actual)
	}

	for name, testCase := range testCases {
		if testCase.expectedPlanWithMultiNodeRemoteExec == "" {
			testCase.expectedPlanWithMultiNodeRemoteExec = testCase.expectedPlan
		}

		if testCase.expectedPlanWithMiddlewareSharding == "" {
			testCase.expectedPlanWithMiddlewareSharding = testCase.expectedPlan
		}

		t.Run(name, func(t *testing.T) {
			t.Run("MQE sharding: multi-node disabled", func(t *testing.T) {
				runTestCase(t, testCase.expr, testCase.expectedPlan, false, false, planning.MaximumSupportedQueryPlanVersion)
			})

			t.Run("MQE sharding: multi-node enabled but not supported by querier", func(t *testing.T) {
				runTestCase(t, testCase.expr, testCase.expectedPlan, false, true, planning.QueryPlanV2)
			})

			t.Run("MQE sharding: multi-node enabled and supported by querier", func(t *testing.T) {
				runTestCase(t, testCase.expr, testCase.expectedPlanWithMultiNodeRemoteExec, false, true, planning.QueryPlanV3)
			})

			t.Run("middleware sharding", func(t *testing.T) {
				runTestCase(t, testCase.expr, testCase.expectedPlanWithMiddlewareSharding, true, false, planning.MaximumSupportedQueryPlanVersion)
			})
		})
	}
}

func rewriteForQuerySharding(ctx context.Context, expr string) (string, error) {
	const maxShards = 2
	stats := astmapper.NewMapperStats()
	squasher := astmapper.EmbeddedQueriesSquasher
	summer := astmapper.NewQueryShardSummer(maxShards, squasher, log.NewNopLogger(), stats)
	ast, err := promqlext.NewPromQLParser().ParseExpr(expr)
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
	ast, err := promqlext.NewPromQLParser().ParseExpr(expr)
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

type mockLimits struct{}

func (m *mockLimits) QueryShardingTotalShards(userID string) int        { return 2 }
func (m *mockLimits) QueryShardingMaxRegexpSizeBytes(userID string) int { return 0 }
func (m *mockLimits) QueryShardingMaxShardedQueries(userID string) int  { return 0 }
func (m *mockLimits) CompactorSplitAndMergeShards(userID string) int    { return 0 }
