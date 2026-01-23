// SPDX-License-Identifier: AGPL-3.0-only

// Different test package name to break import cycle
package plan_test

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
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

const expectedMetricsTemplate = `
	# HELP cortex_mimir_query_engine_narrow_selectors_attempted_total Total number of queries that the optimization pass has attempted to add hints to narrow selectors for.
    # TYPE cortex_mimir_query_engine_narrow_selectors_attempted_total counter
    cortex_mimir_query_engine_narrow_selectors_attempted_total %d
    # HELP cortex_mimir_query_engine_narrow_selectors_modified_total Total number of queries where the optimization pass has been able to add hints to narrow selectors for.
    # TYPE cortex_mimir_query_engine_narrow_selectors_modified_total counter
    cortex_mimir_query_engine_narrow_selectors_modified_total %d
`

func TestNarrowSelectorsOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr             string
		expectedPlan     string
		expectedAttempts int
		expectedModified int
	}{
		"raw vector selector": {
			expr: `some_metric`,
			expectedPlan: `
				- VectorSelector: {__name__="some_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression raw vector selectors": {
			expr: `some_metric + some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression on raw vector selectors": {
			expr: `some_metric + on (cluster) some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS + on (cluster) RHS, hints (cluster)
					- LHS: VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression aggregation LHS raw vector selector RHS": {
			expr: `sum by (region) (some_metric) / some_other_metric`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS, hints (region)
					- LHS: AggregateExpression: sum by (region)
						- VectorSelector: {__name__="some_metric"}
					- RHS: VectorSelector: {__name__="some_other_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 1,
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
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression with no selectors": {
			expr: `vector(1) + vector(0)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: FunctionCall: vector(...)
						- NumberLiteral: 1
					- RHS: FunctionCall: vector(...)
						- NumberLiteral: 0
			`,
			expectedAttempts: 0,
			expectedModified: 0,
		},
		"binary expression on with no selectors": {
			expr: `vector(1) + on (region) vector(0)`,
			expectedPlan: `
				- BinaryExpression: LHS + on (region) RHS
					- LHS: FunctionCall: vector(...)
						- NumberLiteral: 1
					- RHS: FunctionCall: vector(...)
						- NumberLiteral: 0
			`,
			expectedAttempts: 0,
			expectedModified: 0,
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
			expectedAttempts: 0,
			expectedModified: 0,
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
			expectedAttempts: 1,
			expectedModified: 0,
		},
		// Make sure we don't modify query plans that modify labels
		"binary expression with label_replace on one side": {
			expr: `sum by (statefulset) (kube_statefulset_replicas) - sum by (statefulset) (label_replace(not_ready, "statefulset", "$1", "job", ".+/(.+)"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS
					- LHS: AggregateExpression: sum by (statefulset)
						- VectorSelector: {__name__="kube_statefulset_replicas"}
					- RHS: AggregateExpression: sum by (statefulset)
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: VectorSelector: {__name__="not_ready"}
								- param 1: StringLiteral: "statefulset"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "job"
								- param 4: StringLiteral: ".+/(.+)"
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression with label_join on one side": {
			expr: `sum by (statefulset) (kube_statefulset_replicas) - sum by (statefulset) (label_join(not_ready, "statefulset", "job", "workload"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS
					- LHS: AggregateExpression: sum by (statefulset)
						- VectorSelector: {__name__="kube_statefulset_replicas"}
					- RHS: AggregateExpression: sum by (statefulset)
						- DeduplicateAndMerge
							- FunctionCall: label_join(...)
								- param 0: VectorSelector: {__name__="not_ready"}
								- param 1: StringLiteral: "statefulset"
								- param 2: StringLiteral: "job"
								- param 3: StringLiteral: "workload"
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"binary expression with label_replace on one side for non-hint label": {
			expr: `sum by (env, region) (first_metric) - sum by (env, region) (label_replace(second_metric, "region", "$1", "job", ".+/(.+)"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS, hints (env)
					- LHS: AggregateExpression: sum by (env, region)
						- VectorSelector: {__name__="first_metric"}
					- RHS: AggregateExpression: sum by (env, region)
						- DeduplicateAndMerge
							- FunctionCall: label_replace(...)
								- param 0: VectorSelector: {__name__="second_metric"}
								- param 1: StringLiteral: "region"
								- param 2: StringLiteral: "$1"
								- param 3: StringLiteral: "job"
								- param 4: StringLiteral: ".+/(.+)"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"binary expression with label_join on one side for non-hint label": {
			expr: `sum by (env, region) (first_metric) - sum by (env, region) (label_join(second_metric, "region", "job", "workload"))`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS, hints (env)
					- LHS: AggregateExpression: sum by (env, region)
						- VectorSelector: {__name__="first_metric"}
					- RHS: AggregateExpression: sum by (env, region)
						- DeduplicateAndMerge
							- FunctionCall: label_join(...)
								- param 0: VectorSelector: {__name__="second_metric"}
								- param 1: StringLiteral: "region"
								- param 2: StringLiteral: "job"
								- param 3: StringLiteral: "workload"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"multiple binary expressions with label_replace on only a single one": {
			expr: `(first_metric * on (env, region) second_metric) * on (env, region) label_replace(third_metric, "region", "$1", "cluster", ".*")`,
			expectedPlan: `
				- BinaryExpression: LHS * on (env, region) RHS, hints (env)
					- LHS: BinaryExpression: LHS * on (env, region) RHS, hints (env, region)
						- LHS: VectorSelector: {__name__="first_metric"}
						- RHS: VectorSelector: {__name__="second_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: label_replace(...)
							- param 0: VectorSelector: {__name__="third_metric"}
							- param 1: StringLiteral: "region"
							- param 2: StringLiteral: "$1"
							- param 3: StringLiteral: "cluster"
							- param 4: StringLiteral: ".*"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"multiple binary expressions with label_join on only a single one": {
			expr: `(sum by (env, region)(rate(first_metric[5m])) * sum(rate(second_metric[5m]))) / on (env, region) label_join(rate(third_metric[5m]), "region", "job", "workload")`,
			expectedPlan: `
				- BinaryExpression: LHS / on (env, region) RHS, hints (env)
					- LHS: BinaryExpression: LHS * RHS, hints (env, region)
						- LHS: AggregateExpression: sum by (env, region)
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="first_metric"}[5m0s]
						- RHS: AggregateExpression: sum
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="second_metric"}[5m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: label_join(...)
							- param 0: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="third_metric"}[5m0s]
							- param 1: StringLiteral: "region"
							- param 2: StringLiteral: "job"
							- param 3: StringLiteral: "workload"
			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"multiple binary expressions with label_replace on only a single one on LHS": {
			expr: `label_replace(first_metric, "region", "$1", "cluster", ".*") * on (env, region) (second_metric * on (env, region) third_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * on (env, region) RHS, hints (env)
					- LHS: DeduplicateAndMerge
						- FunctionCall: label_replace(...)
							- param 0: VectorSelector: {__name__="first_metric"}
							- param 1: StringLiteral: "region"
							- param 2: StringLiteral: "$1"
							- param 3: StringLiteral: "cluster"
							- param 4: StringLiteral: ".*"
					- RHS: BinaryExpression: LHS * on (env, region) RHS, hints (env, region)
						- LHS: VectorSelector: {__name__="second_metric"}
						- RHS: VectorSelector: {__name__="third_metric"}

			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"multiple binary expressions with nested label_replace on LHS": {
			expr: `
				label_replace(label_replace(first_metric, "region", "$1", "cluster", ".*"), "env", "$1", "deployment", ".*")
				* on (env, region)
				(second_metric * on (env, region) third_metric)
			`,
			expectedPlan: `
				- BinaryExpression: LHS * on (env, region) RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: label_replace(...)
							- param 0: DeduplicateAndMerge
								- FunctionCall: label_replace(...)
									- param 0: VectorSelector: {__name__="first_metric"}
									- param 1: StringLiteral: "region"
									- param 2: StringLiteral: "$1"
									- param 3: StringLiteral: "cluster"
									- param 4: StringLiteral: ".*"
							- param 1: StringLiteral: "env"
							- param 2: StringLiteral: "$1"
							- param 3: StringLiteral: "deployment"
							- param 4: StringLiteral: ".*"
					- RHS: BinaryExpression: LHS * on (env, region) RHS, hints (env, region)
						- LHS: VectorSelector: {__name__="second_metric"}
						- RHS: VectorSelector: {__name__="third_metric"}

			`,
			expectedAttempts: 1,
			expectedModified: 1,
		},
		"logical or binary expression should not have hints added": {
			expr: `
				first_metric
				or on (env, region)
				second_metric
			`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or on (env, region) RHS
						- LHS: VectorSelector: {__name__="first_metric"}
						- RHS: VectorSelector: {__name__="second_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
		"logical unless binary expression should not have hints added": {
			expr: `
				first_metric
				unless on (env, region)
				second_metric
			`,
			expectedPlan: `
				- BinaryExpression: LHS unless on (env, region) RHS
					- LHS: VectorSelector: {__name__="first_metric"}
					- RHS: VectorSelector: {__name__="second_metric"}
			`,
			expectedAttempts: 1,
			expectedModified: 0,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			observer := streamingpromql.NoopPlanningObserver{}

			opts := streamingpromql.NewTestEngineOpts()
			planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)
			planner.RegisterQueryPlanOptimizationPass(plan.NewNarrowSelectorsOptimizationPass(opts.CommonOpts.Reg, opts.Logger))

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			expectedMetrics := fmt.Sprintf(expectedMetricsTemplate, testCase.expectedAttempts, testCase.expectedModified)
			reg := opts.CommonOpts.Reg.(*prometheus.Registry)
			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_mimir_query_engine_narrow_selectors_attempted_total", "cortex_mimir_query_engine_narrow_selectors_modified_total"))
		})
	}
}
