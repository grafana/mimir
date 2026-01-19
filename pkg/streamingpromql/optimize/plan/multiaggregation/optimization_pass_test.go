// SPDX-License-Identifier: AGPL-3.0-only

package multiaggregation_test

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/multiaggregation"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestOptimizationPass(t *testing.T) {
	experimentalFunctionsEnabled := parser.EnableExperimentalFunctions
	parser.EnableExperimentalFunctions = true
	t.Cleanup(func() { parser.EnableExperimentalFunctions = experimentalFunctionsEnabled })

	testCases := map[string]struct {
		expr            string
		expectedPlan    string
		expectUnchanged bool
	}{
		"no common expressions": {
			expr:            `max(foo)`,
			expectUnchanged: true,
		},
		"two aggregations of same selector": {
			expr: `(max(foo) / min(foo)) + count(bar)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS / RHS
						- LHS: MultiAggregationConsumer: max
							- ref#1 MultiAggregationGroup
								- VectorSelector: {__name__="foo"}
						- RHS: MultiAggregationConsumer: min
							- ref#1 MultiAggregationGroup ...
					- RHS: AggregateExpression: count
						- VectorSelector: {__name__="bar"}
						`,
		},
		"multiple aggregations of same selector": {
			expr: `(max by (env) (foo) + avg by (region) (foo)) / (count by (cluster) (foo) + count(bar))`,
			expectedPlan: `
				- BinaryExpression: LHS / RHS
					- LHS: BinaryExpression: LHS + RHS
						- LHS: MultiAggregationConsumer: max by (env)
							- ref#1 MultiAggregationGroup
								- VectorSelector: {__name__="foo"}
						- RHS: MultiAggregationConsumer: avg by (region)
							- ref#1 MultiAggregationGroup ...
					- RHS: BinaryExpression: LHS + RHS
						- LHS: MultiAggregationConsumer: count by (cluster)
							- ref#1 MultiAggregationGroup ...
						- RHS: AggregateExpression: count
							- VectorSelector: {__name__="bar"}
			`,
		},
		"two aggregations of same function": {
			expr: `(max(rate(foo[5m])) / min(rate(foo[5m]))) + count(bar)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS / RHS
						- LHS: MultiAggregationConsumer: max
							- ref#1 MultiAggregationGroup
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="foo"}[5m0s]
						- RHS: MultiAggregationConsumer: min
							- ref#1 MultiAggregationGroup ...
					- RHS: AggregateExpression: count
						- VectorSelector: {__name__="bar"}
						`,
		},
		"multiple different instances where optimization applies": {
			expr: `(max(foo) / min(bar)) + (avg(bar) * sum(foo))`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: BinaryExpression: LHS / RHS
						- LHS: MultiAggregationConsumer: max
							- ref#2 MultiAggregationGroup
								- VectorSelector: {__name__="foo"}
						- RHS: MultiAggregationConsumer: min
							- ref#1 MultiAggregationGroup
								- VectorSelector: {__name__="bar"}
					- RHS: BinaryExpression: LHS * RHS
						- LHS: MultiAggregationConsumer: avg
							- ref#1 MultiAggregationGroup ...
						- RHS: MultiAggregationConsumer: sum
							- ref#2 MultiAggregationGroup ...
			`,
		},
		"common subexpression but not aggregated in either instance": {
			expr:            `foo + abs(foo)`,
			expectUnchanged: true,
		},
		"common subexpression but only aggregated in first instance": {
			expr:            `sum(foo) + foo`,
			expectUnchanged: true,
		},
		"common subexpression but only aggregated in other instance": {
			expr:            `foo + sum(foo)`,
			expectUnchanged: true,
		},

		// Test all of the supported aggregation operations are handled correctly.
		"same selector with sum and count aggregation": {
			expr: `sum(foo) + count(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: MultiAggregationConsumer: sum
						- ref#1 MultiAggregationGroup
							- VectorSelector: {__name__="foo"}
					- RHS: MultiAggregationConsumer: count
						- ref#1 MultiAggregationGroup ...
			`,
		},
		"same selector with min and max aggregation": {
			expr: `min(foo) + max(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: MultiAggregationConsumer: min
						- ref#1 MultiAggregationGroup
							- VectorSelector: {__name__="foo"}
					- RHS: MultiAggregationConsumer: max
						- ref#1 MultiAggregationGroup ...
			`,
		},
		"same selector with avg and group aggregation": {
			expr: `avg(foo) + group(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: MultiAggregationConsumer: avg
						- ref#1 MultiAggregationGroup
							- VectorSelector: {__name__="foo"}
					- RHS: MultiAggregationConsumer: group
						- ref#1 MultiAggregationGroup ...
			`,
		},
		"same selector with stddev and stdvar aggregation": {
			expr: `stddev(foo) + stdvar(foo)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: MultiAggregationConsumer: stddev
						- ref#1 MultiAggregationGroup
							- VectorSelector: {__name__="foo"}
					- RHS: MultiAggregationConsumer: stdvar
						- ref#1 MultiAggregationGroup ...
			`,
		},

		// Test all of the unsupported aggregation operations are handled correctly.
		"same selector but have both supported aggregation and unsupported quantile aggregation": {
			expr:            `sum(foo) + quantile(0.99, foo)`,
			expectUnchanged: true,
		},
		"same selector but have both supported aggregation and unsupported count_values aggregation": {
			expr:            `sum(foo) + quantile(0.99, foo)`,
			expectUnchanged: true,
		},
		"same selector but have both supported aggregation and unsupported topk aggregation": {
			expr:            `sum(foo) + topk(2, foo)`,
			expectUnchanged: true,
		},
		"same selector but have both supported aggregation and unsupported bottomk aggregation": {
			expr:            `sum(foo) + bottomk(2, foo)`,
			expectUnchanged: true,
		},
		"same selector but have both supported aggregation and unsupported limitk aggregation": {
			expr:            `sum(foo) + limitk(5, foo)`,
			expectUnchanged: true,
		},
		"same selector but have both supported aggregation and unsupported limit_ratio aggregation": {
			expr:            `sum(foo) + limit_ratio(0.9, foo)`,
			expectUnchanged: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			if testCase.expectUnchanged {
				testCase.expectedPlan = createPlan(t, testCase.expr, false, planning.MaximumSupportedQueryPlanVersion)
			}

			actual := createPlan(t, testCase.expr, true, planning.MaximumSupportedQueryPlanVersion)
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
}

func TestOptimizationPass_SupportedQueryPlanVersionTooLow(t *testing.T) {
	expr := `max(foo) / min(foo)`

	planWithout := createPlan(t, expr, false, planning.QueryPlanV4)
	planWith := createPlan(t, expr, true, planning.QueryPlanV4)

	require.Equal(t, planWithout, planWith)
}

func createPlan(t *testing.T, expr string, enableOptimizationPass bool, minimumQueryPlanVersion planning.QueryPlanVersion) string {
	ctx := user.InjectOrgID(context.Background(), "tenant-1")
	observer := streamingpromql.NoopPlanningObserver{}
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewStaticQueryPlanVersionProvider(minimumQueryPlanVersion))
	require.NoError(t, err)
	planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, opts.CommonOpts.Reg, opts.Logger))

	if enableOptimizationPass {
		planner.RegisterQueryPlanOptimizationPass(multiaggregation.NewOptimizationPass())
	}

	plan, err := planner.NewQueryPlan(ctx, expr, timeRange, observer)
	require.NoError(t, err)
	return plan.String()
}

func TestIsSupportedAggregationOperation(t *testing.T) {
	for i, name := range core.AggregationOperation_name {
		op := core.AggregationOperation(i)

		if op == core.AGGREGATION_UNKNOWN {
			continue
		}

		_, err := multiaggregation.IsSupportedAggregationOperation(op)
		require.NoErrorf(t, err, "got error for operation %s", name)
	}
}
