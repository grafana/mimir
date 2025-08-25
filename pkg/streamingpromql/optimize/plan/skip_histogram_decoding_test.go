// SPDX-License-Identifier: AGPL-3.0-only

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

func TestSkipHistogramDecodingOptimizationPass(t *testing.T) {
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
		"single vector selector with histogram_count": {
			expr: `histogram_count(some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_count(...)
						- VectorSelector: {__name__="some_metric"}, skip histogram buckets
			`,
		},
		"single vector selector with histogram_sum": {
			expr: `histogram_sum(some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- VectorSelector: {__name__="some_metric"}, skip histogram buckets
			`,
		},
		"single vector selector with histogram_avg": {
			expr: `histogram_avg(some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_avg(...)
						- VectorSelector: {__name__="some_metric"}, skip histogram buckets
			`,
		},
		"single vector selector with histogram_quantile": {
			expr: `histogram_quantile(0.5, some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_quantile(...)
						- param 0: NumberLiteral: 0.5
						- param 1: VectorSelector: {__name__="some_metric"}
			`,
		},
		"single vector selector with histogram_fraction": {
			expr: `histogram_fraction(1, 2, some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_fraction(...)
						- param 0: NumberLiteral: 1
						- param 1: NumberLiteral: 2
						- param 2: VectorSelector: {__name__="some_metric"}
			`,
		},
		"vector selector eligible for skipping decoding in binary expression": {
			expr: `2 * histogram_sum(some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS * RHS
						- LHS: NumberLiteral: 2
						- RHS: DeduplicateAndMerge
							- FunctionCall: histogram_sum(...)
								- VectorSelector: {__name__="some_metric"}, skip histogram buckets
			`,
		},
		"vector selector eligible for skipping decoding in binary expression inside function call": {
			expr: `histogram_sum(2 * some_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- DeduplicateAndMerge
							- BinaryExpression: LHS * RHS
								- LHS: NumberLiteral: 2
								- RHS: VectorSelector: {__name__="some_metric"}, skip histogram buckets
			`,
		},
		"vector selectors eligible for skipping decoding in binary expression inside function call": {
			expr: `histogram_sum(some_metric + some_other_metric)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS + RHS
							- LHS: VectorSelector: {__name__="some_metric"}, skip histogram buckets
							- RHS: VectorSelector: {__name__="some_other_metric"}, skip histogram buckets
			`,
		},
		"inner vector selector not eligible for skipping decoding due to nesting": {
			expr: `histogram_sum(some_metric * histogram_quantile(0.5, some_other_metric))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: VectorSelector: {__name__="some_metric"}, skip histogram buckets
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_quantile(...)
									- param 0: NumberLiteral: 0.5
									- param 1: VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"both vector selectors eligible for skipping decoding despite nesting": {
			expr: `histogram_sum(some_metric * histogram_count(some_other_metric))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: VectorSelector: {__name__="some_metric"}, skip histogram buckets
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_count(...)
									- VectorSelector: {__name__="some_other_metric"}, skip histogram buckets
			`,
		},
		"inner vector selector eligible for skipping decoding due to nesting": {
			expr: `histogram_quantile(0.5, some_metric * histogram_count(some_other_metric))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_quantile(...)
						- param 0: NumberLiteral: 0.5
						- param 1: BinaryExpression: LHS * RHS
							- LHS: VectorSelector: {__name__="some_metric"}
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_count(...)
									- VectorSelector: {__name__="some_other_metric"}, skip histogram buckets
			`,
		},
		"duplicate vector selector not eligible for skipping decoding due to nesting": {
			expr: `histogram_sum(some_metric * histogram_quantile(0.5, some_metric))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_quantile(...)
									- param 0: NumberLiteral: 0.5
									- param 1: ref#1 Duplicate ...
			`,
		},
		"different vector selectors, one eligible for skipping decoding, one not": {
			expr: `histogram_sum(some_metric) * histogram_quantile(0.5, some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- VectorSelector: {__name__="some_metric"}, skip histogram buckets
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: VectorSelector: {__name__="some_other_metric"}
			`,
		},
		"duplicate vector selector, both eligible for skipping decoding": {
			expr: `histogram_sum(some_metric) * histogram_count(some_other_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- VectorSelector: {__name__="some_metric"}, skip histogram buckets
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_count(...)
							- VectorSelector: {__name__="some_other_metric"}, skip histogram buckets
			`,
		},
		"duplicate vector selector, only first instance eligible for skipping decoding": {
			expr: `histogram_sum(some_metric) * histogram_quantile(0.5, some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate ...
			`,
		},
		"duplicate vector selector, only second instance eligible for skipping decoding": {
			expr: `histogram_quantile(0.5, some_metric) * histogram_sum(some_metric)`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate
								- VectorSelector: {__name__="some_metric"}
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate ...
			`,
		},
		"raw matrix selector": {
			expr: `rate(some_metric[1m])`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: rate(...)
						- MatrixSelector: {__name__="some_metric"}[1m0s]
			`,
		},
		"single matrix selector with histogram_count": {
			expr: `histogram_count(rate(some_metric[1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_count(...)
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
			`,
		},
		"single matrix selector with histogram_sum": {
			expr: `histogram_sum(rate(some_metric[1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
			`,
		},
		"single matrix selector with histogram_avg": {
			expr: `histogram_avg(rate(some_metric[1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_avg(...)
						- DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
			`,
		},
		"single matrix selector with histogram_quantile": {
			expr: `histogram_quantile(0.5, rate(some_metric[1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_quantile(...)
						- param 0: NumberLiteral: 0.5
						- param 1: DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="some_metric"}[1m0s]
			`,
		},
		"single matrix selector with histogram_fraction": {
			expr: `histogram_fraction(1, 2, rate(some_metric[1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_fraction(...)
						- param 0: NumberLiteral: 1
						- param 1: NumberLiteral: 2
						- param 2: DeduplicateAndMerge
							- FunctionCall: rate(...)
								- MatrixSelector: {__name__="some_metric"}[1m0s]
			`,
		},
		"matrix selector eligible for skipping decoding in binary expression": {
			expr: `2 * histogram_sum(rate(some_metric[1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS * RHS
						- LHS: NumberLiteral: 2
						- RHS: DeduplicateAndMerge
							- FunctionCall: histogram_sum(...)
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
			`,
		},
		"matrix selector eligible for skipping decoding in binary expression inside function call": {
			expr: `histogram_sum(2 * rate(some_metric[1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- DeduplicateAndMerge
							- BinaryExpression: LHS * RHS
								- LHS: NumberLiteral: 2
								- RHS: DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
			`,
		},
		"matrix selectors eligible for skipping decoding in binary expression inside function call": {
			expr: `histogram_sum(rate(some_metric[1m]) + rate(some_other_metric[1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS + RHS
							- LHS: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
							- RHS: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_other_metric"}[1m0s], skip histogram buckets
			`,
		},
		"inner matrix selector not eligible for skipping decoding due to nesting": {
			expr: `histogram_sum(rate(some_metric[1m]) * histogram_quantile(0.5, rate(some_other_metric[1m])))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_quantile(...)
									- param 0: NumberLiteral: 0.5
									- param 1: DeduplicateAndMerge
										- FunctionCall: rate(...)
											- MatrixSelector: {__name__="some_other_metric"}[1m0s]
			`,
		},
		"both matrix selectors eligible for skipping decoding despite nesting": {
			expr: `histogram_sum(rate(some_metric[1m]) * histogram_count(rate(some_other_metric[1m])))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_count(...)
									- DeduplicateAndMerge
										- FunctionCall: rate(...)
											- MatrixSelector: {__name__="some_other_metric"}[1m0s], skip histogram buckets
			`,
		},
		"inner matrix selector eligible for skipping decoding due to nesting": {
			expr: `histogram_quantile(0.5, rate(some_metric[1m]) * histogram_count(rate(some_other_metric[1m])))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_quantile(...)
						- param 0: NumberLiteral: 0.5
						- param 1: BinaryExpression: LHS * RHS
							- LHS: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_metric"}[1m0s]
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_count(...)
									- DeduplicateAndMerge
										- FunctionCall: rate(...)
											- MatrixSelector: {__name__="some_other_metric"}[1m0s], skip histogram buckets
			`,
		},
		"duplicate matrix selector not eligible for skipping decoding due to nesting": {
			expr: `histogram_sum(rate(some_metric[1m]) * histogram_quantile(0.5, rate(some_metric[1m])))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_sum(...)
						- BinaryExpression: LHS * RHS
							- LHS: ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s]
							- RHS: DeduplicateAndMerge
								- FunctionCall: histogram_quantile(...)
									- param 0: NumberLiteral: 0.5
									- param 1: ref#1 Duplicate ...
			`,
		},
		"different matrix selectors, one eligible for skipping decoding, one not": {
			expr: `histogram_sum(rate(some_metric[1m])) * histogram_quantile(0.5, rate(some_other_metric[1m]))`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_other_metric"}[1m0s]
			`,
		},
		"duplicate matrix selector, both eligible for skipping decoding": {
			expr: `histogram_sum(rate(some_metric[1m])) * histogram_count(rate(some_other_metric[1m]))`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_metric"}[1m0s], skip histogram buckets
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_count(...)
							- DeduplicateAndMerge
								- FunctionCall: rate(...)
									- MatrixSelector: {__name__="some_other_metric"}[1m0s], skip histogram buckets
			`,
		},
		"duplicate matrix selector, only first instance eligible for skipping decoding": {
			expr: `histogram_sum(rate(some_metric[1m])) * histogram_quantile(0.5, rate(some_metric[1m]))`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate ...
			`,
		},
		"duplicate matrix selector, only second instance eligible for skipping decoding": {
			expr: `histogram_quantile(0.5, rate(some_metric[1m])) * histogram_sum(rate(some_metric[1m]))`,
			expectedPlan: `
				- BinaryExpression: LHS * RHS
					- LHS: DeduplicateAndMerge
						- FunctionCall: histogram_quantile(...)
							- param 0: NumberLiteral: 0.5
							- param 1: ref#1 Duplicate
								- DeduplicateAndMerge
									- FunctionCall: rate(...)
										- MatrixSelector: {__name__="some_metric"}[1m0s]
					- RHS: DeduplicateAndMerge
						- FunctionCall: histogram_sum(...)
							- ref#1 Duplicate ...
			`,
		},
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	opts := streamingpromql.NewTestEngineOpts()
	planner := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts)
	planner.RegisterQueryPlanOptimizationPass(commonsubexpressionelimination.NewOptimizationPass(true, nil))
	planner.RegisterQueryPlanOptimizationPass(plan.NewSkipHistogramDecodingOptimizationPass())

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
}
