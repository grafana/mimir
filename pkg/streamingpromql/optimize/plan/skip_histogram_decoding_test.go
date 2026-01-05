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
		"subquery not eligible for skipping decoding to ensure counter reset detection": {
			expr: `histogram_count(increase(some_metric[5m:1m]))`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: histogram_count(...)
						- DeduplicateAndMerge
							- FunctionCall: increase(...)
								- Subquery: [5m0s:1m0s]
									- VectorSelector: {__name__="some_metric"}
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
	}

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	planner.RegisterQueryPlanOptimizationPass(plan.NewSkipHistogramDecodingOptimizationPass())

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, false, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
}
