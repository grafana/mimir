// SPDX-License-Identifier: AGPL-3.0-only

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

func TestProjectionPushdownOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		expr             string
		expectedPlan     string
		expectedModified int
		expectedSkip     map[string]int
	}{
		"raw vector selector": {
			expr:             `foo{job="a"}`,
			expectedPlan:     `- VectorSelector: {job="a", __name__="foo"}`,
			expectedModified: 0,
			expectedSkip:     map[string]int{plan.SkipReasonNoAggregations: 1},
		},

		"binary expression with aggregations": {
			expr: `avg by (zone) (foo) + avg by (zone) (bar)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: avg by (zone)
						- VectorSelector: {__name__="foo"}
					- RHS: AggregateExpression: avg by (zone)
						- VectorSelector: {__name__="bar"}
			`,
			expectedModified: 0,
			expectedSkip:     map[string]int{plan.SkipReasonBinaryOperation: 1},
		},

		"deduplicate node inserted": {
			expr: `sum by (zone) (rate(foo[5m]))`,
			expectedPlan: `
				- AggregateExpression: sum by (zone)
					- DeduplicateAndMerge
						- FunctionCall: rate(...)
							- MatrixSelector: {__name__="foo"}[5m0s]
			`,
			expectedModified: 0,
			expectedSkip:     map[string]int{plan.SkipReasonDeduplicate: 1},
		},

		"ambiguous label requirements": {
			expr: `avg(foo)`,
			expectedPlan: `
				- AggregateExpression: avg
					- VectorSelector: {__name__="foo"}
			`,
			expectedModified: 0,
			expectedSkip:     map[string]int{plan.SkipReasonAmbiguousLabels: 1},
		},

		"single aggregation by label": {
			expr: `avg by (job) (foo)`,
			expectedPlan: `
				- AggregateExpression: avg by (job)
					- VectorSelector: {__name__="foo"}, include ("__series_hash__", "job")
			`,
			expectedModified: 1,
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
			planner.RegisterQueryPlanOptimizationPass(plan.NewProjectionPushdownOptimizationPass(opts.CommonOpts.Reg, opts.Logger))

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, observer)
			require.NoError(t, err)
			actual := p.String()
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			reg := opts.CommonOpts.Reg.(*prometheus.Registry)
			require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_mimir_query_engine_projection_pushdown_modified_total Total number of queries where projections could be used.
				# TYPE cortex_mimir_query_engine_projection_pushdown_modified_total counter
				cortex_mimir_query_engine_projection_pushdown_modified_total %d
				`, testCase.expectedModified)), "cortex_mimir_query_engine_projection_pushdown_modified_total",
			))

			for k, v := range testCase.expectedSkip {
				require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_mimir_query_engine_projection_pushdown_skipped_total Total number of queries where projections could not be used.
					# TYPE cortex_mimir_query_engine_projection_pushdown_skipped_total counter
					cortex_mimir_query_engine_projection_pushdown_skipped_total{reason="%s"} %d
					`, k, v)), "cortex_mimir_query_engine_projection_pushdown_skipped_total",
				))
			}
		})
	}
}
