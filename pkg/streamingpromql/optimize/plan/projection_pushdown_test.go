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
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestProjectionPushdownOptimizationPass(t *testing.T) {
	// Enable experimental functions so that we can verify they are inspected correctly.
	parser.EnableExperimentalFunctions = true

	testCases := map[string]struct {
		expr             string
		expectedPlan     string
		expectedModified int
		expectedSkip     map[plan.SkipReason]int
	}{
		"raw vector selector": {
			expr:             `foo{job="a"}`,
			expectedPlan:     `- VectorSelector: {job="a", __name__="foo"}`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonNoAggregations: 1},
		},
		"binary expression with aggregations": {
			expr: `avg by (zone) (foo) + avg by (zone) (bar)`,
			expectedPlan: `
				- BinaryExpression: LHS + RHS
					- LHS: AggregateExpression: avg by (zone)
						- VectorSelector: {__name__="foo"}, include ("zone")
					- RHS: AggregateExpression: avg by (zone)
						- VectorSelector: {__name__="bar"}, include ("zone")
			`,
			expectedModified: 2,
		},
		"binary expression with literal and aggregation": {
			expr: `vector(1) - avg by (zone) (bar)`,
			expectedPlan: `
				- BinaryExpression: LHS - RHS
					- LHS: StepInvariantExpression
						- FunctionCall: vector(...)
							- NumberLiteral: 1
					- RHS: AggregateExpression: avg by (zone)
						- VectorSelector: {__name__="bar"}, include ("zone")
			`,
			expectedModified: 1,
		},
		"binary expression on with no aggregations": {
			expr: `foo + on (zone) bar`,
			expectedPlan: `
				- BinaryExpression: LHS + on (zone) RHS
					- LHS: VectorSelector: {__name__="foo"}
					- RHS: VectorSelector: {__name__="bar"}
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonBinaryOperation: 2},
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
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonDeduplicate: 1},
		},
		// Note that the projections pass has logic to inspect label_join or label_replace functions
		// and extract the required labels but, we abort trying to use projections as soon as we see a
		// `DeduplicateAndMerge` node.
		"aggregation with label_join function": {
			expr: `avg by (job) (label_join(foo, "dst", "-", "src1", "src2"))`,
			expectedPlan: `
				- AggregateExpression: avg by (job)
					- DeduplicateAndMerge
						- FunctionCall: label_join(...)
							- param 0: VectorSelector: {__name__="foo"}
							- param 1: StringLiteral: "dst"
							- param 2: StringLiteral: "-"
							- param 3: StringLiteral: "src1"
							- param 4: StringLiteral: "src2"
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonDeduplicate: 1},
		},
		"aggregation with label_replace function": {
			expr: `avg by (job) (label_replace(foo, "dst", "$1", "src", ".+/(.+)"))`,
			expectedPlan: `
				- AggregateExpression: avg by (job)
					- DeduplicateAndMerge
						- FunctionCall: label_replace(...)
							- param 0: VectorSelector: {__name__="foo"}
							- param 1: StringLiteral: "dst"
							- param 2: StringLiteral: "$1"
							- param 3: StringLiteral: "src"
							- param 4: StringLiteral: ".+/(.+)"
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonDeduplicate: 1},
		},
		// The label_join and label_replace functions are operating on an aggregation that can make
		// use of projections so we discover the aggregation label and stop traversing the path to
		// the root before we look at the DeduplicateAndMerge node.
		"label_join function with aggregation": {
			expr: `label_join(avg by (job) (foo), "dst", "-", "src1", "src2")`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: label_join(...)
						- param 0: AggregateExpression: avg by (job)
							- VectorSelector: {__name__="foo"}, include ("job")
						- param 1: StringLiteral: "dst"
						- param 2: StringLiteral: "-"
						- param 3: StringLiteral: "src1"
						- param 4: StringLiteral: "src2"
			`,
			expectedModified: 1,
		},
		"label_replace function with aggregation": {
			expr: `label_replace(avg by (job) (foo), "dst", "$1", "src", ".+/(.+)")`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: label_replace(...)
						- param 0: AggregateExpression: avg by (job)
							- VectorSelector: {__name__="foo"}, include ("job")
						- param 1: StringLiteral: "dst"
						- param 2: StringLiteral: "$1"
						- param 3: StringLiteral: "src"
						- param 4: StringLiteral: ".+/(.+)"
			`,
			expectedModified: 1,
		},
		"aggregation without label": {
			expr: `avg without (pod) (foo)`,
			expectedPlan: `
				- AggregateExpression: avg without (pod)
					- VectorSelector: {__name__="foo"}
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonNotSupported: 1},
		},
		"aggregations with and without labels": {
			expr: `avg by (job) (sum without (instance) (foo))`,
			expectedPlan: `
				- AggregateExpression: avg by (job)
					- AggregateExpression: sum without (instance)
						- VectorSelector: {__name__="foo"}
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonNotSupported: 1},
		},
		"aggregations without and with labels": {
			expr: `sum without (instance) (avg by (job) (foo))`,
			expectedPlan: `
				- AggregateExpression: sum without (instance)
					- AggregateExpression: avg by (job)
						- VectorSelector: {__name__="foo"}, include ("job")
			`,
			expectedModified: 1,
		},
		"aggregation with count_values": {
			expr: `count_values("pod", foo)`,
			expectedPlan: `
				- AggregateExpression: count_values
					- expression: VectorSelector: {__name__="foo"}, include ()
					- parameter: StringLiteral: "pod"
			`,
			expectedModified: 1,
		},
		"aggregation with count_values by": {
			expr: `count_values by (job) ("pod", foo)`,
			expectedPlan: `
				- AggregateExpression: count_values by (job)
					- expression: VectorSelector: {__name__="foo"}, include ("job")
					- parameter: StringLiteral: "pod"
			`,
			expectedModified: 1,
		},

		"sort_by_label with aggregation": {
			expr: `sort_by_label(avg by (job) (bar), "zone", "environment")`,
			expectedPlan: `
				- FunctionCall: sort_by_label(...)
					- param 0: AggregateExpression: avg by (job)
						- VectorSelector: {__name__="bar"}, include ("job")
					- param 1: StringLiteral: "zone"
					- param 2: StringLiteral: "environment"
			`,
			expectedModified: 1,
		},
		"sort_by_label_desc with aggregation": {
			expr: `sort_by_label_desc(avg by (job) (bar), "cluster", "region")`,
			expectedPlan: `
				- FunctionCall: sort_by_label_desc(...)
					- param 0: AggregateExpression: avg by (job)
						- VectorSelector: {__name__="bar"}, include ("job")
					- param 1: StringLiteral: "cluster"
					- param 2: StringLiteral: "region"
			`,
			expectedModified: 1,
		},
		"aggregation with sort_by_label": {
			expr: `avg by (job) (sort_by_label(bar, "zone", "environment"))`,
			expectedPlan: `
				- AggregateExpression: avg by (job)
					- FunctionCall: sort_by_label(...)
						- param 0: VectorSelector: {__name__="bar"}, include ("environment", "job", "zone")
						- param 1: StringLiteral: "zone"
						- param 2: StringLiteral: "environment"
			`,
			expectedModified: 1,
		},
		"aggregation with sort_by_label_desc": {
			expr: `avg by (job) (sort_by_label_desc(bar, "cluster", "region"))`,
			expectedPlan: `
				- AggregateExpression: avg by (job)
					- FunctionCall: sort_by_label_desc(...)
						- param 0: VectorSelector: {__name__="bar"}, include ("cluster", "job", "region")
						- param 1: StringLiteral: "cluster"
						- param 2: StringLiteral: "region"
			`,
			expectedModified: 1,
		},
		"aggregation function over subquery": {
			expr: `avg_over_time(foo[5m:30s])`,
			expectedPlan: `
				- DeduplicateAndMerge
					- FunctionCall: avg_over_time(...)
						- Subquery: [5m0s:30s]
							- VectorSelector: {__name__="foo"}
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonDeduplicate: 1},
		},
		"aggregation and aggregation function over subquery": {
			expr: `sum(avg_over_time(foo[5m:30s]))`,
			expectedPlan: `
				- AggregateExpression: sum
					- DeduplicateAndMerge
						- FunctionCall: avg_over_time(...)
							- Subquery: [5m0s:30s]
								- VectorSelector: {__name__="foo"}
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonDeduplicate: 1},
		},
		"unary expression": {
			expr: `-foo`,
			expectedPlan: `
				- DeduplicateAndMerge
					- UnaryExpression: -
						- VectorSelector: {__name__="foo"}
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonDeduplicate: 1},
		},
		"unary expression outside aggregation": {
			expr: `-avg(foo)`,
			expectedPlan: `
				- DeduplicateAndMerge
					- UnaryExpression: -
						- AggregateExpression: avg
							- VectorSelector: {__name__="foo"}, include ()
			`,
			expectedModified: 1,
		},
		"unary expression inside aggregation": {
			expr: `avg(-foo)`,
			expectedPlan: `
				- AggregateExpression: avg
					- DeduplicateAndMerge
						- UnaryExpression: -
							- VectorSelector: {__name__="foo"}
			`,
			expectedModified: 0,
			expectedSkip:     map[plan.SkipReason]int{plan.SkipReasonDeduplicate: 1},
		},
		"aggregation with step invariant expression": {
			expr: `avg(foo @ 0)`,
			expectedPlan: `
				- StepInvariantExpression
					- AggregateExpression: avg
						- VectorSelector: {__name__="foo"} @ 0 (1970-01-01T00:00:00Z), include ()
			`,
			expectedModified: 1,
		},
		"aggregation by no labels": {
			expr: `avg(foo)`,
			expectedPlan: `
				- AggregateExpression: avg
					- VectorSelector: {__name__="foo"}, include ()
			`,
			expectedModified: 1,
		},
		"single aggregation by label": {
			expr: `avg by (job) (foo)`,
			expectedPlan: `
				- AggregateExpression: avg by (job)
					- VectorSelector: {__name__="foo"}, include ("job")
			`,
			expectedModified: 1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()

			// NOTE: We are using a range query time range because `StepInvariantExpression`s are
			// unwrapped when they are part of an instant query (since there's no point).
			now := time.Now()
			timeRange := types.NewRangeQueryTimeRange(now.Add(-time.Hour), now, time.Minute)
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
				# HELP cortex_mimir_query_engine_projection_pushdown_modified_total Total number of selectors where projections could be used.
				# TYPE cortex_mimir_query_engine_projection_pushdown_modified_total counter
				cortex_mimir_query_engine_projection_pushdown_modified_total %d
				`, testCase.expectedModified)), "cortex_mimir_query_engine_projection_pushdown_modified_total",
			))

			// Assert that selectors were skipped or not skipped for the reason we expect.
			require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(fmt.Sprintf(`
					# HELP cortex_mimir_query_engine_projection_pushdown_skipped_total Total number of selectors where projections could not be used.
					# TYPE cortex_mimir_query_engine_projection_pushdown_skipped_total counter
					cortex_mimir_query_engine_projection_pushdown_skipped_total{reason="%s"} %d
					cortex_mimir_query_engine_projection_pushdown_skipped_total{reason="%s"} %d
					cortex_mimir_query_engine_projection_pushdown_skipped_total{reason="%s"} %d
					cortex_mimir_query_engine_projection_pushdown_skipped_total{reason="%s"} %d
					`,
				plan.SkipReasonBinaryOperation, testCase.expectedSkip[plan.SkipReasonBinaryOperation],
				plan.SkipReasonDeduplicate, testCase.expectedSkip[plan.SkipReasonDeduplicate],
				plan.SkipReasonNoAggregations, testCase.expectedSkip[plan.SkipReasonNoAggregations],
				plan.SkipReasonNotSupported, testCase.expectedSkip[plan.SkipReasonNotSupported])), "cortex_mimir_query_engine_projection_pushdown_skipped_total",
			))
		})
	}
}
