// SPDX-License-Identifier: AGPL-3.0-only

package plan_test

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestRemoveStaticallyEmptyExpressionsOptimizationPass(t *testing.T) {
	const lookbackDelta = 5 * time.Minute
	const constant = 1000
	selectorThresholdMs := (constant*time.Second + lookbackDelta).Milliseconds() - 1 // -1 because the lookback window is left-open.
	nonSelectorThresholdMs := (constant * time.Second).Milliseconds()

	testCases := map[string]struct {
		expr            string
		queryStart      time.Time
		useInstantQuery bool
		// expectedPlan is the expected plan, rendered as a tree. Exactly one of expectedPlan,
		// generateExpectedPlanFromExpr or expectUnchanged should be set.
		expectedPlan string
		// generateExpectedPlanFromExpr, when set, computes the expected plan by planning this expression
		// with the optimization pass disabled. Useful when the expected plan is the plan of another
		// expression (eg. a subexpression of expr) that is awkward to write out as a tree by hand.
		generateExpectedPlanFromExpr string
		expectUnchanged              bool
	}{
		"timestamp(v) < C: query time range is on threshold, should optimize": {
			expr:       "timestamp(metric) < CONSTANT",
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) < bool C: would optimise because query time range is on threshold, but skipped due to bool modifier": {
			expr:            "timestamp(metric) < bool CONSTANT",
			queryStart:      time.UnixMilli(selectorThresholdMs),
			expectUnchanged: true,
		},
		"timestamp(v) < C: query time range overlaps threshold, should not optimize": {
			expr:            "timestamp(metric) < CONSTANT",
			queryStart:      time.UnixMilli(selectorThresholdMs - 1),
			expectUnchanged: true,
		},
		"timestamp(v offset -1ms) < C: query time range does not overlaps threshold, should optimize": {
			expr:       "timestamp(metric offset -1ms) < CONSTANT",
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v offset 1ms) < C: query time range overlaps threshold, should not optimize": {
			expr:            "timestamp(metric offset 1ms) < CONSTANT",
			queryStart:      time.UnixMilli(selectorThresholdMs),
			expectUnchanged: true,
		},
		"timestamp(abs(v)) < C: query time range is on threshold, should optimize": {
			expr:       "timestamp(abs(v)) < CONSTANT",
			queryStart: time.UnixMilli(nonSelectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(abs(v)) < C: query time range is above threshold, should optimize": {
			expr:       "timestamp(abs(v)) < CONSTANT",
			queryStart: time.UnixMilli(nonSelectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(abs(v offset 1ms)) < C: query time range is above threshold, should optimize (offset and lookback delta ignored)": {
			expr:       "timestamp(abs(v offset 1ms)) < CONSTANT",
			queryStart: time.UnixMilli(nonSelectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},

		"timestamp(v) <= C: query range starts just above threshold, should optimize": {
			expr:       "timestamp(metric) <= CONSTANT",
			queryStart: time.UnixMilli(selectorThresholdMs + 2),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) <= C: query range starts exactly at threshold, should optimize": {
			expr:       "timestamp(metric) <= CONSTANT",
			queryStart: time.UnixMilli(selectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) <= C: query range starts below threshold, should not optimize": {
			expr:            "timestamp(metric) <= CONSTANT",
			queryStart:      time.UnixMilli(selectorThresholdMs),
			expectUnchanged: true,
		},
		"C > timestamp(v): query range starts at threshold, should optimize": {
			expr:       "CONSTANT > timestamp(metric)",
			queryStart: time.UnixMilli(selectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"C > timestamp(v): query range starts before threshold, should optimize": {
			expr:       "CONSTANT > timestamp(metric)",
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"C >= timestamp(v): query range starts just above threshold, should optimize": {
			expr:       "CONSTANT >= timestamp(metric)",
			queryStart: time.UnixMilli(selectorThresholdMs + 2),
			expectedPlan: `
				- NoOp
			`,
		},
		"C >= timestamp(v): query range starts exactly at threshold, should optimize": {
			expr:       "CONSTANT >= timestamp(metric)",
			queryStart: time.UnixMilli(selectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"C >= timestamp(v): query range starts just below threshold, should not optimize": {
			expr:            "CONSTANT >= timestamp(metric)",
			queryStart:      time.UnixMilli(selectorThresholdMs),
			expectUnchanged: true,
		},

		"non-timestamp comparison: should not optimize": {
			expr:            "metric2 < CONSTANT",
			queryStart:      time.UnixMilli(selectorThresholdMs + 1),
			expectUnchanged: true,
		},

		"timestamp(v) < C with and: query range starts after threshold, should optimize": {
			expr:       "metric and timestamp(metric) < CONSTANT",
			queryStart: time.UnixMilli(selectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) < C with and: query range starts at threshold, should optimize": {
			expr:       "metric and timestamp(metric) < CONSTANT",
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) < C with and: query range starts before threshold, should not optimize": {
			expr:            "metric and timestamp(metric) < CONSTANT",
			queryStart:      time.UnixMilli(selectorThresholdMs - 1),
			expectUnchanged: true,
		},

		"timestamp condition on LHS of and: should optimize": {
			expr:       "timestamp(metric) < CONSTANT and metric",
			queryStart: time.UnixMilli(selectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},

		"nested and expressions: should optimize where inner expression is no-op": {
			// The inner "and" has the timestamp filter → replaced with NoOp.
			// The outer "and" then has NoOp as its LHS → also replaced with NoOp.
			expr:       "(metric and timestamp(metric) < CONSTANT) and metric2",
			queryStart: time.UnixMilli(selectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"nested and expressions: inner not optimized because not a no-op": {
			expr:            "(metric and timestamp(metric) < CONSTANT) and metric2",
			queryStart:      time.UnixMilli(selectorThresholdMs - 1),
			expectUnchanged: true,
		},
		"nested and expressions: should optimize as far as possible": {
			// The inner "and" has the timestamp filter → replaced with NoOp.
			// The outer "or" then has NoOp as its LHS → simplified to just the RHS.
			expr:       "(metric and timestamp(metric) < CONSTANT) or metric2",
			queryStart: time.UnixMilli(selectorThresholdMs + 1),
			expectedPlan: `
				- VectorSelector: {__name__="metric2"}
			`,
		},

		"complex expression inside timestamp() that is a no-op: should optimize": {
			expr:       `foo and timestamp(sum(metric)) < CONSTANT`,
			queryStart: time.UnixMilli(nonSelectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"complex expression inside timestamp() that is not a no-op: should not optimize": {
			expr:            `foo and timestamp(sum(metric)) < CONSTANT`,
			queryStart:      time.UnixMilli(nonSelectorThresholdMs - 1),
			expectUnchanged: true,
		},

		"timestamp condition outside subquery: should optimize": {
			// The "and timestamp(metric) < CONSTANT" is outside the subquery, so it is evaluated
			// at the outer query time range, which is after the threshold.
			expr:       "avg_over_time(metric[5m:1m]) and timestamp(metric) < CONSTANT",
			queryStart: time.UnixMilli(selectorThresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp condition inside subquery: should optimize": {
			// The "and timestamp(metric) < CONSTANT" is inside the subquery, so it is evaluated over the
			// subquery's shifted time range. queryStart is far enough after the threshold that even the
			// subquery's extended (earlier) time range is entirely after it, so the whole thing is empty.
			expr:       "avg_over_time((timestamp(metric) < CONSTANT)[2h:1m])",
			queryStart: time.Unix(10000, 0),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp condition inside subquery, but not empty over the subquery's time range: should not optimize": {
			// timestamp(metric) < CONSTANT would be empty over the outer query time range, but the subquery's
			// time range extends further back (before the threshold), so it can still return results and must
			// not be optimized away.
			expr:            "avg_over_time((timestamp(metric) < CONSTANT)[2h:1m])",
			queryStart:      time.Unix(5000, 0),
			expectUnchanged: true,
		},
		"function over subquery with empty results: should optimize": {
			expr: `max_over_time(EMPTY_RESULT[1d:5m])`,
			expectedPlan: `
				- NoOp
			`,
		},
		"subquery with empty results: should optimize": {
			expr: `(EMPTY_RESULT[1d:5m])`,
			expectedPlan: `
				- NoOp: matrix
			`,
			useInstantQuery: true,
		},
		"non-empty subquery: should not optimize": {
			expr:            `avg_over_time(metric[5m:1m])`,
			expectUnchanged: true,
		},
		"function over subquery binary operation with empty results: should optimize": {
			expr: `max_over_time(metric_a[1d:5m]) + rate(EMPTY_RESULT[5m])`,
			expectedPlan: `
				- NoOp
			`,
		},
		"unary operation on empty results: should optimize": {
			expr: `-rate(EMPTY_RESULT[5m])`,
			expectedPlan: `
				- NoOp
			`,
		},
		"non-conflicting equals matchers: should not optimize": {
			expr:            `metric{pod="foo", env="bar"}`,
			queryStart:      time.UnixMilli(selectorThresholdMs),
			expectUnchanged: true,
		},
		"non-equals matchers: should not optimize": {
			expr:            `metric{pod=~"foo.+", pod!~".+bar"}`,
			queryStart:      time.UnixMilli(selectorThresholdMs),
			expectUnchanged: true,
		},
		"conflicting equals matchers in first info() argument: should optimize": {
			expr:       `info(metric{pod="foo", pod="bar"}, {__name__="other_info"})`,
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"conflicting equals matchers in second info() argument: should not optimize": {
			expr:            `info(metric, {env="prod", env="dev"})`,
			queryStart:      time.UnixMilli(selectorThresholdMs),
			expectUnchanged: true,
		},
		"conflicting equals matchers vector: should optimize": {
			expr:       `metric{pod="foo", pod="bar"}`,
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"conflicting equals matchers matrix: should optimize": {
			expr:       `avg_over_time(metric{pod="foo", pod="bar"}[5m])`,
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"conflicting equals matchers on LHS of binary expression: should optimize": {
			expr:       `metric{pod="foo", pod="bar"} and foo`,
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"conflicting equals matchers on RHS of binary expression: should optimize": {
			expr:       `foo and metric{pod="foo", pod="bar"}`,
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"aggregation over empty result: should optimize": {
			expr:       `sum(EMPTY_RESULT)`,
			queryStart: time.UnixMilli(selectorThresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"aggregation over function over empty result: should optimize": {
			expr: `sum(rate(EMPTY_RESULT[5m]))`,
			expectedPlan: `
				- NoOp
			`,
		},
		"aggregation with grouping over empty result: should optimize": {
			expr: `count by (namespace) (EMPTY_RESULT)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"aggregation without grouping over empty result: should optimize": {
			expr: `sum without (namespace) (EMPTY_RESULT)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary add with empty result on right side: should optimize": {
			expr: `sum(metric_a) + sum(EMPTY_RESULT)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary add with empty result on left side: should optimize": {
			expr: `sum(EMPTY_RESULT) + sum(metric_a)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary modulo with empty result on right side: should optimize": {
			expr: `sum(metric_a) % sum(EMPTY_RESULT)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary exponent with empty result on left side: should optimize": {
			expr: `sum(EMPTY_RESULT) ^ sum(metric_a)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary atan2 with empty result on right side: should optimize": {
			expr: `some_metric atan2 EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary less-than with empty result on right side: should optimize": {
			expr: `some_metric_a < EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary less-than-or-equal with empty result on right side: should optimize": {
			expr: `some_metric_a <= EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary greater-than with empty result on right side: should optimize": {
			expr: `some_metric_a > EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary greater-than-or-equal with empty result on right side: should optimize": {
			expr: `some_metric_a >= EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},
		"binary add with results on both sides: should not optimize": {
			expr:            `sum(metric_a) + sum(metric_b)`,
			expectUnchanged: true,
		},
		"binary greater-than with results on both sides: should not optimize": {
			expr:            `some_metric_a > some_metric_b`,
			expectUnchanged: true,
		},
		"absent over empty result: should only optimize selector": {
			expr: `absent(EMPTY_RESULT)`,
			expectedPlan: `
				- FunctionCall: absent(...)
					- NoOp
			`,
		},
		"absent_over_time over empty result: should only optimize selector": {
			expr: `absent_over_time(EMPTY_RESULT[5m])`,
			expectedPlan: `
				- FunctionCall: absent_over_time(...)
					- NoOp: matrix
			`,
		},
		"abs over empty result: should optimize": {
			expr: `abs(EMPTY_RESULT)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"histogram_quantile over empty result: should optimize": {
			expr: `histogram_quantile(0.99, sum(rate(EMPTY_RESULT[5m])))`,
			expectedPlan: `
				- NoOp
			`,
		},
		"histogram_fraction over empty result: should optimize": {
			expr: `histogram_fraction(0, 0.2, rate(EMPTY_RESULT[5m]))`,
			expectedPlan: `
				- NoOp
			`,
		},
		"non-empty result ANDed with empty result: returns empty result": {
			expr: `metric and EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},
		"non-empty result ANDed with non-empty result: stays as-is": {
			expr:            `metric and other_metric`,
			expectUnchanged: true,
		},
		"empty result ANDed with empty result: returns empty result": {
			expr: `EMPTY_RESULT and EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},

		"empty result ORed with non-empty result: returns non-empty side": {
			expr: `EMPTY_RESULT or metric`,
			expectedPlan: `
				- VectorSelector: {__name__="metric"}
			`,
		},
		"non-empty result ORed with empty result: returns non-empty side": {
			expr: `metric or EMPTY_RESULT`,
			expectedPlan: `
				- VectorSelector: {__name__="metric"}
			`,
		},
		"non-empty result ORed with non-empty result: stays as-is": {
			expr:            `metric or other_metric`,
			expectUnchanged: true,
		},
		"empty result ORed with empty result: returns empty result": {
			expr: `EMPTY_RESULT or EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},

		"empty result UNLESSed with non-empty result: returns empty result": {
			expr: `EMPTY_RESULT unless metric`,
			expectedPlan: `
				- NoOp
			`,
		},
		"non-empty result UNLESSed with empty result: returns non-empty side": {
			expr: `metric unless EMPTY_RESULT`,
			expectedPlan: `
				- VectorSelector: {__name__="metric"}
			`,
		},
		"non-empty result UNLESSed with non-empty result: stays as-is": {
			expr:            `metric unless other_metric`,
			expectUnchanged: true,
		},
		"empty result UNLESSed with empty result: returns empty result": {
			expr: `EMPTY_RESULT unless EMPTY_RESULT`,
			expectedPlan: `
				- NoOp
			`,
		},

		// Constant comparison toggles: vector(N) == M. This is the dashboard "histogram toggle" idiom.

		// Toggle off (N != M): the comparison is statically empty, so the whole 'and' is empty.
		"toggle off: A and on() (vector(0) == 1) is statically empty": {
			expr: `(avg(rate(foo[2m]))) and on() (vector(0) == 1)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"toggle off: A and on() (vector(3) == 4.5) is statically empty": {
			expr: `(avg(rate(foo[2m]))) and on() (vector(3) == 4.5)`,
			expectedPlan: `
				- NoOp
			`,
		},
		// Toggle on (N == M): the comparison always matches, so 'and on()' can be dropped, leaving A.
		"toggle on: A and on() (vector(1) == 1) simplifies to A": {
			expr:                         `(avg(rate(foo[2m]))) and on() (vector(1) == 1)`,
			generateExpectedPlanFromExpr: `avg(rate(foo[2m]))`,
		},
		"toggle on: A and on() (vector(5.5) == 5.5) simplifies to A": {
			expr:                         `(avg(rate(foo[2m]))) and on() (vector(5.5) == 5.5)`,
			generateExpectedPlanFromExpr: `avg(rate(foo[2m]))`,
		},
		// The toggle-on simplification is only valid for on() with no matching labels: with on(<labels>)
		// or ignoring() the result depends on A's labels, so the expression is left unchanged.
		"toggle on with on(id): left unchanged": {
			expr:            `(avg(rate(foo[2m]))) and on(id) (vector(1) == 1)`,
			expectUnchanged: true,
		},
		"toggle on with ignoring(): left unchanged": {
			expr:            `(avg(rate(foo[2m]))) and ignoring() (vector(1) == 1)`,
			expectUnchanged: true,
		},
		// The toggle-on simplification is only valid if the toggle is on the right-hand side.
		"toggle on: (vector(1) == 1) and on() A is not simplified": {
			expr:            `(vector(1) == 1) and on() foo`,
			expectUnchanged: true,
		},
		// A and B with a statically-empty side is empty regardless of the matching modifier or which side is empty.
		"empty on left-hand side of 'and': statically empty": {
			expr: `(vector(0) == 1) and on() (avg(rate(foo[2m])))`,
			expectedPlan: `
				- NoOp
			`,
		},
		"toggle on, with literal on LHS": {
			expr:                         `(avg(rate(foo[2m]))) and on() (1 == vector(1))`,
			generateExpectedPlanFromExpr: `avg(rate(foo[2m]))`,
		},
		"toggle off, with literal on LHS": {
			expr: `(avg(rate(foo[2m]))) and on() (vector(-1) == 1)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"empty toggle with on(id): statically empty": {
			expr: `(avg(rate(foo[2m]))) and on(id) (vector(0) == 1)`,
			expectedPlan: `
				- NoOp
			`,
		},
		"empty toggle with ignoring(): statically empty": {
			expr: `(avg(rate(foo[2m]))) and ignoring() (vector(0) == 1)`,
			expectedPlan: `
				- NoOp
			`,
		},
	}

	ctx := context.Background()
	observer := streamingpromql.NoopPlanningObserver{}

	generatePlan := func(t *testing.T, enableOptimizationPass bool, expr string, timeRange types.QueryTimeRange, reg prometheus.Registerer) string {
		opts := streamingpromql.NewTestEngineOpts()
		planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)

		if enableOptimizationPass {
			planner.RegisterQueryPlanOptimizationPass(plan.NewRemoveStaticallyEmptyExpressionsOptimizationPass(reg, opts.Logger))
		}

		p, err := planner.NewQueryPlan(ctx, expr, timeRange, lookbackDelta, false, observer)
		require.NoError(t, err)

		return p.String()
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			testCase.expr = strings.ReplaceAll(testCase.expr, "CONSTANT", strconv.Itoa(constant))
			testCase.expr = strings.ReplaceAll(testCase.expr, "EMPTY_RESULT", `some_metric{foo="bar", foo="not-bar"}`)

			var timeRange types.QueryTimeRange
			if testCase.useInstantQuery {
				timeRange = types.NewInstantQueryTimeRange(testCase.queryStart)
			} else {
				timeRange = types.NewRangeQueryTimeRange(testCase.queryStart, testCase.queryStart.Add(24*time.Hour), time.Minute)
			}

			expectedModified := 1

			if testCase.expectUnchanged {
				testCase.expectedPlan = generatePlan(t, false, testCase.expr, timeRange, nil)
				expectedModified = 0
			}

			if testCase.generateExpectedPlanFromExpr != "" {
				expectedExpr := strings.ReplaceAll(testCase.generateExpectedPlanFromExpr, "CONSTANT", strconv.Itoa(constant))
				expectedExpr = strings.ReplaceAll(expectedExpr, "EMPTY_RESULT", `some_metric{foo="bar", foo="not-bar"}`)
				testCase.expectedPlan = generatePlan(t, false, expectedExpr, timeRange, nil)
			}

			reg := prometheus.NewPedanticRegistry()
			actual := generatePlan(t, true, testCase.expr, timeRange, reg)
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)

			require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP cortex_mimir_query_engine_remove_statically_empty_expressions_attempted_total Total number of queries that the optimization pass has attempted to skip statically empty expressions for.
				# TYPE cortex_mimir_query_engine_remove_statically_empty_expressions_attempted_total counter
				cortex_mimir_query_engine_remove_statically_empty_expressions_attempted_total 1
				# HELP cortex_mimir_query_engine_remove_statically_empty_expressions_modified_total Total number of queries where the optimization pass has replaced one or more statically empty expressions with a no-op.
				# TYPE cortex_mimir_query_engine_remove_statically_empty_expressions_modified_total counter
				cortex_mimir_query_engine_remove_statically_empty_expressions_modified_total %d
				`, expectedModified)), "cortex_mimir_query_engine_remove_statically_empty_expressions_attempted_total", "cortex_mimir_query_engine_remove_statically_empty_expressions_modified_total",
			))
		})
	}
}

func TestRemoveStaticallyEmptyExpressions_AdaptiveMetrics(t *testing.T) {
	// This test confirms that expressions rewritten by Adaptive Metrics are correctly optimised away.
	// Expressions like timestamp(foo) < C are rewritten to wrapper(timestamp(foo)) < C.

	// These functions aren't registered in open source Mimir, so register dummy instances now.
	require.NotContains(t, functions.RegisteredFunctions, functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_1, "unexpected test environment: expected Adaptive Metrics functions not to be registered")
	require.NotContains(t, functions.RegisteredFunctions, functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_2, "unexpected test environment: expected Adaptive Metrics functions not to be registered")
	require.NoError(t, functions.RegisterFunction(functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_1, "adaptive_metrics_function_1", parser.ValueTypeVector, nil))
	require.NoError(t, functions.RegisterFunction(functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_2, "adaptive_metrics_function_2", parser.ValueTypeVector, nil))

	t.Cleanup(func() {
		delete(functions.RegisteredFunctions, functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_1)
		delete(functions.RegisteredFunctions, functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_2)
	})

	reg := prometheus.NewPedanticRegistry()
	opts := streamingpromql.NewTestEngineOpts()
	optimizationPass := plan.NewRemoveStaticallyEmptyExpressionsOptimizationPass(reg, opts.Logger)

	for _, function := range []functions.Function{functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_1, functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_2} {
		selector := &core.VectorSelector{
			VectorSelectorDetails: &core.VectorSelectorDetails{
				Matchers: []*core.LabelMatcher{
					{
						Type:  labels.MatchEqual,
						Name:  model.MetricNameLabel,
						Value: "metric",
					},
				},
			},
		}

		timestampFunctionCall := &core.FunctionCall{
			FunctionCallDetails: &core.FunctionCallDetails{
				Function: functions.FUNCTION_TIMESTAMP,
			},
			Args: []planning.Node{selector},
		}

		adaptiveMetricsWrapper := &core.FunctionCall{
			FunctionCallDetails: &core.FunctionCallDetails{
				Function: function,
			},
			Args: []planning.Node{
				timestampFunctionCall,
			},
		}

		scalar := &core.NumberLiteral{
			NumberLiteralDetails: &core.NumberLiteralDetails{
				Value: 1000,
			},
		}

		queryPlan := &planning.QueryPlan{
			Root: &core.BinaryExpression{
				LHS: adaptiveMetricsWrapper,
				RHS: scalar,
				BinaryExpressionDetails: &core.BinaryExpressionDetails{
					Op: core.BINARY_LTE,
				},
			},
			Parameters: &planning.QueryParameters{
				TimeRange: types.NewInstantQueryTimeRange(time.Unix(2000, 0)),
			},
		}

		optimizedPlan, err := optimizationPass.Apply(context.Background(), queryPlan, planning.MaximumSupportedQueryPlanVersion)
		require.NoError(t, err)

		require.Equal(t, "- NoOp", optimizedPlan.String())
	}
}

func TestRemoveStaticallyEmptyExpressions_IsAlwaysEmptyFunctionCall_UnknownFunctionsHandled(t *testing.T) {
	params := &planning.QueryParameters{
		TimeRange:     types.NewInstantQueryTimeRange(time.Now()),
		LookbackDelta: 5 * time.Minute,
	}

	maxFuncOrd := getMaxFunctionOrdinal()
	funcCall := &core.FunctionCall{
		FunctionCallDetails: &core.FunctionCallDetails{
			Function: functions.Function(maxFuncOrd + 1),
		},
	}

	_, err := plan.IsAlwaysEmptyFunctionCall(funcCall, params)
	require.ErrorIs(t, err, plan.ErrUnknownFunction)
}

func TestRemoveStaticallyEmptyExpressions_IsAlwaysEmptyFunctionCall_AllKnownFunctionsHandled(t *testing.T) {
	params := &planning.QueryParameters{
		TimeRange:     types.NewInstantQueryTimeRange(time.Now()),
		LookbackDelta: 5 * time.Minute,
	}

	// For every known function, ensure that we either get no error when determining if
	// it is always empty or an error because we haven't passed the expected arguments
	// (because we don't pass any). The idea is to make sure we have explicitly handled
	// every defined function.
	for ord := range functions.Function_name {
		currentFunc := functions.Function(ord)

		// Not a real function so it isn't handled by the optimization pass.
		if currentFunc == functions.FUNCTION_UNKNOWN {
			continue
		}

		funcCall := &core.FunctionCall{
			FunctionCallDetails: &core.FunctionCallDetails{
				Function: currentFunc,
			},
		}

		_, err := plan.IsAlwaysEmptyFunctionCall(funcCall, params)
		if err != nil {
			require.ErrorIs(t, err, plan.ErrInvalidFunctionArgs, "expected no error or invalid arguments error for %s, got %s", currentFunc, err)
		}
	}
}

func getMaxFunctionOrdinal() int32 {
	maxFuncOrd := int32(0)
	for _, i := range functions.Function_value {
		if i > maxFuncOrd {
			maxFuncOrd = i
		}
	}

	return maxFuncOrd
}

func TestRemoveStaticallyEmptyExpressions_Toggles_EndToEnd(t *testing.T) {
	data := `
		load 1m
			foo{series="1"} 0+1x10
			foo{series="2"} 0+2x10
	`

	queries := []string{
		`(avg(rate(foo[2m]))) and on() (vector(0) == 1)`,   // toggle off: statically empty
		`(avg(rate(foo[2m]))) and on() (vector(1) == 1)`,   // toggle on: equal to avg(rate(foo[2m]))
		`(avg(rate(foo[2m]))) and on(id) (vector(1) == 1)`, // left unchanged by the pass
	}

	ctx := context.Background()
	end := timestamp.Time(0).Add(10 * time.Minute)

	storage := promqltest.LoadedStorage(t, data)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	runQuery := func(t *testing.T, expr string, withOptimization bool) string {
		opts := streamingpromql.NewTestEngineOpts()
		planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)

		if withOptimization {
			planner.RegisterQueryPlanOptimizationPass(plan.NewRemoveStaticallyEmptyExpressionsOptimizationPass(opts.CommonOpts.Reg, opts.Logger))
		}

		engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(opts.CommonOpts.Reg), planner)
		require.NoError(t, err)

		q, err := engine.NewInstantQuery(ctx, storage, nil, expr, end)
		require.NoError(t, err)
		t.Cleanup(q.Close)

		res := q.Exec(ctx)
		require.NoError(t, res.Err)
		return res.String()
	}

	for _, expr := range queries {
		t.Run(expr, func(t *testing.T) {
			withOptimization := runQuery(t, expr, true)
			withoutOptimization := runQuery(t, expr, false)
			require.Equal(t, withoutOptimization, withOptimization)
		})
	}
}
