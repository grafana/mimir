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
	"github.com/stretchr/testify/require"

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
		queryStart      time.Time // instant query time
		expectedPlan    string
		expectUnchanged bool
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
		"timestamp condition inside subquery: should not optimize": {
			// The "and timestamp(metric) < CONSTANT" is inside the subquery, so it is ignored.
			expr:            "avg_over_time((metric and timestamp(metric) < CONSTANT)[2h:1m])",
			queryStart:      time.Unix(10000, 0),
			expectUnchanged: true,
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
		"conflicting equals matchers in info function: should not optimize": {
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
				- DeduplicateAndMerge
					- FunctionCall: avg_over_time(...)
						- NoOp: matrix
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

		"empty result ANDed with non-empty result: returns empty result": {
			expr: `EMPTY_RESULT and metric`,
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

			timeRange := types.NewRangeQueryTimeRange(testCase.queryStart, testCase.queryStart.Add(24*time.Hour), time.Minute)
			expectedModified := 1

			if testCase.expectUnchanged {
				testCase.expectedPlan = generatePlan(t, false, testCase.expr, timeRange, nil)
				expectedModified = 0
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

	reg := prometheus.NewPedanticRegistry()
	opts := streamingpromql.NewTestEngineOpts()
	optimizationPass := plan.NewRemoveStaticallyEmptyExpressionsOptimizationPass(reg, opts.Logger)

	for _, function := range []functions.Function{functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_1, functions.FUNCTION_ADAPTIVE_METRICS_RESERVED_2} {
		// We have to manually create the query plan instead of parsing it from an expression because the Adaptive Metrics wrapper
		// functions aren't registered in open source Mimir.
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
