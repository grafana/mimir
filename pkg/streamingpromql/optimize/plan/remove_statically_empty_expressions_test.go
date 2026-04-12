// SPDX-License-Identifier: AGPL-3.0-only

package plan_test

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestRemoveStaticallyEmptyExpressionsOptimizationPass(t *testing.T) {
	const lookbackDelta = 5 * time.Minute
	const constant = 1000
	thresholdMs := (constant*time.Second + lookbackDelta).Milliseconds()

	testCases := map[string]struct {
		expr            string
		queryStart      time.Time // instant query time
		expectedPlan    string
		expectUnchanged bool
	}{
		"timestamp(v) < C: query time range is on threshold, should optimize": {
			expr:       "timestamp(metric) < CONSTANT",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) < bool C: would optimise because query time range is on threshold, but skipped due to bool modifier": {
			expr:            "timestamp(metric) < bool CONSTANT",
			queryStart:      time.UnixMilli(thresholdMs),
			expectUnchanged: true,
		},
		"timestamp(v) < C: query time range overlaps threshold, should not optimize": {
			expr:            "timestamp(metric) < CONSTANT",
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},
		"timestamp(v) <= C: query range starts just above threshold, should optimize": {
			expr:       "timestamp(metric) <= CONSTANT",
			queryStart: time.UnixMilli(thresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) <= C: query range starts exactly at threshold, should not optimize": {
			expr:            "timestamp(metric) <= CONSTANT",
			queryStart:      time.UnixMilli(thresholdMs),
			expectUnchanged: true,
		},
		"C > timestamp(v): query range at threshold, should optimize": {
			expr:       "metric and CONSTANT > timestamp(metric)",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"C > timestamp(v): query range before threshold, should not optimize": {
			expr:            "metric and CONSTANT > timestamp(metric)",
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},
		"C >= timestamp(v): query range just above threshold, should optimize": {
			expr:       "metric and CONSTANT >= timestamp(metric)",
			queryStart: time.UnixMilli(thresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"C >= timestamp(v): query range exactly at threshold, should not optimize": {
			expr:            "metric and CONSTANT >= timestamp(metric)",
			queryStart:      time.UnixMilli(thresholdMs),
			expectUnchanged: true,
		},

		"non-timestamp comparison: should not optimize": {
			expr:            "metric2 < CONSTANT",
			queryStart:      time.UnixMilli(thresholdMs),
			expectUnchanged: true,
		},

		"timestamp(v) < C with and: query range starts exactly at threshold, should optimize": {
			expr:       "metric and timestamp(metric) < CONSTANT",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) < C with and: query range starts just before before threshold, should not optimize": {
			expr:            "metric and timestamp(metric) < CONSTANT",
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},

		"timestamp condition on LHS of and: should optimize": {
			expr:       "timestamp(metric) < CONSTANT and metric",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},

		"nested and expressions: should optimize where inner expression is no-op": {
			// The inner "and" has the timestamp filter → replaced with NoOp.
			// The outer "and" then has NoOp as its LHS → also replaced with NoOp.
			expr:       "(metric and timestamp(metric) < CONSTANT) and metric2",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"nested and expressions: inner not optimized because not a no-op": {
			expr:            "(metric and timestamp(metric) < CONSTANT) and metric2",
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},
		"nested and expressions: should optimize as far as possible": {
			expr:       "(metric and timestamp(metric) < CONSTANT) or metric2",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- DeduplicateAndMerge
					- BinaryExpression: LHS or RHS
						- LHS: NoOp
						- RHS: VectorSelector: {__name__="metric2"}
			`,
		},

		"complex expression inside timestamp() that is a no-op: should optimize": {
			expr:       `foo and timestamp(sum(metric)) < CONSTANT`,
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"complex expression inside timestamp() that is not a no-op: should not optimize": {
			expr:            `foo and timestamp(sum(metric)) < CONSTANT`,
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},

		"timestamp condition outside subquery: should optimize": {
			// The "and timestamp(metric) < CONSTANT" is outside the subquery, so it is evaluated
			// at the outer query time range, which is after the threshold.
			expr:       "avg_over_time(metric[5m:1m]) and timestamp(metric) < CONSTANT",
			queryStart: time.UnixMilli(thresholdMs),
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

			timeRange := types.NewRangeQueryTimeRange(testCase.queryStart, timestamp.Time(math.MaxInt64), time.Minute)
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
