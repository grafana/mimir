// SPDX-License-Identifier: AGPL-3.0-only

package plan_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
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
		"timestamp(v) < C expression without surrounding binop and query time range is on threshold, should optimize": {
			expr:       "timestamp(metric) < 1000",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) < C expression without surrounding binop and query time range is at threshold, should not optimize": {
			expr:            "timestamp(metric) < 1000",
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},

		"timestamp(v) < C: query range starts exactly at threshold, should optimize": {
			expr:       "metric and timestamp(metric) < 1000",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) < C: query range starts just before before threshold, should not optimize": {
			expr:            "metric and timestamp(metric) < 1000",
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},
		"timestamp(v) < C: query range well before threshold, should not optimize": {
			expr:            "metric and timestamp(metric) < 1000",
			queryStart:      time.Unix(500, 0),
			expectUnchanged: true,
		},

		"timestamp(v) <= C: query range starts just before above threshold, should optimize": {
			expr:       "metric and timestamp(metric) <= 1000",
			queryStart: time.UnixMilli(thresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp(v) <= C: query range starts exactly at threshold, should not optimize": {
			expr:            "metric and timestamp(metric) <= 1000",
			queryStart:      time.UnixMilli(thresholdMs),
			expectUnchanged: true,
		},

		"C > timestamp(v) (reversed <): query range at threshold, should optimize": {
			expr:       "metric and 1000 > timestamp(metric)",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"C > timestamp(v) (reversed <): query range before threshold, should not optimize": {
			expr:            "metric and 1000 > timestamp(metric)",
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},
		"C >= timestamp(v) (reversed <=): query range just above threshold, should optimize": {
			expr:       "metric and 1000 >= timestamp(metric)",
			queryStart: time.UnixMilli(thresholdMs + 1),
			expectedPlan: `
				- NoOp
			`,
		},
		"C >= timestamp(v) (reversed <=): query range exactly at threshold, should not optimize": {
			expr:            "metric and 1000 >= timestamp(metric)",
			queryStart:      time.UnixMilli(thresholdMs),
			expectUnchanged: true,
		},

		"timestamp filter on LHS of and: should optimize": {
			expr:       "timestamp(metric) < 1000 and metric",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},

		"non-timestamp comparison on RHS of and: should not optimize": {
			expr:            "metric and metric2 < 1000",
			queryStart:      time.UnixMilli(thresholdMs),
			expectUnchanged: true,
		},

		"nested and expressions: inner replaced first, then outer": {
			// The inner "and" has the timestamp filter → replaced with NoOp.
			// The outer "and" then has NoOp as its LHS → also replaced with NoOp.
			expr:       "(metric and timestamp(metric) < 1000) and metric2",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"nested and expressions: inner not optimized because threshold not met": {
			expr:            "(metric and timestamp(metric) < 1000) and metric2",
			queryStart:      time.UnixMilli(thresholdMs - 1),
			expectUnchanged: true,
		},

		"timestamp filter outside subquery: should optimize": {
			// The "and timestamp(metric) < 1000" is outside the subquery, so it is evaluated
			// at the outer query time range, which is after the threshold.
			expr:       "avg_over_time(metric[5m:1m]) and timestamp(metric) < 1000",
			queryStart: time.UnixMilli(thresholdMs),
			expectedPlan: `
				- NoOp
			`,
		},
		"timestamp filter inside subquery: should not optimize": {
			// The "and timestamp(metric) < 1000" is inside the subquery, so it is evaluated
			// over the subquery's time range, which extends back further than the outer query
			// start. We stop recursion at the Subquery boundary to avoid incorrectly replacing
			// expressions that may return non-empty results at the earlier subquery steps.
			expr:            "avg_over_time((metric and timestamp(metric) < 1000)[2h:1m])",
			queryStart:      time.Unix(10000, 0),
			expectUnchanged: true,
		},
	}

	ctx := context.Background()
	observer := streamingpromql.NoopPlanningObserver{}

	generatePlan := func(t *testing.T, enableOptimizationPass bool, expr string, timeRange types.QueryTimeRange) string {
		opts := streamingpromql.NewTestEngineOpts()
		planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
		require.NoError(t, err)

		if enableOptimizationPass {
			planner.RegisterQueryPlanOptimizationPass(plan.NewRemoveStaticallyEmptyExpressionsOptimizationPass(prometheus.NewPedanticRegistry(), opts.Logger))
		}

		p, err := planner.NewQueryPlan(ctx, expr, timeRange, lookbackDelta, false, observer)
		require.NoError(t, err)

		return p.String()
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			timeRange := types.NewRangeQueryTimeRange(testCase.queryStart, timestamp.Time(math.MaxInt64), time.Minute)

			if testCase.expectUnchanged {
				testCase.expectedPlan = generatePlan(t, false, testCase.expr, timeRange)
			}

			actual := generatePlan(t, true, testCase.expr, timeRange)
			require.Equal(t, testutils.TrimIndent(testCase.expectedPlan), actual)
		})
	}
}
