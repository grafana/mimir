// SPDX-License-Identifier: AGPL-3.0-only

package subqueryspinoff_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/stretchr/testify/require"

	frontendspinoff "github.com/grafana/mimir/pkg/frontend/querymiddleware/subqueryspinoff"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast/subqueryspinoff"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestOptimizationPass(t *testing.T) {
	testCases := map[string]struct {
		input      string
		rangeQuery bool
		disabled   bool

		// expectedOutput is the expected rewritten query. An empty string means the query should be left
		// unchanged.
		expectedOutput string

		expectedAttempts  int
		expectedSuccesses int
		expectedSkipped   string
	}{
		"spin off a single subquery": {
			input:             `max_over_time(rate(metric_counter[1m])[2h:1m])`,
			expectedOutput:    `max_over_time(__vector_evaluation_root__(rate(metric_counter[1m]))[2h:1m])`,
			expectedAttempts:  1,
			expectedSuccesses: 1,
		},
		"spin off multiple subqueries": {
			input:             `avg_over_time((foo * bar)[3d:1m]) * max_over_time((foo * bar)[2d:2m])`,
			expectedOutput:    `avg_over_time(__vector_evaluation_root__((foo * bar))[3d:1m]) * max_over_time(__vector_evaluation_root__((foo * bar))[2d:2m])`,
			expectedAttempts:  1,
			expectedSuccesses: 1,
		},
		"spin off a subquery alongside a downstream query": {
			input:             `avg_over_time((foo * bar)[3d:1m]) * avg_over_time(foo[3d])`,
			expectedOutput:    `avg_over_time(__vector_evaluation_root__((foo * bar))[3d:1m]) * __vector_evaluation_root__(avg_over_time(foo[3d]))`,
			expectedAttempts:  1,
			expectedSuccesses: 1,
		},
		"subquery too simple to spin off": {
			input:            `avg_over_time(foo[3d:1m])`,
			expectedAttempts: 1,
			expectedSkipped:  "no-subqueries",
		},
		"subquery range too short to spin off": {
			input:            `avg_over_time((foo * bar)[30m:1m])`,
			expectedAttempts: 1,
			expectedSkipped:  "no-subqueries",
		},
		"range query is left unchanged even when it contains a spinnable subquery": {
			input:      `max_over_time(rate(metric_counter[1m])[2h:1m])`,
			rangeQuery: true,
			// No attempt is recorded: subquery spin-off does not apply to range queries at all.
		},
		"instant query is left unchanged when spin-off is disabled for a tenant": {
			input:    `max_over_time(rate(metric_counter[1m])[2h:1m])`,
			disabled: true,
			// No attempt is recorded: we bail out before attempting to spin off.
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			opts := streamingpromql.NewTestEngineOpts()
			reg := prometheus.NewPedanticRegistry()
			opts.CommonOpts.Reg = reg

			planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)

			pass := subqueryspinoff.NewOptimizationPass(mockLimits{enabled: !testCase.disabled}, func(int64) int64 { return 1000 }, frontendspinoff.Options{}, reg, log.NewNopLogger())
			planner.RegisterASTOptimizationPass(pass)

			timeRange := types.NewInstantQueryTimeRange(timestamp.Time(0))
			if testCase.rangeQuery {
				timeRange = types.NewRangeQueryTimeRange(timestamp.Time(0), timestamp.Time(3600*1000), time.Minute)
			}

			ctx := user.InjectOrgID(context.Background(), "tenant-1")
			output, err := planner.ParseAndApplyASTOptimizationPasses(ctx, testCase.input, timeRange, streamingpromql.NoopPlanningObserver{})
			require.NoError(t, err)

			expectedOutput := testCase.expectedOutput
			if expectedOutput == "" {
				expectedOutput = testCase.input
			}
			require.Equal(t, expectedOutput, output.String())

			skippedCount := func(reason string) int {
				if reason == testCase.expectedSkipped {
					return 1
				}
				return 0
			}

			expectedMetrics := fmt.Sprintf(`
				# HELP cortex_frontend_subquery_spinoff_attempts_total Total number of queries the query-frontend attempted to spin-off subqueries from.
				# TYPE cortex_frontend_subquery_spinoff_attempts_total counter
				cortex_frontend_subquery_spinoff_attempts_total %d
				# HELP cortex_frontend_subquery_spinoff_successes_total Total number of queries the query-frontend successfully spun off subqueries from.
				# TYPE cortex_frontend_subquery_spinoff_successes_total counter
				cortex_frontend_subquery_spinoff_successes_total %d
				# HELP cortex_frontend_subquery_spinoff_skipped_total Total number of queries the query-frontend skipped or failed to spin-off subqueries from.
				# TYPE cortex_frontend_subquery_spinoff_skipped_total counter
				cortex_frontend_subquery_spinoff_skipped_total{reason="mapping-failed"} %d
				cortex_frontend_subquery_spinoff_skipped_total{reason="no-subqueries"} %d
				cortex_frontend_subquery_spinoff_skipped_total{reason="parsing-failed"} %d
				cortex_frontend_subquery_spinoff_skipped_total{reason="too-many-downstream-queries"} %d
			`,
				testCase.expectedAttempts,
				testCase.expectedSuccesses,
				skippedCount("mapping-failed"),
				skippedCount("no-subqueries"),
				skippedCount("parsing-failed"),
				skippedCount("too-many-downstream-queries"),
			)

			require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics),
				"cortex_frontend_subquery_spinoff_attempts_total",
				"cortex_frontend_subquery_spinoff_successes_total",
				"cortex_frontend_subquery_spinoff_skipped_total",
			))
		})
	}
}

type mockLimits struct {
	enabled bool
}

func (m mockLimits) SubquerySpinOffEnabled(string) bool { return m.enabled }
