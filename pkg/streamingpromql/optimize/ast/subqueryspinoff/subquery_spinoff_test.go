// SPDX-License-Identifier: AGPL-3.0-only

package subqueryspinoff

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
)

// TestSubquerySpinOff_Correctness confirms that spinning off subqueries from instant queries inside the
// Mimir query engine produces the same results as evaluating the original query. The test cases are
// based on those for the query-frontend subquery spin-off middleware.
func TestSubquerySpinOff_Correctness(t *testing.T) {
	data := `
		load 1m
			metric_a{env="prod", instance="a"} 0+5x300
			metric_a{env="prod", instance="b"} 0+7x300
			metric_a{env="dev", instance="a"}  0+11x300
			metric_b{env="prod", instance="a"}   0+3x300
			metric_b{env="prod", instance="b"}   0+2x300
			metric_b{env="dev", instance="a"}    0+4x300
	`

	storage := promqltest.LoadedStorage(t, data)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	evalTime := time.Unix(0, 0).Add(250 * time.Minute)

	queries := []struct {
		expr                 string
		expectSpinOffSuccess bool
	}{
		// Eligible for spin-off.
		{expr: `max_over_time(rate(metric_a[1m])[2h:1m])`, expectSpinOffSuccess: true},
		{expr: `sum_over_time(rate(metric_a[5m])[2h:1m])`, expectSpinOffSuccess: true},
		{expr: `quantile_over_time(0.9, rate(metric_a[1m])[90m:30s])`, expectSpinOffSuccess: true},
		{expr: `max_over_time(sum(rate(metric_a[1m]))[2h:1m])`, expectSpinOffSuccess: true},
		{expr: `min_over_time((metric_b + metric_b)[2h:1m])`, expectSpinOffSuccess: true},
		// A spun-off subquery alongside a downstream query.
		{expr: `max_over_time(rate(metric_a[1m])[2h:1m]) + on (env, instance) avg_over_time(metric_b[1h])`, expectSpinOffSuccess: true},

		// Not eligible for spin-off; results must still match.
		{expr: `min_over_time((metric_b * 2)[2h:1m])`, expectSpinOffSuccess: false}, // Subquery has a single selector, so it's too simple to spin off.
		{expr: `max_over_time(metric_b[2h:1m])`, expectSpinOffSuccess: false},       // Subquery is too simple to spin off.
		{expr: `sum(metric_b)`, expectSpinOffSuccess: false},
		{expr: `metric_b`, expectSpinOffSuccess: false},
	}

	baselineEngine, _ := newSubquerySpinOffTestEngine(t, false)

	for _, q := range queries {
		t.Run(q.expr, func(t *testing.T) {
			spinOffEngine, reg := newSubquerySpinOffTestEngine(t, true)

			ctx := user.InjectOrgID(context.Background(), "tenant-1")

			expected := execInstantQuery(t, ctx, baselineEngine, storage, q.expr, evalTime)
			actual := execInstantQuery(t, ctx, spinOffEngine, storage, q.expr, evalTime)
			require.Equal(t, expected, actual)

			successCount := 0
			if q.expectSpinOffSuccess {
				successCount = 1
			}

			// We only bother checking some of the metrics here, most are covered by the tests for the Map function in pkg/frontend/querymiddleware/subqueryspinoff.
			expectedMetrics := fmt.Sprintf(`
				# HELP cortex_frontend_subquery_spinoff_attempts_total Total number of queries the query-frontend attempted to spin-off subqueries from.
				# TYPE cortex_frontend_subquery_spinoff_attempts_total counter
				cortex_frontend_subquery_spinoff_attempts_total 1
				# HELP cortex_frontend_subquery_spinoff_successes_total Total number of queries the query-frontend successfully spun off subqueries from.
				# TYPE cortex_frontend_subquery_spinoff_successes_total counter
				cortex_frontend_subquery_spinoff_successes_total %d
				`,
				successCount,
			)

			require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(expectedMetrics), "cortex_frontend_subquery_spinoff_attempts_total", "cortex_frontend_subquery_spinoff_successes_total"))
		})
	}
}

// TestSubquerySpinOff_ScalarDownstreamPreservesType is a regression test for a correctness bug:
// we must differentiate between evaluation roots containing instant vectors and scalars.
// This ensures that when the spin-off mapper wraps a scalar downstream
// expression - e.g. the "scalar(...)" operand of a vector/scalar binary operation - the
// resulting __evaluation_root__(scalar(...)) reports a scalar type at the AST level.
func TestSubquerySpinOff_ScalarDownstreamPreservesType(t *testing.T) {
	// metric_a and metric_b are identical except for __name__.
	data := `
		load 1m
			metric_a{instance="x"} 0+5x300
			metric_b{instance="x"} 0+7x300
			metric_c               0+1x300
	`

	storage := promqltest.LoadedStorage(t, data)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	evalTime := time.Unix(0, 0).Add(250 * time.Minute)

	// A spinnable subquery (range >= 1h, >= 10 steps, >1 selector) so spin-off fires, whose
	// result holds two series differing only by __name__ (the empty `unless metric_missing`
	// keeps both), combined with a scalar downstream query via a __name__-dropping operator.
	expr := `last_over_time(({__name__=~"metric_a|metric_b", instance="x"} unless metric_missing)[2h:1m]) + scalar(metric_c)`

	ctx := user.InjectOrgID(context.Background(), "tenant-1")

	baselineEngine, _ := newSubquerySpinOffTestEngine(t, false)
	spinOffEngine, _ := newSubquerySpinOffTestEngine(t, true)

	baselineQuery, err := baselineEngine.NewInstantQuery(ctx, storage, nil, expr, evalTime)
	require.NoError(t, err)
	t.Cleanup(baselineQuery.Close)
	baselineRes := baselineQuery.Exec(ctx)

	spinOffQuery, err := spinOffEngine.NewInstantQuery(ctx, storage, nil, expr, evalTime)
	require.NoError(t, err)
	t.Cleanup(spinOffQuery.Close)
	spinOffRes := spinOffQuery.Exec(ctx)

	require.EqualError(t, baselineRes.Err, "vector cannot contain metrics with the same labelset")
	require.EqualErrorf(t, spinOffRes.Err, baselineRes.Err.Error(),
		"spin-off changed the query outcome: baseline errored with %s but spin-off returned %s", baselineRes.Err.Error(), spinOffRes.String())
}

func newSubquerySpinOffTestEngine(t *testing.T, enableSpinOff bool) (*streamingpromql.Engine, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	opts := streamingpromql.NewTestEngineOpts()
	opts.CommonOpts.Reg = reg
	opts.RangeQuerySplittingAndCaching.SplitEnabled = true
	opts.RangeQuerySplittingAndCaching.SplitInterval = 24 * time.Hour
	opts.RangeQuerySplittingAndCaching.CacheEnabled = true
	opts.RangeQuerySplittingAndCaching.MinCacheExtent = querymiddleware.DefaultMinCacheExtent
	opts.RangeQuerySplittingAndCaching.CacheClient = cache.NewMockCache()

	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	if enableSpinOff {
		planner.RegisterASTOptimizationPass(NewOptimizationPass(spinOffEnabledLimits{}, opts.CommonOpts.NoStepSubqueryIntervalFn, reg, log.NewNopLogger()))
	}

	engine, err := streamingpromql.NewEngine(opts, stats.NewQueryMetrics(nil), planner)
	require.NoError(t, err)

	return engine, reg
}

func execInstantQuery(t *testing.T, ctx context.Context, engine *streamingpromql.Engine, queryable storage.Queryable, expr string, ts time.Time) string {
	q, err := engine.NewInstantQuery(ctx, queryable, nil, expr, ts)
	require.NoError(t, err)
	t.Cleanup(q.Close)

	res := q.Exec(ctx)
	require.NoError(t, res.Err)

	return res.String()
}

type spinOffEnabledLimits struct{}

func (spinOffEnabledLimits) SubquerySpinOffEnabled(string) bool { return true }
