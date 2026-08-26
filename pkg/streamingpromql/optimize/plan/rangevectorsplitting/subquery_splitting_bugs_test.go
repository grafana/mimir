// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
)

// This file demonstrates three bugs found while reviewing subquery support in range vector
// splitting (#16444). All three tests are expected to FAIL against the PR branch.

// Bug 1: a DuplicateFilter directly below a Subquery/StepInvariantExpression crashes.
//
// deduplicateAcrossSplitBlocks (commonsubexpressionelimination/optimization_pass.go) skips
// wrapping a split target's child in a Duplicate node when that child is already a Duplicate OR a
// DuplicateFilter. Skipping is correct for Duplicate (its factory hands out a fresh consumer per
// Produce() call), but MaterializeDuplicateFilter returns a single-use factory. When a
// DuplicateFilter sits directly under a StepInvariantExpression - whose ChildrenTimeRange is
// always the same fixed instant regardless of which split block asks for it - every split block
// ends up materializing the same DuplicateFilter node with an identical OperatorFactoryKey, and
// the single-use factory can only be Produce()d once.
func TestBug_DuplicateFilterUnderStepInvariantExpression_Crashes(t *testing.T) {
	_, splitEngine := setupEngineAndCache(t)

	storage := teststorage.New(t)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	ctx := context.Background()
	app := storage.Appender(ctx)
	for i := 0; i <= 30; i++ {
		ts := timestamp.FromTime(timestamp.Time(0).Add(time.Duration(i) * time.Hour))
		_, err := app.Append(0, labels.FromStrings("__name__", "dup_filter_metric", "a", "1"), ts, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	// Split interval is 2h (see defaultSplittingOpts). At t=24h, both sides of this binary
	// expression split the same subquery into the same blocks, and CSE inserts a DuplicateFilter
	// directly under the subquery's StepInvariantExpression (from the `@ 10h` modifier).
	expr := `sum_over_time((dup_filter_metric{a="1"} @ 10h)[5h:1h]) + on() sum(sum_over_time((dup_filter_metric @ 10h)[3h:1h]))`
	ts := timestamp.Time(0).Add(24 * time.Hour)

	result, _, _ := executeQuery(t, splitEngine, storage, expr, ts)
	require.NoError(t, result.Err, "expected this query to succeed, but subquery splitting's CSE interaction crashes it")
}

// Bug 2: a `smoothed` selector nested inside a split subquery produces permanently stale cache
// entries.
//
// MatrixSelector.IsSplittable() already excludes Anchored and Smoothed selectors from being a
// split target directly, because smoothed's storage read extends `lookbackDelta` past its own
// query end (see selectors.ComputeQueriedTimeRange). Subquery.IsSplittable() is unconditionally
// true, with no check on what's nested inside, so a smoothed selector can now sit inside a split
// subquery. When it lands on (or near) a cacheable block's last evaluation point, its storage read
// reaches past that block's own End boundary - the exact boundary the out-of-order-window check
// uses to decide the block is safe to cache - so the cached result can be computed before data
// that a fresh evaluation would already see has arrived.
func TestBug_SmoothedSelectorInsideSplitSubquery_CacheGoesStale(t *testing.T) {
	cacheKeyGenerator := createEmptyPrefixCacheKeyGenerator()
	backend := caching.NewInMemoryCache()
	irCache := cache.NewCacheFactoryWithBackend(backend, streamingpromql.NewStaticQueryLimitsProvider(), cacheKeyGenerator, prometheus.NewRegistry(), log.NewNopLogger())

	opts := defaultSplittingOpts()
	limits := streamingpromql.NewStaticQueryLimitsProvider()
	limits.MaxOutOfOrderTimeWindow = 0
	opts.Limits = limits

	baseT := timestamp.Time(0)
	fixedNow := baseT.Add(6 * time.Hour)
	opts.TimeNow = func() time.Time { return fixedNow }

	queryPlanner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	splitEngine, err := streamingpromql.NewEngineWithCache(opts, stats.NewQueryMetrics(nil), queryPlanner, irCache)
	require.NoError(t, err)

	storage := teststorage.New(t, func(opt *tsdb.Options) {
		opt.OutOfOrderTimeWindow = 0
	})
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	ctx := context.Background()
	appendAt := func(minutes ...int) {
		app := storage.Appender(ctx)
		for _, m := range minutes {
			ts := timestamp.FromTime(baseT.Add(time.Duration(m) * time.Minute))
			_, err := app.Append(0, labels.FromStrings("__name__", "smoothed_metric", "env", "prod"), ts, float64(m))
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}

	// Samples every 3 minutes from 0 to 357. This leaves a gap spanning the cacheable block's
	// last evaluated grid point at minute 359 (one 1m subquery step before "now"), so the
	// smoothed boundary lookup there must search forward rather than landing exactly on an
	// existing sample.
	minutes := make([]int, 0, 120)
	for m := 0; m <= 357; m += 3 {
		minutes = append(minutes, m)
	}
	appendAt(minutes...)

	// 5h subquery range, 1m step, 2h split interval, evaluated at 6h. The cacheable block
	// (4h-1ms, 6h-1ms] has its last grid point at minute 359 (6h - 1m).
	expr := "sum_over_time(rate(smoothed_metric[3m] smoothed)[5h:1m])"
	ts := fixedNow

	firstResult, _, _ := executeQuery(t, splitEngine, storage, expr, ts)
	require.NoError(t, firstResult.Err)

	// Normal (in-order) ingestion catches up: no OOO involved, just data arriving after the block
	// was cached.
	appendAt(360, 361, 362, 363)

	cachedResult, _, _ := executeQuery(t, splitEngine, storage, expr, ts)
	require.NoError(t, cachedResult.Err)

	freshResult := freshEvaluation(t, storage, expr, ts)

	require.Equal(t, freshResult.Value, cachedResult.Value,
		"cached splitting engine result should match a fresh evaluation once new data has landed, but the cached block was computed before that data existed and never gets invalidated")
}

// Bug 3: the same class of bug as Bug 2, but with a plain negative offset - no `smoothed` or
// `anchored` modifier required. Negative offset is a normal, default-enabled PromQL feature
// (EnableNegativeOffset: true in production), so this is reachable by entirely ordinary queries.
func TestBug_NegativeOffsetSelectorInsideSplitSubquery_CacheGoesStale(t *testing.T) {
	cacheKeyGenerator := createEmptyPrefixCacheKeyGenerator()
	backend := caching.NewInMemoryCache()
	irCache := cache.NewCacheFactoryWithBackend(backend, streamingpromql.NewStaticQueryLimitsProvider(), cacheKeyGenerator, prometheus.NewRegistry(), log.NewNopLogger())

	opts := defaultSplittingOpts()
	limits := streamingpromql.NewStaticQueryLimitsProvider()
	limits.MaxOutOfOrderTimeWindow = 0
	opts.Limits = limits

	baseT := timestamp.Time(0)
	fixedNow := baseT.Add(6 * time.Hour)
	opts.TimeNow = func() time.Time { return fixedNow }

	queryPlanner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	splitEngine, err := streamingpromql.NewEngineWithCache(opts, stats.NewQueryMetrics(nil), queryPlanner, irCache)
	require.NoError(t, err)

	storage := teststorage.New(t, func(opt *tsdb.Options) {
		opt.OutOfOrderTimeWindow = 0
	})
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	ctx := context.Background()
	appendAt := func(minutes ...int) {
		app := storage.Appender(ctx)
		for _, m := range minutes {
			ts := timestamp.FromTime(baseT.Add(time.Duration(m) * time.Minute))
			_, err := app.Append(0, labels.FromStrings("__name__", "negoffset_metric", "env", "prod"), ts, float64(m))
			require.NoError(t, err)
		}
		require.NoError(t, app.Commit())
	}

	// Data initially available only up to minute 349. The nested selector uses `offset -10m`, so
	// at the block's last grid point (minute 359), its own read window is centered on minute 369 -
	// past "now" (minute 360) and past all data that exists yet.
	minutes := make([]int, 0, 350)
	for m := 0; m <= 349; m++ {
		minutes = append(minutes, m)
	}
	appendAt(minutes...)

	expr := "sum_over_time(rate(negoffset_metric[3m] offset -10m)[5h:1m])"
	ts := fixedNow

	firstResult, _, _ := executeQuery(t, splitEngine, storage, expr, ts)
	require.NoError(t, firstResult.Err)

	more := make([]int, 0, 21)
	for m := 350; m <= 370; m++ {
		more = append(more, m)
	}
	appendAt(more...)

	cachedResult, _, _ := executeQuery(t, splitEngine, storage, expr, ts)
	require.NoError(t, cachedResult.Err)

	freshResult := freshEvaluation(t, storage, expr, ts)

	require.Equal(t, freshResult.Value, cachedResult.Value,
		"cached splitting engine result should match a fresh evaluation once new data has landed, but the cached block was computed before that data existed and never gets invalidated")
}

// freshEvaluation runs expr against a splitting-disabled engine, as a source of truth for what
// the correct, non-cached result should be.
func freshEvaluation(t *testing.T, storage storage.Storage, expr string, ts time.Time) *promql.Result {
	t.Helper()

	plainOpts := streamingpromql.NewTestEngineOpts()
	plainPlanner, err := streamingpromql.NewQueryPlanner(plainOpts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)
	plainEngine, err := streamingpromql.NewEngine(plainOpts, stats.NewQueryMetrics(plainOpts.CommonOpts.Reg), plainPlanner)
	require.NoError(t, err)

	freshResult, _, _ := executeQuery(t, plainEngine, storage, expr, ts)
	require.NoError(t, freshResult.Err)
	return freshResult
}
