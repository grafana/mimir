// SPDX-License-Identifier: AGPL-3.0-only

// Package test contains tests for range vector splitting that run against the full
// streaming PromQL engine. This package is separate from the rangevectorsplitting
// package to avoid circular dependencies: the engine depends on the optimization
// pass, so engine-level tests live here.
package test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	dskitcache "github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// Checks the case where results are not cached as the range (1h) is lower than the split interval (test default is 2h).
func TestQuerySplitting_InstantQueryWith1hRange_NotCached(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	storage := promqltest.LoadedStorage(t, `
		load 10m
			some_metric{env="1"} 0+1x40
	`)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	baseT := timestamp.Time(0)
	expr := "sum_over_time(some_metric[1h])"
	ts := baseT.Add(2 * time.Hour)
	expected := &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "1"),
				T:      timestamp.FromTime(ts),
				F:      7 + 8 + 9 + 10 + 11 + 12,
			},
		},
	}

	result := runInstantQuery(t, mimirEngine, storage, expr, ts)
	require.Equal(t, expected, result)

	result = runInstantQuery(t, mimirEngine, storage, expr, ts)
	require.Equal(t, expected, result)

	verifyCacheStats(t, testCache, 0, 0, 0)
}

// TestQuerySplitting_InstantQueryWith5hRange_UsesCache validates query splitting with caching.
//
// Important: SplitRanges use PromQL notation (Start, End] (left-open, right-closed), but storage
// queries use closed intervals [mint, maxt] on both sides. The conversion is:
//   - PromQL range (Start, End] becomes storage query [Start+1, End]
//   - Example: PromQL range (2h-1ms, 4h-1ms] becomes storage [2h, 4h-1ms]
//   - Example: PromQL range (6h-1ms, 6h] becomes storage [6h, 6h]
func TestQuerySplitting_InstantQueryWith5hRange_UsesCache(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			some_metric{env="1"} 0+1x60
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	expr := "sum_over_time(some_metric[5h])"

	// Run query at 6h
	ts := baseT.Add(6 * time.Hour)

	// Splits:
	// - Head: (1h, 2h]
	// - Cached: (2h-4h], (4h-6h] (cache hit on second query)
	// - Tail: (6h, 6h] → empty
	expected := &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "1"),
				T:      timestamp.FromTime(ts),
				F:      645,
				// first sample @ 1h10m = 7, last sample @ 6h = 36, number of samples = 30
				// (7+36)*(30/2) = 645
			},
		},
	}

	// Run query first time (should populate cache)
	result, ranges1 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 6 * hourInMs},
	}, ranges1)
	verifyCacheStats(t, testCache, 2, 0, 2)

	// Run same query again (should hit cache for aligned blocks)
	result, ranges2 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 2*hourInMs - 1}, // Head: (1h, 2h-1ms] -> storage [1h+1ms, 2h-1ms]
		{mint: 6 * hourInMs, maxt: 6 * hourInMs},     // Tail: (6h-1ms, 6h] -> storage [6h, 6h]
	}, ranges2)
	verifyCacheStats(t, testCache, 4, 2, 2) // 2 aligned blocks, both hit on second query

	// Run query at 6h10m
	ts = baseT.Add(6*time.Hour + 10*time.Minute)
	// Splits:
	// - Head: (1h10m, 2h-1ms]
	// - Cached: (2h-1ms, 4h-1ms], (4h-1ms, 6h-1ms] (cache hits)
	// - Tail: (6h-1ms, 6h10m]
	expected = &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "1"),
				T:      timestamp.FromTime(ts),
				F:      675,
				// first sample @ 1h20m = 8, last sample @ 6h10m = 37, number of samples = 30
				// (8+37)*(30/2) = 675
			},
		},
	}
	result, ranges3 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 10*minuteInMs + 1, maxt: 2*hourInMs - 1}, // Head: (1h10m, 2h-1ms] -> storage [1h10m+1ms, 2h-1ms]
		{mint: 6 * hourInMs, maxt: 6*hourInMs + 10*minuteInMs},       // Tail: (6h-1ms, 6h10m] -> storage [6h, 6h10m]
	}, ranges3)
	verifyCacheStats(t, testCache, 6, 4, 2) // Both aligned blocks are cache hits

	// Run query at 7h
	ts = baseT.Add(7 * time.Hour)
	// Splits:
	// - Head: (2h, 4h-1ms]
	// - Cached: (4h-1ms, 6h-1ms] (cache hit)
	// - Tail: (6h-1ms, 7h]
	expected = &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "1"),
				T:      timestamp.FromTime(ts),
				F:      825,
				// first sample @ 2h10m = 13, last sample @ 7h = 42, number of samples = 30
				// (13+42)*(30/2) = 825
			},
		},
	}
	result, ranges4 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (2h, 4h-1ms] -> storage [2h+1ms, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 7 * hourInMs},     // Tail: (6h-1ms, 7h] -> storage [6h, 7h]
	}, ranges4)
	verifyCacheStats(t, testCache, 7, 5, 2) // 1 aligned block checked, 1 hit (4h-1ms, 6h-1ms]

	// Run query at 8h20m
	ts = baseT.Add(8*time.Hour + 20*time.Minute)
	// Splits:
	// - Head: (3h20m, 4h-1ms]
	// - Cached: (4h-1ms, 6h-1ms] (cache hit from query 1)
	// - Block: (6h-1ms, 8h-1ms] (cache miss, new block)
	// - Tail: (8h-1ms, 8h20m]
	// Storage queries merge uncached block + tail
	expected = &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "1"),
				T:      timestamp.FromTime(ts),
				F:      1065,
				// first sample @ 3h30m = 21, last sample @ 8h20m = 50, number of samples = 30
				// (21+50)*(30/2) = 1065
			},
		},
	}
	result, ranges5 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 3*hourInMs + 20*minuteInMs + 1, maxt: 4*hourInMs - 1}, // Head: (3h20m, 4h-1ms] -> storage [3h20m+1ms, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 8*hourInMs + 20*minuteInMs},       // Merged uncached: (6h-1ms, 8h20m] -> storage [6h, 8h20m]
	}, ranges5)
	// Cache stats: Q1: 2 gets (miss), 2 sets | Q2: 2 gets/hits | Q3: 2 gets/hits | Q4: 1 get/hit | Q5: 2 gets, 1 hit, 1 set
	verifyCacheStats(t, testCache, 9, 6, 3) // Total: 9 gets, 6 hits, 3 sets
}

func TestQuerySplitting_MultipleSeriesWithGaps_UsesCache(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	// series1: continuous from 0h-9h
	// series2: 10m-2h, gap 2h-4h, then 4h10m-6h
	// series3: gap 0h-3h10m, then 3h10m-6h, then gap
	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			some_metric{env="1"} 0+1x54
			some_metric{env="2"} _ 0+1x11 _ _ _ _ _ _ _ _ _ _ _ 12+1x11
			some_metric{env="3"} _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ 0+1x17 _ _ _ _ _ _
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	expr := "sum_over_time(some_metric[5h])"
	ts := baseT.Add(6 * time.Hour)

	// Splits:
	// - Head: (1h, 2h]
	// - Cached: (2h-4h], (4h-6h] (cache hit on second query)
	// - Tail: (6h, 6h] → empty
	expected := &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "1"),
				T:      timestamp.FromTime(ts),
				F:      645,
				// first sample @ 1h10m = 7, last sample @ 6h = 36, number of samples = 30
				// (7+36)*(30/2) = 645
			},
			{
				Metric: labels.FromStrings("env", "2"),
				T:      timestamp.FromTime(ts),
				F:      261,
				// first range: first sample @ 1h10m = 6, last sample @ 2h = 11, 6 samples
				// second range: first sample @ 4h10m = 12, last sample @ 6h = 23, 12 samples
				// (6+11)*(6/2) + (12+23)*(12/2) = 51 + 210 = 261
			},
			{
				Metric: labels.FromStrings("env", "3"),
				T:      timestamp.FromTime(ts),
				F:      153,
				// first sample @ 3h10m = 0, last sample @ 6h = 17, number of samples = 18
				// (0+17)*(18/2) = 153
			},
		},
	}

	result, ranges1 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 6 * hourInMs},
	}, ranges1)
	verifyCacheStats(t, testCache, 2, 0, 2)

	// Run same query again (should hit cache for aligned blocks)
	result, ranges2 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 2*hourInMs - 1}, // Head: (1h, 2h-1ms] -> storage [1h+1ms, 2h-1ms]
		{mint: 6 * hourInMs, maxt: 6 * hourInMs},     // Tail: (6h-1ms, 6h] -> storage [6h, 6h]
	}, ranges2)
	verifyCacheStats(t, testCache, 4, 2, 2) // 2 aligned blocks, both hit on second query

	// Run query at 7h
	ts = baseT.Add(7 * time.Hour)
	// Splits:
	// - Head: (2h, 4h-1ms]
	// - Cached: (4h-1ms, 6h-1ms] (cache hit)
	// - Tail: (6h-1ms, 7h]
	expected = &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "1"),
				T:      timestamp.FromTime(ts),
				F:      825,
				// first sample @ 2h10m = 13, last sample @ 7h = 42, number of samples = 30
				// (13+42)*(30/2) = 825
			},
			{
				Metric: labels.FromStrings("env", "2"),
				T:      timestamp.FromTime(ts),
				F:      210,
				// first sample @ 4h10m = 12, last sample @ 6h = 23, 12 samples
				// (12+23)*(12/2) = 210
			},
			{
				Metric: labels.FromStrings("env", "3"),
				T:      timestamp.FromTime(ts),
				F:      153,
				// first sample @ 3h10m = 0, last sample @ 6h = 17, number of samples = 18
				// (0+17)*(18/2) = 153
			},
		},
	}
	result, ranges3 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (2h, 4h-1ms] -> storage [2h+1ms, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 7 * hourInMs},     // Tail: (6h-1ms, 7h] -> storage [6h, 7h]
	}, ranges3)
	verifyCacheStats(t, testCache, 5, 3, 2) // 1 aligned block checked, 1 hit (4h-1ms, 6h-1ms]

	// Run query at 8h20m
	ts = baseT.Add(8*time.Hour + 20*time.Minute)
	// Splits:
	// - Head: (3h20m, 4h-1ms]
	// - Cached: (4h-1ms, 6h-1ms] (cache hit from query 1)
	// - Block: (6h-1ms, 8h-1ms] (cache miss, new block)
	// - Tail: (8h-1ms, 8h20m]
	// Storage queries merge uncached block + tail
	expected = &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "1"),
				T:      timestamp.FromTime(ts),
				F:      1065,
				// first sample @ 3h30m = 21, last sample @ 8h20m = 50, number of samples = 30
				// (21+50)*(30/2) = 1065
			},
			{
				Metric: labels.FromStrings("env", "2"),
				T:      timestamp.FromTime(ts),
				F:      210,
				// first range: 10m-2h outside window (3h20m, 8h20m]
				// second range: first sample @ 4h10m = 12, last sample @ 6h = 23, 12 samples
				// (12+23)*(12/2) = 210
			},
			{
				Metric: labels.FromStrings("env", "3"),
				T:      timestamp.FromTime(ts),
				F:      152,
				// first sample @ 3h30m = 2, last sample @ 6h = 17, number of samples = 16
				// (2+17)*(16/2) = 152
			},
		},
	}
	result, ranges4 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 3*hourInMs + 20*minuteInMs + 1, maxt: 4*hourInMs - 1}, // Head: (3h20m, 4h-1ms] -> storage [3h20m+1ms, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 8*hourInMs + 20*minuteInMs},       // Merged uncached: (6h-1ms, 8h20m] -> storage [6h, 8h20m]
	}, ranges4)
	// Cache stats: Q1: 2 gets (miss), 2 sets | Q2: 2 gets/hits | Q3: 1 get/hit | Q4: 2 gets, 1 hit, 1 set
	verifyCacheStats(t, testCache, 7, 4, 3) // Total: 7 gets, 4 hits, 3 sets
}

func TestQuerySplitting_WithCSE(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x100
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	expr := "sum_over_time(test_metric[5h]) / count_over_time(test_metric[5h])"
	ts := baseT.Add(6 * time.Hour)
	ctx := user.InjectOrgID(context.Background(), "test-user")

	opts := streamingpromql.NewTestEngineOpts()
	opts.RangeVectorSplitting.Enabled = true
	opts.RangeVectorSplitting.SplitInterval = 2 * time.Hour
	require.True(t, opts.EnableCommonSubexpressionElimination, "CSE should be enabled")

	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	plan, err := planner.NewQueryPlan(ctx, expr, types.NewInstantQueryTimeRange(ts), false, &streamingpromql.NoopPlanningObserver{})
	require.NoError(t, err)
	require.NotNil(t, plan)

	expectedPlan := `
		- BinaryExpression: LHS / RHS
			- LHS: SplitFunctionCall: splits=4 [(3600000,7199999], (7199999,14399999]*, (14399999,21599999]*, (21599999,21600000]]
				- FunctionCall: sum_over_time(...)
					- ref#1 Duplicate
						- MatrixSelector: {__name__="test_metric"}[5h0m0s]
			- RHS: SplitFunctionCall: splits=4 [(3600000,7199999], (7199999,14399999]*, (14399999,21599999]*, (21599999,21600000]]
				- FunctionCall: count_over_time(...)
					- ref#1 Duplicate ...
	`
	require.Equal(t, testutils.TrimIndent(expectedPlan), plan.String())

	trackingStorage := trackRanges(promStorage)

	q, err := mimirEngine.NewInstantQuery(ctx, trackingStorage, nil, expr, ts)
	require.NoError(t, err)
	defer q.Close()

	result := q.Exec(ctx)
	require.NoError(t, result.Err)

	// At 6h, looking back 5h: range (1h, 6h]
	// Points at: 70m, 80m, 90m, ..., 360m (values 7, 8, 9, ..., 36)
	// Sum: 7+8+...+36 = 645, Count: 30, Average: 645/30 = 21.5
	expected := &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "prod"),
				T:      timestamp.FromTime(ts),
				F:      21.5,
			},
		},
	}

	require.Equal(t, expected, result)
	require.Greater(t, testCache.sets, 0, "Cache should have been populated")

	// Verify CSE is working: with CSE, the MatrixSelector is shared between sum_over_time
	// and count_over_time via Duplicate, so we should only query storage once, not twice
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 6 * hourInMs},
	}, trackingStorage.ranges)

	// Query 2 at 8h: middle (4h-6h) cached from query 1, head (3h-4h) and tail (6h-8h) uncached
	// With CSE, both sum_over_time and count_over_time share the same MatrixSelector data
	trackingStorage.ranges = nil
	ts2 := baseT.Add(8 * time.Hour)
	q2, err := mimirEngine.NewInstantQuery(ctx, trackingStorage, nil, expr, ts2)
	require.NoError(t, err)
	defer q2.Close()

	result2 := q2.Exec(ctx)
	require.NoError(t, result2.Err)

	// At 8h, looking back 5h: range (3h, 8h]
	// With 2h splits covering the range, some will be cached from query 1
	require.NoError(t, result2.Err)
	require.Len(t, result2.Value.(promql.Vector), 1)
	require.Equal(t, labels.FromStrings("env", "prod"), result2.Value.(promql.Vector)[0].Metric)

	// Verify CSE with partial cache: only 2 storage queries (not 4), one for each uncached range
	require.Equal(t, []storageQueryRange{
		{mint: 3*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (3h, 4h-1ms] -> storage [3h+1, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 8 * hourInMs},     // Tail: (6h-1ms, 8h] -> storage [6h, 8h]
	}, trackingStorage.ranges)
}

func TestQuerySplitting_WithOffset_CacheAlignment(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)
	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x100
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)

	// Splits: head (2h, 4h-1ms], block (4h-1ms, 6h-1ms], tail (6h-1ms, 7h]
	// Only 1 complete cacheable block in this range
	// Data: first sample @ 2h10m = 13, last sample @ 7h = 42, samples = 30
	// Sum: (13+42)*(30/2) = 825
	expr := "sum_over_time(test_metric[5h] offset 1h)"

	ts8h := baseT.Add(8 * time.Hour)
	result1, ranges1 := executeQuery(t, mimirEngine, promStorage, expr, ts8h)
	require.Equal(t, expectedScalarResult(ts8h, 825, "env", "prod"), result1)
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 7 * hourInMs}, // PromQL range (2h, 7h] converts to storage [2h+1, 7h]
	}, ranges1)
	verifyCacheStats(t, testCache, 1, 0, 1) // 1 cacheable block

	// Q1b: no offset at 7h accesses same range (2h, 7h]
	// Cache has block (4h-1ms, 6h-1ms] from Q1, so queries head and tail from storage
	ts7h := baseT.Add(7 * time.Hour)
	exprNoOffset := "sum_over_time(test_metric[5h])"
	result1b, ranges1b := executeQuery(t, mimirEngine, promStorage, exprNoOffset, ts7h)
	require.Equal(t, expectedScalarResult(ts7h, 825, "env", "prod"), result1b)
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (2h, 4h-1ms] -> storage [2h+1, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 7 * hourInMs},     // Tail: (6h-1ms, 7h] -> storage [6h, 7h]
	}, ranges1b)
	verifyCacheStats(t, testCache, 2, 1, 1) // 1 cache hit on the block

	// Q2: offset 1h at 10h accesses (4h, 9h]
	// Splits: head (4h, 6h-1ms], block (6h-1ms, 8h-1ms] (cache miss, new), tail (8h-1ms, 9h]
	// All uncached ranges merge into single storage query
	// Data: first sample @ 4h10m = 25, last sample @ 9h = 54, samples = 30
	// Sum: (25+54)*(30/2) = 1185
	ts10h := baseT.Add(10 * time.Hour)
	result2, ranges2 := executeQuery(t, mimirEngine, promStorage, expr, ts10h)
	require.Equal(t, expectedScalarResult(ts10h, 1185, "env", "prod"), result2)
	require.Equal(t, []storageQueryRange{
		{mint: 4*hourInMs + 1, maxt: 9 * hourInMs}, // Merged: (4h, 9h] -> storage [4h+1, 9h]
	}, ranges2)
	verifyCacheStats(t, testCache, 3, 1, 2) // Q1: 1 get/1 set, Q1b: 1 get/1 hit, Q2: 1 get/1 set
}

func TestQuerySplitting_WithAtModifier_CacheAlignment(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)
	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x100
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)

	// Q1: @ 7h at 8h accesses (2h, 7h]
	// Splits: head (2h, 4h-1ms], block (4h-1ms, 6h-1ms], tail (6h-1ms, 7h]
	// Only 1 complete cacheable block in this range
	expr := "sum_over_time(test_metric[5h] @ 25200)" // 7h in seconds
	ts8h := baseT.Add(8 * time.Hour)
	result1, ranges1 := executeQuery(t, mimirEngine, promStorage, expr, ts8h)
	require.Equal(t, expectedScalarResult(ts8h, 825, "env", "prod"), result1)
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 7 * hourInMs},
	}, ranges1)
	verifyCacheStats(t, testCache, 1, 0, 1) // 1 cacheable block

	// Q2: no @ at 7h accesses same range (2h, 7h]
	// Cache has block (4h-1ms, 6h-1ms] from Q1, so queries head and tail from storage
	exprNoModifier := "sum_over_time(test_metric[5h])"
	ts7h := baseT.Add(7 * time.Hour)
	result2, ranges2 := executeQuery(t, mimirEngine, promStorage, exprNoModifier, ts7h)
	require.Equal(t, expectedScalarResult(ts7h, 825, "env", "prod"), result2)
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (2h, 4h-1ms] -> storage [2h+1, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 7 * hourInMs},     // Tail: (6h-1ms, 7h] -> storage [6h, 7h]
	}, ranges2)
	verifyCacheStats(t, testCache, 2, 1, 1) // 1 cache hit on the block

	// Q3: @ 7h at 10h accesses same range (2h, 7h]
	// Cache has block (4h-1ms, 6h-1ms] from Q1, so queries head and tail from storage
	ts10h := baseT.Add(10 * time.Hour)
	result3, ranges3 := executeQuery(t, mimirEngine, promStorage, expr, ts10h)
	require.Equal(t, expectedScalarResult(ts10h, 825, "env", "prod"), result3)
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (2h, 4h-1ms] -> storage [2h+1, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 7 * hourInMs},     // Tail: (6h-1ms, 7h] -> storage [6h, 7h]
	}, ranges3)
	verifyCacheStats(t, testCache, 3, 2, 1) // Q1: 1 get/1 set, Q2: 1 get/1 hit, Q3: 1 get/1 hit
}

func TestQuerySplitting_With3hRange_NoCacheableRanges(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)
	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x100
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)

	// Query at 5h + 1ms with 3h range: (2h+1ms, 5h+1ms]
	// First aligned boundary after 2h+1ms: 4h
	// Check complete block: 4h + 2h = 6h > 5h+1ms, so NO complete block
	// Query splitting should NOT be applied
	// Data: first sample @ 2h10m = 13, last sample @ 5h = 30, samples = 18
	// Sum: (13+30)*(18/2) = 387
	expr := "sum_over_time(test_metric[3h])"
	ts := baseT.Add(5*time.Hour + time.Millisecond)
	result, ranges := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expectedScalarResult(ts, 387, "env", "prod"), result)

	// Since query splitting is not applied, should be a single storage query
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 2, maxt: 5*hourInMs + 1},
	}, ranges)

	// No cache operations should occur since splitting wasn't applied
	verifyCacheStats(t, testCache, 0, 0, 0)
}

func TestQuerySplitting_With3hRangeAndOffset_NoCacheableRanges(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)
	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x100
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)

	// Query at 4h30m with 3h range and 31m offset
	// Storage time: (4h30m - 3h - 31m, 4h30m - 31m] = (59m, 3h59m]
	// First aligned boundary after 59m: 2h
	// 2h + 2h = 4h > 3h59m (end time of query), so NO complete block
	expr := "sum_over_time(test_metric[3h] offset 31m)"
	ts := baseT.Add(4*time.Hour + 30*time.Minute)

	result, ranges := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expectedScalarResult(ts, 261, "env", "prod"), result)

	// Since query splitting is not applied, should be a single storage query
	require.Equal(t, []storageQueryRange{
		{mint: 59*minuteInMs + 1, maxt: 3*hourInMs + 59*minuteInMs},
	}, ranges)

	// No cache operations should occur since splitting wasn't applied
	verifyCacheStats(t, testCache, 0, 0, 0)
}

func TestQuerySplitting_WithOOOWindow(t *testing.T) {
	backend := newTestCacheBackend()
	irCache := cache.NewCacheFactoryWithBackend(backend, streamingpromql.NewStaticQueryLimitsProvider(), prometheus.NewRegistry(), log.NewNopLogger())

	opts := streamingpromql.NewTestEngineOpts()
	opts.RangeVectorSplitting.Enabled = true
	opts.RangeVectorSplitting.SplitInterval = 2 * time.Hour
	limits := streamingpromql.NewStaticQueryLimitsProvider()
	limits.MaxOutOfOrderTimeWindow = 3 * time.Hour
	opts.Limits = limits

	baseT := timestamp.Time(0)
	fixedNow := baseT.Add(12 * time.Hour)

	queryPlanner, err := streamingpromql.NewQueryPlannerWithTime(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider(), func() time.Time { return fixedNow })
	require.NoError(t, err)

	mimirEngine, err := streamingpromql.NewEngineWithCache(opts, stats.NewQueryMetrics(nil), queryPlanner, irCache)
	require.NoError(t, err)
	// Query at 12h with 7h range: (5h, 12h]
	// Expected splits:
	// - Head: (5h, 6h-1ms]
	// - Block: (6h-1ms, 8h-1ms] - cacheable (before OOO)
	// - Tail: (8h-1ms, 12h] - non-cacheable (in OOO window)

	oooWindowMs := int64(2 * time.Hour / time.Millisecond)
	storage := teststorage.New(t, func(opt *tsdb.Options) {
		opt.OutOfOrderTimeWindow = oooWindowMs
	})
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	ctx := context.Background()
	app := storage.Appender(ctx)
	// Load in-order samples from 0h to 12h (every 10 minutes) for initial query
	for i := 0; i <= 72; i++ {
		ts := timestamp.FromTime(baseT.Add(time.Duration(i) * 10 * time.Minute))
		_, err := app.Append(0, labels.FromStrings("__name__", "test_metric", "env", "prod"), ts, float64(i))
		require.NoError(t, err)
	}
	require.NoError(t, app.Commit())

	expr := "sum_over_time(test_metric[7h])"
	ts := fixedNow

	// First query: should cache the cacheable blocks, but not the OOO range
	// Samples at 5h10m (31) to 12h (72) = 42 samples, sum = (31+72)*42/2 = 2163
	result1, ranges1 := executeQuery(t, mimirEngine, storage, expr, ts)
	require.Equal(t, expectedScalarResult(ts, 2163, "env", "prod"), result1)

	verifyCacheStats(t, backend, 1, 0, 1)
	require.Equal(t, []storageQueryRange{
		{mint: 5*hourInMs + 1, maxt: 12 * hourInMs},
	}, ranges1)

	app = storage.Appender(ctx)
	// Add OOO sample at 9h
	_, err = app.Append(0, labels.FromStrings("__name__", "test_metric", "env", "prod"),
		timestamp.FromTime(baseT.Add(10*time.Hour).Add(1*time.Minute)), 200.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	result2, ranges2 := executeQuery(t, mimirEngine, storage, expr, ts)
	require.Equal(t, expectedScalarResult(ts, 2363, "env", "prod"), result2)

	verifyCacheStats(t, backend, 2, 1, 1)
	require.Equal(t, []storageQueryRange{
		{mint: 5*hourInMs + 1, maxt: 6*hourInMs - 1},
		{mint: 8 * hourInMs, maxt: 12 * hourInMs},
	}, ranges2)

	result3, ranges3 := executeQuery(t, mimirEngine, storage, expr, ts)
	require.Equal(t, expectedScalarResult(ts, 2363, "env", "prod"), result3)

	verifyCacheStats(t, backend, 3, 2, 1)
	require.Equal(t, ranges2, ranges3)
}

func TestQuerySplitting_CacheKeyIsolationAcrossFunctions(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			some_metric{env="1"} 0+1x60
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	ts := baseT.Add(6 * time.Hour)

	// sum_over_time: sum of values 7..36 = 645
	// count_over_time: 30 samples in (1h, 6h]
	expectedSumF := 645.0
	expectedCountF := 30.0

	// Query sum_over_time — populates cache for sum_over_time's function key.
	sumResult := runInstantQuery(t, mimirEngine, promStorage, "sum_over_time(some_metric[5h])", ts)
	require.NoError(t, sumResult.Err)
	require.Equal(t, expectedSumF, sumResult.Value.(promql.Vector)[0].F)
	verifyCacheStats(t, testCache, 2, 0, 2)

	// Query count_over_time on the same metric — should NOT hit sum_over_time's cache entries.
	countResult := runInstantQuery(t, mimirEngine, promStorage, "count_over_time(some_metric[5h])", ts)
	require.NoError(t, countResult.Err)
	require.Equal(t, expectedCountF, countResult.Value.(promql.Vector)[0].F)
	verifyCacheStats(t, testCache, 4, 0, 4) // 2 new gets (miss), 2 new sets

	// Query sum_over_time again — should hit cache from the first query.
	sumResult2 := runInstantQuery(t, mimirEngine, promStorage, "sum_over_time(some_metric[5h])", ts)
	require.NoError(t, sumResult2.Err)
	require.Equal(t, expectedSumF, sumResult2.Value.(promql.Vector)[0].F)
	verifyCacheStats(t, testCache, 6, 2, 4) // 2 hits for sum_over_time blocks

	// Query count_over_time again — should hit cache from the count query, not sum_over_time's.
	countResult2 := runInstantQuery(t, mimirEngine, promStorage, "count_over_time(some_metric[5h])", ts)
	require.NoError(t, countResult2.Err)
	require.Equal(t, expectedCountF, countResult2.Value.(promql.Vector)[0].F)
	verifyCacheStats(t, testCache, 8, 4, 4) // 2 hits for count_over_time blocks
}

func TestQuerySplitting_StorageError(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			some_metric{env="1"} 0+1x60
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	expr := "sum_over_time(some_metric[5h])"
	ts := baseT.Add(6 * time.Hour)
	errStorage := &errorStorage{Storage: promStorage}

	// Q1: error storage, empty cache. Query fails, nothing cached.
	result := runInstantQuery(t, mimirEngine, errStorage, expr, ts)
	require.Error(t, result.Err)
	// 2 gets (cached splits check cache during Prepare), 0 hits, 0 sets
	verifyCacheStats(t, testCache, 2, 0, 0)

	// Q2: real storage. Cache is empty, query succeeds and populates cache.
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.NoError(t, result.Err)
	expectedValue := result.Value
	// Cumulative: 4 gets, 0 hits, 2 sets
	verifyCacheStats(t, testCache, 4, 0, 2)

	// Q3: error storage again. Cached blocks hit, but uncached ranges fail.
	result = runInstantQuery(t, mimirEngine, errStorage, expr, ts)
	require.Error(t, result.Err)
	// Cumulative: 6 gets, 2 hits (cached blocks found), still 2 sets
	verifyCacheStats(t, testCache, 6, 2, 2)

	// Q4: real storage. Cached blocks still work correctly.
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.NoError(t, result.Err)
	require.Equal(t, expectedValue, result.Value)
	// Cumulative: 8 gets, 4 hits, still 2 sets
	verifyCacheStats(t, testCache, 8, 4, 2)
}

// TestQuerySplitting_MiddleCacheEntryEvicted verifies correct behavior when a middle cache entry
// is evicted between queries, producing interleaved cached and uncached splits.
//
// With a 7h range and 2h split interval at t=8h, the range (1h, 8h] splits into:
//   - Head:   (1h, 2h-1ms]
//   - Block1: (2h-1ms, 4h-1ms]  (cacheable)
//   - Block2: (4h-1ms, 6h-1ms]  (cacheable)
//   - Block3: (6h-1ms, 8h-1ms]  (cacheable)
//   - Tail:   (8h-1ms, 8h]
//
// After the first query populates all 3 blocks, we evict Block2. The second query should:
//   - Hit cache for Block1 and Block3
//   - Miss cache for Block2, re-fetching it from storage
func TestQuerySplitting_MiddleCacheEntryEvicted(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x100
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	ts := baseT.Add(8 * time.Hour)
	expr := "sum_over_time(test_metric[7h])"

	// First query populates cache for all 3 cacheable blocks.
	// Data: first sample @ 1h10m (idx 7), last sample @ 8h (idx 48), 42 samples.
	// Sum: (7+48)*42/2 = 1155
	result1, ranges1 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expectedScalarResult(ts, 1155, "env", "prod"), result1)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 8 * hourInMs},
	}, ranges1)
	verifyCacheStats(t, testCache, 3, 0, 3)

	// Evict Block2: (4h-1ms, 6h-1ms].
	block2Key := cache.TestGenerateHashedCacheKey("test-user", functions.FUNCTION_SUM_OVER_TIME, `{__name__="test_metric"}`, 4*hourInMs-1, 6*hourInMs-1, false)
	_, exists := testCache.items[block2Key]
	require.True(t, exists, "Block2 cache key should exist before eviction")
	delete(testCache.items, block2Key)

	// Second query: Block1 and Block3 are cache hits, Block2 is a miss.
	result2, ranges2 := executeQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expectedScalarResult(ts, 1155, "env", "prod"), result2)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 2*hourInMs - 1}, // Head
		{mint: 4 * hourInMs, maxt: 6*hourInMs - 1},   // Block2 (evicted)
		{mint: 8 * hourInMs, maxt: 8 * hourInMs},     // Tail
	}, ranges2)
	// 3 more gets, 2 hits (Block1 + Block3), 1 new set (Block2 re-cached)
	verifyCacheStats(t, testCache, 6, 2, 4)
}

func TestQuerySplitting_DelayedNameRemoval(t *testing.T) {
	sharedCache := newTestCacheBackend()
	cacheFactory := cache.NewCacheFactoryWithBackend(sharedCache, streamingpromql.NewStaticQueryLimitsProvider(), prometheus.NewPedanticRegistry(), log.NewNopLogger())

	engineDisabled := createSplittingEngine(t, prometheus.NewPedanticRegistry(), 2*time.Hour, false, true, cacheFactory)
	engineEnabled := createSplittingEngine(t, prometheus.NewPedanticRegistry(), 2*time.Hour, true, true, cacheFactory)

	promStorage := promqltest.LoadedStorage(t, `
		load 1h
			sum_metric{env="prod"} 10 20 30 40 50 60 70
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	ts := baseT.Add(6 * time.Hour)

	expr := `label_replace(sum_over_time(sum_metric[5h]), "original_name", "$1", "__name__", "(.+)")`

	// Disabled result: sum_over_time eagerly strips __name__, so label_replace's regex (.+) doesn't match the empty name.
	expectedDisabled := expectedScalarResult(ts, 250, "env", "prod")
	// Enabled result: __name__ survives through sum_over_time, so label_replace captures it into original_name.
	expectedEnabled := expectedScalarResult(ts, 250, "env", "prod", "original_name", "sum_metric")

	// Delayed name removal is part of the cache key, so enabled and disabled use separate cache entries.

	// Populate cache with delayed name removal disabled (2 splits → 2 misses, 2 sets).
	result := runInstantQuery(t, engineDisabled, promStorage, expr, ts)
	require.NoError(t, result.Err)
	require.Equal(t, expectedDisabled, result)
	verifyCacheStats(t, sharedCache, 2, 0, 2)

	// Query with delayed name removal enabled: different cache key, so 2 misses and 2 new sets.
	result = runInstantQuery(t, engineEnabled, promStorage, expr, ts)
	require.NoError(t, result.Err)
	require.Equal(t, expectedEnabled, result)
	verifyCacheStats(t, sharedCache, 4, 0, 4)

	// Repeat disabled: hits its own cache entries.
	result = runInstantQuery(t, engineDisabled, promStorage, expr, ts)
	require.NoError(t, result.Err)
	require.Equal(t, expectedDisabled, result)
	verifyCacheStats(t, sharedCache, 6, 2, 4)

	// Repeat enabled: hits its own cache entries.
	result = runInstantQuery(t, engineEnabled, promStorage, expr, ts)
	require.NoError(t, result.Err)
	require.Equal(t, expectedEnabled, result)
	verifyCacheStats(t, sharedCache, 8, 4, 4)
}

func TestQuerySplitting_AnnotationMetricName(t *testing.T) {
	backend, mimirEngine := setupEngineAndCache(t)

	promStorage := teststorage.New(t)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	ctx := context.Background()
	app := promStorage.Appender(ctx)
	baseT := timestamp.Time(0)

	// zzz_total: float samples from 0h to 10h every 10 minutes
	for i := 0; i <= 60; i++ {
		ts := timestamp.FromTime(baseT.Add(time.Duration(i) * 10 * time.Minute))
		_, err := app.Append(0, labels.FromStrings("__name__", "zzz_total", "env", "z"), ts, float64(i))
		require.NoError(t, err)
	}

	// aaa_total: float samples from 4h to 6h10m every 10 minutes (absent from head range)
	for i := 0; i <= 13; i++ {
		ts := timestamp.FromTime(baseT.Add(4*time.Hour + time.Duration(i)*10*time.Minute))
		_, err := app.Append(0, labels.FromStrings("__name__", "aaa_total", "env", "a"), ts, float64(i))
		require.NoError(t, err)
	}

	// aaa_total: histogram at 6h30m → creates mixed float+hist in the tail range (6h-1ms, 7h]
	_, err := app.AppendHistogram(0, labels.FromStrings("__name__", "aaa_total", "env", "a"),
		timestamp.FromTime(baseT.Add(6*time.Hour+30*time.Minute)), nil, &histogram.FloatHistogram{
			Schema: 0, Count: 10, Sum: 100, ZeroThreshold: 0.001, ZeroCount: 2,
			PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{3, 5},
		})
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// rate({__name__=~"aaa_total|zzz_total"}[5h]) at t=7h
	// Splits: head (2h, 4h-1ms], block (4h-1ms, 6h-1ms]*, tail (6h-1ms, 7h]
	expr := `rate({__name__=~"aaa_total|zzz_total"}[5h])`
	ts := baseT.Add(7 * time.Hour)

	// Q1: populates cache for block (4h-1ms, 6h-1ms] with [aaa, zzz] in storage order
	expectedWarning := `PromQL warning: encountered a mix of histograms and floats for metric name "aaa_total"`

	// Q1: populates cache, no ordering mismatch yet
	result1 := runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.NoError(t, result1.Err)
	require.Len(t, result1.Warnings.AsErrors(), 1)
	require.EqualError(t, result1.Warnings.AsErrors()[0], expectedWarning)
	verifyCacheStats(t, backend, 1, 0, 1)

	// Q2: cache hit for the block
	// - Head (2h, 4h-1ms]: only zzz (aaa starts at 4h) → merged starts as [zzz]
	// - Cached (4h-1ms, 6h-1ms]: [aaa, zzz] → aaa is new → merged = [zzz, aaa]
	// - Tail (6h-1ms, 7h]: storage returns [aaa, zzz] → differs from merged order
	//   When processing merged 0 (zzz), first local 0 (aaa) is processed.
	result2 := runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.NoError(t, result2.Err)
	require.Len(t, result2.Warnings.AsErrors(), 1)
	require.EqualError(t, result2.Warnings.AsErrors()[0], expectedWarning)
	verifyCacheStats(t, backend, 2, 1, 1)
}

// TestQuerySplitting_NoMatchingSeries_CachesEmptyResult verifies that when a query matches 0 series,
// the empty result is still cached and subsequent queries hit the cache.
func TestQuerySplitting_NoMatchingSeries_CachesEmptyResult(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			some_metric{env="1"} 0+1x60
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	expr := "sum_over_time(nonexistent_metric[5h])"
	ts := baseT.Add(6 * time.Hour)

	// First query: 0 series match. Should still cache the empty result.
	result := runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.NoError(t, result.Err)
	require.Nil(t, result.Value)
	// 2 gets (cacheable blocks), 0 hits, 2 sets (empty results cached)
	verifyCacheStats(t, testCache, 2, 0, 2)

	// Second query: cache hits for both blocks.
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.NoError(t, result.Err)
	require.Nil(t, result.Value)
	// Cumulative: 4 gets, 2 hits, still 2 sets
	verifyCacheStats(t, testCache, 4, 2, 2)
}

func TestQuerySplitting_NoMetadataConsumption_DoesNotCache(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)
	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x100
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)

	// nonexistent_metric returns no series, so the binary op short-circuits.
	// This means Finalize() is called on the sum_over_time split operator without SeriesMetadata() being called first.
	expr := "nonexistent_metric + sum_over_time(test_metric[24h])"
	ts := baseT.Add(25 * time.Hour)

	result := runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.NoError(t, result.Err)
	require.Empty(t, result.Value)
	// No cache operations should occur since SeriesMetadata() was never called on the split operator.
	verifyCacheStats(t, testCache, 11, 0, 0)
}

func TestQuerySplitting_PartialConsumption_DoesNotCache(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			some_metric{env="1"} 0+1x60
			some_metric{env="2"} 0+2x60
			filter_metric{env="1"} 1+0x60
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	ts := baseT.Add(6 * time.Hour)

	// `and on(env)` matches only env=1. The split operator (left) returns 2 series from
	// SeriesMetadata, but `and` only consumes the first (env=1) via NextSeries before
	// finalizing the left side. The second series (env=2) is never consumed.
	expr := `sum_over_time(some_metric[5h]) and on(env) filter_metric`

	result := runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.NoError(t, result.Err)
	// The query returns a result for the matched series (env=1).
	require.Equal(t, expectedScalarResult(ts, 645, "env", "1"), result)
	// The split operator's series were not fully consumed, so results should not be cached.
	verifyCacheStats(t, testCache, 2, 0, 0)
}

func createSplittingEngineWithCache(t *testing.T, registry *prometheus.Registry, splitInterval time.Duration, enableDelayedNameRemoval bool, enableEliminateDeduplicateAndMerge bool) (promql.QueryEngine, *testCacheBackend) {
	t.Helper()

	cacheBackend := newTestCacheBackend()
	cacheFactory := cache.NewCacheFactoryWithBackend(cacheBackend, streamingpromql.NewStaticQueryLimitsProvider(), registry, log.NewNopLogger())

	engine := createSplittingEngine(t, registry, splitInterval, enableDelayedNameRemoval, enableEliminateDeduplicateAndMerge, cacheFactory)
	return engine, cacheBackend
}

func createSplittingEngine(t *testing.T, registry *prometheus.Registry, splitInterval time.Duration, enableDelayedNameRemoval bool, enableEliminateDeduplicateAndMerge bool, cacheFactory *cache.CacheFactory) promql.QueryEngine {
	t.Helper()

	opts := streamingpromql.NewTestEngineOpts()
	limits := streamingpromql.NewStaticQueryLimitsProvider()
	limits.EnableDelayedNameRemoval = enableDelayedNameRemoval
	opts.Limits = limits
	opts.RangeVectorSplitting.Enabled = true
	opts.RangeVectorSplitting.SplitInterval = splitInterval
	opts.CommonOpts.Reg = registry
	opts.CommonOpts.EnableDelayedNameRemoval = enableDelayedNameRemoval
	if !enableEliminateDeduplicateAndMerge {
		opts.EnableEliminateDeduplicateAndMerge = false
	}

	planner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	engine, err := streamingpromql.NewEngineWithCache(opts, stats.NewQueryMetrics(registry), planner, cacheFactory)
	require.NoError(t, err)

	return engine
}

func setupEngineAndCache(t *testing.T) (*testCacheBackend, promql.QueryEngine) {
	backend := newTestCacheBackend()
	irCache := cache.NewCacheFactoryWithBackend(backend, streamingpromql.NewStaticQueryLimitsProvider(), prometheus.NewRegistry(), log.NewNopLogger())

	opts := streamingpromql.NewTestEngineOpts()
	opts.RangeVectorSplitting.Enabled = true
	opts.RangeVectorSplitting.SplitInterval = 2 * time.Hour

	queryPlanner, err := streamingpromql.NewQueryPlanner(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	mimirEngine, err := streamingpromql.NewEngineWithCache(opts, stats.NewQueryMetrics(nil), queryPlanner, irCache)
	require.NoError(t, err)

	return backend, mimirEngine
}

func runInstantQuery(t *testing.T, eng promql.QueryEngine, storage storage.Storage, expr string, ts time.Time) *promql.Result {
	ctx := user.InjectOrgID(context.Background(), "test-user")
	q, err := eng.NewInstantQuery(ctx, storage, nil, expr, ts)
	require.NoError(t, err)
	defer q.Close()

	return q.Exec(ctx)
}

func executeQuery(t *testing.T, engine promql.QueryEngine, storage storage.Storage, expr string, ts time.Time) (*promql.Result, []storageQueryRange) {
	wrapped := trackRanges(storage)
	ctx := user.InjectOrgID(context.Background(), "test-user")
	q, err := engine.NewInstantQuery(ctx, wrapped, nil, expr, ts)
	require.NoError(t, err)
	result := q.Exec(ctx)
	q.Close()
	return result, wrapped.ranges
}

func expectedScalarResult(ts time.Time, f float64, lbls ...string) *promql.Result {
	return &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings(lbls...),
				T:      timestamp.FromTime(ts),
				F:      f,
			},
		},
	}
}

func verifyCacheStats(t *testing.T, backend *testCacheBackend, expectedGets, expectedHits, expectedSets int) {
	require.Equal(t, expectedGets, backend.gets, "Expected %d cache gets, got %d", expectedGets, backend.gets)
	require.Equal(t, expectedHits, backend.hits, "Expected %d cache hits, got %d", expectedHits, backend.hits)
	require.Equal(t, expectedSets, backend.sets, "Expected %d cache sets, got %d", expectedSets, backend.sets)
}

type storageQueryRange struct {
	mint, maxt int64
}

const (
	hourInMs   = int64(time.Hour / time.Millisecond)
	minuteInMs = int64(time.Minute / time.Millisecond)
)

func trackRanges(promStorage storage.Storage) *rangeTrackingQueryable {
	return &rangeTrackingQueryable{
		inner: promStorage,
	}
}

type testCacheBackend struct {
	items map[string][]byte

	gets int
	hits int
	sets int
}

func newTestCacheBackend() *testCacheBackend {
	return &testCacheBackend{
		items: make(map[string][]byte),
	}
}

func (c *testCacheBackend) GetMulti(_ context.Context, keys []string, _ ...dskitcache.Option) map[string][]byte {
	c.gets++

	result := make(map[string][]byte)
	for _, key := range keys {
		if data, ok := c.items[key]; ok && len(data) > 0 {
			// Clone bytes to simulate network serialization
			result[key] = slices.Clone(data)
			c.hits++
		}
	}

	return result
}

func (c *testCacheBackend) SetMultiAsync(data map[string][]byte, _ time.Duration) {
	c.sets++
	for key, value := range data {
		c.items[key] = value
	}
}

func (c *testCacheBackend) Reset() {
	c.items = make(map[string][]byte)
	c.hits = 0
	c.sets = 0
	c.gets = 0
}

type rangeTrackingQueryable struct {
	inner  storage.Queryable
	ranges []storageQueryRange
}

func (w *rangeTrackingQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	w.ranges = append(w.ranges, storageQueryRange{mint, maxt})
	return w.inner.Querier(mint, maxt)
}

type errorStorage struct {
	storage.Storage
}

func (e *errorStorage) Querier(_, _ int64) (storage.Querier, error) {
	return nil, fmt.Errorf("injected storage error")
}
