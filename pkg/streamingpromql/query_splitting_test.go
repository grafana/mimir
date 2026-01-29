// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	dskitcache "github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/prometheus/prometheus/util/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/testutils"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

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

	// Run query twice, no cache actions expected
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
//   - Example: PromQL range (1h, 2h-1ms] becomes storage [1h+1ms, 2h-1ms]
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

func TestQuerySplitting_VerifyStorageQueries(t *testing.T) {
	_, mimirEngine := setupEngineAndCache(t)
	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x60
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	expr := "sum_over_time(test_metric[5h])"
	ctx := user.InjectOrgID(context.Background(), "test-user")

	//  Query 1 at 6h: all uncached, merges into single storage query
	wrapped1, ranges1 := trackRanges(promStorage)
	q1, err := mimirEngine.NewInstantQuery(ctx, wrapped1, nil, expr, baseT.Add(6*time.Hour))
	require.NoError(t, err)
	result1 := q1.Exec(ctx)
	require.NoError(t, result1.Err)
	q1.Close()
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 6 * hourInMs},
	}, *ranges1)

	// Query 2 at 8h: (4h-6h] cached, queries (3h-4h] and (6h-8h]
	// Uncached ranges are separate because there's a cached block in between
	wrapped2, ranges2 := trackRanges(promStorage)
	q2, err := mimirEngine.NewInstantQuery(ctx, wrapped2, nil, expr, baseT.Add(8*time.Hour))
	require.NoError(t, err)
	result2 := q2.Exec(ctx)
	require.NoError(t, result2.Err)
	q2.Close()
	require.Equal(t, []storageQueryRange{
		{mint: 3*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (3h, 4h-1ms] -> storage [3h+1, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 8 * hourInMs},     // Tail: (6h-1ms, 8h] -> storage [6h, 8h]
	}, *ranges2)
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

	// Create planner to capture plan structure
	opts := NewTestEngineOpts()
	opts.RangeVectorSplitting.Enabled = true
	opts.RangeVectorSplitting.SplitInterval = 2 * time.Hour
	require.True(t, opts.EnableCommonSubexpressionElimination, "CSE should be enabled")

	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	plan, err := planner.NewQueryPlan(ctx, expr, types.NewInstantQueryTimeRange(ts), &NoopPlanningObserver{})
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

	// Execute the query end-to-end with storage query tracking
	wrappedStorage, ranges := trackRanges(promStorage)

	q, err := mimirEngine.NewInstantQuery(ctx, wrappedStorage, nil, expr, ts)
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
	}, *ranges)

	// Query 2 at 8h: middle (4h-6h) cached from query 1, head (3h-4h) and tail (6h-8h) uncached
	// With CSE, both sum_over_time and count_over_time share the same MatrixSelector data
	*ranges = nil
	ts2 := baseT.Add(8 * time.Hour)
	q2, err := mimirEngine.NewInstantQuery(ctx, wrappedStorage, nil, expr, ts2)
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
	}, *ranges)
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

	result1, ranges1 := executeQuery(t, mimirEngine, promStorage, expr, baseT.Add(8*time.Hour))
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 7 * hourInMs}, // PromQL range (2h, 7h] converts to storage [2h+1, 7h]
	}, ranges1)
	require.Equal(t, 825.0, result1.Value.(promql.Vector)[0].F)
	verifyCacheStats(t, testCache, 1, 0, 1) // 1 cacheable block

	// Q1b: no offset at 7h accesses same range (2h, 7h]
	// Cache has block (4h-1ms, 6h-1ms] from Q1, so queries head and tail from storage
	exprNoOffset := "sum_over_time(test_metric[5h])"
	result1b, ranges1b := executeQuery(t, mimirEngine, promStorage, exprNoOffset, baseT.Add(7*time.Hour))
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (2h, 4h-1ms] -> storage [2h+1, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 7 * hourInMs},     // Tail: (6h-1ms, 7h] -> storage [6h, 7h]
	}, ranges1b)
	require.Equal(t, result1b.Value, result1.Value)
	verifyCacheStats(t, testCache, 2, 1, 1) // 1 cache hit on the block

	// Q2: offset 1h at 10h accesses (4h, 9h]
	// Splits: head (4h, 6h-1ms], block (6h-1ms, 8h-1ms] (cache miss, new), tail (8h-1ms, 9h]
	// All uncached ranges merge into single storage query
	// Data: first sample @ 4h10m = 25, last sample @ 9h = 54, samples = 30
	// Sum: (25+54)*(30/2) = 1185
	result2, ranges2 := executeQuery(t, mimirEngine, promStorage, expr, baseT.Add(10*time.Hour))
	require.Equal(t, []storageQueryRange{
		{mint: 4*hourInMs + 1, maxt: 9 * hourInMs}, // Merged: (4h, 9h] -> storage [4h+1, 9h]
	}, ranges2)
	require.Equal(t, 1185.0, result2.Value.(promql.Vector)[0].F)
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
	result1, ranges1 := executeQuery(t, mimirEngine, promStorage, expr, baseT.Add(8*time.Hour))
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 7 * hourInMs},
	}, ranges1)
	require.Equal(t, 825.0, result1.Value.(promql.Vector)[0].F)
	verifyCacheStats(t, testCache, 1, 0, 1) // 1 cacheable block

	// Q2: no @ at 7h accesses same range (2h, 7h]
	// Cache has block (4h-1ms, 6h-1ms] from Q1, so queries head and tail from storage
	exprNoModifier := "sum_over_time(test_metric[5h])"
	result2, ranges2 := executeQuery(t, mimirEngine, promStorage, exprNoModifier, baseT.Add(7*time.Hour))
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (2h, 4h-1ms] -> storage [2h+1, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 7 * hourInMs},     // Tail: (6h-1ms, 7h] -> storage [6h, 7h]
	}, ranges2)
	require.Equal(t, 825.0, result2.Value.(promql.Vector)[0].F)
	require.Equal(t, result1.Value.(promql.Vector)[0].F, result2.Value.(promql.Vector)[0].F)
	verifyCacheStats(t, testCache, 2, 1, 1) // 1 cache hit on the block

	// Q3: @ 7h at 10h accesses same range (2h, 7h]
	// Cache has block (4h-1ms, 6h-1ms] from Q1, so queries head and tail from storage
	result3, ranges3 := executeQuery(t, mimirEngine, promStorage, expr, baseT.Add(10*time.Hour))
	require.Equal(t, []storageQueryRange{
		{mint: 2*hourInMs + 1, maxt: 4*hourInMs - 1}, // Head: (2h, 4h-1ms] -> storage [2h+1, 4h-1ms]
		{mint: 6 * hourInMs, maxt: 7 * hourInMs},     // Tail: (6h-1ms, 7h] -> storage [6h, 7h]
	}, ranges3)
	require.Equal(t, 825.0, result3.Value.(promql.Vector)[0].F)
	require.Equal(t, timestamp.FromTime(baseT.Add(10*time.Hour)), result3.Value.(promql.Vector)[0].T) // @ modifier fixes result timestamp
	verifyCacheStats(t, testCache, 3, 2, 1)                                                           // Q1: 1 get/1 set, Q2: 1 get/1 hit, Q3: 1 get/1 hit
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
	require.Equal(t, 387.0, result.Value.(promql.Vector)[0].F)
	require.Equal(t, timestamp.FromTime(ts), result.Value.(promql.Vector)[0].T)

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
	require.Equal(t, 261.0, result.Value.(promql.Vector)[0].F)
	require.Equal(t, timestamp.FromTime(ts), result.Value.(promql.Vector)[0].T)

	// Since query splitting is not applied, should be a single storage query
	require.Equal(t, []storageQueryRange{
		{mint: 59*minuteInMs + 1, maxt: 3*hourInMs + 59*minuteInMs},
	}, ranges)

	// No cache operations should occur since splitting wasn't applied
	verifyCacheStats(t, testCache, 0, 0, 0)
}

func TestQuerySplitting_TestFiles(t *testing.T) {
	testdataFS := os.DirFS("./testdata")

	oursTests, err := fs.Glob(testdataFS, "ours/*.test")
	require.NoError(t, err)

	oursOnlyTests, err := fs.Glob(testdataFS, "ours-only/*.test")
	require.NoError(t, err)

	upstreamTests, err := fs.Glob(testdataFS, "upstream/*.test")
	require.NoError(t, err)

	allTests := append(oursTests, oursOnlyTests...)
	allTests = append(allTests, upstreamTests...)
	require.NotEmpty(t, allTests, "expected to find test files")

	splitIntervals := []time.Duration{
		1 * time.Second,
		10 * time.Second,
		30 * time.Second,
		5 * time.Minute,
		2 * time.Hour,
	}

	for _, splitInterval := range splitIntervals {
		splitInterval := splitInterval
		t.Run(fmt.Sprintf("split_interval_%v", splitInterval), func(t *testing.T) {
			totalQueries := 0
			queriesWithSplit := 0

			registry := prometheus.NewRegistry()
			engine, cacheBackend := createSplittingEngineWithCache(t, registry, splitInterval, false)

			registryDelayed := prometheus.NewRegistry()
			engineDelayed, cacheBackendDelayed := createSplittingEngineWithCache(t, registryDelayed, splitInterval, true)

			for _, testFile := range allTests {
				t.Run(testFile, func(t *testing.T) {
					enableDelayedNameRemoval := strings.Contains(testFile, "name_label_dropping")

					selectedEngine := engine
					selectedCache := cacheBackend
					if enableDelayedNameRemoval {
						selectedEngine = engineDelayed
						selectedCache = cacheBackendDelayed
					}

					total, withSplit := runTestFileWithSplitting(t, testdataFS, testFile, selectedEngine, selectedCache)
					totalQueries += total
					queriesWithSplit += withSplit
				})
			}

			t.Logf("Total queries executed: %d", totalQueries)
			t.Logf("Queries with splitting applied: %d", queriesWithSplit)
		})
	}
}

// skipUnsupportedTests comments out test cases where the split implementation diverges from the Prometheus/non-split MQE implementations.
func skipUnsupportedTests(t *testing.T, testContent string, testFile string) string {
	var testCasesToSkip []string

	switch testFile {
	case "upstream/native_histograms.test":
		// The split sum_over_time sometimes cannot detect conflicting counter reset warnings.
		// See comments for functions.SplitSumOverTime.
		testCasesToSkip = []string{
			`eval instant at 14m histogram_count(sum_over_time(mixed[10m]))
  expect warn msg:PromQL warning: conflicting counter resets during histogram aggregation
  expect no_info
  {} 93`,

			`eval instant at 11m histogram_count(sum_over_time(mixed[2m]))
  expect warn msg:PromQL warning: conflicting counter resets during histogram aggregation
  expect no_info
  {} 21`,

			`eval instant at 5m histogram_count(sum_over_time(reset{timing="late"}[5m]))
    expect warn msg: PromQL warning: conflicting counter resets during histogram aggregation
    {timing="late"} 7`,
		}

	default:
		return testContent
	}

	modified := testContent
	for i, testCase := range testCasesToSkip {
		if !strings.Contains(modified, testCase) {
			require.FailNow(t, "Failed to find expected test case in "+testFile,
				"Could not find test case at index %d. The test file may have changed.\nLooking for:\n%s", i, testCase)
		}

		lines := strings.Split(testCase, "\n")
		for j, line := range lines {
			lines[j] = "# SKIPPED FOR QUERY SPLITTING: " + line
		}
		commented := strings.Join(lines, "\n")

		modified = strings.Replace(modified, testCase, commented, 1)
	}

	return modified
}

func runTestFileWithSplitting(t *testing.T, testdataFS fs.FS, testFile string, innerEngine promql.QueryEngine, cacheBackend *testCacheBackend) (totalQueries, queriesWithSplit int) {
	t.Helper()

	engine := &testSplittingEngine{
		engine: innerEngine,
		orgID:  "test-user",
		onQueryExec: func(splitQueriesCount uint32) {
			totalQueries++
			if splitQueriesCount > 0 {
				queriesWithSplit++
			}
		},
	}

	newStorage := func(t testutil.T) storage.Storage {
		base := promqltest.LoadedStorage(t, "")
		return &storageWithCloseCallback{
			Storage: base,
			onClose: cacheBackend.Reset,
		}
	}

	f, err := testdataFS.Open(testFile)
	require.NoError(t, err)
	defer f.Close()

	b, err := io.ReadAll(f)
	require.NoError(t, err)

	testScript := skipUnsupportedTests(t, string(b), testFile)

	promqltest.RunTestWithStorage(t, testScript, engine, newStorage)

	return totalQueries, queriesWithSplit
}

func createSplittingEngineWithCache(t *testing.T, registry *prometheus.Registry, splitInterval time.Duration, enableDelayedNameRemoval bool) (promql.QueryEngine, *testCacheBackend) {
	t.Helper()

	opts := NewTestEngineOpts()
	opts.RangeVectorSplitting.Enabled = true
	opts.RangeVectorSplitting.SplitInterval = splitInterval
	opts.CommonOpts.Reg = registry
	opts.CommonOpts.EnableDelayedNameRemoval = enableDelayedNameRemoval
	if enableDelayedNameRemoval {
		// Disable the optimization pass, since it requires delayed name removal to be enabled.
		opts.EnableEliminateDeduplicateAndMerge = false
	}

	planner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	cacheBackend := newTestCacheBackend()
	cacheFactory := cache.NewResultsCacheWithBackend(cacheBackend, registry, log.NewNopLogger())

	engine, err := newEngineWithCache(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(registry), planner, cacheFactory)
	require.NoError(t, err)

	return engine, cacheBackend
}

func setupEngineAndCache(t *testing.T) (*testCacheBackend, promql.QueryEngine) {
	backend := newTestCacheBackend()
	irCache := cache.NewResultsCacheWithBackend(backend, prometheus.NewRegistry(), log.NewNopLogger())

	opts := NewTestEngineOpts()
	opts.RangeVectorSplitting.Enabled = true
	opts.RangeVectorSplitting.SplitInterval = 2 * time.Hour

	queryPlanner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	mimirEngine, err := newEngineWithCache(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), queryPlanner, irCache)
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
	wrapped, ranges := trackRanges(storage)
	ctx := user.InjectOrgID(context.Background(), "test-user")
	q, err := engine.NewInstantQuery(ctx, wrapped, nil, expr, ts)
	require.NoError(t, err)
	result := q.Exec(ctx)
	q.Close()
	return result, *ranges
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

func trackRanges(promStorage storage.Storage) (*wrappedQueryable, *[]storageQueryRange) {
	ranges := &[]storageQueryRange{}
	return &wrappedQueryable{
		inner: promStorage,
		onSelect: func(mint, maxt int64) {
			*ranges = append(*ranges, storageQueryRange{mint, maxt})
		},
	}, ranges
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

type wrappedQueryable struct {
	inner    storage.Queryable
	onSelect func(mint, maxt int64)
}

func (w *wrappedQueryable) Querier(mint, maxt int64) (storage.Querier, error) {
	if w.onSelect != nil {
		w.onSelect(mint, maxt)
	}
	return w.inner.Querier(mint, maxt)
}

type storageWithCloseCallback struct {
	storage.Storage
	onClose func()
}

func (s *storageWithCloseCallback) Close() error {
	if s.onClose != nil {
		s.onClose()
	}
	return s.Storage.Close()
}

type testSplittingEngine struct {
	engine      promql.QueryEngine
	orgID       string
	onQueryExec func(splitQueriesCount uint32)
}

func (e *testSplittingEngine) NewInstantQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, ts time.Time) (promql.Query, error) {
	ctx = user.InjectOrgID(ctx, e.orgID)

	query, err := e.engine.NewInstantQuery(ctx, q, opts, qs, ts)
	if err != nil {
		return nil, err
	}

	return &testSplittingQuery{Query: query, orgID: e.orgID, onExec: e.onQueryExec}, nil
}

func (e *testSplittingEngine) NewRangeQuery(ctx context.Context, q storage.Queryable, opts promql.QueryOpts, qs string, start, end time.Time, interval time.Duration) (promql.Query, error) {
	ctx = user.InjectOrgID(ctx, e.orgID)

	query, err := e.engine.NewRangeQuery(ctx, q, opts, qs, start, end, interval)
	if err != nil {
		return nil, err
	}

	return &testSplittingQuery{Query: query, orgID: e.orgID, onExec: e.onQueryExec}, nil
}

type testSplittingQuery struct {
	promql.Query
	orgID  string
	onExec func(splitQueriesCount uint32)
}

func (q *testSplittingQuery) Exec(ctx context.Context) *promql.Result {
	ctx = user.InjectOrgID(ctx, q.orgID)

	queryStats, ctx := stats.ContextWithEmptyStats(ctx)

	result := q.Query.Exec(ctx)

	if q.onExec != nil {
		q.onExec(queryStats.LoadSplitRangeVectors())
	}

	return result
}

func TestQuerySplitting_WithOOOWindow(t *testing.T) {
	backend := newTestCacheBackend()
	irCache := cache.NewResultsCacheWithBackend(backend, prometheus.NewRegistry(), log.NewNopLogger())

	opts := NewTestEngineOpts()
	opts.RangeVectorSplitting.Enabled = true
	opts.RangeVectorSplitting.SplitInterval = 2 * time.Hour
	opts.Limits = &mockOutOfOrderTimeWindowProvider{
		oooWindow: 3 * time.Hour,
	}

	baseT := timestamp.Time(0)
	fixedNow := baseT.Add(12 * time.Hour)

	queryPlanner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	for _, pass := range queryPlanner.planOptimizationPasses {
		if qsPass, ok := pass.(*rangevectorsplitting.OptimizationPass); ok {
			qsPass.TestOnlySetTimeNow(func() time.Time { return fixedNow })
			break
		}
	}

	mimirEngine, err := newEngineWithCache(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), queryPlanner, irCache)
	require.NoError(t, err)
	// Query at 12h with 7h range: (5h, 12h]
	// Expected splits:
	// - Head: (5h, 6h-1ms]
	// - Block: (6h-1ms, 8h-1ms] - cacheable (before OOO)
	// - Tail: (8h-1ms, 12h] - non-cacheable (in OOO window)

	oooWindowMs := int64(2 * time.Hour / time.Millisecond)
	storage := teststorage.New(t, oooWindowMs)
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
	result1, ranges1 := executeQuery(t, mimirEngine, storage, expr, ts)
	require.NoError(t, result1.Err)
	require.Len(t, result1.Value.(promql.Vector), 1)
	// Expected: sum of values from 5h (exclusive) to 10h (inclusive)
	// Samples at 5h10m (31) to 12h (72) = 42 samples
	// Sum = 31+32+...+60 = (31+72)*42/2 = 2163
	require.Equal(t, 2163.0, result1.Value.(promql.Vector)[0].F)

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
	require.NoError(t, result2.Err)
	require.Len(t, result2.Value.(promql.Vector), 1)
	require.Equal(t, 2363.0, result2.Value.(promql.Vector)[0].F)

	verifyCacheStats(t, backend, 2, 1, 1)
	require.Equal(t, []storageQueryRange{
		{mint: 5*hourInMs + 1, maxt: 6*hourInMs - 1},
		{mint: 8 * hourInMs, maxt: 12 * hourInMs},
	}, ranges2)

	result3, ranges3 := executeQuery(t, mimirEngine, storage, expr, ts)
	require.NoError(t, result3.Err)
	require.Equal(t, result2, result3)

	verifyCacheStats(t, backend, 3, 2, 1)
	require.Equal(t, ranges2, ranges3)
}

type mockOutOfOrderTimeWindowProvider struct {
	oooWindow time.Duration
}

func (m *mockOutOfOrderTimeWindowProvider) OutOfOrderTimeWindow(userID string) time.Duration {
	return m.oooWindow
}
