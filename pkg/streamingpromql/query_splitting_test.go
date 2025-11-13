// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/prometheus/prometheus/storage"
	"github.com/stretchr/testify/require"

	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/cache"
	"github.com/grafana/mimir/pkg/util/limiter"
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
	result := runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)

	verifyCacheStats(t, testCache, 2, 0, 2)

	// Run same query again (should hit cache)
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)

	verifyCacheStats(t, testCache, 4, 2, 2)

	// Run query at 6h10m
	ts = baseT.Add(6*time.Hour + 10*time.Minute)
	// Splits:
	// - Head: (1h10m, 2h]
	// - Cached: (2h-4h], (4h-6h] (cache hits)
	// - Tail: (6h, 6h10m]
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
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)

	verifyCacheStats(t, testCache, 6, 4, 2)

	// Run query at 7h
	ts = baseT.Add(7 * time.Hour)
	// Splits:
	// - Head: (2h, 2h] -> empty
	// - Cached: (2h-4h], (4h-6h] (cache hits)
	// - Tail: (6h, 7h]
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
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)

	verifyCacheStats(t, testCache, 8, 6, 2)

	// Run query at 8h20m
	ts = baseT.Add(8*time.Hour + 20*time.Minute)
	// Splits:
	// - Head: (3h20m, 4h]
	// - Cached: (4h-6h] (hit), (6h-8h] (miss)
	// - Tail: (8h, 8h20m]
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
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)

	verifyCacheStats(t, testCache, 10, 7, 3)
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

	result := runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	verifyCacheStats(t, testCache, 2, 0, 2)

	// Run same query again (should hit cache)
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	verifyCacheStats(t, testCache, 4, 2, 2)

	// Run query at 7h
	ts = baseT.Add(7 * time.Hour)
	// Splits:
	// - Head: (2h, 2h] -> empty
	// - Cached: (2h-4h], (4h-6h] (cache hits)
	// - Tail: (6h, 7h]
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
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	verifyCacheStats(t, testCache, 6, 4, 2)

	// Run query at 8h20m
	ts = baseT.Add(8*time.Hour + 20*time.Minute)
	// Splits:
	// - Head: (3h20m, 4h]
	// - Cached: (4h-6h] (hit), (6h-8h] (miss)
	// - Tail: (8h, 8h20m]
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
	result = runInstantQuery(t, mimirEngine, promStorage, expr, ts)
	require.Equal(t, expected, result)
	verifyCacheStats(t, testCache, 8, 5, 3)
}

func setupEngineAndCache(t *testing.T) (*testIntermediateResultsCache, promql.QueryEngine) {
	testCache := newTestIntermediateResultsCache(t)

	opts := NewTestEngineOpts()
	opts.InstantQuerySplitting.Enabled = true
	opts.InstantQuerySplitting.SplitInterval = 2 * time.Hour

	queryPlanner, err := NewQueryPlanner(opts, NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(t, err)

	mimirEngine, err := newEngineWithCache(opts, NewStaticQueryLimitsProvider(0), stats.NewQueryMetrics(nil), queryPlanner, testCache)
	require.NoError(t, err)

	t.Cleanup(func() {
		testCache.Close()
	})

	return testCache, mimirEngine
}

func runInstantQuery(t *testing.T, eng promql.QueryEngine, storage storage.Storage, expr string, ts time.Time) *promql.Result {
	ctx := user.InjectOrgID(context.Background(), "test-user")
	q, err := eng.NewInstantQuery(ctx, storage, nil, expr, ts)
	require.NoError(t, err)
	defer q.Close()

	return q.Exec(ctx)
}

func verifyCacheStats(t *testing.T, cache *testIntermediateResultsCache, expectedGets, expectedHits, expectedSets int) {
	require.Equal(t, expectedGets, cache.gets, "Expected %d cache gets, got %d", expectedGets, cache.gets)
	require.Equal(t, expectedHits, cache.hits, "Expected %d cache hits, got %d", expectedHits, cache.hits)
	require.Equal(t, expectedSets, cache.sets, "Expected %d cache sets, got %d", expectedSets, cache.sets)
}

type testIntermediateResultsCache struct {
	data map[string]cache.CachedSeries // Store proto like real cache
	gets int
	hits int
	sets int
	t    *testing.T
}

// newTestIntermediateResultsCache creates a new test cache instance.
func newTestIntermediateResultsCache(t *testing.T) *testIntermediateResultsCache {
	return &testIntermediateResultsCache{
		data: make(map[string]cache.CachedSeries),
		t:    t,
	}
}

// ResetStats resets all cache statistics counters.
func (c *testIntermediateResultsCache) ResetStats() {
	c.gets = 0
	c.hits = 0
	c.sets = 0
}

// Stats returns the current cache statistics.
func (c *testIntermediateResultsCache) Stats() (gets, hits, sets int) {
	return c.gets, c.hits, c.sets
}

func (c *testIntermediateResultsCache) Get(user, function, selector string, start int64, end int64, memoryTracker *limiter.MemoryConsumptionTracker) (cache.IntermediateResultBlock, bool) {
	key := fmt.Sprintf("%s:%s:%s:%d:%d", user, function, selector, start, end)
	c.gets++
	cached, ok := c.data[key]
	if !ok {
		return cache.IntermediateResultBlock{}, false
	}

	c.hits++
	// Convert proto back to block with tracking (like real cache does)
	block, err := cache.CachedSeriesToBlock(cached, memoryTracker)
	if err != nil {
		return cache.IntermediateResultBlock{}, false
	}

	return block, true
}

func (c *testIntermediateResultsCache) Set(user, function, selector string, start int64, end int64, block cache.IntermediateResultBlock) error {
	key := fmt.Sprintf("%s:%s:%s:%d:%d", user, function, selector, start, end)
	c.sets++

	// Convert to proto format (like real cache does).
	// This ensures the cache doesn't hold references to the caller's slices.
	cached, err := cache.BlockToCachedSeries(key, block)
	if err != nil {
		return err
	}

	c.data[key] = cached
	return nil
}

func (c *testIntermediateResultsCache) Close() {
	c.data = nil
}
