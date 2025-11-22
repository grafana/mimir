// SPDX-License-Identifier: AGPL-3.0-only

package streamingpromql

import (
	"github.com/prometheus/prometheus/util/teststorage"
	"testing"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/promqltest"
	"github.com/stretchr/testify/require"
)

func TestQuerySplitting_CountOverTime_UsesCache(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 10m
			test_metric{env="prod"} 0+1x60
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	expr := "count_over_time(test_metric[5h])"

	expected := &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "prod"),
				T:      timestamp.FromTime(ts6h),
				F:      30, // 30 samples in 5h range
			},
		},
	}

	verify6HQuery(t, mimirEngine, promStorage, expr, expected, testCache)
}

func TestQuerySplitting_MinOverTime(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 1h
			test_metric{env="prod"} 1 3 5 124372145 -5 7 7
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	expr := "min_over_time(test_metric[5h])"

	expected := &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "prod"),
				T:      timestamp.FromTime(ts6h),
				F:      -5,
			},
		},
	}

	verify6HQuery(t, mimirEngine, promStorage, expr, expected, testCache)
}

func TestQuerySplitting_MaxOverTime(t *testing.T) {
	testCache, mimirEngine := setupEngineAndCache(t)

	promStorage := promqltest.LoadedStorage(t, `
		load 1h
			test_metric{env="prod"} 1 3 5 124372145 -5 7 7
	`)
	t.Cleanup(func() { require.NoError(t, promStorage.Close()) })

	baseT := timestamp.Time(0)
	expr := "max_over_time(test_metric[5h])"
	ts := baseT.Add(6 * time.Hour)

	expected := &promql.Result{
		Value: promql.Vector{
			{
				Metric: labels.FromStrings("env", "prod"),
				T:      timestamp.FromTime(ts),
				F:      124372145,
			},
		},
	}

	verify6HQuery(t, mimirEngine, promStorage, expr, expected, testCache)
}

var ts6h = timestamp.Time(0).Add(6 * time.Hour)

func verify6HQuery(t *testing.T, mimirEngine promql.QueryEngine, promStorage *teststorage.TestStorage, expr string, expected *promql.Result, testCache *testCacheBackend) {
	// Run query first time (should populate cache)
	result, ranges1 := executeQuery(t, mimirEngine, promStorage, expr, ts6h)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 6 * hourInMs},
	}, ranges1)
	verifyCacheStats(t, testCache, 2, 0, 2)

	// Run same query again (should hit cache for aligned blocks)
	result, ranges2 := executeQuery(t, mimirEngine, promStorage, expr, ts6h)
	require.Equal(t, expected, result)
	require.Equal(t, []storageQueryRange{
		{mint: 1*hourInMs + 1, maxt: 2*hourInMs - 1}, // Head: (1h, 2h-1ms] -> storage [1h+1ms, 2h-1ms]
		{mint: 6 * hourInMs, maxt: 6 * hourInMs},     // Tail: (6h-1ms, 6h] -> storage [6h, 6h]
	}, ranges2)
	verifyCacheStats(t, testCache, 4, 2, 2) // 2 aligned blocks, both hit on second query
}
