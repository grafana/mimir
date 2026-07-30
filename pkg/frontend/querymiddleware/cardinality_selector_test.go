// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestCanonicalSelectorString(t *testing.T) {
	t.Run("sorts matchers and formats like labels.Matcher", func(t *testing.T) {
		matchers := []cardinalitySelectorMatcher{
			{typ: labels.MatchRegexp, name: "env", value: "prod"},
			{typ: labels.MatchEqual, name: "__name__", value: "foo"},
		}
		require.Equal(t, `{__name__="foo",env=~"prod"}`, canonicalSelectorString(matchers))
	})

	t.Run("excludes the query-shard matcher so all shards map to the same string", func(t *testing.T) {
		withShard := []cardinalitySelectorMatcher{
			{typ: labels.MatchEqual, name: "__name__", value: "foo"},
			{typ: labels.MatchEqual, name: sharding.ShardLabel, value: "1_of_4"},
		}
		withoutShard := []cardinalitySelectorMatcher{
			{typ: labels.MatchEqual, name: "__name__", value: "foo"},
		}
		require.Equal(t, canonicalSelectorString(withoutShard), canonicalSelectorString(withShard))
		require.Equal(t, `{__name__="foo"}`, canonicalSelectorString(withShard))
	})
}

func TestCollectSelectorTimeRanges(t *testing.T) {
	start := time.Date(2024, 12, 11, 3, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	timeRange := types.NewRangeQueryTimeRange(start, end, time.Minute)
	lookbackDelta := 5 * time.Minute

	t.Run("instant vector selector applies the lookback delta", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
		require.Len(t, ranges, 1)

		expectedMinT, expectedMaxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		require.Equal(t, expectedMinT, ranges[0].minT)
		require.Equal(t, expectedMaxT, ranges[0].maxT)
		require.Equal(t, `{__name__="foo"}`, canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(ranges[0].matchers)))
	})

	t.Run("matrix selector uses its range and does not double-count the inner vector selector", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("rate(bar[10m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
		require.Len(t, ranges, 1)

		expectedMinT, expectedMaxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 10*time.Minute, 0, 0, false, false)
		require.Equal(t, expectedMinT, ranges[0].minT)
		require.Equal(t, expectedMaxT, ranges[0].maxT)
		require.Equal(t, `{__name__="bar"}`, canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(ranges[0].matchers)))
	})

	t.Run("multiple selectors", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo + rate(bar[10m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
		require.Len(t, ranges, 2)
	})
}

func TestCacheCardinalityEstimator(t *testing.T) {
	const userID = "user-1"
	start := time.Date(2024, 12, 11, 3, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	timeRange := types.NewRangeQueryTimeRange(start, end, time.Minute)
	lookbackDelta := 5 * time.Minute

	newCtx := func() context.Context {
		return user.InjectOrgID(context.Background(), userID)
	}

	t.Run("returns nil when nothing is cached", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		estimator := NewCacheCardinalityEstimator(c, lookbackDelta, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(newCtx(), expr, timeRange))
	})

	t.Run("returns the cardinality of a single selector", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		canonical := `{__name__="foo"}`
		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		writeSelectorCardinalityToAllBuckets(t, c, userID, canonical, minT, maxT, 1234)

		estimator := NewCacheCardinalityEstimator(c, lookbackDelta, log.NewNopLogger())
		result := estimator.EstimateSeriesCount(newCtx(), expr, timeRange)
		require.NotNil(t, result)
		require.Equal(t, uint64(1234), result.EstimatedSeriesCount)
	})

	t.Run("returns the maximum cardinality across selectors", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo + bar")
		require.NoError(t, err)

		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		writeSelectorCardinalityToAllBuckets(t, c, userID, `{__name__="foo"}`, minT, maxT, 100)
		writeSelectorCardinalityToAllBuckets(t, c, userID, `{__name__="bar"}`, minT, maxT, 500)

		estimator := NewCacheCardinalityEstimator(c, lookbackDelta, log.NewNopLogger())
		result := estimator.EstimateSeriesCount(newCtx(), expr, timeRange)
		require.NotNil(t, result)
		require.Equal(t, uint64(500), result.EstimatedSeriesCount)
	})

	t.Run("returns the maximum cardinality across the buckets of a single selector", func(t *testing.T) {
		c := cache.NewMockCache()
		// A range that spans several buckets.
		wideTimeRange := types.NewRangeQueryTimeRange(start, start.Add(24*time.Hour), time.Minute)
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		canonical := `{__name__="foo"}`
		minT, maxT := selectors.ComputeQueriedTimeRange(wideTimeRange, nil, 0, 0, lookbackDelta, false, false)
		keys := selectorCardinalityCacheKeys(userID, canonical, minT, maxT, log.NewNopLogger())
		require.Greater(t, len(keys), 1, "expected the wide range to span multiple buckets")

		// Write a different cardinality to each bucket; the estimate should be the maximum.
		for i, k := range keys {
			cardinality := uint64((i + 1) * 10)
			if i == 1 {
				cardinality = 9999
			}
			writeSelectorCardinalityEntry(t, c, k, canonical, cardinality)
		}

		estimator := NewCacheCardinalityEstimator(c, lookbackDelta, log.NewNopLogger())
		result := estimator.EstimateSeriesCount(newCtx(), expr, wideTimeRange)
		require.NotNil(t, result)
		require.Equal(t, uint64(9999), result.EstimatedSeriesCount)
	})

	t.Run("ignores entries whose stored selector does not match (hash collision)", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		canonical := `{__name__="foo"}`
		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		keys := selectorCardinalityCacheKeys(userID, canonical, minT, maxT, log.NewNopLogger())
		for _, k := range keys {
			// Store an entry with a different selector at the same key.
			writeSelectorCardinalityEntry(t, c, k, `{__name__="something-else"}`, 4321)
		}

		estimator := NewCacheCardinalityEstimator(c, lookbackDelta, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(newCtx(), expr, timeRange))
	})

	t.Run("nil cache returns nil", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		estimator := NewCacheCardinalityEstimator(nil, lookbackDelta, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(newCtx(), expr, timeRange))
	})
}

func TestRequestHintsCardinalityEstimator(t *testing.T) {
	estimator := NewRequestHintsCardinalityEstimator()
	expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
	require.NoError(t, err)
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	t.Run("no hints", func(t *testing.T) {
		require.Nil(t, estimator.EstimateSeriesCount(context.Background(), expr, timeRange))
	})

	t.Run("hints with estimate", func(t *testing.T) {
		ctx := ContextWithRequestHints(context.Background(), &Hints{CardinalityEstimate: &EstimatedSeriesCount{EstimatedSeriesCount: 100}})
		result := estimator.EstimateSeriesCount(ctx, expr, timeRange)
		require.NotNil(t, result)
		require.Equal(t, uint64(100), result.EstimatedSeriesCount)
	})
}

func writeSelectorCardinalityEntry(t *testing.T, c cache.Cache, key, canonical string, cardinality uint64) {
	t.Helper()
	entry := &SelectorCardinalityStatistics{Selector: canonical, Cardinality: cardinality}
	data, err := entry.Marshal()
	require.NoError(t, err)
	c.SetMultiAsync(map[string][]byte{key: data}, selectorCardinalityTTL)
}

func writeSelectorCardinalityToAllBuckets(t *testing.T, c cache.Cache, userID, canonical string, minT, maxT int64, cardinality uint64) {
	t.Helper()
	keys := selectorCardinalityCacheKeys(userID, canonical, minT, maxT, log.NewNopLogger())
	for _, k := range keys {
		writeSelectorCardinalityEntry(t, c, k, canonical, cardinality)
	}
}
