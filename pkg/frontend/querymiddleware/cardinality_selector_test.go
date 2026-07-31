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
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
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

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 1)

		expectedMinT, expectedMaxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		require.Equal(t, expectedMinT, ranges[0].minT)
		require.Equal(t, expectedMaxT, ranges[0].maxT)
		require.Equal(t, `{__name__="foo"}`, canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(ranges[0].matchers)))
	})

	t.Run("matrix selector uses its range and does not double-count the inner vector selector", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("rate(bar[10m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 1)

		expectedMinT, expectedMaxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 10*time.Minute, 0, 0, false, false)
		require.Equal(t, expectedMinT, ranges[0].minT)
		require.Equal(t, expectedMaxT, ranges[0].maxT)
		require.Equal(t, `{__name__="bar"}`, canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(ranges[0].matchers)))
	})

	t.Run("multiple selectors", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo + rate(bar[10m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 2)
	})

	t.Run("smoothed range vector selector applies the lookback delta, matching the selector operator", func(t *testing.T) {
		// Build the AST directly so we don't need to enable experimental range modifiers in the parser.
		vs := &parser.VectorSelector{
			Name:          "foo",
			LabelMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo")},
			Smoothed:      true,
		}
		expr := &parser.MatrixSelector{VectorSelector: vs, Range: 10 * time.Minute}

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 1)

		// The selector operator sets LookbackDelta for smoothed selectors, so the queried range must
		// account for the lookback delta and the smoothed modifier.
		expectedMinT, expectedMaxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 10*time.Minute, 0, lookbackDelta, false, true)
		require.Equal(t, expectedMinT, ranges[0].minT)
		require.Equal(t, expectedMaxT, ranges[0].maxT)
	})

	t.Run("selector inside a subquery uses the subquery's expanded time range", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("max_over_time(rate(foo[5m])[1h:1m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 1)

		// The inner selector is evaluated over the subquery's expanded range, so the queried range must
		// be computed from the subquery's child time range (as the planner does), not the outer range.
		subquery := &core.Subquery{SubqueryDetails: &core.SubqueryDetails{Range: time.Hour, Step: time.Minute}}
		childTimeRange := subquery.ChildrenTimeRange(timeRange)
		expectedMinT, expectedMaxT := selectors.ComputeQueriedTimeRange(childTimeRange, nil, 5*time.Minute, 0, 0, false, false)
		require.Equal(t, expectedMinT, ranges[0].minT)
		require.Equal(t, expectedMaxT, ranges[0].maxT)

		// Sanity check: this differs from the naive computation against the outer time range.
		naiveMinT, naiveMaxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 5*time.Minute, 0, 0, false, false)
		require.NotEqual(t, [2]int64{naiveMinT, naiveMaxT}, [2]int64{ranges[0].minT, ranges[0].maxT})
	})
}

func testNoStepSubqueryInterval(int64) int64 {
	return time.Minute.Milliseconds()
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

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(newCtx(), expr, timeRange, lookbackDelta))
	})

	t.Run("returns the cardinality of a single selector", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		canonical := `{__name__="foo"}`
		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		writeSelectorCardinalityToAllBuckets(t, c, userID, canonical, minT, maxT, 1234)

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		result := estimator.EstimateSeriesCount(newCtx(), expr, timeRange, lookbackDelta)
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

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		result := estimator.EstimateSeriesCount(newCtx(), expr, timeRange, lookbackDelta)
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

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		result := estimator.EstimateSeriesCount(newCtx(), expr, wideTimeRange, lookbackDelta)
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

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(newCtx(), expr, timeRange, lookbackDelta))
	})

	t.Run("nil cache returns nil", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		estimator := NewCacheCardinalityEstimator(nil, testNoStepSubqueryInterval, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(newCtx(), expr, timeRange, lookbackDelta))
	})
}

func TestRequestHintsCardinalityEstimator(t *testing.T) {
	estimator := NewRequestHintsCardinalityEstimator()
	expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
	require.NoError(t, err)
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	t.Run("no hints", func(t *testing.T) {
		require.Nil(t, estimator.EstimateSeriesCount(context.Background(), expr, timeRange, 0))
	})

	t.Run("hints with estimate", func(t *testing.T) {
		ctx := ContextWithRequestHints(context.Background(), &Hints{CardinalityEstimate: &EstimatedSeriesCount{EstimatedSeriesCount: 100}})
		result := estimator.EstimateSeriesCount(ctx, expr, timeRange, 0)
		require.NotNil(t, result)
		require.Equal(t, uint64(100), result.EstimatedSeriesCount)
	})
}

func TestCardinalityStoringPostProcessor(t *testing.T) {
	const userID = "user-1"
	start := time.Date(2024, 12, 11, 3, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	timeRange := types.NewRangeQueryTimeRange(start, end, time.Minute)
	lookbackDelta := 5 * time.Minute
	minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)

	fooMatchers := func(extra ...stats.LabelMatcher) []stats.LabelMatcher {
		m := []stats.LabelMatcher{{Type: int32(labels.MatchEqual), Name: "__name__", Value: "foo"}}
		return append(m, extra...)
	}
	shardMatcher := func(value string) stats.LabelMatcher {
		return stats.LabelMatcher{Type: int32(labels.MatchEqual), Name: sharding.ShardLabel, Value: value}
	}

	// estimateFoo runs the cache-backed estimator for the query "foo" against the given cache.
	estimateFoo := func(t *testing.T, c cache.Cache) *EstimatedSeriesCount {
		t.Helper()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)
		ctx := user.InjectOrgID(context.Background(), userID)
		return NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger()).EstimateSeriesCount(ctx, expr, timeRange, lookbackDelta)
	}

	newCtxWithStats := func() (context.Context, *stats.SafeStats) {
		ctx := user.InjectOrgID(context.Background(), userID)
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		return ctx, qs
	}

	t.Run("writes nothing when there are no reported cardinalities", func(t *testing.T) {
		c := cache.NewMockCache()
		ctx, _ := newCtxWithStats()

		require.NoError(t, NewCardinalityStoringPostProcessor(c, log.NewNopLogger()).PostProcess(ctx))
		require.Nil(t, estimateFoo(t, c))
	})

	t.Run("stores a single selector's cardinality", func(t *testing.T) {
		c := cache.NewMockCache()
		ctx, qs := newCtxWithStats()
		qs.AddSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: minT, MaxT: maxT, SeriesCount: 1234})

		require.NoError(t, NewCardinalityStoringPostProcessor(c, log.NewNopLogger()).PostProcess(ctx))

		result := estimateFoo(t, c)
		require.NotNil(t, result)
		require.Equal(t, uint64(1234), result.EstimatedSeriesCount)
	})

	t.Run("sums the cardinality across shards of the same selector", func(t *testing.T) {
		c := cache.NewMockCache()
		ctx, qs := newCtxWithStats()
		qs.AddSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(shardMatcher("1_of_2")), MinT: minT, MaxT: maxT, SeriesCount: 30})
		qs.AddSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(shardMatcher("2_of_2")), MinT: minT, MaxT: maxT, SeriesCount: 40})

		require.NoError(t, NewCardinalityStoringPostProcessor(c, log.NewNopLogger()).PostProcess(ctx))

		result := estimateFoo(t, c)
		require.NotNil(t, result)
		require.Equal(t, uint64(70), result.EstimatedSeriesCount)
	})

	t.Run("does not double-count the same selector reported more than once without sharding", func(t *testing.T) {
		c := cache.NewMockCache()
		ctx, qs := newCtxWithStats()
		qs.AddSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: minT, MaxT: maxT, SeriesCount: 50})
		qs.AddSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: minT, MaxT: maxT, SeriesCount: 50})

		require.NoError(t, NewCardinalityStoringPostProcessor(c, log.NewNopLogger()).PostProcess(ctx))

		result := estimateFoo(t, c)
		require.NotNil(t, result)
		require.Equal(t, uint64(50), result.EstimatedSeriesCount)
	})

	t.Run("nil cache does nothing", func(t *testing.T) {
		ctx, qs := newCtxWithStats()
		qs.AddSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: minT, MaxT: maxT, SeriesCount: 50})
		require.NoError(t, NewCardinalityStoringPostProcessor(nil, log.NewNopLogger()).PostProcess(ctx))
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
