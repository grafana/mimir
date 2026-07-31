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
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/querier/stats"
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
	// Use a query time range that starts and ends on clean millisecond boundaries so the expected
	// queried time ranges below can be given as explicit values: start at 2h and end at 3h after the
	// Unix epoch, with a 1m step and a 5m lookback delta.
	startT := (2 * time.Hour).Milliseconds()
	endT := (3 * time.Hour).Milliseconds()

	timeRange := types.NewRangeQueryTimeRange(timestamp.Time(startT), timestamp.Time(endT), time.Minute)
	lookbackDelta := 5 * time.Minute

	t.Run("instant vector selector applies the lookback delta", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 1)

		// start - 5m lookback + 1ms .. end.
		require.Equal(t, startT-lookbackDelta.Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT, ranges[0].maxT)
		require.Equal(t, `{__name__="foo"}`, canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(ranges[0].matchers)))
	})

	t.Run("matrix selector uses its range", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("rate(bar[10m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 1)

		// Range vector selectors don't apply the lookback delta: start - 10m range + 1ms .. end.
		require.Equal(t, startT-(10*time.Minute).Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT, ranges[0].maxT)
		require.Equal(t, `{__name__="bar"}`, canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(ranges[0].matchers)))
	})

	t.Run("multiple selectors", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo + rate(bar[10m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 2)

		// foo is an instant vector selector: start - 5m lookback + 1ms .. end.
		require.Equal(t, `{__name__="foo"}`, canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(ranges[0].matchers)))
		require.Equal(t, startT-lookbackDelta.Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT, ranges[0].maxT)

		// bar is a range vector selector and doesn't apply the lookback delta: start - 10m range + 1ms .. end.
		require.Equal(t, `{__name__="bar"}`, canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(ranges[1].matchers)))
		require.Equal(t, startT-(10*time.Minute).Milliseconds()+1, ranges[1].minT)
		require.Equal(t, endT, ranges[1].maxT)
	})

	t.Run("smoothed range vector selector", func(t *testing.T) {
		// Build the AST directly so we don't need to enable experimental range modifiers in the parser.
		vs := &parser.VectorSelector{
			Name:          "foo",
			LabelMatchers: []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "__name__", "foo")},
			Smoothed:      true,
		}
		expr := &parser.MatrixSelector{VectorSelector: vs, Range: 10 * time.Minute}

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 1)

		// The selector operator sets LookbackDelta for smoothed selectors, so the queried range accounts
		// for the lookback delta: start - 5m lookback - 10m range + 1ms .. end + 5m lookback.
		require.Equal(t, startT-lookbackDelta.Milliseconds()-(10*time.Minute).Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT+lookbackDelta.Milliseconds(), ranges[0].maxT)
	})

	t.Run("selector inside a subquery uses the subquery's time range", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("max_over_time(rate(foo[7m])[1h:1m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, testNoStepSubqueryInterval)
		require.Len(t, ranges, 1)

		// The inner selector is evaluated over the subquery's expanded, step-aligned range
		// ([3_660_000, 10_800_000]), then the 5m range vector selector widens the start further. This is
		// materially different from the ~6_900_001 start a naive computation against the outer range
		// would produce.
		// Note that the left boundary is open in a subquery.
		require.Equal(t, startT-(time.Hour-time.Minute+7*time.Minute).Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT, ranges[0].maxT)
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

	ctx := user.InjectOrgID(context.Background(), userID)

	t.Run("returns nil when nothing is cached", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(ctx, expr, timeRange, lookbackDelta))
	})

	t.Run("returns the cardinality of a single selector", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		canonical := `{__name__="foo"}`
		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		writeSelectorCardinalityToAllBuckets(t, c, ctx, canonical, minT, maxT, 1234)

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		result := estimator.EstimateSeriesCount(ctx, expr, timeRange, lookbackDelta)
		require.NotNil(t, result)
		require.Equal(t, uint64(1234), result.EstimatedSeriesCount)
	})

	t.Run("returns the maximum cardinality across selectors", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo + bar")
		require.NoError(t, err)

		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		writeSelectorCardinalityToAllBuckets(t, c, ctx, `{__name__="foo"}`, minT, maxT, 100)
		writeSelectorCardinalityToAllBuckets(t, c, ctx, `{__name__="bar"}`, minT, maxT, 500)

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		result := estimator.EstimateSeriesCount(ctx, expr, timeRange, lookbackDelta)
		require.NotNil(t, result)
		require.Equal(t, uint64(500), result.EstimatedSeriesCount)
	})

	t.Run("returns no estimate when some selectors are not cached", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo + bar")
		require.NoError(t, err)

		// Only foo has an entry in the cache; bar has none.
		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		writeSelectorCardinalityToAllBuckets(t, c, ctx, `{__name__="foo"}`, minT, maxT, 100)

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(ctx, expr, timeRange, lookbackDelta))
	})

	t.Run("returns the maximum cardinality across the buckets of a single selector", func(t *testing.T) {
		c := cache.NewMockCache()
		// A range that spans several buckets.
		wideTimeRange := types.NewRangeQueryTimeRange(start, start.Add(24*time.Hour), time.Minute)
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		canonical := `{__name__="foo"}`
		minT, maxT := selectors.ComputeQueriedTimeRange(wideTimeRange, nil, 0, 0, lookbackDelta, false, false)
		keys := selectorCardinalityCacheKeys(ctx, canonical, minT, maxT, log.NewNopLogger())
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
		result := estimator.EstimateSeriesCount(ctx, expr, wideTimeRange, lookbackDelta)
		require.NotNil(t, result)
		require.Equal(t, uint64(9999), result.EstimatedSeriesCount)
	})

	t.Run("ignores entries whose stored selector does not match (hash collision)", func(t *testing.T) {
		c := cache.NewMockCache()
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
		require.NoError(t, err)

		canonical := `{__name__="foo"}`
		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		keys := selectorCardinalityCacheKeys(ctx, canonical, minT, maxT, log.NewNopLogger())
		for _, k := range keys {
			// Store an entry with a different selector at the same key.
			writeSelectorCardinalityEntry(t, c, k, `{__name__="something-else"}`, 4321)
		}

		estimator := NewCacheCardinalityEstimator(c, testNoStepSubqueryInterval, log.NewNopLogger())
		require.Nil(t, estimator.EstimateSeriesCount(ctx, expr, timeRange, lookbackDelta))
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
		m := []stats.LabelMatcher{{Type: labels.MatchEqual, Name: "__name__", Value: "foo"}}
		return append(m, extra...)
	}
	shardMatcher := func(value string) stats.LabelMatcher {
		return stats.LabelMatcher{Type: labels.MatchEqual, Name: sharding.ShardLabel, Value: value}
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

func writeSelectorCardinalityToAllBuckets(t *testing.T, c cache.Cache, ctx context.Context, canonical string, minT, maxT int64, cardinality uint64) {
	t.Helper()
	keys := selectorCardinalityCacheKeys(ctx, canonical, minT, maxT, log.NewNopLogger())
	for _, k := range keys {
		writeSelectorCardinalityEntry(t, c, k, canonical, cardinality)
	}
}
