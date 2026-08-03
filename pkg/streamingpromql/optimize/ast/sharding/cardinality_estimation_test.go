// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"maps"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/promqlext"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

func TestCanonicalSelectorString(t *testing.T) {
	t.Run("sorts matchers and formats like labels.Matcher", func(t *testing.T) {
		matchers := []stats.LabelMatcher{
			{Type: labels.MatchRegexp, Name: "env", Value: "prod"},
			{Type: labels.MatchEqual, Name: "__name__", Value: "foo"},
		}
		formatted, shard := selectorStringWithoutShardingMatcher(matchers)
		require.Equal(t, `__name__="foo",env=~"prod"`, formatted)
		require.Equal(t, "", shard)
	})

	t.Run("excludes the query-shard matcher so all shards map to the same string", func(t *testing.T) {
		withShard := []stats.LabelMatcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "foo"},
			{Type: labels.MatchEqual, Name: sharding.ShardLabel, Value: "1_of_4"},
		}
		formattedWithShard, shard := selectorStringWithoutShardingMatcher(withShard)
		require.Equal(t, `__name__="foo"`, formattedWithShard)
		require.Equal(t, "1_of_4", shard)

		withoutShard := []stats.LabelMatcher{
			{Type: labels.MatchEqual, Name: "__name__", Value: "foo"},
		}
		formattedWithoutShard, _ := selectorStringWithoutShardingMatcher(withoutShard)
		require.Equal(t, formattedWithShard, formattedWithoutShard)
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

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
		require.Len(t, ranges, 1)

		// start - 5m lookback + 1ms .. end.
		require.Equal(t, startT-lookbackDelta.Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT, ranges[0].maxT)
		require.Equal(t, `__name__="foo"`, selectorString(ranges[0].matchers))
	})

	t.Run("matrix selector uses its range", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("rate(bar[10m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
		require.Len(t, ranges, 1)

		// Range vector selectors don't apply the lookback delta: start - 10m range + 1ms .. end.
		require.Equal(t, startT-(10*time.Minute).Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT, ranges[0].maxT)
		require.Equal(t, `__name__="bar"`, selectorString(ranges[0].matchers))
	})

	t.Run("multiple selectors", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("foo + rate(bar[10m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
		require.Len(t, ranges, 2)

		// foo is an instant vector selector: start - 5m lookback + 1ms .. end.
		require.Equal(t, `__name__="foo"`, selectorString(ranges[0].matchers))
		require.Equal(t, startT-lookbackDelta.Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT, ranges[0].maxT)

		// bar is a range vector selector and doesn't apply the lookback delta: start - 10m range + 1ms .. end.
		require.Equal(t, `__name__="bar"`, selectorString(ranges[1].matchers))
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

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
		require.Len(t, ranges, 1)

		// The selector operator sets LookbackDelta for smoothed selectors, so the queried range accounts
		// for the lookback delta: start - 5m lookback - 10m range + 1ms .. end + 5m lookback.
		require.Equal(t, startT-lookbackDelta.Milliseconds()-(10*time.Minute).Milliseconds()+1, ranges[0].minT)
		require.Equal(t, endT+lookbackDelta.Milliseconds(), ranges[0].maxT)
	})

	t.Run("selector inside a subquery uses the subquery's time range", func(t *testing.T) {
		expr, err := promqlext.NewPromQLParser().ParseExpr("max_over_time(rate(foo[7m])[1h:1m])")
		require.NoError(t, err)

		ranges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
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

func TestCacheCardinalityEstimator(t *testing.T) {
	start := time.Date(2024, 12, 11, 3, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)
	timeRange := types.NewRangeQueryTimeRange(start, end, time.Minute)
	lookbackDelta := 5 * time.Minute

	ctx := user.InjectOrgID(context.Background(), "user-1")
	fooMatchers := []stats.LabelMatcher{{Name: "__name__", Value: "foo", Type: labels.MatchEqual}}
	barMatchers := []stats.LabelMatcher{{Name: "__name__", Value: "bar", Type: labels.MatchEqual}}

	t.Run("returns nil when nothing is cached", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		originalExpression := "foo"
		expr, err := promqlext.NewPromQLParser().ParseExpr(originalExpression)
		require.NoError(t, err)

		estimator := NewCacheCardinalityEstimator(cfg, log.NewNopLogger())
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		result, err := estimator.EstimateSeriesCount(ctx, originalExpression, expr, timeRange, lookbackDelta)
		require.NoError(t, err)
		require.Nil(t, result)
		require.Equal(t, 1, c.GetCount)
		require.Equal(t, 1, c.KeysCount)

		require.Empty(t, qs.LoadEstimatedSelectorCardinalities())
	})

	t.Run("returns the cardinality of a single selector", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		originalExpression := "foo"
		expr, err := promqlext.NewPromQLParser().ParseExpr(originalExpression)
		require.NoError(t, err)

		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		expectedEstimatesInStats := writeSelectorCardinalityToAllBuckets(t, ctx, cfg, originalExpression, fooMatchers, minT, maxT, 1234)

		estimator := NewCacheCardinalityEstimator(cfg, log.NewNopLogger())
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		result, err := estimator.EstimateSeriesCount(ctx, originalExpression, expr, timeRange, lookbackDelta)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, uint64(1234), result.EstimatedSeriesCount)
		require.Equal(t, 1, c.GetCount)
		require.Equal(t, 1, c.KeysCount)

		require.Equal(t, expectedEstimatesInStats, qs.LoadEstimatedSelectorCardinalities(), "should store estimated cardinality in stats")
	})

	t.Run("returns the total cardinality across selectors", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		originalExpression := "foo + bar"
		expr, err := promqlext.NewPromQLParser().ParseExpr(originalExpression)
		require.NoError(t, err)

		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)

		expectedEstimatesInStats := []stats.SelectorCardinality{}
		expectedEstimatesInStats = append(expectedEstimatesInStats, writeSelectorCardinalityToAllBuckets(t, ctx, cfg, originalExpression, fooMatchers, minT, maxT, 100)...)
		expectedEstimatesInStats = append(expectedEstimatesInStats, writeSelectorCardinalityToAllBuckets(t, ctx, cfg, originalExpression, barMatchers, minT, maxT, 500)...)

		estimator := NewCacheCardinalityEstimator(cfg, log.NewNopLogger())
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		result, err := estimator.EstimateSeriesCount(ctx, originalExpression, expr, timeRange, lookbackDelta)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, uint64(600), result.EstimatedSeriesCount)
		require.Equal(t, 1, c.GetCount)
		require.Equal(t, 2, c.KeysCount)

		require.Equal(t, expectedEstimatesInStats, qs.LoadEstimatedSelectorCardinalities(), "should store estimated cardinality in stats")
	})

	t.Run("returns no estimate when some selectors are not cached", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		originalExpression := "foo + bar"
		expr, err := promqlext.NewPromQLParser().ParseExpr(originalExpression)
		require.NoError(t, err)

		// Only foo has an entry in the cache; bar has none.
		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		expectedEstimatesInStats := writeSelectorCardinalityToAllBuckets(t, ctx, cfg, originalExpression, fooMatchers, minT, maxT, 100)

		estimator := NewCacheCardinalityEstimator(cfg, log.NewNopLogger())
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		result, err := estimator.EstimateSeriesCount(ctx, originalExpression, expr, timeRange, lookbackDelta)
		require.NoError(t, err)
		require.Nil(t, result)
		require.Equal(t, 1, c.GetCount)
		require.Equal(t, 2, c.KeysCount)

		require.Equal(t, expectedEstimatesInStats, qs.LoadEstimatedSelectorCardinalities(), "should store estimated cardinality in stats")
	})

	t.Run("returns the maximum cardinality across the buckets of a single selector", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		// A range that spans several buckets.
		wideTimeRange := types.NewRangeQueryTimeRange(start, start.Add(24*time.Hour), time.Minute)
		originalExpression := "foo"
		expr, err := promqlext.NewPromQLParser().ParseExpr(originalExpression)
		require.NoError(t, err)

		canonical := `__name__="foo"`
		minT, maxT := selectors.ComputeQueriedTimeRange(wideTimeRange, nil, 0, 0, lookbackDelta, false, false)
		keys, err := selectorCardinalityCacheKeys(ctx, cfg, originalExpression, canonical, minT, maxT, false, newNoOpSpanLogger(t))
		require.NoError(t, err)
		require.Greater(t, len(keys), 1, "expected the wide range to span multiple buckets")

		// Write a different cardinality to each bucket; the estimate should be the maximum.
		expectedEstimatesInStats := []stats.SelectorCardinality{}
		for i, k := range keys {
			cardinality := uint64((i + 1) * 10)
			if i == 1 {
				cardinality = 9999
			}
			expectedEstimatesInStats = append(expectedEstimatesInStats, writeSelectorCardinalityEntry(t, ctx, cfg, k, cardinality, fooMatchers))
		}

		estimator := NewCacheCardinalityEstimator(cfg, log.NewNopLogger())
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		result, err := estimator.EstimateSeriesCount(ctx, originalExpression, expr, wideTimeRange, lookbackDelta)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, uint64(9999), result.EstimatedSeriesCount)
		require.Equal(t, 1, c.GetCount)
		require.Equal(t, len(keys), c.KeysCount)

		require.Equal(t, expectedEstimatesInStats, qs.LoadEstimatedSelectorCardinalities(), "should store estimated cardinality in stats")
	})

	t.Run("ignores entries whose stored selector does not match (hash collision)", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		originalExpression := "foo"
		expr, err := promqlext.NewPromQLParser().ParseExpr(originalExpression)
		require.NoError(t, err)

		canonical := `__name__="foo"`
		minT, maxT := selectors.ComputeQueriedTimeRange(timeRange, nil, 0, 0, lookbackDelta, false, false)
		keys, err := selectorCardinalityCacheKeys(ctx, cfg, originalExpression, canonical, minT, maxT, false, newNoOpSpanLogger(t))
		require.NoError(t, err)
		for _, k := range keys {
			// Store an entry with a different selector at the same key.
			k.plain = append(k.plain, []byte("-different")...)
			writeSelectorCardinalityEntry(t, ctx, cfg, k, 4321, fooMatchers)
		}

		estimator := NewCacheCardinalityEstimator(cfg, log.NewNopLogger())
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		result, err := estimator.EstimateSeriesCount(ctx, originalExpression, expr, timeRange, lookbackDelta)
		require.NoError(t, err)
		require.Nil(t, result)
		require.Equal(t, 1, c.GetCount)
		require.Equal(t, 1, c.KeysCount)

		require.Empty(t, qs.LoadEstimatedSelectorCardinalities())
	})

	t.Run("queries no more than the maximum allowed number of buckets, even if the selector queries a longer time range", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		cfg.MaxBucketsReadPerSelector = 4
		cfg.BucketSize = 5 * time.Minute

		originalExpression := "foo"
		expr, err := promqlext.NewPromQLParser().ParseExpr(originalExpression)
		require.NoError(t, err)

		estimator := NewCacheCardinalityEstimator(cfg, log.NewNopLogger())
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		result, err := estimator.EstimateSeriesCount(ctx, originalExpression, expr, timeRange, lookbackDelta)
		require.NoError(t, err)
		require.Nil(t, result)
		require.Equal(t, 1, c.GetCount)
		require.EqualValues(t, cfg.MaxBucketsReadPerSelector, c.KeysCount)

		require.Empty(t, qs.LoadEstimatedSelectorCardinalities())
	})
}

func TestSelectorCardinalityCacheKeys(t *testing.T) {
	const bucketSize = 5 * time.Minute
	const maxBuckets = 10

	testCases := map[string]struct {
		minT int64
		maxT int64

		expectedBucketIndices []int64
	}{
		"start and end are the same": {
			minT:                  1,
			maxT:                  1,
			expectedBucketIndices: []int64{0},
		},
		"number of buckets queried is much less than limit": {
			minT:                  0,
			maxT:                  bucketSize.Milliseconds() + 1,
			expectedBucketIndices: []int64{0, 1},
		},
		"number of buckets queried is exactly the limit": {
			minT:                  0,
			maxT:                  bucketSize.Milliseconds() * (maxBuckets - 1),
			expectedBucketIndices: []int64{0, 1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
		"number of buckets queried is just beyond the limit, only one needs to be dropped": {
			minT:                  0,
			maxT:                  bucketSize.Milliseconds() * maxBuckets,
			expectedBucketIndices: []int64{0, 1, 2, 3, 4 /* 5 dropped */, 6, 7, 8, 9, 10},
		},
		"number of buckets queried is well beyond the limit, every second bucket needs to be dropped": {
			minT:                  0,
			maxT:                  (bucketSize.Milliseconds() * 19) - 1,
			expectedBucketIndices: []int64{0, 2, 4, 6, 8, 10, 12, 14, 16, 18},
		},
		"number of buckets queried is more than double the limit, three in every four buckets needs to be dropped": {
			minT:                  0,
			maxT:                  (bucketSize.Milliseconds() * 39) - 1,
			expectedBucketIndices: []int64{0, 4, 8, 12, 16, 20, 24, 28, 32, 36},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			require.LessOrEqual(t, len(testCase.expectedBucketIndices), maxBuckets, "invalid test case: more expected buckets than limit")

			_, cfg := setupCardinalityEstimationTest()
			cfg.BucketSize = bucketSize
			cfg.MaxBucketsReadPerSelector = maxBuckets
			ctx := context.Background()

			expr := "foo + bar"
			selector := "foo"
			actualKeys, err := selectorCardinalityCacheKeys(ctx, cfg, expr, selector, testCase.minT, testCase.maxT, true, newNoOpSpanLogger(t))
			require.NoError(t, err)

			// Clear the time ranges: they're not important for this test, and this makes it easier to construct the expected values below.
			for idx := range actualKeys {
				actualKeys[idx].minT = 0
				actualKeys[idx].maxT = 0
			}

			expectedKeys := make([]cacheKey, 0, len(testCase.expectedBucketIndices))
			for _, k := range testCase.expectedBucketIndices {
				key, err := selectorCardinalityCacheKey(ctx, cfg, expr, selector, k, 0, 0)
				require.NoError(t, err)
				expectedKeys = append(expectedKeys, key)
			}

			require.Equal(t, expectedKeys, actualKeys)
		})
	}
}

func TestSelectorCardinalityCacheKey_DifferentOriginalExpressions(t *testing.T) {
	ctx := context.Background()
	_, cfg := setupCardinalityEstimationTest()
	keyForFoo, err := selectorCardinalityCacheKey(ctx, cfg, "foo", "foo", 0, 1000, 2000)
	require.NoError(t, err)

	keyForFooBar, err := selectorCardinalityCacheKey(ctx, cfg, "foo+bar", "foo", 0, 1000, 2000)
	require.NoError(t, err)

	require.NotEqual(t, keyForFoo, keyForFooBar, "identical selectors with different expressions should result in different keys")
}

func TestRequestHintsCardinalityEstimator(t *testing.T) {
	estimator := NewRequestHintsCardinalityEstimator()
	expr, err := promqlext.NewPromQLParser().ParseExpr("foo")
	require.NoError(t, err)
	timeRange := types.NewInstantQueryTimeRange(time.Now())

	t.Run("no hints", func(t *testing.T) {
		result, err := estimator.EstimateSeriesCount(context.Background(), "", expr, timeRange, 0)
		require.NoError(t, err)
		require.Nil(t, result)
	})

	t.Run("hints with estimate", func(t *testing.T) {
		ctx := querymiddleware.ContextWithRequestHints(context.Background(), &querymiddleware.Hints{CardinalityEstimate: &querymiddleware.EstimatedSeriesCount{EstimatedSeriesCount: 100}})
		result, err := estimator.EstimateSeriesCount(ctx, "", expr, timeRange, 0)
		require.NoError(t, err)
		require.Equal(t, uint64(100), result.EstimatedSeriesCount)
	})
}

func TestCardinalityStoringPostProcessor(t *testing.T) {
	const userID = "user-1"
	start := time.Date(2024, 12, 11, 3, 0, 0, 0, time.UTC)
	end := start.Add(10 * time.Minute)
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

	originalExpression := "foo"

	// estimateFoo runs the cache-backed estimator for the query "foo" against the given cache.
	estimateFoo := func(t *testing.T, cfg streamingpromql.CardinalityEstimationConfig) *querymiddleware.EstimatedSeriesCount {
		t.Helper()
		expr, err := promqlext.NewPromQLParser().ParseExpr(originalExpression)
		require.NoError(t, err)
		ctx := user.InjectOrgID(context.Background(), userID)
		result, err := NewCacheCardinalityEstimator(cfg, log.NewNopLogger()).EstimateSeriesCount(ctx, originalExpression, expr, timeRange, lookbackDelta)
		require.NoError(t, err)
		return result
	}

	newCtxWithStats := func() (context.Context, *stats.SafeStats) {
		ctx := user.InjectOrgID(context.Background(), userID)
		qs, ctx := stats.ContextWithEmptyStats(ctx)
		return ctx, qs
	}

	t.Run("writes nothing when there are no reported cardinalities", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		ctx, _ := newCtxWithStats()

		require.NoError(t, NewCardinalityStoringPostProcessor(cfg, log.NewNopLogger()).PostProcess(ctx, originalExpression))
		require.Zero(t, c.SetCount)

		require.Nil(t, estimateFoo(t, cfg))
	})

	t.Run("stores a single selector's cardinality", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		ctx, qs := newCtxWithStats()
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: minT, MaxT: maxT, SeriesCount: 1234})

		require.NoError(t, NewCardinalityStoringPostProcessor(cfg, log.NewNopLogger()).PostProcess(ctx, originalExpression))
		require.Equal(t, 1, c.SetCount)

		result := estimateFoo(t, cfg)
		require.NotNil(t, result)
		require.Equal(t, uint64(1234), result.EstimatedSeriesCount)
	})

	t.Run("sums the cardinality across shards of the same selector", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		ctx, qs := newCtxWithStats()
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(shardMatcher("1_of_2")), MinT: minT, MaxT: maxT, SeriesCount: 30})
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(shardMatcher("2_of_2")), MinT: minT, MaxT: maxT, SeriesCount: 40})

		require.NoError(t, NewCardinalityStoringPostProcessor(cfg, log.NewNopLogger()).PostProcess(ctx, originalExpression))
		require.Equal(t, 1, c.SetCount)

		result := estimateFoo(t, cfg)
		require.NotNil(t, result)
		require.Equal(t, uint64(70), result.EstimatedSeriesCount)
	})

	t.Run("does not double-count the same selector reported more than once without sharding", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		ctx, qs := newCtxWithStats()
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: minT, MaxT: maxT, SeriesCount: 50})
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: minT, MaxT: maxT, SeriesCount: 50})

		require.NoError(t, NewCardinalityStoringPostProcessor(cfg, log.NewNopLogger()).PostProcess(ctx, originalExpression))
		require.Equal(t, 1, c.SetCount)

		result := estimateFoo(t, cfg)
		require.NotNil(t, result)
		require.Equal(t, uint64(50), result.EstimatedSeriesCount)
	})

	t.Run("records cache entries for all buckets covered by the selector, even if this is more than the configured limit for number of buckets read per selector", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		cfg.MaxBucketsReadPerSelector = 5
		cfg.BucketSize = time.Minute
		ctx, qs := newCtxWithStats()
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 0, MaxT: 10 * time.Minute.Milliseconds(), SeriesCount: 50})

		require.NoError(t, NewCardinalityStoringPostProcessor(cfg, log.NewNopLogger()).PostProcess(ctx, originalExpression))
		require.Equal(t, 11, c.SetCount)
	})

	t.Run("does not overwrite a cached estimate with a lower observed cardinality", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		cfg.BucketSize = time.Millisecond // Use millisecond buckets to make the tests below clearer (otherwise we need to account for the bucket smearing offsets).
		cfg.MaxBucketsReadPerSelector = math.MaxInt64

		ctx, qs := newCtxWithStats()
		qs.AddEstimatedSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 5, MaxT: 5, SeriesCount: 1001})
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 5, MaxT: 5, SeriesCount: 1000})
		require.NoError(t, NewCardinalityStoringPostProcessor(cfg, log.NewNopLogger()).PostProcess(ctx, originalExpression))
		require.Equal(t, 0, c.SetCount)
	})

	t.Run("overwrites the cached estimate when the observed cardinality is at least the estimate", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		cfg.BucketSize = time.Millisecond // Use millisecond buckets to make the tests below clearer (otherwise we need to account for the bucket smearing offsets).
		cfg.MaxBucketsReadPerSelector = math.MaxInt64

		ctx, qs := newCtxWithStats()
		qs.AddEstimatedSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 5, MaxT: 5, SeriesCount: 1000})
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 5, MaxT: 5, SeriesCount: 1001})
		require.NoError(t, NewCardinalityStoringPostProcessor(cfg, log.NewNopLogger()).PostProcess(ctx, originalExpression))

		writtenKeys := slices.Collect(maps.Keys(c.Entries))
		expectedKey, err := selectorCardinalityCacheKey(ctx, cfg, originalExpression, `__name__="foo"`, 5, 5, 5)
		require.NoError(t, err)
		require.Equal(t, []string{expectedKey.hashed}, writtenKeys)
		require.Equal(t, 1, c.SetCount)
	})

	t.Run("writes cache entries for all unseen and updated estimates", func(t *testing.T) {
		c, cfg := setupCardinalityEstimationTest()
		cfg.BucketSize = time.Millisecond // Use millisecond buckets to make the tests below clearer (otherwise we need to account for the bucket smearing offsets).
		cfg.MaxBucketsReadPerSelector = math.MaxInt64

		ctx, qs := newCtxWithStats()

		// Seen cardinality is greater: we expect the cache entry to be updated.
		qs.AddEstimatedSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 5, MaxT: 5, SeriesCount: 1000})
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 5, MaxT: 5, SeriesCount: 1001})

		// Seen cardinality is lower or the same: we expect the cache entry to be unchanged.
		qs.AddEstimatedSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 7, MaxT: 7, SeriesCount: 910})
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 7, MaxT: 7, SeriesCount: 900})

		qs.AddEstimatedSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 9, MaxT: 9, SeriesCount: 915})
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 9, MaxT: 9, SeriesCount: 915})

		// Seen cardinality does not match any known estimate: we expect the cache entry to be written.
		qs.AddSeenSelectorCardinality(stats.SelectorCardinality{Matchers: fooMatchers(), MinT: 11, MaxT: 11, SeriesCount: 805})

		require.NoError(t, NewCardinalityStoringPostProcessor(cfg, log.NewNopLogger()).PostProcess(ctx, originalExpression))

		writtenKeys := slices.Collect(maps.Keys(c.Entries))
		expectedKeys := []string{}

		for _, ts := range []int64{5, 11} {
			expectedKey, err := selectorCardinalityCacheKey(ctx, cfg, originalExpression, `__name__="foo"`, ts, ts, ts)
			require.NoError(t, err)
			expectedKeys = append(expectedKeys, expectedKey.hashed)
		}

		require.ElementsMatch(t, expectedKeys, writtenKeys)
		require.Equal(t, 2, c.SetCount)
	})
}

func setupCardinalityEstimationTest() (*caching.InMemoryCache, streamingpromql.CardinalityEstimationConfig) {
	cfg := streamingpromql.CardinalityEstimationConfig{}
	flagext.DefaultValues(&cfg)
	backend := caching.NewInMemoryCache()
	cfg.Backend = backend
	cfg.CacheKeyGenerator = caching.NewCacheKeyGenerator(caching.StaticPrefixGenerator("non-hashable-prefix:"), caching.StaticPrefixGenerator("hashable-prefix:"))

	return backend, cfg
}

func writeSelectorCardinalityEntry(t *testing.T, ctx context.Context, cfg streamingpromql.CardinalityEstimationConfig, key cacheKey, cardinality uint64, matchers []stats.LabelMatcher) stats.SelectorCardinality {
	t.Helper()
	entry := &SelectorCardinalityStatistics{Key: key.plain, Cardinality: cardinality}
	data, err := entry.Marshal()
	require.NoError(t, err)
	require.NoError(t, cfg.Backend.SetMultiAsync(ctx, map[string][]byte{key.hashed: data}, cfg.TTL))

	return stats.SelectorCardinality{
		Matchers:    matchers,
		MinT:        key.minT,
		MaxT:        key.maxT,
		SeriesCount: cardinality,
	}
}

func writeSelectorCardinalityToAllBuckets(t *testing.T, ctx context.Context, cfg streamingpromql.CardinalityEstimationConfig, originalExpression string, matchers []stats.LabelMatcher, minT, maxT int64, cardinality uint64) []stats.SelectorCardinality {
	t.Helper()
	canonical, _ := selectorStringWithoutShardingMatcher(matchers)
	keys, err := selectorCardinalityCacheKeys(ctx, cfg, originalExpression, canonical, minT, maxT, false, newNoOpSpanLogger(t))
	require.NoError(t, err)

	statsCardinalities := make([]stats.SelectorCardinality, 0, len(keys))

	for _, k := range keys {
		statsCardinalities = append(statsCardinalities, writeSelectorCardinalityEntry(t, ctx, cfg, k, cardinality, matchers))
	}

	return statsCardinalities
}

func newNoOpSpanLogger(t *testing.T) *spanlogger.SpanLogger {
	logger, _ := spanlogger.New(t.Context(), log.NewNopLogger(), tracer, "newNoOpSpanLogger")
	t.Cleanup(logger.Finish)
	return logger
}
