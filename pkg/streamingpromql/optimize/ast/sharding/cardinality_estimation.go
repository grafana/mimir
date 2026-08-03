// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// CardinalityEstimator estimates the number of series that will be selected by a query, so that the
// sharding optimization pass can limit the number of shards accordingly.
type CardinalityEstimator interface {
	// EstimateSeriesCount returns an estimate of the number of series selected by expr over timeRange
	// with the given lookback delta, or nil if no estimate is available.
	// originalExpression must be the original expression provided in the request. This is used to
	// ensure that requests containing selectors narrowed due to binop narrowing use accurate
	// estimates.
	EstimateSeriesCount(ctx context.Context, originalExpression string, expr parser.Expr, timeRange types.QueryTimeRange, lookbackDelta time.Duration) (*querymiddleware.EstimatedSeriesCount, error)
}

// requestHintsCardinalityEstimator returns the cardinality estimate carried on the request hints,
// which are populated by the cardinality-estimation middleware.
type requestHintsCardinalityEstimator struct{}

// NewRequestHintsCardinalityEstimator returns a CardinalityEstimator that reads the estimate from
// the request hints (as populated by the cardinality-estimation middleware).
func NewRequestHintsCardinalityEstimator() CardinalityEstimator {
	return requestHintsCardinalityEstimator{}
}

func (requestHintsCardinalityEstimator) EstimateSeriesCount(ctx context.Context, _ string, _ parser.Expr, _ types.QueryTimeRange, _ time.Duration) (*querymiddleware.EstimatedSeriesCount, error) {
	if hints := querymiddleware.RequestHintsFromContext(ctx); hints != nil {
		return hints.GetCardinalityEstimate(), nil
	}

	return nil, nil
}

// cacheCardinalityEstimator estimates a query's cardinality from the per-selector cardinality cache
// entries written by the cardinality-storing query post-processor.
type cacheCardinalityEstimator struct {
	cfg    streamingpromql.CardinalityEstimationConfig
	logger log.Logger
}

// NewCacheCardinalityEstimator returns a CardinalityEstimator that estimates a query's cardinality
// from the per-selector cardinality cache. noStepSubqueryIntervalFn must match the value used by the
// engine, and the lookback delta passed to EstimateSeriesCount must be the query's lookback delta, so
// that the queried time ranges (and therefore the cache keys) line up with those used when writing
// the cache entries.
func NewCacheCardinalityEstimator(cfg streamingpromql.CardinalityEstimationConfig, logger log.Logger) CardinalityEstimator {
	return &cacheCardinalityEstimator{
		cfg:    cfg,
		logger: logger,
	}
}

func (e *cacheCardinalityEstimator) EstimateSeriesCount(ctx context.Context, originalExpression string, expr parser.Expr, timeRange types.QueryTimeRange, lookbackDelta time.Duration) (*querymiddleware.EstimatedSeriesCount, error) {
	spanLogger, ctx := spanlogger.New(ctx, e.logger, tracer, "EstimateSeriesCount")
	defer spanLogger.Finish()
	spanLogger.SetTag("timeRange", timeRange)
	spanLogger.SetTag("lookbackDelta", lookbackDelta)

	queryStats := stats.FromContext(ctx)
	selectorRanges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta)
	if len(selectorRanges) == 0 {
		return nil, nil
	}

	// Build the set of cache keys to look up, keeping track of which keys belong to which selector so
	// that we can take the maximum per selector afterwards.
	type selectorLookup struct {
		matchers  []stats.LabelMatcher
		selector  string
		cacheKeys []cacheKey
	}
	selectorsToLookUp := make([]selectorLookup, 0, len(selectorRanges))
	keysToLookUp := make(map[string]struct{})

	for _, sr := range selectorRanges {
		selector := selectorString(sr.matchers)
		cacheKeys, err := selectorCardinalityCacheKeys(ctx, e.cfg, originalExpression, selector, sr.minT, sr.maxT, true, spanLogger)
		if err != nil {
			return nil, err
		}
		selectorsToLookUp = append(selectorsToLookUp, selectorLookup{
			matchers:  toStatsMatchers(sr.matchers),
			selector:  selector,
			cacheKeys: cacheKeys,
		})

		for _, k := range cacheKeys {
			if _, ok := keysToLookUp[k.hashed]; ok {
				continue
			}
			keysToLookUp[k.hashed] = struct{}{}
		}
	}

	// Fetch all cache entries in a single request.
	res, err := e.cfg.Backend.GetMulti(ctx, slices.Collect(maps.Keys(keysToLookUp)))
	if err != nil {
		return nil, err
	}
	if len(res) == 0 {
		return nil, nil
	}

	decoded := make(map[string]*SelectorCardinalityStatistics, len(res))
	for k, v := range res {
		entry := &SelectorCardinalityStatistics{}
		if err := proto.Unmarshal(v, entry); err != nil {
			level.Warn(spanLogger).Log("msg", "failed to unmarshal selector cardinality cache entry", "err", err, "key", k)
			continue
		}
		decoded[k] = entry
	}

	// The estimate for the whole expression is the maximum cardinality across its selectors, and the
	// cardinality of a single selector is the maximum across the buckets it spans.
	// If there is no information available for a selector, then we return no estimate at all.
	var overallEstimate uint64
	sawAllSelectors := true

	for _, s := range selectorsToLookUp {
		hitCount := 0
		var selectorEstimate uint64

		for _, k := range s.cacheKeys {
			entry, hit := decoded[k.hashed]
			if !hit {
				continue
			}
			// Guard against hashed key collisions.
			if !bytes.Equal(entry.Key, k.plain) {
				level.Warn(spanLogger).Log(
					"msg", "possible cache key collision: raw key in entry does not match desired key, ignoring value",
					"key", k,
				)
				continue
			}
			hitCount++
			selectorEstimate = max(selectorEstimate, entry.Cardinality)

			queryStats.AddEstimatedSelectorCardinality(stats.SelectorCardinality{
				Matchers:    s.matchers,
				MinT:        k.minT,
				MaxT:        k.maxT,
				SeriesCount: entry.Cardinality,
			})
		}

		if hitCount == 0 {
			spanLogger.DebugLog(
				"msg", "could not find cached cardinality estimate for selector",
				"selector", s.selector,
				"requested_cache_entries_count", len(s.cacheKeys),
			)

			// We don't return early because we still want to populate all the estimated selector cardinalities in the query stats
			// so we don't bother writing them to the cache again in the post-processor.
			sawAllSelectors = false
			continue
		}

		spanLogger.DebugLog(
			"msg", "computed cardinality estimate for selector",
			"selector", s.selector,
			"requested_cache_entries_count", len(s.cacheKeys),
			"hit_count", hitCount,
			"estimate", selectorEstimate,
		)

		overallEstimate += selectorEstimate
	}

	if !sawAllSelectors {
		return nil, nil
	}

	spanLogger.DebugLog("msg", "computed estimated cardinality for entire expression", "estimate", overallEstimate)

	return &querymiddleware.EstimatedSeriesCount{EstimatedSeriesCount: overallEstimate}, nil
}

// selectorTimeRange is a selector's matchers together with the time range it queries from storage.
type selectorTimeRange struct {
	matchers   []*labels.Matcher
	minT, maxT int64
}

// collectSelectorTimeRanges returns the selectors in expr, each with the time range it queries from
// storage. The time range accounts for the selector's range, offset, @ modifier, the lookback delta
// (for instant vector selectors and anchored/smoothed range selectors) and any enclosing subqueries,
// matching the computation done by the selectors when they report their cardinality.
//
// FIXME: ideally we'd run sharding over the query plan (rather than AST) and therefore be able to
// reuse the existing QueriedTimeRange method rather than implementing it again here.
func collectSelectorTimeRanges(expr parser.Expr, timeRange types.QueryTimeRange, lookbackDelta time.Duration) []selectorTimeRange {
	var out []selectorTimeRange

	// visit descends the expression carrying the time range that applies at the current node, which is
	// widened whenever we enter a subquery.
	var visit func(node parser.Node, tr types.QueryTimeRange)
	visit = func(node parser.Node, tr types.QueryTimeRange) {
		switch n := node.(type) {
		case *parser.MatrixSelector:
			vs, ok := n.VectorSelector.(*parser.VectorSelector)
			if !ok {
				return
			}

			// Range vector selectors only apply the lookback delta when they use the anchored or
			// smoothed range modifiers, matching the selector operator (see MatrixSelector
			// materialization) so that the queried time range (and therefore the cache keys) line up
			// with those used when the cardinality was written.
			lookback := time.Duration(0)
			if vs.Anchored || vs.Smoothed {
				lookback = lookbackDelta
			}

			minT, maxT := selectors.ComputeQueriedTimeRange(tr, vs.Timestamp, n.Range, vs.OriginalOffset.Milliseconds(), lookback, vs.Anchored, vs.Smoothed)
			out = append(out, selectorTimeRange{matchers: vs.LabelMatchers, minT: minT, maxT: maxT})
			// Don't descend into the inner vector selector, which is handled here.

		case *parser.VectorSelector:
			minT, maxT := selectors.ComputeQueriedTimeRange(tr, n.Timestamp, 0, n.OriginalOffset.Milliseconds(), lookbackDelta, false, false)
			out = append(out, selectorTimeRange{matchers: n.LabelMatchers, minT: minT, maxT: maxT})

		case *parser.SubqueryExpr:
			childTimeRange := core.SubqueryChildrenTimeRange(tr, n.Range, n.Step, n.OriginalOffset, core.TimeFromTimestamp(n.Timestamp))
			visit(n.Expr, childTimeRange)

		default:
			for child := range parser.ChildrenIter(node) {
				visit(child, tr)
			}
		}
	}

	visit(expr, timeRange)

	return out
}

// cardinalityStoringPostProcessor is a streamingpromql.QueryPostProcessor that stores the cardinality
// of each selector evaluated by a successful query in the per-selector cardinality cache, so that the
// cache-backed CardinalityEstimator can use it to estimate the cardinality of future queries.
type cardinalityStoringPostProcessor struct {
	cfg    streamingpromql.CardinalityEstimationConfig
	logger log.Logger
}

// NewCardinalityStoringPostProcessor returns a query post-processor that stores per-selector
// cardinality in the given cache.
func NewCardinalityStoringPostProcessor(cfg streamingpromql.CardinalityEstimationConfig, logger log.Logger) streamingpromql.QueryPostProcessor {
	return &cardinalityStoringPostProcessor{
		cfg:    cfg,
		logger: logger,
	}
}

// PostProcess stores new or updated cardinality values in the cache.
func (p *cardinalityStoringPostProcessor) PostProcess(ctx context.Context, originalExpression string) error {
	spanLogger, ctx := spanlogger.New(ctx, p.logger, tracer, "cardinalityStoringPostProcessor.PostProcess")
	defer spanLogger.Finish()

	queryStats := stats.FromContext(ctx)
	seenCardinalities := queryStats.LoadSeenSelectorCardinalities()
	if len(seenCardinalities) == 0 {
		spanLogger.DebugLog("msg", "query stats reports no seen selector cardinalities")
		return nil
	}

	// Group the reported cardinalities by selector (ignoring the query-shard matcher) and time range,
	// then aggregate within each group. Different shards of the same logical selector each report a
	// disjoint subset of the series, so their cardinalities are summed. A selector that is reported
	// more than once with the same shard (for example the same selector appearing twice in a query
	// without common-subexpression elimination) is counted once, using the maximum reported value.
	type groupKey struct {
		selector   string
		minT, maxT int64
	}
	seenGroups := make(map[groupKey]map[string]uint64)

	for _, c := range seenCardinalities {
		selector, shard := selectorStringWithoutShardingMatcher(c.Matchers)
		gk := groupKey{selector: selector, minT: c.MinT, maxT: c.MaxT}

		byShard := seenGroups[gk]
		if byShard == nil {
			byShard = make(map[string]uint64)
			seenGroups[gk] = byShard
		}
		byShard[shard] = max(byShard[shard], c.SeriesCount)
	}

	estimatedGroups := make(map[groupKey]uint64)
	estimatedCardinalities := queryStats.LoadEstimatedSelectorCardinalities()
	for _, c := range estimatedCardinalities {
		selector, _ := selectorStringWithoutShardingMatcher(c.Matchers) // Estimated cardinalities should have no shard matcher.
		gk := groupKey{selector: selector, minT: c.MinT, maxT: c.MaxT}
		estimatedGroups[gk] = c.SeriesCount
	}

	entries := make(map[string][]byte)

	for gk, byShard := range seenGroups {
		var total uint64
		for _, count := range byShard {
			total += count
		}

		// Get all the cache keys (ie. buckets) that this seen selector maps to.
		// We don't need to do the same thing below for the estimates, because they're read and stored in the stats exactly
		// as they were in the cache (ie. bucketed).
		keys, err := selectorCardinalityCacheKeys(ctx, p.cfg, originalExpression, gk.selector, gk.minT, gk.maxT, false, spanLogger)
		if err != nil {
			return err
		}

		for _, k := range keys {
			existingEstimate := estimatedGroups[groupKey{selector: gk.selector, minT: k.minT, maxT: k.maxT}]
			updateThreshold := float64(existingEstimate) * (p.cfg.EstimateUpdateThreshold + 1)
			if existingEstimate > 0 && float64(total) < updateThreshold {
				// The existing estimate is not above the threshold to write an updated entry,
				// so leave it as-is.
				continue
			}

			entry := &SelectorCardinalityStatistics{Key: k.plain, Cardinality: total}
			data, err := entry.Marshal()
			if err != nil {
				return err
			}

			entries[k.hashed] = data
		}
	}

	spanLogger.DebugLog(
		"msg", "writing updated cardinality estimates (if any)",
		"seen_count", len(seenGroups),
		"estimates_count", len(estimatedGroups),
		"new_or_updated_estimates", len(entries),
	)

	if len(entries) == 0 {
		return nil
	}

	return p.cfg.Backend.SetMultiAsync(ctx, entries, p.cfg.TTL)
}

// selectorStringWithoutShardingMatcher returns a stable string representation of the given matchers, excluding any
// query-shard matcher so that all shards of the same logical selector map to the same string. The
// format matches labels.Matcher.String() so that the read and write paths agree.
//
// The sharding label is returned separately.
func selectorStringWithoutShardingMatcher(matchers []stats.LabelMatcher) (string, string) {
	strs := make([]string, 0, len(matchers))
	shard := ""

	for _, m := range matchers {
		if m.Name == sharding.ShardLabel {
			shard = m.Value
			continue
		}
		matcher := labels.Matcher{Name: m.Name, Value: m.Value, Type: m.Type}
		strs = append(strs, matcher.String())
	}
	slices.Sort(strs)
	return strings.Join(strs, ","), shard
}

func selectorString(matchers []*labels.Matcher) string {
	strs := make([]string, 0, len(matchers))
	for _, m := range matchers {
		strs = append(strs, m.String())
	}
	slices.Sort(strs)
	return strings.Join(strs, ",")
}

func toStatsMatchers(matchers []*labels.Matcher) []stats.LabelMatcher {
	statsMatchers := make([]stats.LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		statsMatchers = append(statsMatchers, stats.LabelMatcher{
			Name:  m.Name,
			Value: m.Value,
			Type:  m.Type,
		})
	}

	return statsMatchers
}

// selectorCardinalityCacheKeys returns the cache keys for the given selector over [minT, maxT], one
// per selectorCardinalityBucketSize-wide bucket that the range overlaps.
//
// If limitBucketCount is true and the range overlaps more than MaxBucketsReadPerSelector buckets, an
// evenly-spaced subset of at most MaxBucketsReadPerSelector buckets is returned instead, to bound the
// size of the cache request.
func selectorCardinalityCacheKeys(ctx context.Context, cfg streamingpromql.CardinalityEstimationConfig, originalExpression string, canonicalSelector string, minT, maxT int64, limitBucketCount bool, logger *spanlogger.SpanLogger) ([]cacheKey, error) {
	bucketMs := cfg.BucketSize.Milliseconds()
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(originalExpression))
	offset := int64(hasher.Sum64() % uint64(bucketMs))

	firstBucket := (minT + offset) / bucketMs
	lastBucket := (maxT + offset) / bucketMs
	if lastBucket < firstBucket {
		return nil, fmt.Errorf("last bucket must not be before first bucket, but got minT=%d and maxT=%d", minT, maxT)
	}

	desiredBucketCount := lastBucket - firstBucket + 1
	maxBuckets := cfg.MaxBucketsReadPerSelector
	needToApplyLimit := limitBucketCount && desiredBucketCount > maxBuckets
	outputBucketCount := desiredBucketCount

	if needToApplyLimit {
		logger.DebugLog(
			"msg", "selector cardinality time range spans more buckets than the maximum; only a subset of buckets will be queried",
			"max_buckets", maxBuckets,
			"bucket_count", desiredBucketCount,
			"selector", canonicalSelector,
		)

		outputBucketCount = maxBuckets
	}

	keys := make([]cacheKey, 0, outputBucketCount)
	for i := range outputBucketCount {
		bucketIndex := firstBucket + i

		if needToApplyLimit {
			// Space the selected buckets evenly across [firstBucket, lastBucket], rounding to the nearest
			// bucket so the first and last buckets in the range are always included.
			bucketIndex = firstBucket + (i*desiredBucketCount*2+maxBuckets)/(maxBuckets*2)
		}

		bucketMinT := bucketIndex*cfg.BucketSize.Milliseconds() - offset
		bucketMaxT := bucketMinT + cfg.BucketSize.Milliseconds() - 1
		key, err := selectorCardinalityCacheKey(ctx, cfg, originalExpression, canonicalSelector, bucketIndex, bucketMinT, bucketMaxT)
		if err != nil {
			return nil, err
		}

		keys = append(keys, key)
	}

	return keys, nil
}

func selectorCardinalityCacheKey(ctx context.Context, cfg streamingpromql.CardinalityEstimationConfig, originalExpression string, canonicalSelector string, bucketIndex int64, minT, maxT int64) (cacheKey, error) {
	suffix := bytes.Join([][]byte{
		[]byte(originalExpression),
		[]byte(canonicalSelector),
		[]byte(strconv.FormatInt(bucketIndex, 10)),
	}, []byte(":"))

	plain, hashed, err := cfg.CacheKeyGenerator.ComputeCacheKey(ctx, suffix)
	if err != nil {
		return cacheKey{}, err
	}

	return cacheKey{
		plain:  plain,
		hashed: hashed,
		minT:   minT,
		maxT:   maxT,
	}, nil
}

type cacheKey struct {
	plain  []byte
	hashed string
	minT   int64
	maxT   int64
}
