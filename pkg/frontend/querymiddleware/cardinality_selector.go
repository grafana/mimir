// SPDX-License-Identifier: AGPL-3.0-only

package querymiddleware

import (
	"context"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	// selectorCardinalityCacheKeyPrefix is the prefix used for per-selector cardinality cache keys.
	// It is deliberately different from the "QS" prefix used by the cardinality-estimation middleware
	// so that the two sets of cache entries do not conflict.
	selectorCardinalityCacheKeyPrefix = "SC"

	// selectorCardinalityBucketSize is the width of the time buckets that a selector's queried time
	// range is split into. Each bucket gets its own cache entry.
	selectorCardinalityBucketSize = 4 * time.Hour

	// selectorCardinalityTTL is how long a per-selector cardinality cache entry lives without being
	// written to.
	selectorCardinalityTTL = 7 * 24 * time.Hour

	// maxSelectorCardinalityBuckets caps the number of cache entries generated for a single selector,
	// to bound the size of the GetMulti / SetMultiAsync calls for very long time ranges.
	maxSelectorCardinalityBuckets = 168 // 168 * 4h = 28 days.
)

// CardinalityEstimator estimates the number of series that will be selected by a query, so that the
// sharding optimization pass can limit the number of shards accordingly.
type CardinalityEstimator interface {
	// EstimateSeriesCount returns an estimate of the number of series selected by expr over timeRange
	// with the given lookback delta, or nil if no estimate is available.
	EstimateSeriesCount(ctx context.Context, expr parser.Expr, timeRange types.QueryTimeRange, lookbackDelta time.Duration) *EstimatedSeriesCount
}

// requestHintsCardinalityEstimator returns the cardinality estimate carried on the request hints,
// which are populated by the cardinality-estimation middleware.
type requestHintsCardinalityEstimator struct{}

// NewRequestHintsCardinalityEstimator returns a CardinalityEstimator that reads the estimate from
// the request hints (as populated by the cardinality-estimation middleware).
func NewRequestHintsCardinalityEstimator() CardinalityEstimator {
	return requestHintsCardinalityEstimator{}
}

func (requestHintsCardinalityEstimator) EstimateSeriesCount(ctx context.Context, _ parser.Expr, _ types.QueryTimeRange, _ time.Duration) *EstimatedSeriesCount {
	if hints := RequestHintsFromContext(ctx); hints != nil {
		return hints.GetCardinalityEstimate()
	}

	return nil
}

// cacheCardinalityEstimator estimates a query's cardinality from the per-selector cardinality cache
// entries written by the cardinality-storing query post-processor.
type cacheCardinalityEstimator struct {
	cache                    cache.Cache
	noStepSubqueryIntervalFn func(rangeMillis int64) int64
	logger                   log.Logger
}

// NewCacheCardinalityEstimator returns a CardinalityEstimator that estimates a query's cardinality
// from the per-selector cardinality cache. noStepSubqueryIntervalFn must match the value used by the
// engine, and the lookback delta passed to EstimateSeriesCount must be the query's lookback delta, so
// that the queried time ranges (and therefore the cache keys) line up with those used when writing
// the cache entries.
func NewCacheCardinalityEstimator(cache cache.Cache, noStepSubqueryIntervalFn func(rangeMillis int64) int64, logger log.Logger) CardinalityEstimator {
	return &cacheCardinalityEstimator{
		cache:                    cache,
		noStepSubqueryIntervalFn: noStepSubqueryIntervalFn,
		logger:                   logger,
	}
}

func (e *cacheCardinalityEstimator) EstimateSeriesCount(ctx context.Context, expr parser.Expr, timeRange types.QueryTimeRange, lookbackDelta time.Duration) *EstimatedSeriesCount {
	spanLogger, ctx := spanlogger.New(ctx, e.logger, tracer, "EstimateSeriesCount")
	defer spanLogger.Finish()
	spanLogger.SetTag("timeRange", timeRange)
	spanLogger.SetTag("lookbackDelta", lookbackDelta)

	selectorRanges := collectSelectorTimeRanges(expr, timeRange, lookbackDelta, e.noStepSubqueryIntervalFn)
	if len(selectorRanges) == 0 {
		return nil
	}

	// Build the set of cache keys to look up, keeping track of which keys belong to which selector so
	// that we can take the maximum per selector afterwards.
	type selectorLookup struct {
		canonical string
		keys      []string
	}
	lookups := make([]selectorLookup, 0, len(selectorRanges))
	seen := make(map[string]struct{})
	allKeys := make([]string, 0, len(selectorRanges))

	for _, sr := range selectorRanges {
		canonical := canonicalSelectorString(cardinalitySelectorMatchersFromLabelMatchers(sr.matchers))
		keys := selectorCardinalityCacheKeys(ctx, canonical, sr.minT, sr.maxT, spanLogger)
		lookups = append(lookups, selectorLookup{canonical: canonical, keys: keys})

		for _, k := range keys {
			if _, ok := seen[k]; ok {
				continue
			}
			seen[k] = struct{}{}
			allKeys = append(allKeys, k)
		}
	}

	if len(allKeys) == 0 {
		return nil
	}

	// Fetch all cache entries in a single request.
	res := e.cache.GetMulti(ctx, allKeys)
	if len(res) == 0 {
		return nil
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
	var estimate uint64

	for _, lookup := range lookups {
		hitCount := 0
		var selectorMax uint64

		for _, k := range lookup.keys {
			entry, ok := decoded[k]
			if !ok {
				continue
			}
			// Guard against hashed key collisions.
			if entry.Selector != lookup.canonical {
				continue
			}
			hitCount++
			selectorMax = max(selectorMax, entry.Cardinality)
		}

		if hitCount == 0 {
			spanLogger.DebugLog(
				"msg", "could not find cached cardinality estimate for selector",
				"selector", lookup.canonical,
				"requested_cache_entries_count", len(lookup.keys),
			)
			return nil
		}

		spanLogger.DebugLog(
			"msg", "computed cardinality estimate for selector",
			"selector", lookup.canonical,
			"requested_cache_entries_count", len(lookup.keys),
			"hit_count", hitCount,
			"estimate", estimate,
		)

		estimate = max(estimate, selectorMax)
	}

	spanLogger.DebugLog("msg", "computed estimated cardinality for entire expression", "estimate", estimate)

	return &EstimatedSeriesCount{EstimatedSeriesCount: estimate}
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
func collectSelectorTimeRanges(expr parser.Expr, timeRange types.QueryTimeRange, lookbackDelta time.Duration, noStepSubqueryIntervalFn func(rangeMillis int64) int64) []selectorTimeRange {
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
			// Selectors inside a subquery are evaluated over a widened time range. Compute it exactly
			// as the planner does (see the SubqueryExpr case in QueryPlanner.nodeFromExpr and
			// Subquery.ChildrenTimeRange) so that the cache keys line up with the write path.
			step := n.Step
			if step == 0 {
				step = time.Duration(noStepSubqueryIntervalFn(n.Range.Milliseconds())) * time.Millisecond
			}

			childTimeRange := core.SubqueryChildrenTimeRange(tr, n.Range, step, n.OriginalOffset, core.TimeFromTimestamp(n.Timestamp))
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
	cache  cache.Cache
	logger log.Logger
}

// NewCardinalityStoringPostProcessor returns a query post-processor that stores per-selector
// cardinality in the given cache.
func NewCardinalityStoringPostProcessor(cache cache.Cache, logger log.Logger) streamingpromql.QueryPostProcessor {
	return &cardinalityStoringPostProcessor{
		cache:  cache,
		logger: logger,
	}
}

func (p *cardinalityStoringPostProcessor) PostProcess(ctx context.Context) error {
	if p.cache == nil {
		return nil
	}

	queryStats := stats.FromContext(ctx)
	cardinalities := queryStats.LoadSelectorCardinalities()
	if len(cardinalities) == 0 {
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
	groups := make(map[groupKey]map[string]uint64)

	for _, c := range cardinalities {
		selector := canonicalSelectorString(c.Matchers)
		gk := groupKey{selector: selector, minT: c.MinT, maxT: c.MaxT}

		byShard := groups[gk]
		if byShard == nil {
			byShard = make(map[string]uint64)
			groups[gk] = byShard
		}
		shard := shardLabelValue(c.Matchers)
		byShard[shard] = max(byShard[shard], c.SeriesCount)
	}

	entries := make(map[string][]byte)

	for gk, byShard := range groups {
		var total uint64
		for _, count := range byShard {
			total += count
		}

		entry := &SelectorCardinalityStatistics{Selector: gk.selector, Cardinality: total}
		data, err := entry.Marshal()
		if err != nil {
			level.Warn(p.logger).Log("msg", "failed to marshal selector cardinality cache entry", "err", err)
			continue
		}

		for _, k := range selectorCardinalityCacheKeys(ctx, gk.selector, gk.minT, gk.maxT, p.logger) {
			entries[k] = data
		}
	}

	if len(entries) > 0 {
		p.cache.SetMultiAsync(entries, selectorCardinalityTTL)
	}

	return nil
}

// shardLabelValue returns the value of the query-shard matcher in matchers, or "" if there is none.
func shardLabelValue(matchers []stats.LabelMatcher) string {
	for _, m := range matchers {
		if m.Name == sharding.ShardLabel {
			return m.Value
		}
	}
	return ""
}

func cardinalitySelectorMatchersFromLabelMatchers(matchers []*labels.Matcher) []stats.LabelMatcher {
	out := make([]stats.LabelMatcher, 0, len(matchers))
	for _, m := range matchers {
		out = append(out, stats.LabelMatcher{Type: m.Type, Name: m.Name, Value: m.Value})
	}
	return out
}

// canonicalSelectorString returns a stable string representation of the given matchers, excluding any
// query-shard matcher so that all shards of the same logical selector map to the same string. The
// format matches labels.Matcher.String() so that the read and write paths agree.
func canonicalSelectorString(matchers []stats.LabelMatcher) string {
	strs := make([]string, 0, len(matchers))
	for _, m := range matchers {
		if m.Name == sharding.ShardLabel {
			continue
		}
		strs = append(strs, fmt.Sprintf("%s%s%q", m.Name, m.Type, m.Value))
	}
	slices.Sort(strs)
	return "{" + strings.Join(strs, ",") + "}"
}

// selectorCardinalityCacheKeys returns the cache keys for the given selector over [minT, maxT], one
// per selectorCardinalityBucketSize-wide bucket that the range overlaps. A per-selector offset is
// applied so that entries for different selectors don't all expire at the same bucket boundary.
func selectorCardinalityCacheKeys(ctx context.Context, canonicalSelector string, minT, maxT int64, logger log.Logger) []string {
	tenants, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil
	}
	userID := tenant.JoinTenantIDs(tenants)

	bucketMs := selectorCardinalityBucketSize.Milliseconds()

	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(canonicalSelector))
	offset := int64(hasher.Sum64() % uint64(bucketMs))

	firstBucket := (minT + offset) / bucketMs
	lastBucket := (maxT + offset) / bucketMs
	if lastBucket < firstBucket {
		return nil
	}

	if lastBucket-firstBucket+1 > maxSelectorCardinalityBuckets {
		level.Debug(logger).Log("msg", "selector cardinality time range spans more buckets than the maximum; only the first buckets are used", "max_buckets", maxSelectorCardinalityBuckets, "selector", canonicalSelector)
		lastBucket = firstBucket + maxSelectorCardinalityBuckets - 1
	}

	userIDHash := hashCacheKey(userID)
	selectorHash := hashCacheKey(canonicalSelector)

	keys := make([]string, 0, lastBucket-firstBucket+1)
	for b := firstBucket; b <= lastBucket; b++ {
		keys = append(keys, fmt.Sprintf("%s:%s:%s:%d", selectorCardinalityCacheKeyPrefix, userIDHash, selectorHash, b))
	}

	return keys
}
