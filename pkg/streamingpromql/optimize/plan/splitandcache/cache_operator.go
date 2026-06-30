// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tracing"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"
	"go.opentelemetry.io/otel"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/querydetails"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/querierpb"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/sliceutil"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

// cacheVersion is the version of cache entries read and written by this operator.
// If a backwards-incompatible change is made to the cache entry format, or a bug is discovered that means all existing
// cache entries should be invalidated, this version number should be incremented.
const cacheVersion = 1

var tracer = otel.Tracer("pkg/streamingpromql/optimize/plan/splitandcache")

// CacheOperator is an operator that uses a cache to avoid recomputing previously computed results.
// It works with a single cache entry made up of multiple extents.
type CacheOperator struct {
	Backend                  caching.Backend
	Materializer             InstantVectorMaterializer
	Inner                    planning.Node
	DesiredTimeRange         types.QueryTimeRange
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker

	expressionPosition posrange.PositionRange
	queryParameters    *planning.QueryParameters
	limitsProvider     LimitsProvider
	logger             log.Logger
	cacheEntryInterval time.Duration
	metrics            *ResultsCacheMetrics

	// minCacheExtent is the minimum length of a cached extent for it to be used.
	// Extents smaller than this are ignored and re-evaluated, to avoid freshly evaluating many small extents.
	// If the desired time range is smaller than minCacheExtent, then minCacheExtent is ignored and all cache extents are used.
	// A value of zero disables small extent avoidance.
	minCacheExtent time.Duration

	evaluationTime     time.Time
	ttlForNonOOOExtent time.Duration
	ttlForOOOExtent    time.Duration
	oooWindow          time.Duration

	key       []byte
	hashedKey string
	extents   extents
	prepared  bool

	// outputSeries contains one entry per output series, with each entry containing the source series index into each source extent.
	// outputSeries is nil if there is only one extent in the desired time range.
	outputSeries        []splitOrCacheOutputSeries
	nextOutputSeriesIdx int

	// These fields store the data to be written in the extent covering the desired time range.
	seriesMetadata []types.SeriesMetadata
	data           []types.InstantVectorSeriesData

	finishedReading bool // Set if FinishedReading is called.

	timeNow           func() time.Time // Used to override the current time in tests.
	getCurrentTraceID func(context.Context) (string, bool)
}

var _ types.InstantVectorOperator = &CacheOperator{}

func newCacheOperator(
	backend caching.Backend,
	materializer InstantVectorMaterializer,
	inner planning.Node,
	desiredTimeRange types.QueryTimeRange,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	expressionPosition posrange.PositionRange,
	queryParameters *planning.QueryParameters,
	limitsProvider LimitsProvider,
	logger log.Logger,
	cacheEntryInterval time.Duration,
	minCacheExtent time.Duration,
	metrics *ResultsCacheMetrics,
) *CacheOperator {
	return &CacheOperator{
		Backend:                  backend,
		Materializer:             materializer,
		Inner:                    inner,
		DesiredTimeRange:         desiredTimeRange,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
		queryParameters:          queryParameters,
		limitsProvider:           limitsProvider,
		logger:                   logger,
		cacheEntryInterval:       cacheEntryInterval,
		minCacheExtent:           minCacheExtent,
		timeNow:                  time.Now,
		getCurrentTraceID:        tracing.ExtractTraceID,
		metrics:                  metrics,
	}
}

func (c *CacheOperator) computeCacheKey(ctx context.Context) ([]byte, error) {
	orgID, err := user.ExtractOrgID(ctx)
	if err != nil {
		return nil, err
	}

	startTime := timestamp.Time(c.DesiredTimeRange.StartT)
	cacheEntryStartT := timestamp.FromTime(startTime.Truncate(c.cacheEntryInterval))

	// We need to consider the offset from the step grid when generating the cache key.
	// For example, if the time range is 08:00:00 to 10:00:00 with a step of 1m, this cannot use the same cache entry
	// as a query with the time range 08:00:30 to 10:00:30 with a step of 1m: the first will have points at 08:00:00, 08:01:00 etc.,
	// while the second will have points at 08:00:30, 08:01:30 etc.
	offsetFromStepGrid := (c.DesiredTimeRange.StartT - cacheEntryStartT) % c.DesiredTimeRange.IntervalMilliseconds

	encodedQueryPlanBytes, err := c.encodeNodeForCacheKey() // This includes the query, time range and all other parameters that affect query results.
	if err != nil {
		return nil, err
	}

	key := bytes.Join(
		[][]byte{
			[]byte(orgID),
			[]byte(strconv.FormatInt(cacheEntryStartT, 10)),
			[]byte(strconv.FormatInt(c.DesiredTimeRange.IntervalMilliseconds, 10)),
			[]byte(strconv.FormatInt(offsetFromStepGrid, 10)),
			encodedQueryPlanBytes,
		},
		[]byte(":"),
	)

	return key, nil
}

func (c *CacheOperator) encodeNodeForCacheKey() ([]byte, error) {
	// Make a copy of the parameters and clear the time range used for the overall request.
	// We want to retain all the other parameters (lookback window, delayed name removal state etc.),
	// as these influence the results of the query.
	// This includes the original expression, as this is used when producing annotations.
	cacheKeyParams := *c.queryParameters
	cacheKeyParams.TimeRange = types.QueryTimeRange{}

	plan := &planning.QueryPlan{Root: c.Inner, Parameters: &cacheKeyParams}
	encoded, _, err := plan.ToEncodedPlan(false, true)
	if err != nil {
		return nil, fmt.Errorf("encoding plan for cache key: %w", err)
	}

	b, err := encoded.Marshal()
	if err != nil {
		return nil, fmt.Errorf("marshalling encoded plan for cache key: %w", err)
	}
	return b, nil
}

func (c *CacheOperator) populateCacheKey(ctx context.Context) error {
	var err error
	c.key, err = c.computeCacheKey(ctx)
	if err != nil {
		return err
	}

	c.hashedKey = caching.HashCacheKey(c.key)
	return nil
}

func (c *CacheOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	if c.prepared {
		// If this CacheOperator is nested directly beneath a SplitOperator, it will
		// prepare all CacheOperators in a single PrepareCacheOperators call, so that
		// all cache fetches happen in one call.
		return errors.New("cache operator already prepared")
	}

	return PrepareCacheOperators(ctx, params, []*CacheOperator{c}, c.timeNow())
}

func (c *CacheOperator) prepareFrom(ctx context.Context, cacheHit []byte, params *types.PrepareParams) error {
	spanLogger, ctx := spanlogger.New(ctx, c.logger, tracer, "CacheOperator.prepare")
	defer spanLogger.Finish()

	spanLogger.SetTag("hashed_key", c.hashedKey)
	spanLogger.SetTag("desired_time_range", c.DesiredTimeRange)
	c.prepared = true

	existingExtents, err := c.decodeAndFilterCacheEntry(cacheHit, spanLogger)
	if err != nil {
		return err
	}

	c.extents, err = c.calculateExtents(ctx, existingExtents, spanLogger)
	if err != nil {
		return err
	}

	for _, e := range c.extents.inDesiredTimeRange {
		if err := e.Prepare(ctx, params); err != nil {
			return err
		}
	}

	return nil
}

func (c *CacheOperator) decodeAndFilterCacheEntry(cacheHit []byte, spanLogger *spanlogger.SpanLogger) ([]CachedExtent, error) {
	if cacheHit == nil {
		spanLogger.DebugLog(
			"msg", "no cache entry found",
			"hashed_key", c.hashedKey,
			"desired_time_range", c.DesiredTimeRange,
		)

		return nil, nil
	}

	var cacheEntry CacheEntry
	if err := cacheEntry.Unmarshal(cacheHit); err != nil {
		// Returning an error here is simplest, but in the future we may want to log it and continue as if there was no cached result.
		return nil, err
	}

	if !bytes.Equal(cacheEntry.CacheKey, c.key) {
		// The cache key in the entry does not match the expected key: we've encountered a hash collision, so don't use the cached results.
		spanLogger.DebugLog(
			"msg", "ignoring cache entry as the cache key in the entry does not match the expected key, presumably due to a hash collision",
			"hashed_key", c.hashedKey,
			"desired_time_range", c.DesiredTimeRange,
		)
		return nil, nil
	}

	cacheEntry.Extents = slices.DeleteFunc(cacheEntry.Extents, func(extent CachedExtent) bool {
		ttl := c.ttlForExtent(extent)
		expirationTime := timestamp.Time(extent.OldestEvaluationTime).Add(ttl)
		hasExpired := c.evaluationTime.After(expirationTime)

		if hasExpired {
			spanLogger.DebugLog(
				"msg", "ignoring cached extent as it has expired",
				"hashed_key", c.hashedKey,
				"extent_oldest_evaluation_time", extent.OldestEvaluationTime,
				"extent_end_ts", extent.EndT,
				"ttl", ttl,
				"expiration_time", expirationTime,
				"evaluation_time", c.evaluationTime,
				"desired_time_range", c.DesiredTimeRange,
			)
		}

		return hasExpired
	})

	// If we have any extents that haven't expired, include their size in the memory consumption estimate.
	for _, e := range cacheEntry.Extents {
		if err := e.addToMemoryConsumptionEstimate(c.MemoryConsumptionTracker); err != nil {
			return nil, err
		}
	}

	spanLogger.DebugLog(
		"msg", "cache hit",
		"hashed_key", c.hashedKey,
		"desired_time_range", c.DesiredTimeRange,
		"non_expired_extent_count", len(cacheEntry.Extents),
	)

	return cacheEntry.Extents, nil
}

func (c *CacheOperator) calculateExtents(ctx context.Context, existingExtents []CachedExtent, spanLogger *spanlogger.SpanLogger) (extents, error) {
	nextStartT := c.DesiredTimeRange.StartT
	nextExistingExtentIdx := 0

	// Calculate the last step within the desired time range.
	// For example, if the desired time range is T=5m to T=8m30s with a step of 1m, then the last step is T=8m.
	stepAlignedEndT := calculateLastStepAlignedPoint(c.DesiredTimeRange.StartT, c.DesiredTimeRange.EndT, c.DesiredTimeRange.IntervalMilliseconds)

	existingExtents = c.discardSmallExtents(existingExtents, stepAlignedEndT, spanLogger)

	maxFreshness, err := c.limitsProvider.GetMaxCacheFreshness(ctx)
	if err != nil {
		return extents{}, err
	}

	maxFreshnessThreshold := timestamp.FromTime(c.evaluationTime.Add(-maxFreshness))
	result := extents{
		cacheableRangeEndT: calculateLastStepAlignedPoint(c.DesiredTimeRange.StartT, min(maxFreshnessThreshold, stepAlignedEndT), c.DesiredTimeRange.IntervalMilliseconds),
	}

	spanLogger.DebugLog(
		"msg", "calculated freshness threshold",
		"max_freshness_threshold", maxFreshnessThreshold,
		"step_aligned_end_ts", stepAlignedEndT,
		"cacheable_range_end_ts", result.cacheableRangeEndT,
	)

	for nextStartT <= stepAlignedEndT {
		if nextExistingExtentIdx < len(existingExtents) && existingExtents[nextExistingExtentIdx].StartT <= nextStartT {
			extent := existingExtents[nextExistingExtentIdx]
			inDesiredTimeRange := extent.EndT >= nextStartT
			endsOneStepBeforeDesiredTimeRange := extent.EndT >= nextStartT-c.DesiredTimeRange.IntervalMilliseconds
			willWriteCacheEntry := nextStartT < maxFreshnessThreshold
			shouldMergeThisExtentWithExtentsInDesiredTimeRange := endsOneStepBeforeDesiredTimeRange && willWriteCacheEntry

			if inDesiredTimeRange || shouldMergeThisExtentWithExtentsInDesiredTimeRange {
				result.appendExtentInDesiredTimeRange(newCachedExtentReader(extent, c), extent.StartT, extent.EndT, maxFreshnessThreshold, true)

				// Note that we don't have to align the extent's end time to the step, as it would have been aligned when the extent was written.
				nextStartT = extent.EndT + c.DesiredTimeRange.IntervalMilliseconds

				c.logUsedExtent(spanLogger, extent)
			} else {
				result.cacheableExtentsBeforeDesiredTimeRange = append(result.cacheableExtentsBeforeDesiredTimeRange, extent)
				c.logUnusedExtent(spanLogger, extent, "before")
			}

			nextExistingExtentIdx++
			continue
		}

		extentStartT := nextStartT
		extentEndT := stepAlignedEndT

		// We need to evaluate some part of the desired time range.
		// Check if there's another extent that overlaps the desired time range: if so, then we don't need to evaluate the
		// time range covered by that extent.
		// (We'll add this extent to cacheableExtentsBeforeDesiredTimeRange on the next iteration of this loop.)
		if nextExistingExtentIdx < len(existingExtents) {
			nextExtent := existingExtents[nextExistingExtentIdx]
			if nextExtent.StartT <= stepAlignedEndT {
				extentEndT = nextExtent.StartT - c.DesiredTimeRange.IntervalMilliseconds
			}
		}

		timeRange := types.NewRangeQueryTimeRange(timestamp.Time(extentStartT), timestamp.Time(extentEndT), time.Duration(c.DesiredTimeRange.IntervalMilliseconds)*time.Millisecond)
		operator, err := c.Materializer.ConvertNodeToInstantVectorOperator(ctx, c.Inner, timeRange)
		if err != nil {
			return extents{}, err
		}

		extent := newEvaluatedExtent(operator, c, extentStartT, extentEndT)
		result.appendExtentInDesiredTimeRange(extent, extentStartT, extentEndT, maxFreshnessThreshold, false)

		if extentStartT < maxFreshnessThreshold {
			// Only write a cache entry if the extent is cacheable (not entirely within the max freshness window).
			result.shouldWriteCacheEntry = true
		}

		nextStartT = extentEndT + c.DesiredTimeRange.IntervalMilliseconds

		spanLogger.DebugLog(
			"msg", "will freshly evaluate at least part of the desired time range",
			"extent_start_ts", extentStartT,
			"extent_end_ts", extentEndT,
		)
	}

	// If there is an extent just after the desired time range, include that in extentsForDesiredTimeRange
	// so that it is merged with the extents in the desired time range when we write the cache entry.
	if len(existingExtents) > nextExistingExtentIdx && result.shouldWriteCacheEntry {
		extent := existingExtents[nextExistingExtentIdx]
		startsOneStepAfterDesiredTimeRange := extent.StartT <= c.DesiredTimeRange.EndT+c.DesiredTimeRange.IntervalMilliseconds

		if startsOneStepAfterDesiredTimeRange {
			result.appendExtentInDesiredTimeRange(newCachedExtentReader(extent, c), extent.StartT, extent.EndT, maxFreshnessThreshold, true)
			nextExistingExtentIdx++
		}
	}

	result.cacheableExtentsAfterDesiredTimeRange = existingExtents[nextExistingExtentIdx:]
	for _, e := range result.cacheableExtentsAfterDesiredTimeRange {
		c.logUnusedExtent(spanLogger, e, "after")
	}

	spanLogger.DebugLog(
		"msg", "finished",
		"existing_extents_before_desired_time_range", len(result.cacheableExtentsBeforeDesiredTimeRange),
		"new_or_existing_extents_in_desired_time_range", len(result.inDesiredTimeRange),
		"existing_extents_after_desired_time_range", len(result.cacheableExtentsAfterDesiredTimeRange),
		"should_write_cache_entry", result.shouldWriteCacheEntry,
		"cacheable_range_start_ts", result.cacheableRangeStartT,
		"cacheable_range_end_ts", result.cacheableRangeEndT,
	)

	return result, nil
}

func (c *CacheOperator) logUnusedExtent(spanLogger *spanlogger.SpanLogger, extent CachedExtent, timeRelationship string) {
	spanLogger.DebugLog(
		"msg", "ignoring cached extent "+timeRelationship+" desired time range",
		"extent_oldest_evaluation_time", extent.OldestEvaluationTime,
		"extent_start_ts", extent.StartT,
		"extent_end_ts", extent.EndT,
		"extent_newest_evaluation_trace_id", extent.NewestEvaluationTraceID,
		"extent_series_count", len(extent.SeriesMetadata),
	)
}

func (c *CacheOperator) logUsedExtent(spanLogger *spanlogger.SpanLogger, extent CachedExtent) {
	spanLogger.DebugLog(
		"msg", "will use cached extent for at least part of the desired time range",
		"extent_oldest_evaluation_time", extent.OldestEvaluationTime,
		"extent_start_ts", extent.StartT,
		"extent_end_ts", extent.EndT,
		"extent_newest_evaluation_trace_id", extent.NewestEvaluationTraceID,
		"extent_series_count", len(extent.SeriesMetadata),
	)
}

// discardSmallExtents removes cached extents that overlap the desired time range but are smaller than minCacheExtent,
// returning the remaining extents (still in time order). Re-evaluating these small extents as part of a larger query is
// more efficient than reading many small extents, and means future queries can reuse a single larger cached extent.
// Extents that fall entirely outside the desired time range are always retained, as they aren't re-evaluated by this
// query and so discarding them would simply throw away valid cached data.
//
// This mirrors the behaviour of the split-and-cache query middleware (see partitionCacheExtents in
// pkg/frontend/querymiddleware/results_cache.go).
func (c *CacheOperator) discardSmallExtents(existingExtents []CachedExtent, stepAlignedEndT int64, logger *spanlogger.SpanLogger) []CachedExtent {
	// Only discard small extents if the desired time range is itself large: if the query is small, re-evaluating is no
	// cheaper than reading the cached extent. Instant queries (start == end) are never affected.
	minCacheExtentMilliseconds := c.minCacheExtent.Milliseconds()
	desiredTimeRangeIsLarge := c.DesiredTimeRange.StartT != c.DesiredTimeRange.EndT &&
		c.DesiredTimeRange.EndT-c.DesiredTimeRange.StartT > minCacheExtentMilliseconds

	if c.minCacheExtent <= 0 || !desiredTimeRangeIsLarge {
		return existingExtents
	}

	kept := existingExtents[:0]

	for _, extent := range existingExtents {
		extentIsSmall := extent.EndT-extent.StartT < minCacheExtentMilliseconds
		overlapsDesiredTimeRange := extent.EndT >= c.DesiredTimeRange.StartT && extent.StartT <= stepAlignedEndT

		if extentIsSmall && overlapsDesiredTimeRange {
			// We won't read this extent, so release the memory we reserved for it in fetchExistingExtents.
			logger.DebugLog(
				"msg", "ignoring small extent in desired time range",
				"extent_oldest_evaluation_time", extent.OldestEvaluationTime,
				"extent_start_ts", extent.StartT,
				"extent_end_ts", extent.EndT,
				"extent_newest_evaluation_trace_id", extent.NewestEvaluationTraceID,
				"extent_series_count", len(extent.SeriesMetadata),
			)

			extent.close(c.MemoryConsumptionTracker)
			continue
		}

		kept = append(kept, extent)
	}

	// Clear any extents still retained in the slice, so they can be garbage collected.
	clear(existingExtents[len(kept):])

	return kept
}

// calculateLastStepAlignedPoint returns the last step-aligned point within the given time range.
func calculateLastStepAlignedPoint(start int64, end int64, step int64) int64 {
	return end - ((end - start) % step)
}

type extents struct {
	// cacheableExtentsBeforeDesiredTimeRange and cacheableExtentsAfterDesiredTimeRange contain extents entirely before and after
	// the desired time range, respectively, that are eligible to be written back to the cache.
	// The extents are not immediately adjacent to other extents, and so do not need to be merged with other extents
	// before being written back to the cache.
	// All extents are in time order (earliest extents first).
	cacheableExtentsBeforeDesiredTimeRange []CachedExtent
	cacheableExtentsAfterDesiredTimeRange  []CachedExtent

	// shouldWriteCacheEntry is true if any part of the desired time range will be freshly evaluated and that freshly evaluated data
	// is eligible to be written back to the cache.
	shouldWriteCacheEntry bool

	// inDesiredTimeRange contains all extents that contain data for the desired time range, in time order (earliest extents first).
	// If there are any existing extents that are immediately adjacent to the desired time range, they are also included here so that they
	// can be merged with the extents inside the desired time range, rather than fragmenting the cache entry.
	inDesiredTimeRange []extent

	// cacheableRangeStartT and cacheableRangeEndT are the start and end timestamps of the cacheable range of the extents in inDesiredTimeRange.
	// These may extend beyond the desired time range if there are existing extents that extend beyond the desired time range.
	// cacheableRangeEndT will never exceed the max freshness threshold.
	cacheableRangeStartT int64
	cacheableRangeEndT   int64
}

// appendExtentInDesiredTimeRange appends an extent to the list of extents for the desired time range.
// It must be called in time order (earliest extents first).
func (e *extents) appendExtentInDesiredTimeRange(extent extent, startT int64, endT int64, maxFreshnessThreshold int64, isCachedExtent bool) {
	if len(e.inDesiredTimeRange) == 0 {
		e.cacheableRangeStartT = startT
	}

	if isCachedExtent && endT > e.cacheableRangeEndT {
		// If this cached extent extends beyond the desired time range, then don't drop any samples between the end of the desired time range
		// and the max freshness threshold.
		e.cacheableRangeEndT = min(endT, maxFreshnessThreshold)
	}

	e.inDesiredTimeRange = append(e.inDesiredTimeRange, extent)
}

func (c *CacheOperator) AfterPrepare(ctx context.Context) error {
	for _, e := range c.extents.inDesiredTimeRange {
		if err := e.AfterPrepare(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (c *CacheOperator) SeriesMetadata(ctx context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	// Pass nil matchers: unlike TimeRangeSplitOperator, CacheOperator does not push matchers down, as we
	// need the full set of series to store in the cache.
	series, outputSeries, err := mergeSeriesMetadataFromMultipleSources(ctx, c.extents.inDesiredTimeRange, nil, c.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	c.outputSeries = outputSeries

	if c.extents.shouldWriteCacheEntry {
		if err := c.bufferSeriesMetadataForCacheEntry(series); err != nil {
			return nil, err
		}

		c.data = make([]types.InstantVectorSeriesData, len(series))
	}

	return series, nil
}

func (c *CacheOperator) bufferSeriesMetadataForCacheEntry(series []types.SeriesMetadata) error {
	// Callers of SeriesMetadata are permitted to modify the returned slice, so
	// make a clone of the slice so we can store it as-is in the cache.
	var err error
	c.seriesMetadata, err = types.SeriesMetadataSlicePool.Get(len(series), c.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	c.seriesMetadata = append(c.seriesMetadata, series...)

	for _, s := range series {
		// Even though both slices share a shallow copy of the immutable labels, we need to increase the memory consumption
		// estimate for the retained slice's labels, as otherwise the memory consumption estimate will be incorrect when
		// the first slice is returned to the pool by the caller of SeriesMetadata.
		if err := c.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(s.Labels); err != nil {
			return err
		}
	}

	return nil
}

func (c *CacheOperator) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	desiredTimeRangeData, cacheableTimeRangeData, seriesIdx, err := c.getDataForNextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if c.extents.shouldWriteCacheEntry {
		c.data[seriesIdx] = cacheableTimeRangeData
	}

	return desiredTimeRangeData, nil
}

func (c *CacheOperator) getDataForNextSeries(ctx context.Context) (desiredTimeRangeData types.InstantVectorSeriesData, cacheableTimeRangeData types.InstantVectorSeriesData, seriesIdx int, err error) {
	var sourceSeriesIndices []int
	thisSeriesIndex := c.nextOutputSeriesIdx
	c.nextOutputSeriesIdx++

	if len(c.extents.inDesiredTimeRange) == 1 {
		// We only have one extent, and therefore outputSeries isn't populated because we'll just return all series from that
		// sole extent, so we have to construct it here.
		sourceSeriesIndices = []int{thisSeriesIndex}
	} else {
		if thisSeriesIndex >= len(c.outputSeries) {
			// We've reached the end of the output series, so we're done.
			// We don't need a similar check in the one-extent case above, as we rely on the inner operator to return EOS in that case.
			return types.InstantVectorSeriesData{}, types.InstantVectorSeriesData{}, -1, types.EOS
		}

		sourceSeriesIndices = c.outputSeries[thisSeriesIndex].sourceSeriesIndices
		defer types.IntSlicePool.Put(&c.outputSeries[thisSeriesIndex].sourceSeriesIndices, c.MemoryConsumptionTracker)
	}

	for extentIdx, sourceSeriesIdx := range sourceSeriesIndices {
		if sourceSeriesIdx == -1 {
			continue
		}

		extent := c.extents.inDesiredTimeRange[extentIdx]
		data, err := extent.GetSeries(ctx, sourceSeriesIdx)
		if err != nil {
			return types.InstantVectorSeriesData{}, types.InstantVectorSeriesData{}, -1, err
		}

		if err := c.accumulateDataForSeries(data, &desiredTimeRangeData, &cacheableTimeRangeData); err != nil {
			return types.InstantVectorSeriesData{}, types.InstantVectorSeriesData{}, -1, err
		}
	}

	return desiredTimeRangeData, cacheableTimeRangeData, thisSeriesIndex, nil
}

func (c *CacheOperator) accumulateDataForSeries(data types.InstantVectorSeriesData, desiredTimeRangeData *types.InstantVectorSeriesData, cacheableTimeRangeData *types.InstantVectorSeriesData) error {
	if c.extents.shouldWriteCacheEntry {
		if err := c.accumulateCacheableFloats(data, cacheableTimeRangeData); err != nil {
			return err
		}

		if err := c.accumulateCacheableHistograms(data, cacheableTimeRangeData); err != nil {
			return err
		}
	}

	if err := c.accumulateDesiredFloats(data, desiredTimeRangeData); err != nil {
		return err
	}

	if err := c.accumulateDesiredHistograms(data, desiredTimeRangeData); err != nil {
		return err
	}

	return nil
}

func (c *CacheOperator) accumulateCacheableFloats(data types.InstantVectorSeriesData, cacheableTimeRangeData *types.InstantVectorSeriesData) error {
	if len(data.Floats) == 0 {
		return nil
	}

	firstCacheableIndex := c.indexOfFirstFPointAtOrAfter(data.Floats, c.extents.cacheableRangeStartT)
	if firstCacheableIndex == -1 {
		return nil
	}

	lastCacheableIndex := c.indexOfLastFPointAtOrBefore(data.Floats, c.extents.cacheableRangeEndT)
	if lastCacheableIndex < firstCacheableIndex {
		return nil
	}

	var err error
	cacheableTimeRangeData.Floats, err = types.FPointSlicePool.AppendToSlice(cacheableTimeRangeData.Floats, c.MemoryConsumptionTracker, data.Floats[firstCacheableIndex:lastCacheableIndex+1]...)
	if err != nil {
		return err
	}

	return nil
}

// accumulateCacheableHistograms accumulates cacheable histogram points from the given data into cacheableTimeRangeData.
// It clones any accumulated FloatHistogram instances.
func (c *CacheOperator) accumulateCacheableHistograms(data types.InstantVectorSeriesData, cacheableTimeRangeData *types.InstantVectorSeriesData) error {
	if len(data.Histograms) == 0 {
		return nil
	}

	firstCacheableIndex := c.indexOfFirstHPointAtOrAfter(data.Histograms, c.extents.cacheableRangeStartT)
	if firstCacheableIndex == -1 {
		return nil
	}

	lastCacheableIndex := c.indexOfLastHPointAtOrBefore(data.Histograms, c.extents.cacheableRangeEndT)
	if lastCacheableIndex < firstCacheableIndex {
		return nil
	}

	var err error
	cacheableTimeRangeData.Histograms, err = types.AppendHPointCopies(cacheableTimeRangeData.Histograms, data.Histograms[firstCacheableIndex:lastCacheableIndex+1], c.MemoryConsumptionTracker)
	return err
}

// accumulateDesiredFloats accumulates desired float points from the given data into desiredTimeRangeData.
// It takes ownership of data.Floats, either returning it as desiredTimeRangeData.Floats or returning it to a pool.
// It expects that data.Floats has already been accounted for in the memory consumption estimate.
func (c *CacheOperator) accumulateDesiredFloats(data types.InstantVectorSeriesData, desiredTimeRangeData *types.InstantVectorSeriesData) error {
	if len(data.Floats) == 0 {
		types.FPointSlicePool.Put(&data.Floats, c.MemoryConsumptionTracker)
		return nil
	}

	firstDesiredIndex := c.indexOfFirstFPointAtOrAfter(data.Floats, c.DesiredTimeRange.StartT)
	if firstDesiredIndex == -1 {
		types.FPointSlicePool.Put(&data.Floats, c.MemoryConsumptionTracker)
		return nil
	}

	lastDesiredIndex := c.indexOfLastFPointAtOrBefore(data.Floats, c.DesiredTimeRange.EndT)
	if lastDesiredIndex < firstDesiredIndex {
		types.FPointSlicePool.Put(&data.Floats, c.MemoryConsumptionTracker)
		return nil
	}

	if desiredTimeRangeData.Floats == nil {
		// We don't already have a slice, so reuse data.Floats.
		pointCount := lastDesiredIndex - firstDesiredIndex + 1

		if firstDesiredIndex > 0 {
			copy(data.Floats[0:pointCount], data.Floats[firstDesiredIndex:lastDesiredIndex+1])
		}

		desiredTimeRangeData.Floats = data.Floats[:pointCount]
	} else {
		var err error
		desiredTimeRangeData.Floats, err = types.FPointSlicePool.AppendToSlice(desiredTimeRangeData.Floats, c.MemoryConsumptionTracker, data.Floats[firstDesiredIndex:lastDesiredIndex+1]...)
		if err != nil {
			return err
		}

		types.FPointSlicePool.Put(&data.Floats, c.MemoryConsumptionTracker)
	}

	return nil
}

// accumulateDesiredHistograms accumulates desired histograms points from the given data into desiredTimeRangeData.
// It takes ownership of data.Histograms, either returning it as desiredTimeRangeData.Histograms or returning it to a pool.
// It expects that data.Floats has already been accounted for in the memory consumption estimate.
func (c *CacheOperator) accumulateDesiredHistograms(data types.InstantVectorSeriesData, desiredTimeRangeData *types.InstantVectorSeriesData) error {
	if len(data.Histograms) == 0 {
		types.HPointSlicePool.Put(&data.Histograms, c.MemoryConsumptionTracker)
		return nil
	}

	firstDesiredIndex := c.indexOfFirstHPointAtOrAfter(data.Histograms, c.DesiredTimeRange.StartT)
	if firstDesiredIndex == -1 {
		types.HPointSlicePool.Put(&data.Histograms, c.MemoryConsumptionTracker)
		return nil
	}

	lastDesiredIndex := c.indexOfLastHPointAtOrBefore(data.Histograms, c.DesiredTimeRange.EndT)
	if lastDesiredIndex < firstDesiredIndex {
		types.HPointSlicePool.Put(&data.Histograms, c.MemoryConsumptionTracker)
		return nil
	}

	if desiredTimeRangeData.Histograms == nil {
		// We don't already have a slice, so reuse data.Histograms.
		pointCount := lastDesiredIndex - firstDesiredIndex + 1

		if firstDesiredIndex > 0 {
			copy(data.Histograms[0:pointCount], data.Histograms[firstDesiredIndex:lastDesiredIndex+1])
		}

		clear(data.Histograms[pointCount:len(data.Histograms)]) // Clear any values remaining in the slice so we don't have two HPoint instances that reference the same FloatHistogram instance.
		desiredTimeRangeData.Histograms = data.Histograms[:pointCount]
	} else {
		var err error
		desiredTimeRangeData.Histograms, err = types.HPointSlicePool.AppendToSlice(desiredTimeRangeData.Histograms, c.MemoryConsumptionTracker, data.Histograms[firstDesiredIndex:lastDesiredIndex+1]...)
		if err != nil {
			return err
		}

		// AppendToSlice copied the desired points into desiredTimeRangeData, but those copies share their FloatHistogram
		// instances with data.Histograms. Clear the copied range before returning data.Histograms to the pool so the
		// pooled slice doesn't retain references to instances now owned by desiredTimeRangeData: otherwise a later reuse
		// of the pooled slice could alias those instances and corrupt the result.
		clear(data.Histograms[firstDesiredIndex : lastDesiredIndex+1])
		types.HPointSlicePool.Put(&data.Histograms, c.MemoryConsumptionTracker)
	}

	return nil
}

func (c *CacheOperator) indexOfFirstFPointAtOrAfter(p []promql.FPoint, t int64) int {
	return slices.IndexFunc(p, func(p promql.FPoint) bool { return p.T >= t })
}

func (c *CacheOperator) indexOfLastFPointAtOrBefore(p []promql.FPoint, t int64) int {
	return sliceutil.BackwardsIndexFunc(p, func(p promql.FPoint) bool { return p.T <= t })
}

func (c *CacheOperator) indexOfFirstHPointAtOrAfter(p []promql.HPoint, t int64) int {
	return slices.IndexFunc(p, func(p promql.HPoint) bool { return p.T >= t })
}

func (c *CacheOperator) indexOfLastHPointAtOrBefore(p []promql.HPoint, t int64) int {
	return sliceutil.BackwardsIndexFunc(p, func(p promql.HPoint) bool { return p.T <= t })
}

func (c *CacheOperator) FinishedReading(ctx context.Context) error {
	c.finishedReading = true

	for _, e := range c.extents.inDesiredTimeRange {
		if err := e.FinishedReading(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (c *CacheOperator) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	desiredTimeRangeStats, cacheableTimeRangeStats, annos, err := c.finalizeExtents(ctx)
	if err != nil {
		return nil, nil, err
	}

	if c.extents.shouldWriteCacheEntry {
		defer cacheableTimeRangeStats.Close()

		if err := c.writeCacheEntry(ctx, cacheableTimeRangeStats, annos); err != nil {
			return nil, nil, err
		}
	}

	queryDetails := querydetails.QueryDetailsFromContext(ctx)

	for _, e := range c.extents.inDesiredTimeRange {
		if size, cacheHit := e.GetEstimatedSize(); cacheHit {
			if queryDetails != nil {
				queryDetails.ResultsCacheHitBytes += int(size)
			}
			c.metrics.UsedExtents.Inc()
		} else {
			if queryDetails != nil {
				queryDetails.ResultsCacheMissBytes += int(size)
			}
			c.metrics.EvaluatedExtents.Inc()
		}
	}

	return desiredTimeRangeStats, annos, nil
}

func (c *CacheOperator) finalizeExtents(ctx context.Context) (desiredTimeRangeStats *types.OperatorEvaluationStats, cacheableTimeRangeStats *types.OperatorEvaluationStats, annos annotations.Annotations, err error) {
	for _, extent := range c.extents.inDesiredTimeRange {
		extentStats, extentAnnotations, err := extent.Finalize(ctx)
		if err != nil {
			return nil, nil, nil, err
		}

		if extent.GetEndT() >= c.DesiredTimeRange.StartT && extent.GetStartT() <= c.DesiredTimeRange.EndT {
			// Only try to add the extent to the desired time range stats if it overlaps with the desired time range.
			// We might have an extent that starts or ends one step either side of the desired time range, and we want to merge them into
			// a single extent with the extents inside the desired time range, but they don't contribute to the stats
			// (and AddSubRange below will return an error if they don't overlap with the desired time range).

			if desiredTimeRangeStats == nil {
				desiredTimeRangeStats, err = types.NewOperatorEvaluationStats(ctx, c.DesiredTimeRange, c.MemoryConsumptionTracker, extentStats.GetSubsetCount())
				if err != nil {
					return nil, nil, nil, err
				}
			}

			if err := desiredTimeRangeStats.AddSubRange(extentStats); err != nil {
				return nil, nil, nil, err
			}
		}

		if c.extents.shouldWriteCacheEntry && extent.GetStartT() <= c.extents.cacheableRangeEndT {
			// Only try to add the extent to the cached time range stats if it overlaps with the cached time range.
			// We might have an extent that starts after the cached time range (due to the max freshness window),
			// and AddSubRange below will return an error if they don't overlap with the cached time range.
			if cacheableTimeRangeStats == nil {
				cacheableTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(c.extents.cacheableRangeStartT), timestamp.Time(c.extents.cacheableRangeEndT), time.Duration(c.DesiredTimeRange.IntervalMilliseconds)*time.Millisecond)
				cacheableTimeRangeStats, err = types.NewOperatorEvaluationStats(ctx, cacheableTimeRange, c.MemoryConsumptionTracker, extentStats.GetSubsetCount())
				if err != nil {
					return nil, nil, nil, err
				}
			}

			if err := cacheableTimeRangeStats.AddSubRange(extentStats); err != nil {
				return nil, nil, nil, err
			}
		}

		extentStats.Close()

		annos.Merge(extentAnnotations)
	}

	return desiredTimeRangeStats, cacheableTimeRangeStats, annos, nil
}

func (c *CacheOperator) writeCacheEntry(ctx context.Context, stats *types.OperatorEvaluationStats, annos annotations.Annotations) error {
	if c.seriesMetadata == nil {
		// SeriesMetadata() never called, don't write cache entry.
		return nil
	}

	if c.nextOutputSeriesIdx < len(c.seriesMetadata) {
		// Not all series were read, don't write cache entry.
		return nil
	}

	if !c.finishedReading {
		return errors.New("CacheOperator.writeCacheEntry() called (via Finalize()) before FinishedReading(), this should never happen")
	}

	extents := make([]CachedExtent, 0, len(c.extents.cacheableExtentsBeforeDesiredTimeRange)+len(c.extents.cacheableExtentsAfterDesiredTimeRange)+1)
	extents = append(extents, c.extents.cacheableExtentsBeforeDesiredTimeRange...)

	traceID, _ := c.getCurrentTraceID(ctx)

	seriesMetadata, data := c.encodeSeriesForCacheEntry()

	desiredTimeRangeExtent := CachedExtent{
		StartT:                  c.extents.cacheableRangeStartT,
		EndT:                    c.extents.cacheableRangeEndT,
		SeriesMetadata:          seriesMetadata,
		Data:                    data,
		Annotations:             querierpb.EncodeAnnotations(annos, c.queryParameters.OriginalExpression),
		Stats:                   stats.Encode(),
		NewestEvaluationTraceID: traceID,
		OldestEvaluationTime:    c.determineNewExtentEvaluationTimestamp(),
	}

	extents = append(extents, desiredTimeRangeExtent)
	extents = append(extents, c.extents.cacheableExtentsAfterDesiredTimeRange...)

	entry := CacheEntry{
		CacheKey: c.key,
		Extents:  extents,
	}

	ttl := c.ttlForExtents(extents)

	// FIXME: this is potentially a large amount of memory, but we don't have a good way to consider it as part of the memory consumption estimate,
	// as there's no way for us to know when the async set below is complete, and the cache client could also drop the set altogether if the
	// entry is too large or its internal queue is full.
	// Maybe we could simply include it in the memory consumption estimate for the rest of the life of the query evaluation?
	value, err := entry.Marshal()
	if err != nil {
		return err
	}

	spanLogger := spanlogger.FromContext(ctx, c.logger)
	spanLogger.DebugLog(
		"msg", "storing new entry in cache",
		"key", c.hashedKey,
		"extent_count", len(extents),
		"entry_size_bytes", len(value),
	)

	if err := c.Backend.SetAsync(ctx, c.hashedKey, value, ttl); err != nil {
		return fmt.Errorf("storing cached results with key %q: %w", c.hashedKey, err)
	}

	return nil
}

func (c *CacheOperator) ttlForExtents(extents []CachedExtent) time.Duration {
	if len(extents) == 0 {
		return 0
	}

	ttl := c.ttlForExtent(extents[0])

	for _, e := range extents[1:] {
		ttl = min(ttl, c.ttlForExtent(e))
	}

	return ttl
}

func (c *CacheOperator) ttlForExtent(extent CachedExtent) time.Duration {
	oooThreshold := timestamp.Time(extent.OldestEvaluationTime).Add(-c.oooWindow)

	if c.oooWindow != 0 && timestamp.Time(extent.EndT).After(oooThreshold) {
		return c.ttlForOOOExtent
	}

	return c.ttlForNonOOOExtent
}

func (c *CacheOperator) determineNewExtentEvaluationTimestamp() int64 {
	return slices.MinFunc(c.extents.inDesiredTimeRange, func(a, b extent) int {
		return int(a.GetEvaluationTimestamp() - b.GetEvaluationTimestamp())
	}).GetEvaluationTimestamp()
}

// encodeSeriesForCacheEntry encodes the series metadata and data to be written to the cache.
// Series with no samples in the cacheable time range are not stored: they would only consume cache space,
// and an absent series is equivalent to an empty one when the extent is read back and merged with others.
func (c *CacheOperator) encodeSeriesForCacheEntry() ([]querierpb.SeriesMetadata, []querierpb.InstantVectorSeriesData) {
	seriesMetadata := make([]querierpb.SeriesMetadata, 0, len(c.data))
	data := make([]querierpb.InstantVectorSeriesData, 0, len(c.data))

	for i, d := range c.data {
		if len(d.Floats) == 0 && len(d.Histograms) == 0 {
			// Don't store empty series in the cache.
			continue
		}

		seriesMetadata = append(seriesMetadata, querierpb.EncodeSeriesMetadata(c.seriesMetadata[i]))

		// EncodeInstantVectorSeriesData does unsafe casts and does not copy the data from the slices, but this is OK as we're immediately
		// serializing the message and writing it to the cache before these slices are returned to their respective pools.
		data = append(data, querierpb.EncodeInstantVectorSeriesData(d))
	}

	return seriesMetadata, data
}

func (c *CacheOperator) ExpressionPosition() posrange.PositionRange {
	return c.expressionPosition
}

func (c *CacheOperator) Close() {
	for _, s := range [][]CachedExtent{c.extents.cacheableExtentsBeforeDesiredTimeRange, c.extents.cacheableExtentsAfterDesiredTimeRange} {
		for _, e := range s {
			e.close(c.MemoryConsumptionTracker)
		}
	}

	c.extents.cacheableExtentsBeforeDesiredTimeRange = nil
	c.extents.cacheableExtentsAfterDesiredTimeRange = nil

	for _, e := range c.extents.inDesiredTimeRange {
		e.Close()
	}

	c.extents.inDesiredTimeRange = nil

	if len(c.outputSeries) > 0 && c.nextOutputSeriesIdx < len(c.outputSeries) {
		for _, e := range c.outputSeries[c.nextOutputSeriesIdx:] {
			types.IntSlicePool.Put(&e.sourceSeriesIndices, c.MemoryConsumptionTracker)
		}

		c.outputSeries = nil
	}

	types.SeriesMetadataSlicePool.Put(&c.seriesMetadata, c.MemoryConsumptionTracker)

	for _, d := range c.data {
		types.PutInstantVectorSeriesData(d, c.MemoryConsumptionTracker)
	}

	c.data = nil

}

type extent interface {
	GetStartT() int64
	GetEndT() int64

	Prepare(ctx context.Context, params *types.PrepareParams) error
	AfterPrepare(ctx context.Context) error
	FinishedReading(ctx context.Context) error

	// SeriesMetadata returns the metadata for all series in this extent.
	// Callers may modify the returned slice.
	SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error)

	// GetSeries returns the samples for the series at the given index.
	// Callers may modify the returned slices.
	// The returned slices must be accounted for in the memory consumption estimate.
	GetSeries(ctx context.Context, seriesIdx int) (types.InstantVectorSeriesData, error)

	Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error)

	GetEvaluationTimestamp() int64

	// GetEstimatedSize() returns an estimate of the size of the extent, in bytes, and true if
	// this extent was a cache hit.
	GetEstimatedSize() (uint64, bool)

	Close()
}

type cachedExtentReader struct {
	extent CachedExtent
	parent *CacheOperator

	sizeEstimator *extentSizeEstimator
}

func newCachedExtentReader(extent CachedExtent, parent *CacheOperator) *cachedExtentReader {
	return &cachedExtentReader{extent: extent, parent: parent, sizeEstimator: &extentSizeEstimator{}}
}

func (c *cachedExtentReader) GetStartT() int64 {
	return c.extent.StartT
}

func (c *cachedExtentReader) GetEndT() int64 {
	return c.extent.EndT
}

func (c *cachedExtentReader) Prepare(_ context.Context, _ *types.PrepareParams) error {
	// Nothing to do.
	return nil
}

func (c *cachedExtentReader) AfterPrepare(_ context.Context) error {
	// Nothing to do.
	return nil
}

func (c *cachedExtentReader) SeriesMetadata(_ context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	seriesMetadata, err := types.SeriesMetadataSlicePool.Get(len(c.extent.SeriesMetadata), c.parent.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	seriesMetadata = seriesMetadata[:len(c.extent.SeriesMetadata)]

	for i, entry := range c.extent.SeriesMetadata {
		seriesMetadata[i] = querierpb.DecodeSeriesMetadata(entry)
		if err := c.parent.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(seriesMetadata[i].Labels); err != nil {
			return nil, err
		}
	}

	c.extent.closeSeriesMetadata(c.parent.MemoryConsumptionTracker)
	c.sizeEstimator.AccumulateSeriesMetadata(seriesMetadata)

	return seriesMetadata, nil
}

func (c *cachedExtentReader) GetSeries(_ context.Context, seriesIdx int) (types.InstantVectorSeriesData, error) {
	if seriesIdx >= len(c.extent.Data) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	cachedData := c.extent.Data[seriesIdx]

	// DecodeInstantVectorSeriesData returns shallow copies of the FPoint / HPoint slices.
	// This is OK as CacheOperator.NextSeries will copy any data it needs to retain for the merged extent before returning it to the caller.
	// We don't need to add this to the memory consumption estimate as this was already accounted for in CachedExtent.addToMemoryConsumptionEstimate,
	// called from decodeAndFilterCacheEntry.
	mqeData := querierpb.DecodeInstantVectorSeriesData(cachedData)

	// Clear the cached data so we don't try to remove it from the memory consumption estimate later.
	// We don't need to retain this data here if this extent will be merged into others: that is the responsibility of CacheOperator.
	c.extent.Data[seriesIdx] = querierpb.InstantVectorSeriesData{}

	c.sizeEstimator.AccumulateSeriesData(mqeData)

	var err error

	if mqeData.Floats, err = types.EnsureFPointSliceCapacityIsPowerOfTwo(mqeData.Floats, c.parent.MemoryConsumptionTracker); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if mqeData.Histograms, err = types.EnsureHPointSliceCapacityIsPowerOfTwo(mqeData.Histograms, c.parent.MemoryConsumptionTracker); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return mqeData, nil
}

func (c *cachedExtentReader) FinishedReading(_ context.Context) error {
	c.extent.closeSeriesMetadata(c.parent.MemoryConsumptionTracker)
	c.extent.closeRemainingData(c.parent.MemoryConsumptionTracker)
	return nil
}

func (c *cachedExtentReader) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	stats, err := c.extent.Stats.Decode(ctx, c.parent.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, err
	}

	return stats, c.extent.Annotations.Decode(), nil
}

func (c *cachedExtentReader) GetEvaluationTimestamp() int64 {
	return c.extent.OldestEvaluationTime
}

func (c *cachedExtentReader) GetEstimatedSize() (uint64, bool) {
	return c.sizeEstimator.estimatedSizeBytes, true
}

func (c *cachedExtentReader) Close() {
	c.extent.close(c.parent.MemoryConsumptionTracker)
}

type evaluatedExtent struct {
	inner  types.InstantVectorOperator
	parent *CacheOperator
	buffer *operators.InstantVectorOperatorBuffer
	startT int64
	endT   int64

	sizeEstimator *extentSizeEstimator
}

func newEvaluatedExtent(inner types.InstantVectorOperator, parent *CacheOperator, startT int64, endT int64) *evaluatedExtent {
	return &evaluatedExtent{
		inner:         inner,
		parent:        parent,
		startT:        startT,
		endT:          endT,
		sizeEstimator: &extentSizeEstimator{},
	}
}

func (e *evaluatedExtent) GetStartT() int64 {
	return e.startT
}

func (e *evaluatedExtent) GetEndT() int64 {
	return e.endT
}

func (e *evaluatedExtent) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return e.inner.Prepare(ctx, params)
}

func (e *evaluatedExtent) AfterPrepare(ctx context.Context) error {
	return e.inner.AfterPrepare(ctx)
}

func (e *evaluatedExtent) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	metadata, err := e.inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	e.sizeEstimator.AccumulateSeriesMetadata(metadata)
	e.buffer = operators.NewInstantVectorOperatorBuffer(e.inner, nil, len(metadata)-1, e.parent.MemoryConsumptionTracker)

	return metadata, nil
}

func (e *evaluatedExtent) GetSeries(ctx context.Context, seriesIdx int) (types.InstantVectorSeriesData, error) {
	series, err := e.buffer.GetSeries(ctx, []int{seriesIdx})
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := series[0]
	e.sizeEstimator.AccumulateSeriesData(data)

	return data, nil
}

func (e *evaluatedExtent) FinishedReading(ctx context.Context) error {
	if e.buffer != nil {
		e.buffer.FinishedReading()
	}

	return e.inner.FinishedReading(ctx)
}

func (e *evaluatedExtent) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return e.inner.Finalize(ctx)
}

func (e *evaluatedExtent) GetEvaluationTimestamp() int64 {
	return timestamp.FromTime(e.parent.evaluationTime)
}

func (e *evaluatedExtent) GetEstimatedSize() (uint64, bool) {
	return e.sizeEstimator.estimatedSizeBytes, false
}

func (e *evaluatedExtent) Close() {
	e.inner.Close()
}

type InstantVectorMaterializer interface {
	ConvertNodeToInstantVectorOperator(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error)
}

type LimitsProvider interface {
	GetMinResultsCacheTTL(ctx context.Context) (time.Duration, error)
	GetMinOutOfOrderResultsCacheTTL(ctx context.Context) (time.Duration, error)
	GetMaxCacheFreshness(ctx context.Context) (time.Duration, error)
	GetMaxOutOfOrderTimeWindow(ctx context.Context) (time.Duration, error)
	AllowCachingUnalignedQueries(ctx context.Context) (bool, error)
}

// addToMemoryConsumptionEstimate estimates the memory consumption of this extent and adds it to the given memory consumption tracker.
func (e *CachedExtent) addToMemoryConsumptionEstimate(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(e.SeriesMetadata))*types.SeriesMetadataSize, limiter.SeriesMetadataSlices); err != nil {
		return err
	}

	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(e.estimateLabelsMemoryConsumption(), limiter.Labels); err != nil {
		return err
	}

	fPointCount := uint64(0)
	hPointCount := uint64(0)

	for _, d := range e.Data {
		fPointCount += uint64(cap(d.Floats))
		hPointCount += uint64(cap(d.Histograms))
	}

	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(fPointCount*types.FPointSize, limiter.FPointSlices); err != nil {
		return err
	}

	if err := memoryConsumptionTracker.IncreaseMemoryConsumption(hPointCount*types.HPointSize, limiter.HPointSlices); err != nil {
		return err
	}

	return nil
}

func (e *CachedExtent) estimateLabelsMemoryConsumption() uint64 {
	labelsSize := uint64(0)

	for _, s := range e.SeriesMetadata {
		labelsSize += estimateLabelsSize(s.Labels)
	}

	return labelsSize
}

func (e *CachedExtent) closeSeriesMetadata(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	memoryConsumptionTracker.DecreaseMemoryConsumption(uint64(cap(e.SeriesMetadata))*types.SeriesMetadataSize, limiter.SeriesMetadataSlices)
	memoryConsumptionTracker.DecreaseMemoryConsumption(e.estimateLabelsMemoryConsumption(), limiter.Labels)
	e.SeriesMetadata = nil
}

func (e *CachedExtent) closeRemainingData(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	fPointCount := uint64(0)
	hPointCount := uint64(0)

	for _, d := range e.Data {
		fPointCount += uint64(cap(d.Floats))
		hPointCount += uint64(cap(d.Histograms))
	}

	memoryConsumptionTracker.DecreaseMemoryConsumption(fPointCount*types.FPointSize, limiter.FPointSlices)
	memoryConsumptionTracker.DecreaseMemoryConsumption(hPointCount*types.HPointSize, limiter.HPointSlices)

	e.Data = nil
}

func (e *CachedExtent) close(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	e.closeSeriesMetadata(memoryConsumptionTracker)
	e.closeRemainingData(memoryConsumptionTracker)
}

func estimateLabelsSize(labels []mimirpb.LabelAdapter) uint64 {
	var size uint64
	for _, l := range labels {
		size += uint64(len(l.Name)) + uint64(len(l.Value))
	}
	return size
}

type extentSizeEstimator struct {
	estimatedSizeBytes uint64
}

func (e *extentSizeEstimator) AccumulateSeriesMetadata(series []types.SeriesMetadata) {
	e.estimatedSizeBytes += uint64(len(series)) * types.SeriesMetadataSize
	for _, s := range series {
		e.estimatedSizeBytes += s.Labels.ByteSize()
	}
}

func (e *extentSizeEstimator) AccumulateSeriesData(data types.InstantVectorSeriesData) {
	e.estimatedSizeBytes += uint64(len(data.Floats)) * types.FPointSize
	e.estimatedSizeBytes += uint64(len(data.Histograms)) * types.HPointSize
}

func PrepareCacheOperators(ctx context.Context, params *types.PrepareParams, operators []*CacheOperator, now time.Time) error {
	if len(operators) == 0 {
		return nil
	}

	logger := operators[0].logger // We don't bother checking that each operator has the same logger and metrics like we do for the limits provider or cache backend below, as this won't affect query correctness, just debug logging and metrics.
	metrics := operators[0].metrics
	spanLogger, ctx := spanlogger.New(ctx, logger, tracer, "PrepareCacheOperators")
	defer spanLogger.Finish()
	spanLogger.SetTag("operator_count", len(operators))

	limitsProvider := operators[0].limitsProvider
	cacheBackend := operators[0].Backend
	operatorMap := make(map[string]*CacheOperator, len(operators))
	keys := make([]string, 0, len(operators))

	for _, o := range operators {
		if o.prepared {
			return errors.New("one or more cache operators already prepared")
		}

		if o.limitsProvider != limitsProvider || o.Backend != cacheBackend {
			return errors.New("cache operators prepared together must use the same limits provider and cache backend")
		}

		if err := o.populateCacheKey(ctx); err != nil {
			return err
		}

		if _, ok := operatorMap[o.hashedKey]; ok {
			return fmt.Errorf("cache operators prepared together must have unique cache keys, but have at least two with key %q", o.hashedKey)
		}

		operatorMap[o.hashedKey] = o
		keys = append(keys, o.hashedKey)
	}

	if err := populateTTLs(ctx, operators, limitsProvider, now); err != nil {
		return err
	}

	spanLogger.DebugLog("msg", "fetching cached results", "keys", keys)
	metrics.CacheRequests.Add(float64(len(keys)))
	cacheHits, err := cacheBackend.GetMulti(ctx, keys)
	if err != nil {
		return fmt.Errorf("fetching cached results: %w", err)
	}

	metrics.CacheHits.Add(float64(len(cacheHits)))

	for key, o := range operatorMap {
		cacheHit := cacheHits[key]

		if err := o.prepareFrom(ctx, cacheHit, params); err != nil {
			return err
		}
	}

	return nil
}

func populateTTLs(ctx context.Context, operators []*CacheOperator, limitsProvider LimitsProvider, now time.Time) error {
	ttlForNonOOOExtent, err := limitsProvider.GetMinResultsCacheTTL(ctx)
	if err != nil {
		return err
	}

	ttlForOOOExtent, err := limitsProvider.GetMinOutOfOrderResultsCacheTTL(ctx)
	if err != nil {
		return err
	}

	oooWindow, err := limitsProvider.GetMaxOutOfOrderTimeWindow(ctx)
	if err != nil {
		return err
	}

	for _, o := range operators {
		o.ttlForNonOOOExtent = ttlForNonOOOExtent
		o.ttlForOOOExtent = ttlForOOOExtent
		o.oooWindow = oooWindow
		o.evaluationTime = now
	}

	return nil
}
