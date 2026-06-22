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

	evaluationTime     time.Time
	ttlForNonOOOExtent time.Duration
	ttlForOOOExtent    time.Duration
	oooWindow          time.Duration

	key               []byte
	hashedKey         string
	cachedExtentsSize uint64 // Approximate size, in bytes, of extents loaded from the cache that have not expired.

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

	// extentsForDesiredTimeRange contains all extents that contain data for the desired time range, in time order (earliest extents first).
	// If there are any existing extents that are immediately adjacent to the desired time range, they are also included here so that they
	// can be merged with the extents inside the desired time range, rather than fragmenting the cache entry.
	extentsForDesiredTimeRange []extent

	// cacheableRangeStartT and cacheableRangeEndT are the start and end timestamps of the cacheable range of the extents in extentsForDesiredTimeRange.
	// These may extend beyond the desired time range if there are existing extents that extend beyond the desired time range.
	// cacheableRangeEndT will never exceed the max freshness threshold.
	cacheableRangeStartT int64
	cacheableRangeEndT   int64

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
		timeNow:                  time.Now,
		getCurrentTraceID:        tracing.ExtractTraceID,
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

func (c *CacheOperator) Prepare(ctx context.Context, params *types.PrepareParams) error {
	c.evaluationTime = c.timeNow()
	if err := c.populateTTLs(ctx); err != nil {
		return err
	}

	existingExtents, err := c.fetchExistingExtents(ctx)
	if err != nil {
		return err
	}

	if err := c.calculateExtents(ctx, existingExtents); err != nil {
		return err
	}

	for _, e := range c.extentsForDesiredTimeRange {
		if err := e.Prepare(ctx, params); err != nil {
			return err
		}
	}

	return nil
}

func (c *CacheOperator) fetchExistingExtents(ctx context.Context) ([]CachedExtent, error) {
	spanLogger := spanlogger.FromContext(ctx, c.logger)

	var err error
	c.key, err = c.computeCacheKey(ctx)
	if err != nil {
		return nil, err
	}
	c.hashedKey = caching.HashCacheKey(c.key)
	keys := []string{c.hashedKey}

	cacheHits := c.Backend.GetMulti(ctx, keys)
	cacheHit := cacheHits[c.hashedKey]

	if cacheHit == nil {
		return nil, nil
	}

	var cacheEntry CacheEntry
	if err := cacheEntry.Unmarshal(cacheHit); err != nil {
		// Returning an error here is simplest, but in the future we may want to log it and continue as if there was no cached result.
		return nil, err
	}

	if !bytes.Equal(cacheEntry.CacheKey, c.key) {
		// The cache key in the entry does not match the expected key: we've encountered a hash collision, so don't use the cached results.
		spanLogger.DebugLog("msg", "ignoring cache entry as the cache key in the entry does not match the expected key, presumably due to a hash collision", "hashed_key", c.hashedKey)
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
			)
		}

		return hasExpired
	})

	// If we have any extents that haven't expired, include their size in the memory consumption estimate.
	cachedExtentsSize := c.estimateExtentsSize(cacheEntry.Extents)
	if err := c.MemoryConsumptionTracker.IncreaseMemoryConsumption(cachedExtentsSize, limiter.CachedResponses); err != nil {
		return nil, err
	}
	c.cachedExtentsSize = cachedExtentsSize // Only set this now, so that if the increase above fails, calling Close doesn't cause a panic for trying to apply a decrease of the same magnitude.

	return cacheEntry.Extents, nil
}

// estimateExtentsSize estimates the memory consumption of the given extents.
// This allows us to include them in the memory consumption estimate for the query, as they would if
// they were evaluated fresh.
func (c *CacheOperator) estimateExtentsSize(extents []CachedExtent) uint64 {
	seriesCount := uint64(0)
	labelsSize := uint64(0)
	fPointCount := uint64(0)
	hPointCount := uint64(0)

	for _, e := range extents {
		seriesCount += uint64(cap(e.SeriesMetadata))

		for _, s := range e.SeriesMetadata {
			labelsSize += estimateLabelsSize(s.Labels)
		}

		for _, d := range e.Data {
			fPointCount += uint64(cap(d.Floats))
			hPointCount += uint64(cap(d.Histograms))
		}
	}

	return seriesCount*types.SeriesMetadataSize + labelsSize + fPointCount*types.FPointSize + hPointCount*types.HPointSize
}

func estimateLabelsSize(labels []mimirpb.LabelAdapter) uint64 {
	var size uint64
	for _, l := range labels {
		size += uint64(len(l.Name)) + uint64(len(l.Value))
	}
	return size
}

func (c *CacheOperator) calculateExtents(ctx context.Context, existingExtents []CachedExtent) error {
	nextStartT := c.DesiredTimeRange.StartT
	nextExistingExtentIdx := 0

	// Calculate the last step within the desired time range.
	// For example, if the desired time range is T=5m to T=8m30s with a step of 1m, then the last step is T=8m.
	stepAlignedEndT := calculateLastStepAlignedPoint(c.DesiredTimeRange.StartT, c.DesiredTimeRange.EndT, c.DesiredTimeRange.IntervalMilliseconds)

	maxFreshness, err := c.limitsProvider.GetMaxCacheFreshness(ctx)
	if err != nil {
		return err
	}

	maxFreshnessThreshold := timestamp.FromTime(c.evaluationTime.Add(-maxFreshness))
	c.cacheableRangeEndT = calculateLastStepAlignedPoint(c.DesiredTimeRange.StartT, min(maxFreshnessThreshold, stepAlignedEndT), c.DesiredTimeRange.IntervalMilliseconds)

	for nextStartT <= stepAlignedEndT {
		if nextExistingExtentIdx < len(existingExtents) && existingExtents[nextExistingExtentIdx].StartT <= nextStartT {
			extent := existingExtents[nextExistingExtentIdx]
			inDesiredTimeRange := extent.EndT >= nextStartT
			endsOneStepBeforeDesiredTimeRange := extent.EndT >= nextStartT-c.DesiredTimeRange.IntervalMilliseconds
			willWriteCacheEntry := nextStartT < maxFreshnessThreshold
			shouldMergeThisExtentWithExtentsInDesiredTimeRange := endsOneStepBeforeDesiredTimeRange && willWriteCacheEntry

			if inDesiredTimeRange || shouldMergeThisExtentWithExtentsInDesiredTimeRange {
				c.appendExtentInDesiredTimeRange(newCachedExtentReader(extent, c), extent.StartT, extent.EndT, maxFreshnessThreshold, true)

				// Note that we don't have to align the extent's end time to the step, as it would have been aligned when the extent was written.
				nextStartT = extent.EndT + c.DesiredTimeRange.IntervalMilliseconds
			} else {
				c.cacheableExtentsBeforeDesiredTimeRange = append(c.cacheableExtentsBeforeDesiredTimeRange, extent)
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
		operator, err := c.Materializer.ConvertNodeToInstantVectorOperator(c.Inner, timeRange)
		if err != nil {
			return err
		}

		extent := newEvaluatedExtent(operator, c, extentStartT, extentEndT)
		c.appendExtentInDesiredTimeRange(extent, extentStartT, extentEndT, maxFreshnessThreshold, false)

		if extentStartT <= maxFreshnessThreshold {
			// Only write a cache entry if the extent is cacheable (not entirely within the max freshness window).
			c.shouldWriteCacheEntry = true
		}

		nextStartT = extentEndT + c.DesiredTimeRange.IntervalMilliseconds
	}

	// If there is an extent just after the desired time range, include that in extentsForDesiredTimeRange
	// so that it is merged with the extents in the desired time range when we write the cache entry.
	if len(existingExtents) > nextExistingExtentIdx && c.shouldWriteCacheEntry {
		extent := existingExtents[nextExistingExtentIdx]
		startsOneStepAfterDesiredTimeRange := extent.StartT <= c.DesiredTimeRange.EndT+c.DesiredTimeRange.IntervalMilliseconds

		if startsOneStepAfterDesiredTimeRange {
			c.appendExtentInDesiredTimeRange(newCachedExtentReader(extent, c), extent.StartT, extent.EndT, maxFreshnessThreshold, true)
			nextExistingExtentIdx++
		}
	}

	c.cacheableExtentsAfterDesiredTimeRange = existingExtents[nextExistingExtentIdx:]

	return nil
}

func (c *CacheOperator) populateTTLs(ctx context.Context) error {
	var err error
	c.ttlForNonOOOExtent, err = c.limitsProvider.GetMinResultsCacheTTL(ctx)
	if err != nil {
		return err
	}

	c.ttlForOOOExtent, err = c.limitsProvider.GetMinOutOfOrderResultsCacheTTL(ctx)
	if err != nil {
		return err
	}

	c.oooWindow, err = c.limitsProvider.GetMaxOutOfOrderTimeWindow(ctx)
	if err != nil {
		return err
	}

	return nil
}

// calculateLastStepAlignedPoint returns the last step-aligned point within the given time range.
func calculateLastStepAlignedPoint(start int64, end int64, step int64) int64 {
	return end - ((end - start) % step)
}

// appendExtentInDesiredTimeRange appends an extent to the list of extents for the desired time range.
// It must be called in time order (earliest extents first).
func (c *CacheOperator) appendExtentInDesiredTimeRange(extent extent, startT int64, endT int64, maxFreshnessThreshold int64, isCachedExtent bool) {
	if len(c.extentsForDesiredTimeRange) == 0 {
		c.cacheableRangeStartT = startT
	}

	if isCachedExtent && endT > c.cacheableRangeEndT {
		// If this cached extent extends beyond the desired time range, then don't drop any samples between the end of the desired time range
		// and the max freshness threshold.
		c.cacheableRangeEndT = min(endT, maxFreshnessThreshold)
	}

	c.extentsForDesiredTimeRange = append(c.extentsForDesiredTimeRange, extent)
}

func (c *CacheOperator) AfterPrepare(ctx context.Context) error {
	for _, e := range c.extentsForDesiredTimeRange {
		if err := e.AfterPrepare(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (c *CacheOperator) SeriesMetadata(ctx context.Context, _ types.Matchers) ([]types.SeriesMetadata, error) {
	series, err := c.computeMergedSeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	if c.shouldWriteCacheEntry {
		if err := c.bufferSeriesMetadataForCacheEntry(series); err != nil {
			return nil, err
		}

		c.data = make([]types.InstantVectorSeriesData, len(series))
	}

	return series, nil
}

func (c *CacheOperator) computeMergedSeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Use the slice from the first extent as the base for the returned series metadata.
	allSeries, err := c.extentsForDesiredTimeRange[0].SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	if len(c.extentsForDesiredTimeRange) == 1 {
		// There are no extents to merge, so just return the series metadata from the first extent.
		return allSeries, nil
	}

	// Build up a map of the series labels to their index in the output.
	seriesIndices := make(map[string]int, len(allSeries))
	labelBytesBuf := make([]byte, 0, 1024)
	for seriesIdx, series := range allSeries {
		labelBytesBuf = series.Labels.Bytes(labelBytesBuf)
		seriesIndices[string(labelBytesBuf)] = seriesIdx // Important: don't extract the string(...) call here - passing it directly allows us to avoid allocating it.

		if err := c.addNewOutputSeries(0, seriesIdx); err != nil {
			return nil, err
		}
	}

	// Now go through the remaining extents.
	for extentIdx := 1; extentIdx < len(c.extentsForDesiredTimeRange); extentIdx++ {
		e := c.extentsForDesiredTimeRange[extentIdx]
		extentSeries, err := e.SeriesMetadata(ctx)
		if err != nil {
			return nil, err
		}

		for extentSeriesIdx, series := range extentSeries {
			labelBytesBuf = series.Labels.Bytes(labelBytesBuf)

			// Important: don't extract the string(...) call in the map lookup below - passing it directly allows us to avoid allocating it.
			if outputSeriesIdx, seenAlready := seriesIndices[string(labelBytesBuf)]; seenAlready {
				if series.DropName != allSeries[outputSeriesIdx].DropName {
					return nil, fmt.Errorf("series with labels %s has conflicting drop name values in different extents", series.Labels.String())
				}

				c.outputSeries[outputSeriesIdx].sourceSeriesIndices[extentIdx] = extentSeriesIdx

				// We're not going to keep this labels instance (we already have it from a previous extent), so
				// decrease memory consumption now.
				c.MemoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(series.Labels)
			} else {
				seriesIndices[string(labelBytesBuf)] = len(allSeries)
				allSeries, err = types.SeriesMetadataSlicePool.AppendToSlice(allSeries, c.MemoryConsumptionTracker, series)
				if err != nil {
					return nil, err
				}

				if err := c.addNewOutputSeries(extentIdx, extentSeriesIdx); err != nil {
					return nil, err
				}
			}

			// We've already accounted for the memory consumption of this series' labels with the DecreaseMemoryConsumptionForLabels
			// or AppendToSlice calls above, so clear the labels now so they're not double-decremented when we return the series
			// slice to the pool below.
			extentSeries[extentSeriesIdx] = types.SeriesMetadata{}
		}

		types.SeriesMetadataSlicePool.Put(&extentSeries, c.MemoryConsumptionTracker)
	}

	return allSeries, nil
}

func (c *CacheOperator) addNewOutputSeries(sourceExtentIndex int, sourceExtentSeriesIndex int) error {
	sourceSeriesIndices, err := types.IntSlicePool.Get(len(c.extentsForDesiredTimeRange), c.MemoryConsumptionTracker)
	if err != nil {
		return err
	}

	sourceSeriesIndices = sourceSeriesIndices[:len(c.extentsForDesiredTimeRange)]

	for idx := range c.extentsForDesiredTimeRange {
		if idx == sourceExtentIndex {
			sourceSeriesIndices[idx] = sourceExtentSeriesIndex
		} else {
			sourceSeriesIndices[idx] = -1
		}
	}

	c.outputSeries = append(c.outputSeries, splitOrCacheOutputSeries{sourceSeriesIndices: sourceSeriesIndices})
	return nil
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
	desiredTimeRangeData, cacheableTimeRangeData, err := c.getDataForNextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if c.shouldWriteCacheEntry {
		c.data[c.nextOutputSeriesIdx] = cacheableTimeRangeData
	}

	c.nextOutputSeriesIdx++

	return desiredTimeRangeData, nil
}

func (c *CacheOperator) getDataForNextSeries(ctx context.Context) (desiredTimeRangeData types.InstantVectorSeriesData, cacheableTimeRangeData types.InstantVectorSeriesData, err error) {
	var sourceSeriesIndices []int

	if len(c.extentsForDesiredTimeRange) == 1 {
		// We only have one extent, and therefore outputSeries isn't populated because we'll just return all series from that
		// sole extent, so we have to construct it here.
		sourceSeriesIndices = []int{c.nextOutputSeriesIdx}
	} else {
		if c.nextOutputSeriesIdx >= len(c.outputSeries) {
			// We've reached the end of the output series, so we're done.
			// We don't need a similar check in the one-extent case above, as we rely on the inner operator to return EOS in that case.
			return types.InstantVectorSeriesData{}, types.InstantVectorSeriesData{}, types.EOS
		}

		thisOutputSeries := c.outputSeries[c.nextOutputSeriesIdx]
		sourceSeriesIndices = thisOutputSeries.sourceSeriesIndices

		defer types.IntSlicePool.Put(&thisOutputSeries.sourceSeriesIndices, c.MemoryConsumptionTracker)
	}

	for extentIdx, sourceSeriesIdx := range sourceSeriesIndices {
		if sourceSeriesIdx == -1 {
			continue
		}

		extent := c.extentsForDesiredTimeRange[extentIdx]
		data, err := extent.GetSeries(ctx, sourceSeriesIdx)
		if err != nil {
			return types.InstantVectorSeriesData{}, types.InstantVectorSeriesData{}, err
		}

		if err := c.accumulateDataForSeries(data, &desiredTimeRangeData, &cacheableTimeRangeData); err != nil {
			return types.InstantVectorSeriesData{}, types.InstantVectorSeriesData{}, err
		}
	}

	return desiredTimeRangeData, cacheableTimeRangeData, nil
}

func (c *CacheOperator) accumulateDataForSeries(data types.InstantVectorSeriesData, desiredTimeRangeData *types.InstantVectorSeriesData, cacheableTimeRangeData *types.InstantVectorSeriesData) error {
	if c.shouldWriteCacheEntry {
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

	firstCacheableIndex := c.indexOfFirstFPointAtOrAfter(data.Floats, c.cacheableRangeStartT)
	if firstCacheableIndex == -1 {
		return nil
	}

	lastCacheableIndex := c.indexOfLastFPointAtOrBefore(data.Floats, c.cacheableRangeEndT)
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

	firstCacheableIndex := c.indexOfFirstHPointAtOrAfter(data.Histograms, c.cacheableRangeStartT)
	if firstCacheableIndex == -1 {
		return nil
	}

	lastCacheableIndex := c.indexOfLastHPointAtOrBefore(data.Histograms, c.cacheableRangeEndT)
	if lastCacheableIndex < firstCacheableIndex {
		return nil
	}

	newPointCount := lastCacheableIndex - firstCacheableIndex + 1
	existingPointCount := len(cacheableTimeRangeData.Histograms)
	neededSliceCapacity := existingPointCount + newPointCount

	if cap(cacheableTimeRangeData.Histograms) < neededSliceCapacity {
		oldSlice := cacheableTimeRangeData.Histograms
		var err error
		cacheableTimeRangeData.Histograms, err = types.HPointSlicePool.Get(neededSliceCapacity, c.MemoryConsumptionTracker)
		if err != nil {
			return err
		}
		cacheableTimeRangeData.Histograms = append(cacheableTimeRangeData.Histograms, oldSlice...)
		clear(oldSlice)
		types.HPointSlicePool.Put(&oldSlice, c.MemoryConsumptionTracker)
	}

	cacheableTimeRangeData.Histograms = cacheableTimeRangeData.Histograms[:neededSliceCapacity]

	for i := range newPointCount {
		sourcePoint := data.Histograms[firstCacheableIndex+i]
		destinationIdx := existingPointCount + i
		cacheableTimeRangeData.Histograms[destinationIdx].T = sourcePoint.T

		// Reuse existing FloatHistogram instance in the destination slice if we can.
		if cacheableTimeRangeData.Histograms[destinationIdx].H == nil {
			cacheableTimeRangeData.Histograms[destinationIdx].H = sourcePoint.H.Copy()
		} else {
			sourcePoint.H.CopyTo(cacheableTimeRangeData.Histograms[destinationIdx].H)
		}
	}

	return nil
}

// accumulateDesiredFloats accumulates desired float points from the given data into desiredTimeRangeData.
// It takes ownership of data.Floats, either returning it as desiredTimeRangeData.Floats or returning it to a pool.
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

	for _, e := range c.extentsForDesiredTimeRange {
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

	if c.shouldWriteCacheEntry {
		defer cacheableTimeRangeStats.Close()

		if err := c.writeCacheEntry(ctx, cacheableTimeRangeStats, annos); err != nil {
			return nil, nil, err
		}
	}

	return desiredTimeRangeStats, annos, nil
}

func (c *CacheOperator) finalizeExtents(ctx context.Context) (desiredTimeRangeStats *types.OperatorEvaluationStats, cacheableTimeRangeStats *types.OperatorEvaluationStats, annos annotations.Annotations, err error) {
	for _, extent := range c.extentsForDesiredTimeRange {
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

		if c.shouldWriteCacheEntry {
			if cacheableTimeRangeStats == nil {
				cacheableTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(c.cacheableRangeStartT), timestamp.Time(c.cacheableRangeEndT), time.Duration(c.DesiredTimeRange.IntervalMilliseconds)*time.Millisecond)
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

	if c.nextOutputSeriesIdx != len(c.seriesMetadata) {
		// Not all series were read, don't write cache entry.
		return nil
	}

	if !c.finishedReading {
		return errors.New("CacheOperator.writeCacheEntry() called (via Finalize()) before FinishedReading(), this should never happen")
	}

	extents := make([]CachedExtent, 0, len(c.cacheableExtentsBeforeDesiredTimeRange)+len(c.cacheableExtentsAfterDesiredTimeRange)+1)
	extents = append(extents, c.cacheableExtentsBeforeDesiredTimeRange...)

	traceID, _ := c.getCurrentTraceID(ctx)

	desiredTimeRangeExtent := CachedExtent{
		StartT:                  c.cacheableRangeStartT,
		EndT:                    c.cacheableRangeEndT,
		SeriesMetadata:          querierpb.EncodeSeriesMetadataSlice(c.seriesMetadata),
		Data:                    c.encodeDataForCacheEntry(),
		Annotations:             querierpb.EncodeAnnotations(annos, c.queryParameters.OriginalExpression),
		Stats:                   stats.Encode(),
		NewestEvaluationTraceID: traceID,
		OldestEvaluationTime:    c.determineNewExtentEvaluationTimestamp(),
	}

	extents = append(extents, desiredTimeRangeExtent)
	extents = append(extents, c.cacheableExtentsAfterDesiredTimeRange...)

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

	c.Backend.SetAsync(c.hashedKey, value, ttl)
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

	if timestamp.Time(extent.EndT).After(oooThreshold) {
		return c.ttlForOOOExtent
	}

	return c.ttlForNonOOOExtent
}

func (c *CacheOperator) determineNewExtentEvaluationTimestamp() int64 {
	t := c.extentsForDesiredTimeRange[0].GetEvaluationTimestamp()

	for _, e := range c.extentsForDesiredTimeRange[1:] {
		t = min(e.GetEvaluationTimestamp(), t)
	}

	return t
}

func (c *CacheOperator) encodeDataForCacheEntry() []querierpb.InstantVectorSeriesData {
	encoded := make([]querierpb.InstantVectorSeriesData, len(c.data))
	for i, d := range c.data {
		// The methods below do unsafe casts and do not copy the data from the slices, but this is OK as we're immediately
		// serializing the message and writing it to the cache before these slices are returned to their respective pools.
		encoded[i].Floats = mimirpb.FromFPointsToSamples(d.Floats)
		encoded[i].Histograms = mimirpb.FromHPointsToHistograms(d.Histograms)
	}
	return encoded
}

func (c *CacheOperator) ExpressionPosition() posrange.PositionRange {
	return c.expressionPosition
}

func (c *CacheOperator) Close() {
	c.cacheableExtentsBeforeDesiredTimeRange = nil
	c.cacheableExtentsAfterDesiredTimeRange = nil

	for _, e := range c.extentsForDesiredTimeRange {
		e.Close()
	}

	c.extentsForDesiredTimeRange = nil

	if len(c.outputSeries) > 0 {
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

	c.MemoryConsumptionTracker.DecreaseMemoryConsumption(c.cachedExtentsSize, limiter.CachedResponses)
	c.cachedExtentsSize = 0
}

type extent interface {
	GetStartT() int64
	GetEndT() int64

	Prepare(ctx context.Context, params *types.PrepareParams) error
	AfterPrepare(ctx context.Context) error
	FinishedReading(ctx context.Context) error

	// SeriesMetadata returns the metadata for all series in this extent.
	// Callers may modify the returned slice.
	SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error)

	// GetSeries returns the samples for the series at the given index.
	// Callers may modify the returned slices.
	GetSeries(ctx context.Context, seriesIdx int) (types.InstantVectorSeriesData, error)

	Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error)

	GetEvaluationTimestamp() int64

	Close()
}

type cachedExtentReader struct {
	extent CachedExtent
	parent *CacheOperator
}

func newCachedExtentReader(extent CachedExtent, parent *CacheOperator) *cachedExtentReader {
	return &cachedExtentReader{extent: extent, parent: parent}
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

func (c *cachedExtentReader) SeriesMetadata(_ context.Context) ([]types.SeriesMetadata, error) {
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

	return seriesMetadata, nil
}

func (c *cachedExtentReader) GetSeries(_ context.Context, seriesIdx int) (types.InstantVectorSeriesData, error) {
	if seriesIdx >= len(c.extent.Data) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	cachedData := c.extent.Data[seriesIdx]

	mqeData := types.InstantVectorSeriesData{
		Floats:     mimirpb.FromSamplesToFPoints(cachedData.Floats),
		Histograms: mimirpb.FromHistogramsToHPoints(cachedData.Histograms),
	}

	// FromSamplesToFPoints and FromHistogramsToHPoints don't account for memory consumption, so do that now.
	if err := c.parent.MemoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(mqeData.Floats))*types.FPointSize, limiter.FPointSlices); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if err := c.parent.MemoryConsumptionTracker.IncreaseMemoryConsumption(uint64(cap(mqeData.Histograms))*types.HPointSize, limiter.HPointSlices); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

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
	// Nothing to do.
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

func (c *cachedExtentReader) Close() {
	// Nothing to do.
}

type evaluatedExtent struct {
	inner  types.InstantVectorOperator
	parent *CacheOperator
	buffer *operators.InstantVectorOperatorBuffer
	startT int64
	endT   int64
}

func newEvaluatedExtent(inner types.InstantVectorOperator, parent *CacheOperator, startT int64, endT int64) *evaluatedExtent {
	return &evaluatedExtent{
		inner:  inner,
		parent: parent,
		startT: startT,
		endT:   endT,
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

func (e *evaluatedExtent) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	metadata, err := e.inner.SeriesMetadata(ctx, nil)
	if err != nil {
		return nil, err
	}

	e.buffer = operators.NewInstantVectorOperatorBuffer(e.inner, nil, len(metadata)-1, e.parent.MemoryConsumptionTracker)

	return metadata, nil
}

func (e *evaluatedExtent) GetSeries(ctx context.Context, seriesIdx int) (types.InstantVectorSeriesData, error) {
	series, err := e.buffer.GetSeries(ctx, []int{seriesIdx})
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return series[0], nil
}

func (e *evaluatedExtent) FinishedReading(ctx context.Context) error {
	e.buffer.FinishedReading()

	return e.inner.FinishedReading(ctx)
}

func (e *evaluatedExtent) Finalize(ctx context.Context) (*types.OperatorEvaluationStats, annotations.Annotations, error) {
	return e.inner.Finalize(ctx)
}

func (e *evaluatedExtent) GetEvaluationTimestamp() int64 {
	return timestamp.FromTime(e.parent.evaluationTime)
}

func (e *evaluatedExtent) Close() {
	e.inner.Close()
}

type InstantVectorMaterializer interface {
	ConvertNodeToInstantVectorOperator(node planning.Node, timeRange types.QueryTimeRange) (types.InstantVectorOperator, error)
}

type LimitsProvider interface {
	GetMinResultsCacheTTL(ctx context.Context) (time.Duration, error)
	GetMinOutOfOrderResultsCacheTTL(ctx context.Context) (time.Duration, error)
	GetMaxCacheFreshness(ctx context.Context) (time.Duration, error)
	GetMaxOutOfOrderTimeWindow(ctx context.Context) (time.Duration, error)
}
