// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package querysplitting

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/streamingpromql/cache"
	promts "github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// FunctionOverRangeVector performs range vector function calculation with intermediate result caching.
type FunctionOverRangeVector struct {
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Func                     functions.FunctionOverRangeVectorDefinition
	Annotations              *annotations.Annotations
	metricNames              *operators.MetricNames
	timeRange                types.QueryTimeRange
	enableDelayedNameRemoval bool
	expressionPosition       posrange.PositionRange
	emitAnnotationFunc       types.EmitAnnotationFunc
	seriesValidationFunc     functions.RangeVectorSeriesValidationFunction

	irCache cache.IntermediateResultsCache

	innerNode      planning.Node
	materializer   *planning.Materializer
	queryTimeRange types.QueryTimeRange
	innerCacheKey  string
	splitRanges    []SplitRange

	splits []Split
	// seriesToSplits is ordered the same way as SeriesMetadata
	seriesToSplits   [][]SplitSeries
	currentSeriesIdx int
}

var _ types.InstantVectorOperator = &FunctionOverRangeVector{}

func NewFunctionOverRangeVector(
	innerNode planning.Node,
	materializer *planning.Materializer,
	timeRange types.QueryTimeRange,
	ranges []SplitRange,
	cacheKey string,
	irCache cache.IntermediateResultsCache,
	funcDef functions.FunctionOverRangeVectorDefinition,
	expressionPosition posrange.PositionRange,
	annotations *annotations.Annotations,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	enableDelayedNameRemoval bool,
) (*FunctionOverRangeVector, error) {
	if !timeRange.IsInstant {
		return nil, fmt.Errorf("FunctionOverRangeVector only supports instant queries")
	}

	o := &FunctionOverRangeVector{
		innerNode:                innerNode,
		materializer:             materializer,
		queryTimeRange:           timeRange,
		splitRanges:              ranges,
		innerCacheKey:            cacheKey,
		irCache:                  irCache,
		Func:                     funcDef,
		Annotations:              annotations,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		expressionPosition:       expressionPosition,
		timeRange:                timeRange,
		enableDelayedNameRemoval: enableDelayedNameRemoval,
	}

	if funcDef.SeriesValidationFuncFactory != nil {
		o.seriesValidationFunc = funcDef.SeriesValidationFuncFactory()
	}

	if funcDef.NeedsSeriesNamesForAnnotations {
		o.metricNames = &operators.MetricNames{}
	}

	o.emitAnnotationFunc = o.emitAnnotation

	return o, nil
}

func (m *FunctionOverRangeVector) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverRangeVector) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	var err error
	var metadata []types.SeriesMetadata
	metadata, m.seriesToSplits, err = m.mergeSplitsMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if m.metricNames != nil {
		m.metricNames.CaptureMetricNames(metadata)
	}

	if m.Func.SeriesMetadataFunction.Func != nil {
		return m.Func.SeriesMetadataFunction.Func(metadata, m.MemoryConsumptionTracker, m.enableDelayedNameRemoval)
	}

	return metadata, nil
}

func (m *FunctionOverRangeVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if m.currentSeriesIdx >= len(m.seriesToSplits) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	defer func() {
		m.currentSeriesIdx++
	}()

	splitSeriesList := m.seriesToSplits[m.currentSeriesIdx]

	var pieces []cache.IntermediateResult
	for _, splitSeries := range splitSeriesList {
		split := m.splits[splitSeries.SplitIdx]
		results, err := split.GetResultsAtIdx(ctx, splitSeries.SplitLocalIdx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		pieces = append(pieces, results...)
	}

	f, hasFloat, h, err := m.Func.CombineFunc(pieces, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{}

	stepT := m.queryTimeRange.StartT

	if hasFloat {
		data.Floats, err = types.FPointSlicePool.Get(1, m.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		data.Floats = append(data.Floats, promql.FPoint{T: stepT, F: f})
	}
	if h != nil {
		data.Histograms, err = types.HPointSlicePool.Get(1, m.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		data.Histograms = append(data.Histograms, promql.HPoint{T: stepT, H: h})
	}

	// Validation after single step, won't work for range queries if we supported them for splitting.
	if m.seriesValidationFunc != nil {
		m.seriesValidationFunc(data, m.metricNames.GetMetricNameForSeries(m.currentSeriesIdx), m.emitAnnotationFunc)
	}

	return data, nil
}

func (m *FunctionOverRangeVector) emitAnnotation(generator types.AnnotationGenerator) {
	metricName := m.metricNames.GetMetricNameForSeries(m.currentSeriesIdx)
	pos := m.innerNode.ExpressionPosition()

	m.Annotations.Add(generator(metricName, pos))
}

func (m *FunctionOverRangeVector) Prepare(ctx context.Context, params *types.PrepareParams) error {
	var err error
	m.splits, err = m.createSplits(ctx)
	if err != nil {
		return err
	}
	for _, split := range m.splits {
		err = split.Prepare(ctx, params)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *FunctionOverRangeVector) Finalize(ctx context.Context) error {
	for _, split := range m.splits {
		if err := split.Finalize(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *FunctionOverRangeVector) Close() {
	for _, split := range m.splits {
		split.Close()
	}
}

// createSplits creates splits for the given time range, checking for cache entries and merging contiguous uncached
// split ranges to create uncached splits.
// Uses pre-computed split ranges from the optimization pass.
func (m *FunctionOverRangeVector) createSplits(ctx context.Context) ([]Split, error) {
	var splits []Split
	var currentUncachedStart int64
	var currentUncachedRanges []SplitRange

	// Iterate through the pre-computed ranges
	for _, splitRange := range m.splitRanges {
		// For cacheable (aligned) ranges, check the cache
		if splitRange.Cacheable {
			cacheEntry, found, err := m.irCache.Get(ctx, m.Func.Name, m.innerCacheKey, splitRange.Start, splitRange.End)
			if err != nil {
				return nil, err
			}

			if found {
				// Found in cache - finalize any pending uncached ranges, then add cached split
				if currentUncachedRanges != nil {
					lastRange := currentUncachedRanges[len(currentUncachedRanges)-1]
					operator, err := m.materializeOperatorForTimeRange(currentUncachedStart, lastRange.End)
					if err != nil {
						return nil, err
					}

					split, err := NewUncachedSplit(ctx, currentUncachedRanges, operator, m)
					if err != nil {
						return nil, err
					}

					splits = append(splits, split)
					currentUncachedRanges = nil
				}

				splits = append(splits, NewCachedSplit(cacheEntry, m))
				continue
			}
		}

		// Not found in cache or not cacheable - add to current uncached ranges
		if currentUncachedRanges == nil {
			currentUncachedStart = splitRange.Start
			currentUncachedRanges = []SplitRange{splitRange}
		} else {
			currentUncachedRanges = append(currentUncachedRanges, splitRange)
		}
	}

	// Finalize any remaining uncached ranges
	if currentUncachedRanges != nil {
		lastRange := currentUncachedRanges[len(currentUncachedRanges)-1]
		operator, err := m.materializeOperatorForTimeRange(currentUncachedStart, lastRange.End)
		if err != nil {
			return nil, err
		}

		split, err := NewUncachedSplit(ctx, currentUncachedRanges, operator, m)
		if err != nil {
			return nil, err
		}

		splits = append(splits, split)
	}

	return splits, nil
}

func (m *FunctionOverRangeVector) materializeOperatorForTimeRange(start int64, end int64) (types.RangeVectorOperator, error) {
	subRange := time.Duration(end-start) * time.Millisecond

	overrideTimeParams := types.TimeRangeParams{
		Range: subRange,

		// The offset and timestamp are cleared
		Offset:    0,
		Timestamp: nil,
	}

	splitTimeRange := types.NewInstantQueryTimeRange(promts.Time(end))

	op, err := m.materializer.ConvertNodeToOperatorWithSubRange(m.innerNode, splitTimeRange, util.Some(overrideTimeParams))
	if err != nil {
		return nil, err
	}

	innerOperator, ok := op.(types.RangeVectorOperator)
	if !ok {
		return nil, fmt.Errorf("error materializing subnode: expected RangeVectorOperator, got %T", op)
	}

	return innerOperator, nil
}

func (m *FunctionOverRangeVector) mergeSplitsMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, [][]SplitSeries, error) {
	if len(m.splits) == 0 {
		return nil, nil, nil
	}

	seriesMap := make(map[string]int)
	// TODO: track memory usage of seriesToSplits? Could be large if lots of series + lots of splits.
	var seriesToSplits [][]SplitSeries

	labelBytes := make([]byte, 0, 1024)

	mergedMetadata, err := m.splits[0].SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, nil, err
	}
	for splitLocalIdx, serieMetadata := range mergedMetadata {
		labelBytes = serieMetadata.Labels.Bytes(labelBytes)
		key := string(labelBytes)

		// TODO: is it possible to have the same series returned twice in series metadata?
		seriesMap[key] = splitLocalIdx
		seriesToSplits = append(seriesToSplits, []SplitSeries{{0, splitLocalIdx}})
	}

	for splitIdx := 1; splitIdx < len(m.splits); splitIdx++ {
		split := m.splits[splitIdx]
		splitMetadata, err := split.SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}

		for splitLocalIdx, serieMetadata := range splitMetadata {
			labelBytes = serieMetadata.Labels.Bytes(labelBytes)
			key := string(labelBytes)

			mergedIdx, exists := seriesMap[key]
			if !exists {
				mergedIdx = len(mergedMetadata)
				seriesMap[key] = mergedIdx
				mergedMetadata, err = types.SeriesMetadataSlicePool.AppendToSlice(mergedMetadata, m.MemoryConsumptionTracker, serieMetadata)
				if err != nil {
					// TODO: do we need to return the splitMetadata slice back to the pool here if there's an error?
					return nil, nil, err
				}
				seriesToSplits = append(seriesToSplits, nil)
			} else {
				// We don't need the metadata value anymore if it's a duplicate series
				m.MemoryConsumptionTracker.DecreaseMemoryConsumptionForLabels(serieMetadata.Labels)
			}
			seriesToSplits[mergedIdx] = append(seriesToSplits[mergedIdx], SplitSeries{
				SplitIdx:      splitIdx,
				SplitLocalIdx: splitLocalIdx,
			})

			labelBytes = labelBytes[:0]
		}

		// Clear elements in metadata before putting back in pool, since element decrease is already accounted for.
		// TODO: this is not great in the non-streaming case
		//  in those cases, either metadata is duplicated for each split or we need to hold onto the metadata for longer (as it could be shared by several splits). an issue about delaying metadata slice put is now that labels are shared in the mergedmetadata
		//  so we would need carefully do the tracking
		//  Possible change - have a releaseReturnedMetadata() method for splits
		//  to decide if we need to avoid deduping and just copy labels and not
		//  return metadata slice to the pool - the uncached case could return
		//  false and the cached true
		splitMetadata = splitMetadata[:0]
		types.SeriesMetadataSlicePool.Put(&splitMetadata, m.MemoryConsumptionTracker)
	}

	return mergedMetadata, seriesToSplits, nil
}

type Split interface {
	Prepare(ctx context.Context, params *types.PrepareParams) error
	SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error)
	// TODO: just return merged result instead of slices of results?
	GetResultsAtIdx(ctx context.Context, splitLocalIdx int) ([]cache.IntermediateResult, error)
	Finalize(ctx context.Context) error
	Close()
}

type SplitSeries struct {
	SplitIdx      int
	SplitLocalIdx int
}

type CachedSplit struct {
	cachedResults cache.CacheReadEntry
	parent        *FunctionOverRangeVector
}

func NewCachedSplit(cachedResults cache.CacheReadEntry, parent *FunctionOverRangeVector) *CachedSplit {
	return &CachedSplit{
		cachedResults: cachedResults,
		parent:        parent,
	}
}

func (p *CachedSplit) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return nil
}

func (c *CachedSplit) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return c.cachedResults.ReadSeriesMetadata(c.parent.MemoryConsumptionTracker)
}

func (c *CachedSplit) GetResultsAtIdx(ctx context.Context, splitLocalIdx int) ([]cache.IntermediateResult, error) {
	result, err := c.cachedResults.ReadResultAtIdx(splitLocalIdx)
	if err != nil {
		return nil, err
	}
	return []cache.IntermediateResult{result}, nil
}

func (c *CachedSplit) Finalize(ctx context.Context) error {
	return nil
}

func (c *CachedSplit) Close() {
}

type UncachedSplit struct {
	ranges           []SplitRange
	operator         types.RangeVectorOperator
	offsetFromStartT int64

	parent *FunctionOverRangeVector

	cacheWriteEntries []cache.CacheWriteEntry
	writtenToCache    bool

	seriesCount   int
	resultBuffer  map[int][]cache.IntermediateResult
	nextSeriesIdx int
}

func NewUncachedSplit(
	ctx context.Context,
	ranges []SplitRange,
	operator types.RangeVectorOperator,
	parent *FunctionOverRangeVector,
) (*UncachedSplit, error) {
	cacheEntries := make([]cache.CacheWriteEntry, len(ranges))
	var err error

	for i, splitRange := range ranges {
		if !splitRange.Cacheable {
			continue
		}

		cacheEntries[i], err = parent.irCache.NewWriteEntry(ctx, parent.Func.Name, parent.innerCacheKey, splitRange.Start, splitRange.End)
		if err != nil {
			return nil, err
		}
	}
	return &UncachedSplit{
		ranges:   ranges,
		operator: operator,
		parent:   parent,

		cacheWriteEntries: cacheEntries,
		writtenToCache:    false,
	}, nil
}

func (p *UncachedSplit) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return p.operator.Prepare(ctx, params)
}

func (p *UncachedSplit) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	seriesMetadata, err := p.operator.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	// TODO: is it good to release series metadata when this returns in the caller (split operator)? if we flush multiple ranges, each would have dupe protos in memory at the same time if we cannot stream to cache
	// TODO: not all ranges will have the same metadata, if we use a single shared metadata for all ranges in the split then it could inflate cache entry size unnecessarily
	for rangeIdx, splitRange := range p.ranges {
		if !splitRange.Cacheable {
			continue
		}

		err = p.cacheWriteEntries[rangeIdx].WriteSeriesMetadata(seriesMetadata)
		if err != nil {
			return nil, err
		}
	}

	p.seriesCount = len(seriesMetadata)

	return seriesMetadata, nil
}

func (p *UncachedSplit) GetResultsAtIdx(ctx context.Context, splitSeriesIdx int) ([]cache.IntermediateResult, error) {
	if splitSeriesIdx >= p.seriesCount {
		return nil, fmt.Errorf("series index %d out of range (have %d series)", splitSeriesIdx, p.seriesCount)
	}

	// if series is buffered
	if splitSeriesIdx < p.nextSeriesIdx {
		result, ok := p.resultBuffer[splitSeriesIdx]
		if !ok {
			return nil, fmt.Errorf("could not find buffered result for index %d, nextSeriesIdx %d", splitSeriesIdx, p.nextSeriesIdx)
		}
		// a series should only be read once
		delete(p.resultBuffer, splitSeriesIdx)
		return result, nil
	}

	// buffer series results before the requested series
	for ; splitSeriesIdx < p.seriesCount && p.nextSeriesIdx < splitSeriesIdx; {
		results, err := p.resultsForNextSeries(ctx)
		if err != nil {
			return nil, err
		}
		p.resultBuffer[p.nextSeriesIdx-1] = results
	}

	if splitSeriesIdx >= p.seriesCount {
		// this shouldn't happen
		return nil, fmt.Errorf("could not get results for requested series at index %d, unexpectedly out of range (have %d series)", splitSeriesIdx, p.seriesCount)
	}

	return p.resultsForNextSeries(ctx)
}

// resultsForNextSeries will calculate and cache the result for the next series
func (p *UncachedSplit) resultsForNextSeries(ctx context.Context) ([]cache.IntermediateResult, error) {
	if err := p.operator.NextSeries(ctx); err != nil {
		return nil, err
	}
	step, err := p.operator.NextStepSamples(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]cache.IntermediateResult, len(p.ranges))
	for rangeIdx, splitRange := range p.ranges {
		rangeStep, err := step.SubStep(splitRange.Start, splitRange.End)
		if err != nil {
			return nil, err
		}

		result, err := p.parent.Func.GenerateFunc(rangeStep, []types.ScalarData{}, p.parent.emitAnnotationFunc, p.parent.MemoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
		results[rangeIdx] = result

		if splitRange.Cacheable {
			// TODO: should we return error here or silently fail?
			err = p.cacheWriteEntries[rangeIdx].WriteNextResult(result)
			if err != nil {
				return nil, err
			}
		}
	}
	p.nextSeriesIdx++
	return results, nil
}

func (p *UncachedSplit) Finalize(ctx context.Context) error {
	if p.writtenToCache {
		return nil
	}

	for rangeIdx, splitRange := range p.ranges {
		if !splitRange.Cacheable {
			continue
		}

		// TODO: should we return error here or silently fail?
		err := p.cacheWriteEntries[rangeIdx].Finalize()
		if err != nil {
			return err
		}
	}

	p.writtenToCache = true
	return nil
}

func (p *UncachedSplit) Close() {
	if p.operator != nil {
		p.operator.Close()
	}
}
