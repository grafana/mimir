// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"
	"fmt"
	"time"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/querysplitting/cache"
	promts "github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// SplittingFunctionOverRangeVector performs range vector function calculation with intermediate result caching.
// T is the type of intermediate result produced by the function's generate step.
type SplittingFunctionOverRangeVector[T any] struct {
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	FuncId                   Function
	FuncDef                  FunctionOverRangeVectorDefinition
	Annotations              *annotations.Annotations
	metricNames              *operators.MetricNames
	timeRange                types.QueryTimeRange
	enableDelayedNameRemoval bool
	expressionPosition       posrange.PositionRange
	emitAnnotationFunc       types.EmitAnnotationFunc
	seriesValidationFunc     RangeVectorSeriesValidationFunction

	cache *cache.Cache
	codec cache.SplitCodec[T]

	innerNode      planning.Node
	materializer   *planning.Materializer
	queryTimeRange types.QueryTimeRange
	innerCacheKey  string
	splitRanges    []Range

	generateFunc SplitGenerateFunc[T]
	combineFunc  SplitCombineFunc[T]

	splits []Split[T]
	// seriesToSplits is ordered the same way as SeriesMetadata
	seriesToSplits   [][]SplitSeries
	currentSeriesIdx int
}

var _ types.InstantVectorOperator = (*SplittingFunctionOverRangeVector[any])(nil)

func NewSplittingFunctionOverRangeVector[T any](
	innerNode planning.Node,
	materializer *planning.Materializer,
	timeRange types.QueryTimeRange,
	ranges []Range,
	innerCacheKey string,
	irCache *cache.Cache,
	funcId Function,
	funcDef FunctionOverRangeVectorDefinition,
	generateFunc SplitGenerateFunc[T],
	combineFunc SplitCombineFunc[T],
	codec cache.SplitCodec[T],
	expressionPosition posrange.PositionRange,
	annotations *annotations.Annotations,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	enableDelayedNameRemoval bool,
) (*SplittingFunctionOverRangeVector[T], error) {
	if !timeRange.IsInstant {
		return nil, fmt.Errorf("SplittingFunctionOverRangeVector only supports instant queries")
	}

	o := &SplittingFunctionOverRangeVector[T]{
		innerNode:                innerNode,
		materializer:             materializer,
		queryTimeRange:           timeRange,
		splitRanges:              ranges,
		innerCacheKey:            innerCacheKey,
		cache:                    irCache,
		codec:                    codec,
		FuncId:                   funcId,
		FuncDef:                  funcDef,
		generateFunc:             generateFunc,
		combineFunc:              combineFunc,
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

func (m *SplittingFunctionOverRangeVector[T]) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *SplittingFunctionOverRangeVector[T]) Prepare(ctx context.Context, params *types.PrepareParams) error {
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

// createSplits creates splits for the given time range, checking for cache entries and merging contiguous uncached
// split ranges to create uncached splits.
// Uses pre-computed split ranges from the optimization pass.
func (m *SplittingFunctionOverRangeVector[T]) createSplits(ctx context.Context) ([]Split[T], error) {
	var splits []Split[T]
	var currentUncachedStart int64
	var currentUncachedRanges []Range

	// Iterate through the pre-computed ranges
	for _, splitRange := range m.splitRanges {
		// For cacheable (aligned) ranges, check the cache
		if splitRange.Cacheable {
			cacheEntry, found, err := cache.NewReadEntry[T](m.cache, m.codec, ctx, int32(m.FuncId), m.innerCacheKey, splitRange.Start, splitRange.End)
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
			currentUncachedRanges = []Range{splitRange}
		} else {
			currentUncachedRanges = append(currentUncachedRanges, splitRange)
		}
	}

	// Flush any remaining uncached ranges
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

func (m *SplittingFunctionOverRangeVector[T]) materializeOperatorForTimeRange(start int64, end int64) (types.RangeVectorOperator, error) {
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

func (m *SplittingFunctionOverRangeVector[T]) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	var err error
	var metadata []types.SeriesMetadata
	metadata, m.seriesToSplits, err = m.mergeSplitsMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if m.metricNames != nil {
		m.metricNames.CaptureMetricNames(metadata)
	}

	if m.FuncDef.SeriesMetadataFunction.Func != nil {
		return m.FuncDef.SeriesMetadataFunction.Func(metadata, m.MemoryConsumptionTracker, m.enableDelayedNameRemoval)
	}

	return metadata, nil
}

func (m *SplittingFunctionOverRangeVector[T]) mergeSplitsMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, [][]SplitSeries, error) {
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

func (m *SplittingFunctionOverRangeVector[T]) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if m.currentSeriesIdx >= len(m.seriesToSplits) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	defer func() {
		m.currentSeriesIdx++
	}()

	splitSeriesList := m.seriesToSplits[m.currentSeriesIdx]

	var pieces []T
	for _, splitSeries := range splitSeriesList {
		results, err := m.splits[splitSeries.SplitIdx].ReadResultsAt(ctx, splitSeries.SplitLocalIdx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		pieces = append(pieces, results...)
	}

	f, hasFloat, h, err := m.combineFunc(pieces, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
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

func (m *SplittingFunctionOverRangeVector[T]) emitAnnotation(generator types.AnnotationGenerator) {
	metricName := m.metricNames.GetMetricNameForSeries(m.currentSeriesIdx)
	pos := m.innerNode.ExpressionPosition()

	m.Annotations.Add(generator(metricName, pos))
}

func (m *SplittingFunctionOverRangeVector[T]) Finalize(ctx context.Context) error {
	for _, split := range m.splits {
		if err := split.Finalize(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *SplittingFunctionOverRangeVector[T]) Close() {
	for _, split := range m.splits {
		split.Close()
	}
}

type Split[T any] interface {
	Prepare(ctx context.Context, params *types.PrepareParams) error
	SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error)
	ReadResultsAt(ctx context.Context, idx int) ([]T, error)
	Finalize(ctx context.Context) error
	Close()
}

type SplitSeries struct {
	SplitIdx      int
	SplitLocalIdx int
}

type CachedSplit[T any] struct {
	cachedResults cache.ReadEntry[T]
	parent        *SplittingFunctionOverRangeVector[T]
}

func NewCachedSplit[T any](cachedResults cache.ReadEntry[T], parent *SplittingFunctionOverRangeVector[T]) *CachedSplit[T] {
	return &CachedSplit[T]{
		cachedResults: cachedResults,
		parent:        parent,
	}
}

func (p *CachedSplit[T]) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return nil
}

func (c *CachedSplit[T]) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	// TODO: is matchers important here?
	return c.cachedResults.ReadSeriesMetadata(c.parent.MemoryConsumptionTracker)
}

func (c *CachedSplit[T]) ReadResultsAt(_ context.Context, idx int) ([]T, error) {
	result, err := c.cachedResults.ReadResultAt(idx)
	if err != nil {
		return nil, err
	}

	return []T{result}, nil
}

func (c *CachedSplit[T]) Finalize(ctx context.Context) error {
	return nil
}

func (c *CachedSplit[T]) Close() {
}

type UncachedSplit[T any] struct {
	ranges   []Range
	operator types.RangeVectorOperator

	parent *SplittingFunctionOverRangeVector[T]

	cacheWriteEntries []cache.WriteEntry[T]
	finalized         bool

	resultGetter *ResultGetter[T]
}

func NewUncachedSplit[T any](
	ctx context.Context,
	ranges []Range,
	operator types.RangeVectorOperator,
	parent *SplittingFunctionOverRangeVector[T],
) (*UncachedSplit[T], error) {
	cacheEntries := make([]cache.WriteEntry[T], len(ranges))
	var err error

	for i, splitRange := range ranges {
		if !splitRange.Cacheable {
			continue
		}

		cacheEntries[i], err = cache.NewWriteEntry[T](parent.cache, parent.codec, ctx, int32(parent.FuncId), parent.innerCacheKey, splitRange.Start, splitRange.End)
		if err != nil {
			return nil, err
		}
	}

	return &UncachedSplit[T]{
		ranges:   ranges,
		operator: operator,
		parent:   parent,

		cacheWriteEntries: cacheEntries,
		finalized:         false,
	}, nil
}

func (p *UncachedSplit[T]) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return p.operator.Prepare(ctx, params)
}

// TODO: is it good to release series metadata when this returns in the caller (split operator)? if we flush multiple ranges, each would have dupe protos in memory at the same time if we cannot stream to cache
// TODO: not all ranges will have the same metadata, if we use a single shared metadata for all ranges in the split then it could inflate cache entry size unnecessarily
func (p *UncachedSplit[T]) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	seriesMetadata, err := p.operator.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	for rangeIdx, splitRange := range p.ranges {
		if !splitRange.Cacheable {
			continue
		}

		err = p.cacheWriteEntries[rangeIdx].WriteSeriesMetadata(seriesMetadata)
		if err != nil {
			return nil, err
		}
	}
	p.resultGetter = NewResultGetter(p.NextSeries)

	return seriesMetadata, nil
}

func (p *UncachedSplit[T]) ReadResultsAt(ctx context.Context, idx int) ([]T, error) {
	return p.resultGetter.GetResultsAtIdx(ctx, idx)
}

func (p *UncachedSplit[T]) NextSeries(ctx context.Context) ([]T, error) {
	if err := p.operator.NextSeries(ctx); err != nil {
		return nil, err
	}
	step, err := p.operator.NextStepSamples(ctx)
	if err != nil {
		return nil, err
	}
	results := make([]T, len(p.ranges))
	for rangeIdx, splitRange := range p.ranges {
		rangeStep, err := step.SubStep(splitRange.Start, splitRange.End)
		if err != nil {
			return nil, err
		}

		result, err := p.parent.generateFunc(rangeStep, []types.ScalarData{}, p.parent.emitAnnotationFunc, p.parent.MemoryConsumptionTracker)
		if err != nil {
			return nil, err
		}
		results[rangeIdx] = result

		if splitRange.Cacheable {
			if err := p.cacheWriteEntries[rangeIdx].WriteNextResult(result); err != nil {
				return nil, fmt.Errorf("error writing results to cache: %w", err)
			}
		}
	}
	return results, nil
}

func (p *UncachedSplit[T]) Finalize(ctx context.Context) error {
	if p.finalized {
		return nil
	}

	for rangeIdx, splitRange := range p.ranges {
		if !splitRange.Cacheable {
			continue
		}

		if err := p.cacheWriteEntries[rangeIdx].Finalize(); err != nil {
			return err
		}
	}

	p.finalized = true
	return nil
}

func (p *UncachedSplit[T]) Close() {
	if p.operator != nil {
		p.operator.Close()
	}
}

type ResultGetter[T any] struct {
	resultBuffer   map[int][]T
	nextSeriesIdx  int
	nextSeriesFunc func(ctx context.Context) ([]T, error)
}

func NewResultGetter[T any](nextSeriesFunc func(ctx context.Context) ([]T, error)) *ResultGetter[T] {
	return &ResultGetter[T]{
		resultBuffer:   make(map[int][]T),
		nextSeriesIdx:  0,
		nextSeriesFunc: nextSeriesFunc,
	}
}

func (p *ResultGetter[T]) GetResultsAtIdx(ctx context.Context, splitSeriesIdx int) ([]T, error) {
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
	for ; p.nextSeriesIdx < splitSeriesIdx; {
		results, err := p.nextSeriesFunc(ctx)
		if err != nil {
			return nil, err
		}
		p.resultBuffer[p.nextSeriesIdx-1] = results
	}

	result, err := p.nextSeriesFunc(ctx)
	if err != nil {
		return nil, err
	}
	p.nextSeriesIdx++
	return result, nil
}
