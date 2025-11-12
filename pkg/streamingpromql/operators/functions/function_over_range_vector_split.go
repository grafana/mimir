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

	"github.com/grafana/mimir/pkg/streamingpromql/cache"
	promts "github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// FunctionOverRangeVectorSplit performs range vector function calculation with intermediate result caching.
type FunctionOverRangeVectorSplit struct {
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Func                     FunctionOverRangeVectorDefinition
	Annotations              *annotations.Annotations
	metricNames              *operators.MetricNames
	timeRange                types.QueryTimeRange
	enableDelayedNameRemoval bool
	expressionPosition       posrange.PositionRange
	emitAnnotationFunc       types.EmitAnnotationFunc
	seriesValidationFunc     RangeVectorSeriesValidationFunction

	irCache *cache.IntermediateResultTenantCache

	innerNode      RangeVectorNode
	materializer   *planning.Materializer
	queryTimeRange types.QueryTimeRange
	splitDuration  time.Duration
	innerCacheKey  string

	splits []Split
	// seriesToSplits is ordered the same way as SeriesMetadata
	seriesToSplits   [][]SplitSeries
	currentSeriesIdx int
}

var _ types.InstantVectorOperator = &FunctionOverRangeVectorSplit{}

func NewFunctionOverRangeVectorSplit(
	innerNode RangeVectorNode,
	materializer *planning.Materializer,
	timeRange types.QueryTimeRange,
	splitDuration time.Duration,
	irCache *cache.IntermediateResultTenantCache,
	funcDef FunctionOverRangeVectorDefinition,
	expressionPosition posrange.PositionRange,
	annotations *annotations.Annotations,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	enableDelayedNameRemoval bool,
) (*FunctionOverRangeVectorSplit, error) {
	if !timeRange.IsInstant {
		return nil, fmt.Errorf("FunctionOverRangeVectorSplit only supports instant queries")
	}

	innerCacheKey := planning.CacheKey(innerNode)

	o := &FunctionOverRangeVectorSplit{
		innerNode:                innerNode,
		materializer:             materializer,
		queryTimeRange:           timeRange,
		splitDuration:            splitDuration,
		innerCacheKey:            innerCacheKey,
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

func (m *FunctionOverRangeVectorSplit) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverRangeVectorSplit) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	var err error

	m.splits, err = m.createSplits()
	if err != nil {
		return nil, err
	}

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

func (m *FunctionOverRangeVectorSplit) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
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

func (m *FunctionOverRangeVectorSplit) emitAnnotation(generator types.AnnotationGenerator) {
	metricName := m.metricNames.GetMetricNameForSeries(m.currentSeriesIdx)
	pos := m.innerNode.ExpressionPosition()

	m.Annotations.Add(generator(metricName, pos))
}

func (m *FunctionOverRangeVectorSplit) Prepare(ctx context.Context, params *types.PrepareParams) error {
	return nil
}

func (m *FunctionOverRangeVectorSplit) Finalize(ctx context.Context) error {
	for _, split := range m.splits {
		// TODO: ideally write results to cache earlier than this so we avoid holding all intermediate results in memory when not needed
		if err := split.Finalize(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *FunctionOverRangeVectorSplit) Close() {
	for _, split := range m.splits {
		split.Close()
	}
}

// createSplits creates splits for the given time range, checking for cache entries and merging contiguous uncached
// split ranges to create uncached splits.
// Uses query time range (m.timeRange.StartT - innerRange to m.timeRange.StartT) to calculate split boundaries.
// Currently calculates split boundaries based on the query time range (m.timeRange.StartT - innerRange to m.timeRange.StartT).
// TODO: Should instead account for timestamp and offset to align with how samples are divided into blocks in storage.
// Possibly can use some logic in QueriedTimeRange to decide on best way to create splits
func (m *FunctionOverRangeVectorSplit) createSplits() ([]Split, error) {
	var splits []Split
	splitDurationMs := m.splitDuration.Milliseconds()

	innerRange := m.innerNode.GetRange().Milliseconds()
	startTs := m.timeRange.StartT - innerRange
	endTs := m.timeRange.StartT

	alignedStart := (startTs / splitDurationMs) * splitDurationMs
	if alignedStart < startTs {
		alignedStart += splitDurationMs
	}

	var currentUncachedStart int64
	var currentUncachedRanges []SplitRange
	currentPos := startTs

	if currentPos < alignedStart {
		headRange := SplitRange{
			Start:     currentPos,
			End:       alignedStart,
			Cacheable: false,
		}
		currentUncachedStart = currentPos
		currentUncachedRanges = []SplitRange{headRange}
		currentPos = alignedStart + 1
	}

	for splitStart := alignedStart; splitStart+splitDurationMs <= endTs; splitStart += splitDurationMs {
		splitEnd := splitStart + splitDurationMs

		cachedBlock, found := m.irCache.Get(m.Func.Name, m.innerCacheKey, splitStart, splitEnd, m.MemoryConsumptionTracker)

		if found {
			if currentUncachedRanges != nil {
				lastRange := currentUncachedRanges[len(currentUncachedRanges)-1]
				operator, err := m.materializeOperatorForTimeRange(currentUncachedStart, lastRange.End)
				if err != nil {
					return nil, err
				}

				splits = append(splits, NewUncachedSplit(
					currentUncachedRanges,
					operator,
					m,
				))
				currentUncachedRanges = nil
			}

			splits = append(splits, NewCachedSplit(
				&cachedBlock,
				m,
			))
			currentPos = splitEnd + 1
		} else {
			uncachedRange := SplitRange{
				Start:     splitStart,
				End:       splitEnd,
				Cacheable: true,
			}

			if currentUncachedRanges == nil {
				currentUncachedStart = splitStart
				currentUncachedRanges = []SplitRange{uncachedRange}
			} else {
				currentUncachedRanges = append(currentUncachedRanges, uncachedRange)
			}
			currentPos = splitEnd + 1
		}
	}

	if currentPos < endTs {
		tailRange := SplitRange{
			Start:     currentPos,
			End:       endTs,
			Cacheable: false,
		}

		if currentUncachedRanges == nil {
			operator, err := m.materializeOperatorForTimeRange(currentPos, endTs)
			if err != nil {
				return nil, err
			}

			splits = append(splits, NewUncachedSplit(
				[]SplitRange{tailRange},
				operator,
				m,
			))
		} else {
			currentUncachedRanges = append(currentUncachedRanges, tailRange)
		}
	}

	if currentUncachedRanges != nil {
		lastRange := currentUncachedRanges[len(currentUncachedRanges)-1]
		operator, err := m.materializeOperatorForTimeRange(currentUncachedStart, lastRange.End)
		if err != nil {
			return nil, err
		}

		splits = append(splits, NewUncachedSplit(
			currentUncachedRanges,
			operator,
			m,
		))
	}

	return splits, nil
}

func (m *FunctionOverRangeVectorSplit) materializeOperatorForTimeRange(start int64, end int64) (types.RangeVectorOperator, error) {
	subRange := time.Duration(end-start) * time.Millisecond
	subNode := m.innerNode.CreateNodeForSubRange(subRange)
	// Set the time range for the split rather than adding to the offset so right timestamps get returned
	splitTimeRange := types.NewInstantQueryTimeRange(promts.Time(end))

	op, err := m.materializer.ConvertNodeToOperator(subNode, splitTimeRange)
	if err != nil {
		return nil, err
	}

	innerOperator, ok := op.(types.RangeVectorOperator)
	if !ok {
		return nil, fmt.Errorf("error materializing subnode: expected RangeVectorOperator, got %T", op)
	}

	return innerOperator, nil
}

func (m *FunctionOverRangeVectorSplit) mergeSplitsMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, [][]SplitSeries, error) {
	seriesMap := make(map[string]int)
	// TODO: track memory usage of seriesToSplits? Could be large if lots of series + lots of splits.
	var seriesToSplits [][]SplitSeries

	// First get the metadata for each split.
	// Merging will be done in a separate for loop.
	// We have two loops because getting a metadata slice from the pool requires providing a max size for the slice -
	// the memory consumption tracker increments the memory usage by the capacity of the slice when getting from the
	// pool and decrements by the capacity when returning to the pool. The capacity should not change when using the
	// slice, otherwise the memory tracking messes up.
	// We estimate the metadata slice length by adding up the lengths of all the metadata from all the splits.
	// TODO: this could result in very large slices being requested but a lot of unused capacity if the splits share
	//  the same series (which can be likely). It also requires us to keep metadata for all the splits in memory at the
	//  same time.
	splitsMetadata := make([][]types.SeriesMetadata, 0, len(m.splits))
	maxPossible := 0

	for _, split := range m.splits {
		splitSeries, err := split.SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}
		splitsMetadata = append(splitsMetadata, splitSeries)
		maxPossible += len(splitSeries) // Sum for absolute maximum (no deduplication yet)
	}

	mergedMetadata, err := types.SeriesMetadataSlicePool.Get(maxPossible, m.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, err
	}

	labelBytes := make([]byte, 0, 1024)

	for splitIdx, splitSeries := range splitsMetadata {
		for splitLocalIdx, serieMetadata := range splitSeries {
			labelBytes = serieMetadata.Labels.Bytes(labelBytes)
			key := string(labelBytes)

			mergedIdx, exists := seriesMap[key]
			if !exists {
				mergedIdx = len(mergedMetadata)
				seriesMap[key] = mergedIdx

				// Clone labels for mergedMetadata and track them.
				// This separates the merged metadata from the metadata for each split, making memory tracking easier at
				// the expense of duplicating data (i.e. increased memory usage).
				// TODO: stop duplicating metadata when unnecessary
				clonedMetadata := types.SeriesMetadata{
					Labels:   serieMetadata.Labels.Copy(),
					DropName: serieMetadata.DropName,
				}
				if err := m.MemoryConsumptionTracker.IncreaseMemoryConsumptionForLabels(clonedMetadata.Labels); err != nil {
					return nil, nil, err
				}
				mergedMetadata = append(mergedMetadata, clonedMetadata) // Safe - no reallocation
				seriesToSplits = append(seriesToSplits, nil)
			}
			seriesToSplits[mergedIdx] = append(seriesToSplits[mergedIdx], SplitSeries{
				SplitIdx:      splitIdx,
				SplitLocalIdx: splitLocalIdx,
			})

			labelBytes = labelBytes[:0]
		}
	}

	return mergedMetadata, seriesToSplits, nil
}

type RangeVectorNode interface {
	planning.Node
	GetRange() time.Duration
	CreateNodeForSubRange(updatedRange time.Duration) planning.Node
}

// SplitRange represents a time range within a split.
// Start is exclusive (points with timestamp > Start are included).
// End is inclusive (points with timestamp <= End are included).
type SplitRange struct {
	Start     int64
	End       int64
	Cacheable bool
}

type Split interface {
	SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error)
	GetResultsAtIdx(ctx context.Context, splitLocalIdx int) ([]cache.IntermediateResult, error)
	Finalize(ctx context.Context) error
	Close()
}

type SplitSeries struct {
	SplitIdx      int
	SplitLocalIdx int
}

type CachedSplit struct {
	cachedResults *cache.IntermediateResultBlock
	parent        *FunctionOverRangeVectorSplit
}

func NewCachedSplit(cachedResults *cache.IntermediateResultBlock, parent *FunctionOverRangeVectorSplit) *CachedSplit {
	return &CachedSplit{
		cachedResults: cachedResults,
		parent:        parent,
	}
}

func (c *CachedSplit) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	return c.cachedResults.Series, nil
}

func (c *CachedSplit) GetResultsAtIdx(ctx context.Context, splitLocalIdx int) ([]cache.IntermediateResult, error) {
	if splitLocalIdx >= len(c.cachedResults.Results) {
		return nil, fmt.Errorf("series index %d out of range (have %d series)", splitLocalIdx, len(c.cachedResults.Results))
	}
	return []cache.IntermediateResult{c.cachedResults.Results[splitLocalIdx]}, nil
}

func (c *CachedSplit) Finalize(ctx context.Context) error {
	return nil
}

func (c *CachedSplit) Close() {
	if c.cachedResults != nil && c.cachedResults.Series != nil {
		types.SeriesMetadataSlicePool.Put(&c.cachedResults.Series, c.parent.MemoryConsumptionTracker)
		c.cachedResults.Series = nil
	}
}

type UncachedSplit struct {
	ranges   []SplitRange
	operator types.RangeVectorOperator

	parent *FunctionOverRangeVectorSplit

	seriesMetadata []types.SeriesMetadata
	computedBlocks []cache.IntermediateResultBlock
	writtenToCache bool
}

func NewUncachedSplit(
	ranges []SplitRange,
	operator types.RangeVectorOperator,
	parent *FunctionOverRangeVectorSplit,
) *UncachedSplit {
	return &UncachedSplit{
		ranges:         ranges,
		operator:       operator,
		parent:         parent,
		writtenToCache: false,
	}
}

func (p *UncachedSplit) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	var err error
	p.seriesMetadata, err = p.operator.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, err
	}

	p.computedBlocks = make([]cache.IntermediateResultBlock, len(p.ranges))
	for rangeIdx := range p.ranges {
		p.computedBlocks[rangeIdx].Series = p.seriesMetadata
		// TODO: should results be tracked by memory tracker?
		p.computedBlocks[rangeIdx].Results = make([]cache.IntermediateResult, 0, len(p.seriesMetadata))
	}

	for seriesIdx := 0; seriesIdx < len(p.seriesMetadata); seriesIdx++ {
		if err := p.operator.NextSeries(ctx); err != nil {
			return nil, err
		}

		step, err := p.operator.NextStepSamples(ctx)
		if err != nil {
			return nil, err
		}

		for rangeIdx, splitRange := range p.ranges {
			rangeStep, err := step.SubStep(splitRange.Start, splitRange.End)
			if err != nil {
				return nil, err
			}

			result, err := p.parent.Func.GenerateFunc(rangeStep, []types.ScalarData{}, p.parent.emitAnnotationFunc, p.parent.MemoryConsumptionTracker)
			if err != nil {
				return nil, err
			}
			p.computedBlocks[rangeIdx].Results = append(p.computedBlocks[rangeIdx].Results, result)
		}
	}

	return p.seriesMetadata, nil
}

func (p *UncachedSplit) GetResultsAtIdx(ctx context.Context, splitLocalIdx int) ([]cache.IntermediateResult, error) {
	if splitLocalIdx >= len(p.seriesMetadata) {
		return nil, fmt.Errorf("series index %d out of range (have %d series)", splitLocalIdx, len(p.seriesMetadata))
	}

	results := make([]cache.IntermediateResult, 0, len(p.computedBlocks))
	for _, block := range p.computedBlocks {
		if splitLocalIdx < len(block.Results) {
			results = append(results, block.Results[splitLocalIdx])
		}
	}
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

		block := p.computedBlocks[rangeIdx]
		block.Version = 1
		block.StartTimestampMs = int(splitRange.Start)
		block.EndTimestampMs = int(splitRange.End)
		_ = p.parent.irCache.Set(p.parent.Func.Name, p.parent.innerCacheKey, splitRange.Start, splitRange.End, block)
	}

	p.writtenToCache = true
	return nil
}

func (p *UncachedSplit) Close() {
	if p.seriesMetadata != nil {
		types.SeriesMetadataSlicePool.Put(&p.seriesMetadata, p.parent.MemoryConsumptionTracker)
		p.seriesMetadata = nil
	}

	if p.operator != nil {
		p.operator.Close()
	}
}
