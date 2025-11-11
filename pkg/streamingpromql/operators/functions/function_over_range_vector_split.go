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

		cachedBlock, found := m.irCache.Get(m.Func.Name, m.innerCacheKey, splitStart, splitEnd)

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
	var mergedMetadata []types.SeriesMetadata
	var seriesToSplits [][]SplitSeries

	labelBytes := make([]byte, 0, 1024)

	for splitIdx, split := range m.splits {
		splitSeries, err := split.SeriesMetadata(ctx, matchers)
		if err != nil {
			return nil, nil, err
		}

		for splitLocalIdx, serieMetadata := range splitSeries {
			labelBytes = serieMetadata.Labels.Bytes(labelBytes)
			key := string(labelBytes)

			mergedIdx, exists := seriesMap[key]
			if !exists {
				mergedIdx = len(mergedMetadata)
				seriesMap[key] = mergedIdx
				mergedMetadata = append(mergedMetadata, serieMetadata)
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
}

func NewCachedSplit(cachedResults *cache.IntermediateResultBlock) *CachedSplit {
	return &CachedSplit{
		cachedResults: cachedResults,
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
		p.computedBlocks[rangeIdx].Series = make([]types.SeriesMetadata, 0, len(p.seriesMetadata))
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
			if splitRange.End < splitRange.Start {
				zeroResult := cache.IntermediateResult{
					SumOverTime: cache.SumOverTimeIntermediate{ // TODO: empty rather than zero?
						SumF:     0,
						HasFloat: true,
					},
				}
				p.computedBlocks[rangeIdx].Series = append(p.computedBlocks[rangeIdx].Series, p.seriesMetadata[seriesIdx])
				p.computedBlocks[rangeIdx].Results = append(p.computedBlocks[rangeIdx].Results, zeroResult)
				continue
			}

			rangeStep, err := step.SubStep(splitRange.Start, splitRange.End)
			if err != nil {
				return nil, err
			}

			result, err := p.parent.Func.GenerateFunc(rangeStep, []types.ScalarData{}, p.parent.emitAnnotationFunc, p.parent.MemoryConsumptionTracker)
			if err != nil {
				return nil, err
			}
			p.computedBlocks[rangeIdx].Series = append(p.computedBlocks[rangeIdx].Series, p.seriesMetadata[seriesIdx])
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
	if p.operator != nil {
		p.operator.Close()
	}
}
