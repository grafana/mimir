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
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// FunctionOverRangeVectorSplit performs a range vector function calculation with intermediate result caching.
// It splits the query range into fixed-size blocks and caches intermediate results to improve performance
// for repeated queries.
type FunctionOverRangeVectorSplit struct {
	Inner                    types.RangeVectorOperator
	ScalarArgs               []types.ScalarOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Func                     FunctionOverRangeVectorDefinition

	Annotations *annotations.Annotations

	scalarArgsData []types.ScalarData

	metricNames        *operators.MetricNames
	currentSeriesIndex int

	timeRange                types.QueryTimeRange
	enableDelayedNameRemoval bool

	expressionPosition   posrange.PositionRange
	emitAnnotationFunc   types.EmitAnnotationFunc
	seriesValidationFunc RangeVectorSeriesValidationFunction

	// Intermediate result caching
	cacheKey            string
	intermediateResults []IntermediateResultBlock
	// seriesToIRRefs is in the same order as metadata returned from []SeriesMetadata
	// If there is uncached data, it will be the first element in the slice, all other refs will be ordered by time
	seriesToIRRefs [][]IRSeriesRef
	IRCache        *cache.IntermediateResultTenantCache
	metadata       []types.SeriesMetadata
}

var _ types.InstantVectorOperator = &FunctionOverRangeVectorSplit{}

func NewFunctionOverRangeVectorSplit(
	inner types.RangeVectorOperator,
	scalarArgs []types.ScalarOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	f FunctionOverRangeVectorDefinition,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
	enableDelayedNameRemoval bool,
	cacheKey string,
	irCache *cache.IntermediateResultTenantCache,
	blocks []IntermediateResultBlock,
) *FunctionOverRangeVectorSplit {
	o := &FunctionOverRangeVectorSplit{
		Inner:                    inner,
		ScalarArgs:               scalarArgs,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Func:                     f,
		Annotations:              annotations,
		expressionPosition:       expressionPosition,
		timeRange:                timeRange,
		enableDelayedNameRemoval: enableDelayedNameRemoval,
		cacheKey:                 cacheKey,
		IRCache:                  irCache,
		intermediateResults:      blocks,
	}

	if f.SeriesValidationFuncFactory != nil {
		o.seriesValidationFunc = f.SeriesValidationFuncFactory()
	}

	if f.NeedsSeriesNamesForAnnotations {
		o.metricNames = &operators.MetricNames{}
	}

	o.emitAnnotationFunc = o.emitAnnotation

	return o
}

func (m *FunctionOverRangeVectorSplit) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverRangeVectorSplit) SeriesMetadata(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, error) {
	if err := m.processScalarArgs(ctx); err != nil {
		return nil, err
	}

	// Blocks must have been calculated during the planning phase
	if m.intermediateResults == nil {
		return nil, fmt.Errorf("BUG: FunctionOverRangeVectorSplit created without cache blocks - blocks must be calculated in planning phase")
	}

	// Load blocks from cache
	for i := range m.intermediateResults {
		duration := time.Duration(m.intermediateResults[i].DurationMs) * time.Millisecond
		b, found := m.IRCache.Get(m.Func.Name, m.cacheKey, m.intermediateResults[i].StartTimestampMs, duration)
		if found {
			m.intermediateResults[i].ir = b
			m.intermediateResults[i].loadedFromCache = true
		} else {
			m.intermediateResults[i].ir = cache.IntermediateResultBlock{
				Version:          -1, // -1 means not found in cache
				StartTimestampMs: int(m.intermediateResults[i].StartTimestampMs),
				DurationMs:       int(m.intermediateResults[i].DurationMs),
				Series:           nil,
				Results:          nil,
			}
		}
	}

	var err error
	m.metadata, m.seriesToIRRefs, err = m.seriesMetadataWithIntermediate(ctx, matchers)
	if err != nil {
		return nil, err
	}

	if m.metricNames != nil {
		m.metricNames.CaptureMetricNames(m.metadata)
	}

	if m.Func.SeriesMetadataFunction.Func != nil {
		return m.Func.SeriesMetadataFunction.Func(m.metadata, m.MemoryConsumptionTracker, m.enableDelayedNameRemoval)
	}

	return m.metadata, nil
}

func (m *FunctionOverRangeVectorSplit) processScalarArgs(ctx context.Context) error {
	if len(m.ScalarArgs) == 0 {
		return nil
	}

	m.scalarArgsData = make([]types.ScalarData, 0, len(m.ScalarArgs))

	for _, arg := range m.ScalarArgs {
		d, err := arg.GetValues(ctx)
		if err != nil {
			return err
		}
		m.scalarArgsData = append(m.scalarArgsData, d)
	}

	return nil
}

func (m *FunctionOverRangeVectorSplit) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	// For now, only support instant queries (range queries will fall back to non-split version)
	if !m.timeRange.IsInstant {
		return types.InstantVectorSeriesData{}, fmt.Errorf("FunctionOverRangeVectorSplit only supports instant queries for now")
	}

	if m.currentSeriesIndex >= len(m.seriesToIRRefs) {
		return types.InstantVectorSeriesData{}, types.EOS
	}

	result := m.seriesToIRRefs[m.currentSeriesIndex]
	var cachedBlockRefsForSeries []IRSeriesRef
	var hasUncached bool

	// If the series has an uncached component, get the next series from inner operator
	if len(result) > 0 && result[0].IRBlockIdx == UncachedSeriesRef {
		hasUncached = true
		if err := m.Inner.NextSeries(ctx); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		cachedBlockRefsForSeries = result[1:] // cached blocks for this series
	} else {
		cachedBlockRefsForSeries = result // purely cached series
	}

	defer func() {
		m.currentSeriesIndex++
	}()

	data := types.InstantVectorSeriesData{}

	// For instant query, there's only one step
	var step *types.RangeVectorStepData
	var err error

	if hasUncached {
		// Get the single step from inner operator
		step, err = m.Inner.NextStepSamples(ctx)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	} else {
		// For purely cached series, calculate step info directly
		// (no need for StepMover since instant queries have only one step)
		params := m.Inner.StepCalculationParams()
		stepT := m.timeRange.StartT
		rangeEnd := stepT
		if params.Timestamp != nil {
			rangeEnd = *params.Timestamp
		}
		rangeEnd = rangeEnd - params.Offset
		rangeStart := rangeEnd - params.RangeMilliseconds

		step = &types.RangeVectorStepData{
			StepT:      stepT,
			RangeStart: rangeStart,
			RangeEnd:   rangeEnd,
		}
	}

	// Find which cached blocks overlap with this step's range
	var currentIRBlockStart, currentIRBlockEnd int
	for ; currentIRBlockStart < len(m.intermediateResults) && step.RangeStart >= m.intermediateResults[currentIRBlockStart].StartTimestampMs+m.intermediateResults[currentIRBlockStart].DurationMs; currentIRBlockStart++ {
	}
	for ; currentIRBlockEnd < len(m.intermediateResults) && step.RangeEnd >= m.intermediateResults[currentIRBlockEnd].StartTimestampMs+m.intermediateResults[currentIRBlockEnd].DurationMs; currentIRBlockEnd++ {
	}

	pieces := []cache.IntermediateResult{}

	// Calculate head piece (data before first cached block)
	if hasUncached && currentIRBlockStart < len(m.intermediateResults) && step.RangeStart < m.intermediateResults[currentIRBlockStart].StartTimestampMs {
		headEnd := m.intermediateResults[currentIRBlockStart].StartTimestampMs
		if step.RangeEnd < headEnd {
			headEnd = step.RangeEnd
		}
		headStep, err := step.SubStep(step.RangeStart, headEnd)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		headPiece, err := m.Func.GenerateFunc(headStep, m.scalarArgsData, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		pieces = append(pieces, headPiece)
	}

	// Process middle blocks (use cached results if available, otherwise compute)
	currentCachedBlockRefIdx := 0
	for currentBlock := currentIRBlockStart; currentBlock < currentIRBlockEnd; currentBlock++ {
		var foundCached bool
		if m.intermediateResults[currentBlock].loadedFromCache && len(cachedBlockRefsForSeries) > 0 {
			// Find the cached result for this block and series (if it exists)
			for ; currentCachedBlockRefIdx < len(cachedBlockRefsForSeries) && cachedBlockRefsForSeries[currentCachedBlockRefIdx].IRBlockIdx < currentBlock; currentCachedBlockRefIdx++ {
			}
			if currentCachedBlockRefIdx < len(cachedBlockRefsForSeries) && cachedBlockRefsForSeries[currentCachedBlockRefIdx].IRBlockIdx == currentBlock {
				pieces = append(pieces, m.intermediateResults[currentBlock].ir.Results[cachedBlockRefsForSeries[currentCachedBlockRefIdx].SeriesIdx])
				foundCached = true
			}
		}

		// If not cached, compute it (only if we have sample data)
		if !foundCached && hasUncached {
			blockStep, err := step.SubStep(m.intermediateResults[currentBlock].StartTimestampMs, m.intermediateResults[currentBlock].StartTimestampMs+m.intermediateResults[currentBlock].DurationMs)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			piece, err := m.Func.GenerateFunc(blockStep, m.scalarArgsData, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			pieces = append(pieces, piece)
		}
	}

	// Calculate tail piece (data after last cached block)
	if hasUncached {
		var tailStart int64
		if currentIRBlockEnd > 0 && currentIRBlockEnd <= len(m.intermediateResults) {
			tailStart = m.intermediateResults[currentIRBlockEnd-1].StartTimestampMs + m.intermediateResults[currentIRBlockEnd-1].DurationMs
		} else if currentIRBlockStart < len(m.intermediateResults) {
			tailStart = m.intermediateResults[currentIRBlockStart].StartTimestampMs
		} else {
			tailStart = step.RangeStart
		}

		if tailStart < step.RangeEnd {
			tailStep, err := step.SubStep(tailStart, step.RangeEnd)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			tailPiece, err := m.Func.GenerateFunc(tailStep, m.scalarArgsData, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
			if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			pieces = append(pieces, tailPiece)
		}
	}

	// Combine all pieces into final result
	f, hasFloat, h, err := m.Func.CombineFunc(pieces, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	// For instant query, we only have one point
	if hasFloat {
		data.Floats, err = types.FPointSlicePool.Get(1, m.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		data.Floats = append(data.Floats, promql.FPoint{T: step.StepT, F: f})
	}
	if h != nil {
		data.Histograms, err = types.HPointSlicePool.Get(1, m.MemoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		data.Histograms = append(data.Histograms, promql.HPoint{T: step.StepT, H: h})
	}

	if m.seriesValidationFunc != nil {
		m.seriesValidationFunc(data, m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex), m.emitAnnotationFunc)
	}

	return data, nil
}

func (m *FunctionOverRangeVectorSplit) emitAnnotation(generator types.AnnotationGenerator) {
	metricName := m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex)
	pos := m.Inner.ExpressionPosition()

	if m.Func.UseFirstArgumentPositionForAnnotations {
		pos = m.ScalarArgs[0].ExpressionPosition()
	}

	m.Annotations.Add(generator(metricName, pos))
}

func (m *FunctionOverRangeVectorSplit) Prepare(ctx context.Context, params *types.PrepareParams) error {
	err := m.Inner.Prepare(ctx, params)
	if err != nil {
		return err
	}

	for _, sa := range m.ScalarArgs {
		err := sa.Prepare(ctx, params)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *FunctionOverRangeVectorSplit) Finalize(ctx context.Context) error {
	// Write intermediate results to cache
	if len(m.intermediateResults) > 0 {
		for i := range m.intermediateResults {
			// Only write blocks that we computed (not loaded from cache)
			if !m.intermediateResults[i].loadedFromCache {
				duration := time.Duration(m.intermediateResults[i].DurationMs) * time.Millisecond
				_ = m.IRCache.Set(m.Func.Name, m.cacheKey, m.intermediateResults[i].StartTimestampMs, duration, m.intermediateResults[i].ir)
				// Mark as loaded so we don't write again on subsequent Finalize() calls (pedantic mode calls twice)
				m.intermediateResults[i].loadedFromCache = true
				// Ignore errors - cache write failures shouldn't fail the query
			}
		}
	}

	err := m.Inner.Finalize(ctx)
	if err != nil {
		return err
	}

	for _, sa := range m.ScalarArgs {
		err := sa.Finalize(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *FunctionOverRangeVectorSplit) Close() {
	m.Inner.Close()

	for _, d := range m.scalarArgsData {
		types.FPointSlicePool.Put(&d.Samples, m.MemoryConsumptionTracker)
	}

	m.scalarArgsData = nil

	// Note: m.metadata is returned from SeriesMetadata() and owned by the caller (Query),
	// which will clean it up in returnResultToPool(). We don't clean it up here.

	m.intermediateResults = nil
}

// seriesMetadataWithIntermediate merges the series metadata from uncached samples with cached blocks.
// This logic is based on deduplicate_and_merge.go but simplified for instant queries only.
func (m *FunctionOverRangeVectorSplit) seriesMetadataWithIntermediate(ctx context.Context, matchers types.Matchers) ([]types.SeriesMetadata, [][]IRSeriesRef, error) {
	// Why use a string, rather than the labels hash as a key here? This avoids any issues with hash collisions.
	labelsToRefUncached := map[string][]IRSeriesRef{}

	// Why 1024 bytes? It's what labels.Labels.String() uses as a buffer size, so we use that as a sensible starting point too.
	labelBytes := make([]byte, 0, 1024)

	// Get metadata for uncached series (instant query has single time point)
	uncachedMetadata, err := m.Inner.SeriesMetadata(ctx, matchers)
	if err != nil {
		return nil, nil, err
	}

	for seriesIdx, series := range uncachedMetadata {
		labelBytes = series.Labels.Bytes(labelBytes)
		g, groupExists := labelsToRefUncached[string(labelBytes)]
		if !groupExists {
			labelsToRefUncached[string(labelBytes)] = []IRSeriesRef{{UncachedSeriesRef, seriesIdx}}
		} else {
			labelsToRefUncached[string(labelBytes)] = append(g, IRSeriesRef{UncachedSeriesRef, seriesIdx})
		}
	}

	// Separate map for cache-only series (uncached series must maintain their original order)
	labelsToRefCachedOnly := make(map[string][]IRSeriesRef)

	for irIdx, result := range m.intermediateResults {
		for seriesIdx, series := range result.ir.Series {
			labelBytes = series.Labels.Bytes(labelBytes)
			g, groupExists := labelsToRefUncached[string(labelBytes)]
			if !groupExists {
				cg, cgroupExists := labelsToRefCachedOnly[string(labelBytes)]
				if !cgroupExists {
					labelsToRefCachedOnly[string(labelBytes)] = []IRSeriesRef{{irIdx, seriesIdx}}
				} else {
					labelsToRefCachedOnly[string(labelBytes)] = append(cg, IRSeriesRef{irIdx, seriesIdx})
				}
			} else {
				labelsToRefUncached[string(labelBytes)] = append(g, IRSeriesRef{irIdx, seriesIdx})
			}
		}
	}

	seriesToBlockRefs := make([][]IRSeriesRef, 0, len(labelsToRefUncached)+len(labelsToRefCachedOnly))

	// Order by uncached metadata (as Inner.NextSeries() is called in this order)
	// TODO: Series ordering might not be stable if we make multiple SeriesMetadata calls for different time ranges.
	// This could be an issue if the underlying data source doesn't guarantee stable ordering.
	for _, metadata := range uncachedMetadata {
		labelBytes = metadata.Labels.Bytes(labelBytes)
		seriesToBlockRefs = append(seriesToBlockRefs, labelsToRefUncached[string(labelBytes)])
	}

	// Add any cache-only series to the end
	// TODO: Deterministic order needed for cache-only series
	for _, metadata := range labelsToRefCachedOnly {
		seriesToBlockRefs = append(seriesToBlockRefs, metadata)
	}

	outputMetadata, err := types.SeriesMetadataSlicePool.Get(len(seriesToBlockRefs), m.MemoryConsumptionTracker)

	if err != nil {
		return nil, nil, err
	}

	for _, blockRefs := range seriesToBlockRefs {
		first := blockRefs[0]
		if first.IRBlockIdx == UncachedSeriesRef {
			// Uncached metadata: labels are tracked in uncachedMetadata slice, need to track in outputMetadata too
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, uncachedMetadata[first.SeriesIdx])
			if err != nil {
				return nil, nil, err
			}
		} else {
			// Cached metadata needs to be tracked (it's coming from cache into query memory)
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, m.intermediateResults[first.IRBlockIdx].ir.Series[first.SeriesIdx])
			if err != nil {
				return nil, nil, err
			}
		}
	}

	// Now we have a new metadata slice with all labels tracked, return uncachedMetadata to pool
	types.SeriesMetadataSlicePool.Put(&uncachedMetadata, m.MemoryConsumptionTracker)

	return outputMetadata, seriesToBlockRefs, nil
}

type IntermediateResultBlock struct {
	loadedFromCache  bool
	StartTimestampMs int64 // Exported for use by querysplitting package
	DurationMs       int64 // Exported for use by querysplitting package
	ir               cache.IntermediateResultBlock
}

type IRSeriesRef struct {
	IRBlockIdx int
	// SeriesIdx is the index of the series in the intermediate result block
	SeriesIdx int
}

const UncachedSeriesRef = -1
