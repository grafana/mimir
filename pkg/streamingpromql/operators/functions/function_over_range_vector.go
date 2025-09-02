// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

// FunctionOverRangeVector performs a rate calculation over a range vector.
// This struct represents one invocation of a function.
type FunctionOverRangeVector struct {
	Inner                    types.RangeVectorOperator
	ScalarArgs               []types.ScalarOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Func                     FunctionOverRangeVectorDefinition

	Annotations *annotations.Annotations

	scalarArgsData []types.ScalarData

	metricNames        *operators.MetricNames
	currentSeriesIndex int

	timeRange types.QueryTimeRange

	buildingIntermediate bool
	usingIntermediate    bool
	intermediateResults  []IntermediateResultBlock
	seriesToBlockRefs    [][]BlockSeriesRef

	expressionPosition   posrange.PositionRange
	emitAnnotationFunc   types.EmitAnnotationFunc
	seriesValidationFunc RangeVectorSeriesValidationFunction
}

var _ types.InstantVectorOperator = &FunctionOverRangeVector{}

func NewFunctionOverRangeVector(
	inner types.RangeVectorOperator,
	scalarArgs []types.ScalarOperator,
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker,
	f FunctionOverRangeVectorDefinition,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
	timeRange types.QueryTimeRange,
) *FunctionOverRangeVector {
	o := &FunctionOverRangeVector{
		Inner:                    inner,
		ScalarArgs:               scalarArgs,
		MemoryConsumptionTracker: memoryConsumptionTracker,
		Func:                     f,
		Annotations:              annotations,
		expressionPosition:       expressionPosition,
		timeRange:                timeRange,
	}

	if f.SeriesValidationFuncFactory != nil {
		o.seriesValidationFunc = f.SeriesValidationFuncFactory()
	}

	if f.NeedsSeriesNamesForAnnotations {
		o.metricNames = &operators.MetricNames{}
	}

	o.emitAnnotationFunc = o.emitAnnotation // This is an optimisation to avoid creating the EmitAnnotationFunc instance on every usage.

	return o
}

func (m *FunctionOverRangeVector) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverRangeVector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	if err := m.processScalarArgs(ctx); err != nil {
		return nil, err
	}

	if m.Func.GenerateFunc != nil {
		if m.Inner.Range() > minDurationToCache {
			// We probably have the user somewhere.
			// Probably don't have the selector?  FIXME
			// Should we have an upper limit on the range we will attempt to cache?  FIXME
			m.intermediateResults = fakeCache("user", m.Func.Name, "selector", m.timeRange.StartT, m.Inner.Range())
			if len(m.intermediateResults) == int(m.Inner.Range()/intermediateCacheBlockLength) {
				m.usingIntermediate = true
			} else {
				m.buildingIntermediate = true
			}
		}
	}

	var metadata []types.SeriesMetadata
	var err error

	if m.usingIntermediate || m.buildingIntermediate {
		var seriesToBlockRefs [][]BlockSeriesRef
		metadata, seriesToBlockRefs, err = m.seriesMetadataWithIntermediate(ctx)
		if err != nil {
			return nil, err
		}
		m.seriesToBlockRefs = seriesToBlockRefs
	} else {
		metadata, err = m.Inner.SeriesMetadata(ctx)
		if err != nil {
			return nil, err
		}
	}

	if m.metricNames != nil {
		m.metricNames.CaptureMetricNames(metadata)
	}

	if m.Func.SeriesMetadataFunction.Func != nil {
		return m.Func.SeriesMetadataFunction.Func(metadata, m.MemoryConsumptionTracker)
	}

	return metadata, nil
}

func fakeCache(user, function, selector string, start int64, duration time.Duration) []IntermediateResultBlock {
	// Quantize and split the time range.
	return []IntermediateResultBlock{
		{
			StartTimestampMs: 0,
		},
	}
}

func (m *FunctionOverRangeVector) processScalarArgs(ctx context.Context) error {
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

// Compute the function for one series, and return results for all steps computed by m.Inner.
func (m *FunctionOverRangeVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	// Decide whether we need to move to the next uncached series
	var cachedResult []BlockSeriesRef
	var hasUncached bool
	if (m.usingIntermediate || m.buildingIntermediate) && m.currentSeriesIndex < len(m.seriesToBlockRefs) {
		result := m.seriesToBlockRefs[m.currentSeriesIndex]
		// If the ordered series groups has an uncached component, move to the next inner series
		// The uncached component is always the first elem in the slice
		// TODO: refactor and encapsulate cached pieces more, so instead of all this lookups being done here, the logic
		// should just be to check the next series coming which source (cached, uncached, or both).
		if result[0].SeriesIdx == UncachedSeriesRef {
			hasUncached = true
			if err := m.Inner.NextSeries(ctx); err != nil {
				return types.InstantVectorSeriesData{}, err
			}
			cachedResult = result[1:] // ignore uncached results
		} else {
			cachedResult = result
		}
	} else {
		if err := m.Inner.NextSeries(ctx); err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}
	defer func() {
		m.currentSeriesIndex++
	}()

	data := types.InstantVectorSeriesData{}
	var pieces []IntermediateResult

	// FIXME: compile time err introduced after rebasing - m.Inner.Range() was removed in https://github.com/grafana/mimir/pull/12489
	if m.usingIntermediate || m.buildingIntermediate {
		pieces = make([]IntermediateResult, (m.Inner.Range()/intermediateCacheBlockLength)+2) // add two for head and tail
	}

	for {
		// FIXME when using cached pieces we only want the head and tail here; when building pieces we want multiple blocks.
		// TODO: handle nil case when hasUncached == false
		var step *types.RangeVectorStepData
		var err error
		if hasUncached {
			step, err = m.Inner.NextStepSamples(ctx)

			// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
			if err == types.EOS { // FIXME: have this run for a cache-only step too
				if m.seriesValidationFunc != nil {
					// TODO: make sure this works with the combined metadata
					m.seriesValidationFunc(data, m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex), m.emitAnnotationFunc)
				}

				return data, nil
			} else if err != nil {
				return types.InstantVectorSeriesData{}, err
			}
		}

		var (
			f        float64
			hasFloat bool
			h        *histogram.FloatHistogram
		)
		// when building pieces, we want to do a range query where the step is the cache block length
		if m.usingIntermediate || m.buildingIntermediate {
			//TODO: range start/end or stepT?
			// Get start/end for possible cached blocks
			blockLengthMs := intermediateCacheBlockLength.Milliseconds()
			cachedStart := ((step.RangeStart / blockLengthMs) + 1) * blockLengthMs
			cachedEnd := (step.RangeEnd / blockLengthMs) * blockLengthMs

			// calculate head piece
			// todo: create splitStep function to split the current step into a selected time range
			updatedStep := splitStep(step, step.RangeStart, cachedStart)
			headPiece, err := m.Func.GenerateFunc(
				updatedStep,
				0, //FIXME
				m.scalarArgsData, m.timeRange, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
			if err == nil {
				return types.InstantVectorSeriesData{}, err
			}
			pieces[0] = headPiece
			piecesIdx := 1
			cachedIdx := 0
			// calculate missing pieces in the middle (or use cached version)
			for ; cachedStart < cachedEnd; cachedStart += blockLengthMs {
				// find the cached piece/series that corresponds to this value
				foundCached := false
				for ; cachedIdx < len(cachedResult); cachedIdx++ {
					if m.intermediateResults[cachedResult[cachedIdx].BlockIdx].StartTimestampMs < int(cachedStart) {
						continue
					}
					if m.intermediateResults[cachedResult[cachedIdx].BlockIdx].StartTimestampMs == int(cachedStart) {
						foundCached = true
						break
					}
					// getting here means too far
					break
				}

				if foundCached {
					pieces[piecesIdx] = m.intermediateResults[cachedResult[cachedIdx].BlockIdx].Results[cachedResult[cachedIdx].SeriesIdx]
				} else {
					updatedStep = splitStep(step, cachedStart, cachedStart+intermediateCacheBlockLength.Milliseconds())
					// TODO: cache this
					pieces[piecesIdx], err = m.Func.GenerateFunc(
						updatedStep,
						0, //FIXME
						m.scalarArgsData, m.timeRange, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
					if err != nil {
						return types.InstantVectorSeriesData{}, err
					}
				}
				piecesIdx++
			}

			// calculate tail piece
			updatedStep = splitStep(step, cachedEnd, step.RangeEnd)
			tailPiece, err := m.Func.GenerateFunc(
				updatedStep,
				0, //FIXME
				m.scalarArgsData, m.timeRange, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
			if err == nil {
				return types.InstantVectorSeriesData{}, err
			}
			pieces[piecesIdx] = tailPiece

			f, hasFloat, h, err = m.Func.CombineFunc(pieces, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
		} else {
			f, hasFloat, h, err = m.Func.StepFunc(step, m.scalarArgsData, m.timeRange, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
		}
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
		if hasFloat {
			if data.Floats == nil {
				// Only get FPoint slice once we are sure we have float points.
				// This potentially over-allocates as some points may be histograms, but this is expected to be rare.

				remainingStepCount := m.timeRange.StepCount - int(m.timeRange.PointIndex(step.StepT)) // Only get a slice for the number of points remaining in the query range.
				data.Floats, err = types.FPointSlicePool.Get(remainingStepCount, m.MemoryConsumptionTracker)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Floats = append(data.Floats, promql.FPoint{T: step.StepT, F: f})
		}
		if h != nil {
			if data.Histograms == nil {
				// Only get HPoint slice once we are sure we have histogram points.
				// This potentially over-allocates as some points may be floats, but this is expected to be rare.

				remainingStepCount := m.timeRange.StepCount - int(m.timeRange.PointIndex(step.StepT)) // Only get a slice for the number of points remaining in the query range.
				data.Histograms, err = types.HPointSlicePool.Get(remainingStepCount, m.MemoryConsumptionTracker)
				if err != nil {
					return types.InstantVectorSeriesData{}, err
				}
			}
			data.Histograms = append(data.Histograms, promql.HPoint{T: step.StepT, H: h})
		}
	}
}

func (m *FunctionOverRangeVector) emitAnnotation(generator types.AnnotationGenerator) {
	metricName := m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex)
	pos := m.Inner.ExpressionPosition()

	if m.Func.UseFirstArgumentPositionForAnnotations {
		pos = m.ScalarArgs[0].ExpressionPosition()
	}

	m.Annotations.Add(generator(metricName, pos))
}

const (
	intermediateCacheBlockLength = time.Minute * 5
	minDurationToCache           = intermediateCacheBlockLength * 6
)

type IntermediateResultBlock struct {
	Version          int
	StartTimestampMs int
	DurationMs       int
	Series           []types.SeriesMetadata
	Results          []IntermediateResult // Per-series, matching indexes of Series.
}

type IntermediateResult struct {
	sumOverTime sumOverTimeIntermediate
}

/*
1. Break the range down into intervals
2. Check if there's a cached value for each interval, keyed on the interval time range + inner node cache key.
   If there is a cached value for that interval, load that cached set of data. (Ideally the loading here would
   be done in a streaming way, but that could be a later improvement.)
4. If some intervals aren't present in the cache: merge adjacent uncached intervals into one range, then
   materialise operators for each range from the query plan
5. Compute final results for each series as you've outlined above, storing intermediate results in the cache where necessary
*/

func (m *FunctionOverRangeVector) Prepare(ctx context.Context, params *types.PrepareParams) error {
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

func (m *FunctionOverRangeVector) Finalize(ctx context.Context) error {
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

func (m *FunctionOverRangeVector) Close() {
	m.Inner.Close()

	for _, d := range m.scalarArgsData {
		types.FPointSlicePool.Put(&d.Samples, m.MemoryConsumptionTracker)
	}

	m.scalarArgsData = nil
}

type BlockSeriesRef struct {
	BlockIdx  int
	SeriesIdx int
}

const UncachedSeriesRef = -1

// This code is mostly copied from deduplicate_and_merge.go
// Merges the series metadata from uncached samples with the cached pieces
// TODO: can we guarantee series are sorted in lexiographical order? or a stable order? I don't think so, e.g. if there's a subquery with sort(), then different time ranges might return values in different orders
//  we might just have to filter out cases where that happens (while sort() only works on instant queries, if we might make multiple instant queries over different time ranges)
//  sort_by_label() (experimental) might have similar effects though in this case the order should be the same (assuming a stable sort), it just can't be assumed to be lexicographical wrt all series
func (m *FunctionOverRangeVector) seriesMetadataWithIntermediate(ctx context.Context) ([]types.SeriesMetadata, [][]BlockSeriesRef, error) {
	// Why use a string, rather than the labels hash as a key here? This avoids any issues with hash collisions.
	labelsToRefUncached := map[string][]BlockSeriesRef{}

	// Why 1024 bytes? It's what labels.Labels.String() uses as a buffer size, so we use that as a sensible starting point too.
	labelBytes := make([]byte, 0, 1024)

	// TODO: only load raw samples where needed (when cached is not enough) - this will result in a list of time ranges being passed in to inner (or the inner node to be materialized)
	uncachedMetadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, nil, err
	}

	for seriesIdx, series := range uncachedMetadata {
		labelBytes = series.Labels.Bytes(labelBytes)
		g, groupExists := labelsToRefUncached[string(labelBytes)]
		if !groupExists {
			labelsToRefUncached[string(labelBytes)] = []BlockSeriesRef{{UncachedSeriesRef, seriesIdx}}
		} else {
			labelsToRefUncached[string(labelBytes)] = append(g, BlockSeriesRef{UncachedSeriesRef, seriesIdx})
		}
	}

	// separate map for cache-only (as we need the uncached parts to be ordered as they are returned)
	labelsToRefCachedOnly := make(map[string][]BlockSeriesRef)

	for irIdx, result := range m.intermediateResults {
		for seriesIdx, series := range result.Series {
			labelBytes = series.Labels.Bytes(labelBytes)
			g, groupExists := labelsToRefUncached[string(labelBytes)]
			if !groupExists {
				cg, cgroupExists := labelsToRefCachedOnly[string(labelBytes)]
				if !cgroupExists {
					labelsToRefCachedOnly[string(labelBytes)] = []BlockSeriesRef{{irIdx, seriesIdx}}
				} else {
					labelsToRefCachedOnly[string(labelBytes)] = append(cg, BlockSeriesRef{irIdx, seriesIdx})
				}
			} else {
				labelsToRefUncached[string(labelBytes)] = append(g, BlockSeriesRef{irIdx, seriesIdx})
			}
		}
	}

	seriesToBlockRefs := make([][]BlockSeriesRef, 0, len(labelsToRefUncached))
	for _, group := range labelsToRefUncached {
		seriesToBlockRefs = append(seriesToBlockRefs, group)
	}
	// order by uncached metadata (as Inner.NextSeries() is called in defined order)
	for _, metadata := range uncachedMetadata {
		labelBytes = metadata.Labels.Bytes(labelBytes)
		seriesToBlockRefs = append(seriesToBlockRefs, labelsToRefUncached[string(labelBytes)])
	}
	// Add any cache-only groups to the end of the list
	// TODO: deterministic order
	for _, metadata := range labelsToRefCachedOnly {
		seriesToBlockRefs = append(seriesToBlockRefs, metadata)
	}

	outputMetadata, err := types.SeriesMetadataSlicePool.Get(len(seriesToBlockRefs), m.MemoryConsumptionTracker)
	if err != nil {
		return nil, nil, err
	}

	for _, blockRefs := range seriesToBlockRefs {
		first := blockRefs[0]
		if first.BlockIdx == UncachedSeriesRef {
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, uncachedMetadata[first.SeriesIdx])
		} else {
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, m.intermediateResults[first.BlockIdx].Series[first.SeriesIdx])
		}
		if err != nil {
			return nil, nil, err
		}
	}

	return outputMetadata, seriesToBlockRefs, nil
}
