// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"
	"slices"
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
	// Separate out operators for intermediate result caching
	// should this be moved somewhere else (not sure if FunctionOverRangeVector is the most suitable)
	// TODO: this won't work properly for range queries, as head will need to update over time
	InnerHead types.RangeVectorOperator
	InnerTail types.RangeVectorOperator
	// TODO: is middle necessary? if we always recompute if missing a block
	InnerMiddle types.RangeVectorOperator

	Inner                    types.RangeVectorOperator
	ScalarArgs               []types.ScalarOperator
	MemoryConsumptionTracker *limiter.MemoryConsumptionTracker
	Func                     FunctionOverRangeVectorDefinition

	Annotations *annotations.Annotations

	scalarArgsData []types.ScalarData

	metricNames        *operators.MetricNames
	currentSeriesIndex int

	timeRange    types.QueryTimeRange
	rangeSeconds float64

	buildingIntermediate bool
	usingIntermediate    bool
	intermediateResults  []IntermediateResultBlock

	expressionPosition   posrange.PositionRange
	emitAnnotationFunc   types.EmitAnnotationFunc
	seriesValidationFunc RangeVectorSeriesValidationFunction

	groups [][]PiecePair
}

const HeadMetadataGroupIdx = -1
const TailMetadataGroupIdx = -2

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

	// Set up caching for instant query
	if timeRange.IntervalMilliseconds == 1 { // documentation says this can identify an instant query
		start := timeRange.StartT - inner.Range().Milliseconds()
		end := timeRange.StartT

		// Get start/end for possible cached blocks
		blockLengthMs := intermediateCacheBlockLength.Milliseconds()
		cachedStart := ((start / blockLengthMs) + 1) * blockLengthMs
		cachedEnd := (end / blockLengthMs) * blockLengthMs

		// TODO: clone plan.Node for time range instead of inner operator (we can delay materialisation following https://github.com/grafana/mimir/pull/12377/)
		o.InnerHead = inner.CloneForTimeRange(time.UnixMilli(start), time.UnixMilli(cachedStart))
		o.InnerMiddle = inner.CloneForTimeRange(time.UnixMilli(cachedStart), time.UnixMilli(cachedEnd))
		o.InnerTail = inner.CloneForTimeRange(time.UnixMilli(cachedEnd), time.UnixMilli(end))
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

	if m.usingIntermediate {
		var err error
		metadata, err = m.mergeSeriesMetadataWithIntermediate(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		// If we are building intermediate, we /probably/ just need series metadata from inner
		// In this case we always recompute everything for now
		var err error
		metadata, err = m.Inner.SeriesMetadata(ctx)
		if err != nil {
			return nil, err
		}
	}

	if m.metricNames != nil {
		m.metricNames.CaptureMetricNames(metadata)
	}

	m.rangeSeconds = m.Inner.Range().Seconds()

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
	if err := m.Inner.NextSeries(ctx); err != nil {
		return types.InstantVectorSeriesData{}, err
	}
	//TODO: need to control next series for head and tail too
	//

	defer func() {
		m.currentSeriesIndex++
	}()

	data := types.InstantVectorSeriesData{}
	var pieces []IntermediateResult

	if m.usingIntermediate || m.buildingIntermediate {
		pieces = make([]IntermediateResult, m.Inner.Range()/intermediateCacheBlockLength)
	}

	for {
		// for instant queries only atm
		head, err := m.InnerHead.NextStepSamples()
		if err != nil { // TODO: EOS is probably valid
			return types.InstantVectorSeriesData{}, err
		}

		// FIXME when using cached pieces we only want the head and tail here; when building pieces we want multiple blocks.
		step, err := m.Inner.NextStepSamples()

		// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
		if err == types.EOS {
			if m.seriesValidationFunc != nil {
				m.seriesValidationFunc(data, m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex), m.emitAnnotationFunc)
			}

			return data, nil
		} else if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		var (
			f        float64
			hasFloat bool
			h        *histogram.FloatHistogram
		)
		// when building pieces, we want to do a range query where the step is the cache block length
		if m.usingIntermediate {
			f, hasFloat, h, err = m.Func.CombineFunc(pieces, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
		} else {
			f, hasFloat, h, err = m.Func.StepFunc(step, m.rangeSeconds, m.scalarArgsData, m.timeRange, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
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

func (m *FunctionOverRangeVector) Close() {
	m.Inner.Close()

	for _, d := range m.scalarArgsData {
		types.FPointSlicePool.Put(&d.Samples, m.MemoryConsumptionTracker)
	}

	m.scalarArgsData = nil
}

// TODO: don't need struct, can just append as two numbers in slice, we know even idx = piece, odd = idx in piece
// TODO: think of less awful name
type PiecePair struct {
	Piece int
	Idx   int
}

// TODO: is it worth tracking the metadata + idx? with range queries this won't be foolproof (new head in next step will not follow these idx rules)
// This code is mostly from deduplicate_and_merge.go
func (m *FunctionOverRangeVector) mergeSeriesMetadataWithIntermediate(ctx context.Context) ([]types.SeriesMetadata, error) {
	// Why use a string, rather than the labels hash as a key here? This avoids any issues with hash collisions.
	outputGroupMap := map[string][]PiecePair{}

	// Why 1024 bytes? It's what labels.Labels.String() uses as a buffer size, so we use that as a sensible starting point too.
	labelBytes := make([]byte, 0, 1024)

	// merge metadata from start / end + intermediate results
	startMetadata, err := m.InnerHead.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}
	endMetadata, err := m.InnerTail.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	for seriesIdx, series := range startMetadata {
		labelBytes = series.Labels.Bytes(labelBytes)
		g, groupExists := outputGroupMap[string(labelBytes)]
		if !groupExists {
			outputGroupMap[string(labelBytes)] = []PiecePair{{HeadMetadataGroupIdx, seriesIdx}}
		} else {
			outputGroupMap[string(labelBytes)] = append(g, PiecePair{HeadMetadataGroupIdx, seriesIdx})
		}
	}

	for irIdx, result := range m.intermediateResults {
		for seriesIdx, series := range result.Series {
			labelBytes = series.Labels.Bytes(labelBytes)
			g, groupExists := outputGroupMap[string(labelBytes)]
			if !groupExists {
				outputGroupMap[string(labelBytes)] = []PiecePair{{irIdx, seriesIdx}}
			} else {
				outputGroupMap[string(labelBytes)] = append(g, PiecePair{irIdx, seriesIdx})
			}
		}
	}

	for seriesIdx, series := range endMetadata {
		labelBytes = series.Labels.Bytes(labelBytes)
		g, groupExists := outputGroupMap[string(labelBytes)]
		if !groupExists {
			outputGroupMap[string(labelBytes)] = []PiecePair{{TailMetadataGroupIdx, seriesIdx}}
		} else {
			outputGroupMap[string(labelBytes)] = append(g, PiecePair{TailMetadataGroupIdx, seriesIdx})
		}
	}

	outputGroups := make([][]PiecePair, 0, len(outputGroupMap))
	for _, group := range outputGroupMap {
		outputGroups = append(outputGroups, group)
	}

	// Sort the groups so that the groups that can be completed earliest are returned earliest.
	// TODO: we might want to sort by label name instead of idxs- if we have a new head returned (if range query that's shifted), we will need to match its series metadata with what's here
	// or is it even worth calculating order here?
	slices.SortFunc(outputGroups, func(a, b []PiecePair) int {
		aLastIndex := a[len(a)-1].Piece
		bLastIndex := b[len(b)-1].Piece

		if aLastIndex == bLastIndex {
			return a[len(a)-1].Idx - b[len(b)-1].Idx
		}

		// if is tail piece (-2) that can't be an earlier piece than bLastIndex
		if aLastIndex == -2 {
			return 1
		}

		return aLastIndex - bLastIndex
	})

	// TODO: sort out admin on memory consumption tracker
	// Now that we know which series we'll return, and in what order, create the list of output series.
	outputMetadata, err := types.SeriesMetadataSlicePool.Get(len(outputGroups), m.MemoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	for _, group := range outputGroups {
		first := group[0]
		if first.Piece == HeadMetadataGroupIdx {
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, startMetadata[first.Idx])
		} else if first.Piece == TailMetadataGroupIdx {
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, endMetadata[first.Idx])
		} else {
			outputMetadata, err = types.AppendSeriesMetadata(m.MemoryConsumptionTracker, outputMetadata, m.intermediateResults[first.Piece].Series[first.Idx])
		}
		if err != nil {
			return nil, err
		}
	}

	m.groups = outputGroups

	return outputMetadata, nil
}
