// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"context"
	"time"

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

	intermediateResults []IntermediateResultBlock

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

	if m.Func.PiecewiseStepFunc != nil {
		if m.Inner.Range() > minDurationToCache {
			// We probably have the user somewhere.
			// Probably don't have the selector?  FIXME
			m.intermediateResults = fakeCache("user", m.Func.Name, "selector", m.timeRange.StartT, m.Inner.Range())
		}
	}

	metadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
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

func (m *FunctionOverRangeVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if err := m.Inner.NextSeries(ctx); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	defer func() {
		m.currentSeriesIndex++
	}()

	data := types.InstantVectorSeriesData{}

	for {
		step, err := m.Inner.NextStepSamples(ctx)

		// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
		if err == types.EOS {
			if m.seriesValidationFunc != nil {
				m.seriesValidationFunc(data, m.metricNames.GetMetricNameForSeries(m.currentSeriesIndex), m.emitAnnotationFunc)
			}

			return data, nil
		} else if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		f, hasFloat, h, err := m.Func.StepFunc(step, m.scalarArgsData, m.timeRange, m.emitAnnotationFunc, m.MemoryConsumptionTracker)
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
	sumOverTime []sumOverTimeIntermediate
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
