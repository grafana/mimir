// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"
	"strings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// FunctionOverRangeVector performs a rate calculation over a range vector.
type FunctionOverRangeVector struct {
	Inner types.RangeVectorOperator
	Pool  *pooling.LimitingPool

	Annotations *annotations.Annotations

	numSteps        int
	rangeSeconds    float64
	floatBuffer     *types.FPointRingBuffer
	histogramBuffer *types.HPointRingBuffer

	expressionPosition posrange.PositionRange
}

var _ types.InstantVectorOperator = &FunctionOverRangeVector{}

func NewFunctionOverRangeVector(
	inner types.RangeVectorOperator,
	pool *pooling.LimitingPool,
	annotations *annotations.Annotations,
	expressionPosition posrange.PositionRange,
) *FunctionOverRangeVector {
	return &FunctionOverRangeVector{
		Inner:              inner,
		Pool:               pool,
		Annotations:        annotations,
		expressionPosition: expressionPosition,
	}
}

func (m *FunctionOverRangeVector) ExpressionPosition() posrange.PositionRange {
	return m.expressionPosition
}

func (m *FunctionOverRangeVector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	metadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	m.checkForPossibleNonCounterMetrics(metadata) // FIXME: only relevant for rate() and increase()

	for i := range metadata {
		metadata[i].Labels = metadata[i].Labels.DropMetricName()
	}

	m.numSteps = m.Inner.StepCount()
	m.rangeSeconds = m.Inner.Range().Seconds()

	return metadata, nil
}

func (m *FunctionOverRangeVector) checkForPossibleNonCounterMetrics(metadata []types.SeriesMetadata) {
	// Most of the time, rate() is called over series with the same metric name.
	// So rather than tracking all metric names we've checked so far, just remember the last one we've seen.
	// This allows us to avoid most of the suffix checks in the common case without the cost of keeping track of all metric
	// names we've seen.
	// Annotations.Add() does deduplication of the annotations added, so there's no risk of generating many annotations for
	// the same metric if multiple metric names are present.
	lastCheckedMetricName := ""

	for _, series := range metadata {
		metricName := series.Labels.Get(labels.MetricName)

		if metricName == "" || metricName == lastCheckedMetricName {
			continue
		}

		if !strings.HasSuffix(metricName, "_total") && !strings.HasSuffix(metricName, "_count") && !strings.HasSuffix(metricName, "_sum") && !strings.HasSuffix(metricName, "_bucket") {
			m.Annotations.Add(annotations.NewPossibleNonCounterInfo(metricName, m.Inner.ExpressionPosition()))
		}

		lastCheckedMetricName = metricName
	}
}

func (m *FunctionOverRangeVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if err := m.Inner.NextSeries(ctx); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if m.floatBuffer == nil {
		m.floatBuffer = types.NewFPointRingBuffer(m.Pool)
	}

	if m.histogramBuffer == nil {
		m.histogramBuffer = types.NewHPointRingBuffer(m.Pool)
	}

	m.floatBuffer.Reset()
	m.histogramBuffer.Reset()

	data := types.InstantVectorSeriesData{}

	for {
		step, err := m.Inner.NextStepSamples(m.floatBuffer, m.histogramBuffer)

		// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
		if err == types.EOS {
			return data, nil
		} else if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		err = m.computeNextStep(&data, step)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}
	}
}

func (m *FunctionOverRangeVector) computeNextStep(data *types.InstantVectorSeriesData, step types.RangeVectorStepData) error {
	var err error
	// Floats
	fHead, fTail := m.floatBuffer.UnsafePoints(step.RangeEnd)
	fCount := len(fHead) + len(fTail)

	// Histograms
	hHead, hTail := m.histogramBuffer.UnsafePoints(step.RangeEnd)
	hCount := len(hHead) + len(hTail)

	if fCount > 0 && hCount > 0 {
		// We need either at least two Histograms and no Floats, or at least two
		// Floats and no Histograms to calculate a rate. Otherwise, drop this
		// Vector element.
		return nil
	}

	if fCount >= 2 {
		firstPoint := m.floatBuffer.First()
		lastPoint, _ := m.floatBuffer.LastAtOrBefore(step.RangeEnd) // We already know there is a point at or before this time, no need to check.
		delta := lastPoint.F - firstPoint.F
		previousValue := firstPoint.F

		accumulate := func(points []promql.FPoint) {
			for _, p := range points {
				if p.F < previousValue {
					// Counter reset.
					delta += previousValue
				}

				previousValue = p.F
			}
		}

		accumulate(fHead)
		accumulate(fTail)

		val := m.calculateFloatRate(step.RangeStart, step.RangeEnd, firstPoint, lastPoint, delta, fCount)
		if data.Floats == nil {
			// Only get fPoint slice once we are sure we have float points.
			// This potentially over-allocates as some points in the steps may be histograms,
			// but this is expected to be rare.
			data.Floats, err = m.Pool.GetFPointSlice(m.numSteps)
			if err != nil {
				return err
			}
		}
		data.Floats = append(data.Floats, promql.FPoint{T: step.StepT, F: val})
	}

	if hCount >= 2 {
		firstPoint := m.histogramBuffer.First()
		lastPoint, _ := m.histogramBuffer.LastAtOrBefore(step.RangeEnd) // We already know there is a point at or before this time, no need to check.

		currentSchema := firstPoint.H.Schema
		if lastPoint.H.Schema < currentSchema {
			currentSchema = lastPoint.H.Schema
		}

		delta := lastPoint.H.CopyToSchema(currentSchema)
		_, err = delta.Sub(firstPoint.H)
		if err != nil {
			return err
		}
		previousValue := firstPoint.H

		accumulate := func(points []promql.HPoint) error {
			for _, p := range points {
				if p.H.DetectReset(previousValue) {
					// Counter reset.
					_, err = delta.Add(previousValue)
					if err != nil {
						return err
					}
				}
				if p.H.Schema < currentSchema {
					delta = delta.CopyToSchema(p.H.Schema)
				}

				previousValue = p.H
			}
			return nil
		}

		err = accumulate(hHead)
		if err != nil {
			return err
		}
		err = accumulate(hTail)
		if err != nil {
			return err
		}

		val := m.calculateHistogramRate(step.RangeStart, step.RangeEnd, firstPoint, lastPoint, delta, hCount)
		if data.Histograms == nil {
			// Only get hPoint slice once we are sure we have histogram points.
			// This potentially over-allocates as some points in the steps may be floats,
			// but this is expected to be rare.
			data.Histograms, err = m.Pool.GetHPointSlice(m.numSteps)
			if err != nil {
				return err
			}
		}

		data.Histograms = append(data.Histograms, promql.HPoint{T: step.StepT, H: val})
	}
	return nil
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func (m *FunctionOverRangeVector) calculateHistogramRate(rangeStart, rangeEnd int64, firstPoint, lastPoint promql.HPoint, delta *histogram.FloatHistogram, count int) *histogram.FloatHistogram {
	durationToStart := float64(firstPoint.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-lastPoint.T) / 1000

	sampledInterval := float64(lastPoint.T-firstPoint.T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(count-1)

	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart >= extrapolationThreshold {
		durationToStart = averageDurationBetweenSamples / 2
	}

	extrapolateToInterval += durationToStart

	if durationToEnd >= extrapolationThreshold {
		durationToEnd = averageDurationBetweenSamples / 2
	}

	extrapolateToInterval += durationToEnd

	factor := extrapolateToInterval / sampledInterval
	factor /= m.rangeSeconds
	delta.CounterResetHint = histogram.GaugeType
	delta.Compact(0)
	return delta.Mul(factor)
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func (m *FunctionOverRangeVector) calculateFloatRate(rangeStart, rangeEnd int64, firstPoint, lastPoint promql.FPoint, delta float64, count int) float64 {
	durationToStart := float64(firstPoint.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-lastPoint.T) / 1000

	sampledInterval := float64(lastPoint.T-firstPoint.T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(count-1)

	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart >= extrapolationThreshold {
		durationToStart = averageDurationBetweenSamples / 2
	}

	if delta > 0 && firstPoint.F >= 0 {
		// Counters cannot be negative. If we have any slope at all
		// (i.e. delta went up), we can extrapolate the zero point
		// of the counter. If the duration to the zero point is shorter
		// than the durationToStart, we take the zero point as the start
		// of the series, thereby avoiding extrapolation to negative
		// counter values.
		durationToZero := sampledInterval * (firstPoint.F / delta)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	extrapolateToInterval += durationToStart

	if durationToEnd >= extrapolationThreshold {
		durationToEnd = averageDurationBetweenSamples / 2
	}

	extrapolateToInterval += durationToEnd

	factor := extrapolateToInterval / sampledInterval
	factor /= m.rangeSeconds
	return delta * factor
}

func (m *FunctionOverRangeVector) Close() {
	m.Inner.Close()

	if m.floatBuffer != nil {
		m.floatBuffer.Close()
	}
	if m.histogramBuffer != nil {
		m.histogramBuffer.Close()
	}
}
