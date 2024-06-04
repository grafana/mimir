// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operators

import (
	"context"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// FunctionOverRangeVector performs a rate calculation over a range vector.
type FunctionOverRangeVector struct {
	Inner types.RangeVectorOperator
	Pool  *pooling.LimitingPool

	numSteps     int
	rangeSeconds float64
	buffer       *types.RingBuffer
}

var _ types.InstantVectorOperator = &FunctionOverRangeVector{}

func (m *FunctionOverRangeVector) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	metadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	for i := range metadata {
		metadata[i].Labels = metadata[i].Labels.DropMetricName()
	}

	m.numSteps = m.Inner.StepCount()
	m.rangeSeconds = m.Inner.Range().Seconds()

	return metadata, nil
}

func (m *FunctionOverRangeVector) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	if err := m.Inner.NextSeries(ctx); err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	if m.buffer == nil {
		m.buffer = types.NewRingBuffer(m.Pool)
	}

	m.buffer.Reset()

	floats, err := m.Pool.GetFPointSlice(m.numSteps) // TODO: only allocate this if we have any floats (once we support native histograms)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: floats,
	}

	for {
		step, err := m.Inner.NextStepSamples(m.buffer)

		// nolint:errorlint // errors.Is introduces a performance overhead, and NextStepSamples is guaranteed to return exactly EOS, never a wrapped error.
		if err == types.EOS {
			return data, nil
		} else if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		head, tail := m.buffer.UnsafePoints(step.RangeEnd)
		count := len(head) + len(tail)

		if count < 2 {
			// Not enough points, skip.
			continue
		}

		firstPoint := m.buffer.First()
		lastPoint, _ := m.buffer.LastAtOrBefore(step.RangeEnd) // We already know there is a point at or before this time, no need to check.
		delta := lastPoint.F - firstPoint.F
		previousValue := firstPoint.F

		accumulate := func(points []promql.FPoint) {
			for _, p := range points {
				if p.T > step.RangeEnd { // The buffer is already guaranteed to only contain points >= rangeStart.
					return
				}

				if p.F < previousValue {
					// Counter reset.
					delta += previousValue
				}

				previousValue = p.F
			}
		}

		accumulate(head)
		accumulate(tail)

		val := m.calculateRate(step.RangeStart, step.RangeEnd, firstPoint, lastPoint, delta, count)

		data.Floats = append(data.Floats, promql.FPoint{T: step.StepT, F: val})
	}
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func (m *FunctionOverRangeVector) calculateRate(rangeStart, rangeEnd int64, firstPoint, lastPoint promql.FPoint, delta float64, count int) float64 {
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

	if m.buffer != nil {
		m.buffer.Close()
	}
}
