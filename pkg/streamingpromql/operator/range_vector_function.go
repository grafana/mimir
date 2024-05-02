// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// RangeVectorFunction performs a rate calculation over a range vector.
type RangeVectorFunction struct {
	Inner RangeVectorOperator

	numSteps     int
	rangeSeconds float64
	buffer       *RingBuffer
}

var _ InstantVectorOperator = &RangeVectorFunction{}

func (m *RangeVectorFunction) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
	metadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	lb := labels.NewBuilder(labels.EmptyLabels())
	for i := range metadata {
		metadata[i].Labels = dropMetricName(metadata[i].Labels, lb)
	}

	m.numSteps = m.Inner.StepCount()
	m.rangeSeconds = m.Inner.Range().Seconds()

	return metadata, nil
}

func dropMetricName(l labels.Labels, lb *labels.Builder) labels.Labels {
	lb.Reset(l)
	lb.Del(labels.MetricName)
	return lb.Labels()
}

func (m *RangeVectorFunction) Next(ctx context.Context) (InstantVectorSeriesData, error) {
	if err := m.Inner.Next(ctx); err != nil {
		return InstantVectorSeriesData{}, err
	}

	if m.buffer == nil {
		m.buffer = &RingBuffer{}
	}

	m.buffer.Reset()

	data := InstantVectorSeriesData{
		Floats: GetFPointSlice(m.numSteps), // TODO: only allocate this if we have any floats (once we support native histograms)
	}

	for {
		step, err := m.Inner.NextStep(m.buffer)

		// nolint:errorlint // errors.Is introduces a performance overhead, and NextStep is guaranteed to return exactly EOS, never a wrapped error.
		if err == EOS {
			return data, nil
		} else if err != nil {
			return InstantVectorSeriesData{}, err
		}

		head, tail := m.buffer.HeadAndTail()
		count := len(head) + len(tail)

		if count < 2 {
			// Not enough points, skip.
			continue
		}

		firstPoint := m.buffer.First()
		lastPoint := m.buffer.Last()
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
func (m *RangeVectorFunction) calculateRate(rangeStart, rangeEnd int64, firstPoint, lastPoint promql.FPoint, delta float64, count int) float64 {
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

func (m *RangeVectorFunction) Close() {
	if m.Inner != nil {
		m.Inner.Close()
	}

	if m.buffer != nil {
		m.buffer.Close()
	}
}
