// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"
	"fmt"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/value"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// RangeVectorSelectorWithTransformation performs a rate calculation over a range vector selector.
//
// This will one day be split into two operators: the rate calculation operator and a range vector selector operator.
type RangeVectorSelectorWithTransformation struct {
	Selector *Selector

	rangeMilliseconds int64
	numSteps          int

	chunkIterator chunkenc.Iterator
	buffer        *RingBuffer
}

var _ InstantVectorOperator = &RangeVectorSelectorWithTransformation{}

func (m *RangeVectorSelectorWithTransformation) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
	// Compute values we need on every call to Next() once, here.
	m.rangeMilliseconds = m.Selector.Range.Milliseconds()
	m.numSteps = stepCount(m.Selector.Start, m.Selector.End, m.Selector.Interval)

	metadata, err := m.Selector.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	lb := labels.NewBuilder(labels.EmptyLabels())
	for i := range metadata {
		metadata[i].Labels = dropMetricName(metadata[i].Labels, lb)
	}

	return metadata, nil
}

func dropMetricName(l labels.Labels, lb *labels.Builder) labels.Labels {
	lb.Reset(l)
	lb.Del(labels.MetricName)
	return lb.Labels()
}

func (m *RangeVectorSelectorWithTransformation) Next(_ context.Context) (InstantVectorSeriesData, error) {
	if m.buffer == nil {
		m.buffer = &RingBuffer{}
	}

	var err error
	m.chunkIterator, err = m.Selector.Next(m.chunkIterator)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}

	m.buffer.Reset()

	data := InstantVectorSeriesData{
		Floats: GetFPointSlice(m.numSteps), // TODO: only allocate this if we have any floats (once we support native histograms)
	}

	// TODO: handle native histograms
	for stepT := m.Selector.Start; stepT <= m.Selector.End; stepT += m.Selector.Interval {
		rangeEnd := stepT

		if m.Selector.Timestamp != nil {
			rangeEnd = *m.Selector.Timestamp
		}

		rangeStart := rangeEnd - m.rangeMilliseconds
		m.buffer.DiscardPointsBefore(rangeStart)

		if err := m.fillBuffer(rangeStart, rangeEnd); err != nil {
			return InstantVectorSeriesData{}, err
		}

		head, tail := m.buffer.Points()
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
				if p.T > rangeEnd { // The buffer is already guaranteed to only contain points >= rangeStart.
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

		val := m.calculateRate(rangeStart, rangeEnd, firstPoint, lastPoint, delta, count)

		data.Floats = append(data.Floats, promql.FPoint{T: stepT, F: val})
	}

	return data, nil
}

func (m *RangeVectorSelectorWithTransformation) fillBuffer(rangeStart, rangeEnd int64) error {
	// Keep filling the buffer until we reach the end of the range or the end of the iterator.
	for {
		valueType := m.chunkIterator.Next()

		switch valueType {
		case chunkenc.ValNone:
			// No more data. We are done.
			return m.chunkIterator.Err()
		case chunkenc.ValFloat:
			t, f := m.chunkIterator.At()
			if value.IsStaleNaN(f) || t < rangeStart {
				continue
			}

			m.buffer.Append(promql.FPoint{T: t, F: f})

			if t >= rangeEnd {
				return nil
			}
		default:
			// TODO: handle native histograms
			return fmt.Errorf("unknown value type %s", valueType.String())
		}
	}
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func (m *RangeVectorSelectorWithTransformation) calculateRate(rangeStart, rangeEnd int64, firstPoint, lastPoint promql.FPoint, delta float64, count int) float64 {
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
	factor /= m.Selector.Range.Seconds()
	return delta * factor
}

func (m *RangeVectorSelectorWithTransformation) Close() {
	if m.Selector != nil {
		m.Selector.Close()
	}

	if m.buffer != nil {
		m.buffer.Close()
	}
}
