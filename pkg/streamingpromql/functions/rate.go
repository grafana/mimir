// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func Rate(step types.RangeVectorStepData, rangeSeconds float64, floatBuffer *types.FPointRingBuffer, histogramBuffer *types.HPointRingBuffer) (*promql.FPoint, *promql.HPoint, error) {
	var err error
	// Floats
	fHead, fTail := floatBuffer.UnsafePoints(step.RangeEnd)
	fCount := len(fHead) + len(fTail)

	// Histograms
	hHead, hTail := histogramBuffer.UnsafePoints(step.RangeEnd)
	hCount := len(hHead) + len(hTail)

	if fCount > 0 && hCount > 0 {
		// We need either at least two Histograms and no Floats, or at least two
		// Floats and no Histograms to calculate a rate. Otherwise, drop this
		// Vector element.
		return nil, nil, nil
	}

	if fCount >= 2 {
		firstPoint := floatBuffer.First()
		lastPoint, _ := floatBuffer.LastAtOrBefore(step.RangeEnd) // We already know there is a point at or before this time, no need to check.
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

		val := calculateFloatRate(step.RangeStart, step.RangeEnd, rangeSeconds, firstPoint, lastPoint, delta, fCount)
		return &promql.FPoint{T: step.StepT, F: val}, nil, nil
	}

	if hCount >= 2 {
		firstPoint := histogramBuffer.First()
		lastPoint, _ := histogramBuffer.LastAtOrBefore(step.RangeEnd) // We already know there is a point at or before this time, no need to check.

		currentSchema := firstPoint.H.Schema
		if lastPoint.H.Schema < currentSchema {
			currentSchema = lastPoint.H.Schema
		}

		delta := lastPoint.H.CopyToSchema(currentSchema)
		_, err = delta.Sub(firstPoint.H)
		if err != nil {
			return nil, nil, err
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
			return nil, nil, err
		}
		err = accumulate(hTail)
		if err != nil {
			return nil, nil, err
		}

		val := calculateHistogramRate(step.RangeStart, step.RangeEnd, rangeSeconds, firstPoint, lastPoint, delta, hCount)
		return nil, &promql.HPoint{T: step.StepT, H: val}, nil
	}
	return nil, nil, nil
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func calculateHistogramRate(rangeStart, rangeEnd int64, rangeSeconds float64, firstPoint, lastPoint promql.HPoint, delta *histogram.FloatHistogram, count int) *histogram.FloatHistogram {
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
	factor /= rangeSeconds
	delta.CounterResetHint = histogram.GaugeType
	delta.Compact(0)
	return delta.Mul(factor)
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func calculateFloatRate(rangeStart, rangeEnd int64, rangeSeconds float64, firstPoint, lastPoint promql.FPoint, delta float64, count int) float64 {
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
	factor /= rangeSeconds
	return delta * factor
}
