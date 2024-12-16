// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"strings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var Rate = FunctionOverRangeVectorDefinition{
	StepFunc:                       rate(true, true),
	SeriesValidationFuncFactory:    rateSeriesValidator,
	SeriesMetadataFunction:         DropSeriesName,
	NeedsSeriesNamesForAnnotations: true,
}

var Increase = FunctionOverRangeVectorDefinition{
	StepFunc:                       rate(true, false),
	SeriesValidationFuncFactory:    rateSeriesValidator,
	SeriesMetadataFunction:         DropSeriesName,
	NeedsSeriesNamesForAnnotations: true,
}

var Delta = FunctionOverRangeVectorDefinition{
	StepFunc:                       rate(false, false),
	SeriesMetadataFunction:         DropSeriesName,
	NeedsSeriesNamesForAnnotations: true,
}

// rate is utility function for rate/increase/delta.
// It calculates the rate (allowing for counter resets if isCounter is true for rate and increase),
// extrapolates if the first/last sample is close to the boundary, and returns
// the result as either per-second (if isRate is true) or overall (this is increase function).
// If isCounter and isRate are both false, this function will calculate the delta.
func rate(isCounter, isRate bool) RangeVectorStepFunction {
	return func(step *types.RangeVectorStepData, rangeSeconds float64, emitAnnotation types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
		fHead, fTail := step.Floats.UnsafePoints()
		fCount := len(fHead) + len(fTail)

		hHead, hTail := step.Histograms.UnsafePoints()
		hCount := len(hHead) + len(hTail)

		if fCount > 0 && hCount > 0 {
			// We need either at least two histograms and no floats, or at least two floats and no histograms to calculate a rate.
			// Otherwise, emit a warning and drop this sample.
			emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
			return 0, false, nil, nil
		}

		if fCount >= 2 {
			// TODO: just pass step here? (and below)
			val := floatRate(isCounter, isRate, fCount, fHead, fTail, step.RangeStart, step.RangeEnd, rangeSeconds)
			return val, true, nil, nil
		}

		if hCount >= 2 {
			val, err := histogramRate(isCounter, isRate, hCount, hHead, hTail, step.RangeStart, step.RangeEnd, rangeSeconds, emitAnnotation)
			if err != nil {
				err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
				return 0, false, nil, err
			}
			return 0, false, val, nil
		}

		return 0, false, nil, nil
	}
}

func histogramRate(isCounter, isRate bool, hCount int, hHead []promql.HPoint, hTail []promql.HPoint, rangeStart int64, rangeEnd int64, rangeSeconds float64, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	firstPoint := hHead[0]
	hHead = hHead[1:]

	var lastPoint promql.HPoint
	if len(hTail) > 0 {
		lastPoint = hTail[len(hTail)-1]
	} else {
		lastPoint = hHead[len(hHead)-1]
	}

	if isCounter && (firstPoint.H.CounterResetHint == histogram.GaugeType || lastPoint.H.CounterResetHint == histogram.GaugeType) {
		emitAnnotation(annotations.NewNativeHistogramNotCounterWarning)
	}

	desiredSchema := firstPoint.H.Schema
	if lastPoint.H.Schema < desiredSchema {
		desiredSchema = lastPoint.H.Schema
	}

	usingCustomBuckets := firstPoint.H.UsesCustomBuckets()
	if lastPoint.H.UsesCustomBuckets() != usingCustomBuckets {
		return nil, histogram.ErrHistogramsIncompatibleSchema
	}

	delta := lastPoint.H.CopyToSchema(desiredSchema)
	_, err := delta.Sub(firstPoint.H)
	if err != nil {
		return nil, err
	}
	previousValue := firstPoint.H

	accumulate := func(points []promql.HPoint) error {
		for _, p := range points {
			if p.H.DetectReset(previousValue) {
				// Counter reset.
				if _, err := delta.Add(previousValue); err != nil {
					return err
				}
			}

			if p.H.UsesCustomBuckets() != usingCustomBuckets {
				return histogram.ErrHistogramsIncompatibleSchema
			}

			if p.H.Schema < desiredSchema {
				desiredSchema = p.H.Schema
			}

			if p.H.CounterResetHint == histogram.GaugeType {
				emitAnnotation(annotations.NewNativeHistogramNotCounterWarning)
			}

			previousValue = p.H
		}
		return nil
	}

	if isCounter {
		err = accumulate(hHead)
		if err != nil {
			return nil, err

		}
		err = accumulate(hTail)
		if err != nil {
			return nil, err
		}
	}

	if !isCounter && (firstPoint.H.CounterResetHint != histogram.GaugeType || lastPoint.H.CounterResetHint != histogram.GaugeType) {
		emitAnnotation(annotations.NewNativeHistogramNotGaugeWarning)
	}

	if delta.Schema != desiredSchema {
		delta = delta.CopyToSchema(desiredSchema)
	}

	val := calculateHistogramRate(isRate, rangeStart, rangeEnd, rangeSeconds, firstPoint, lastPoint, delta, hCount)
	return val, err
}

func floatRate(isCounter, isRate bool, fCount int, fHead []promql.FPoint, fTail []promql.FPoint, rangeStart int64, rangeEnd int64, rangeSeconds float64) float64 {
	firstPoint := fHead[0]
	fHead = fHead[1:]

	var lastPoint promql.FPoint
	if len(fTail) > 0 {
		lastPoint = fTail[len(fTail)-1]
	} else {
		lastPoint = fHead[len(fHead)-1]
	}

	delta := lastPoint.F - firstPoint.F
	if isCounter {
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
	}

	val := calculateFloatRate(isCounter, isRate, rangeStart, rangeEnd, rangeSeconds, firstPoint, lastPoint, delta, fCount)
	return val
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func calculateHistogramRate(isRate bool, rangeStart, rangeEnd int64, rangeSeconds float64, firstPoint, lastPoint promql.HPoint, delta *histogram.FloatHistogram, count int) *histogram.FloatHistogram {
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
	if isRate {
		factor /= rangeSeconds
	}
	delta.CounterResetHint = histogram.GaugeType
	delta.Compact(0)
	return delta.Mul(factor)
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func calculateFloatRate(isCounter, isRate bool, rangeStart, rangeEnd int64, rangeSeconds float64, firstPoint, lastPoint promql.FPoint, delta float64, count int) float64 {
	durationToStart := float64(firstPoint.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-lastPoint.T) / 1000

	sampledInterval := float64(lastPoint.T-firstPoint.T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(count-1)

	extrapolationThreshold := averageDurationBetweenSamples * 1.1
	extrapolateToInterval := sampledInterval

	if durationToStart >= extrapolationThreshold {
		durationToStart = averageDurationBetweenSamples / 2
	}

	if isCounter && delta > 0 && firstPoint.F >= 0 {
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

	if isRate {
		factor /= rangeSeconds
	}
	return delta * factor
}

func rateSeriesValidator() RangeVectorSeriesValidationFunction {
	// Most of the time, rate() is performed over many series with the same metric name, so we can save some time
	// by only checking a name we haven't already checked.
	lastCheckedMetricName := ""

	return func(data types.InstantVectorSeriesData, metricName string, emitAnnotation types.EmitAnnotationFunc) {
		if len(data.Floats) == 0 {
			return
		}

		if metricName == "" || metricName == lastCheckedMetricName {
			return
		}

		if !strings.HasSuffix(metricName, "_total") && !strings.HasSuffix(metricName, "_count") && !strings.HasSuffix(metricName, "_sum") && !strings.HasSuffix(metricName, "_bucket") {
			emitAnnotation(annotations.NewPossibleNonCounterInfo)
		}

		lastCheckedMetricName = metricName
	}
}
