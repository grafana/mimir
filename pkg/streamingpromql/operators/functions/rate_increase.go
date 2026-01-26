// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"strings"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var Rate = FunctionOverRangeVectorDefinition{
	StepFunc:                       rate(true),
	SeriesValidationFuncFactory:    rateSeriesValidator,
	SeriesMetadataFunction:         DropSeriesName,
	NeedsSeriesNamesForAnnotations: true,
}

var Increase = FunctionOverRangeVectorDefinition{
	StepFunc:                       rate(false),
	SeriesValidationFuncFactory:    rateSeriesValidator,
	SeriesMetadataFunction:         DropSeriesName,
	NeedsSeriesNamesForAnnotations: true,
}

var Delta = FunctionOverRangeVectorDefinition{
	StepFunc:                       delta,
	SeriesMetadataFunction:         DropSeriesName,
	NeedsSeriesNamesForAnnotations: true,
}

// isRate is true for `rate` function, or false for `instant` function
func rate(isRate bool) RangeVectorStepFunction {
	return func(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
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

		rangeSeconds := float64(step.RangeEnd-step.RangeStart) / 1000

		if fCount >= 2 {
			// TODO: just pass step here? (and below)
			val := floatRate(isRate, fCount, fHead, fTail, step.RangeStart, step.RangeEnd, rangeSeconds, step.Smoothed || step.Anchored)
			return val, true, nil, nil
		}

		if hCount >= 2 {
			val, err := histogramRate(isRate, hCount, hHead, hTail, step.RangeStart, step.RangeEnd, rangeSeconds, emitAnnotation)
			if err != nil {
				err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
				return 0, false, nil, err
			}
			return 0, false, val, nil
		}

		return 0, false, nil, nil
	}
}

func histogramRate(isRate bool, hCount int, hHead []promql.HPoint, hTail []promql.HPoint, rangeStart int64, rangeEnd int64, rangeSeconds float64, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	firstPoint := hHead[0]
	hHead = hHead[1:]

	if firstPoint.H.CounterResetHint == histogram.GaugeType {
		emitAnnotation(annotations.NewNativeHistogramNotCounterWarning)
	}

	var secondPoint promql.HPoint
	if len(hHead) > 0 {
		secondPoint = hHead[0]
	} else {
		secondPoint = hTail[0]
	}

	// Store the original first point count before potential reset.
	// It's needed to calculate the rate correctly later.
	fpHistCount := firstPoint.H.Count

	// Ignore the first point if there is a counter reset between the first and second point.
	// This means we'll ignore any incompatibility between the layout of the first and second point,
	// which doesn't matter because we're not going to use the first point any way.
	// See https://github.com/prometheus/prometheus/pull/15902 for more explanation.
	if secondPoint.H.DetectReset(firstPoint.H) {
		firstPoint.H = &histogram.FloatHistogram{
			Schema:       secondPoint.H.Schema,
			CustomValues: secondPoint.H.CustomValues,
		}
	}

	var lastPoint promql.HPoint
	if len(hTail) > 0 {
		lastPoint = hTail[len(hTail)-1]
	} else {
		lastPoint = hHead[len(hHead)-1]
	}

	if lastPoint.H.CounterResetHint == histogram.GaugeType {
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
	_, _, nhcbBoundsReconciled, err := delta.Sub(firstPoint.H)
	if err != nil {
		return nil, err
	}
	if nhcbBoundsReconciled {
		emitAnnotation(newSubMismatchedCustomBucketsHistogramInfo)
	}
	previousValue := firstPoint.H

	accumulate := func(points []promql.HPoint) error {
		for _, p := range points {
			if p.H.DetectReset(previousValue) {
				// Counter reset.
				_, _, nhcbBoundsReconciled, err := delta.Add(previousValue)
				if err != nil {
					return err
				}
				if nhcbBoundsReconciled {
					emitAnnotation(newAddMismatchedCustomBucketsHistogramInfo)
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

	err = accumulate(hHead)
	if err != nil {
		return nil, err

	}
	err = accumulate(hTail)
	if err != nil {
		return nil, err

	}

	if delta.Schema != desiredSchema {
		delta = delta.CopyToSchema(desiredSchema)
	}

	val := calculateHistogramRate(true, isRate, rangeStart, rangeEnd, rangeSeconds, firstPoint, lastPoint, delta, hCount, fpHistCount)
	return val, err
}

func floatRate(isRate bool, fCount int, fHead []promql.FPoint, fTail []promql.FPoint, rangeStart int64, rangeEnd int64, rangeSeconds float64, smoothedOrAnchored bool) float64 {
	firstPoint := fHead[0]
	fHead = fHead[1:]

	var lastPoint promql.FPoint
	if len(fTail) > 0 {
		lastPoint = fTail[len(fTail)-1]
	} else {
		lastPoint = fHead[len(fHead)-1]
	}

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

	val := calculateFloatRate(true, isRate, rangeStart, rangeEnd, rangeSeconds, firstPoint, lastPoint, delta, fCount, smoothedOrAnchored)
	return val
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func calculateHistogramRate(isCounter, isRate bool, rangeStart, rangeEnd int64, rangeSeconds float64, firstPoint, lastPoint promql.HPoint, delta *histogram.FloatHistogram, count int, fpHistCountBeforeReset float64) *histogram.FloatHistogram {
	durationToStart := float64(firstPoint.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-lastPoint.T) / 1000

	sampledInterval := float64(lastPoint.T-firstPoint.T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(count-1)

	extrapolationThreshold := averageDurationBetweenSamples * 1.1

	if durationToStart >= extrapolationThreshold {
		durationToStart = averageDurationBetweenSamples / 2
	}

	if isCounter && delta.Count > 0 && fpHistCountBeforeReset >= 0 {
		// Counters cannot be negative. If we have any slope at all
		// (i.e. delta went up), we can extrapolate the zero point
		// of the counter. If the duration to the zero point is shorter
		// than the durationToStart, we take the zero point as the start
		// of the series, thereby avoiding extrapolation to negative
		// counter values.
		// Use the original count before any reset handling.
		durationToZero := sampledInterval * (fpHistCountBeforeReset / delta.Count)
		if durationToZero < durationToStart {
			durationToStart = durationToZero
		}
	}

	if durationToEnd >= extrapolationThreshold {
		durationToEnd = averageDurationBetweenSamples / 2
	}

	factor := (sampledInterval + durationToStart + durationToEnd) / sampledInterval
	if isRate {
		factor /= rangeSeconds
	}
	delta.CounterResetHint = histogram.GaugeType
	delta.Compact(0)
	return delta.Mul(factor)
}

// This is based on extrapolatedRate from promql/functions.go.
// https://github.com/prometheus/prometheus/pull/13725 has a good explanation of the intended behaviour here.
func calculateFloatRate(isCounter, isRate bool, rangeStart, rangeEnd int64, rangeSeconds float64, firstPoint, lastPoint promql.FPoint, delta float64, count int, smoothedOrAnchored bool) float64 {

	if smoothedOrAnchored {
		// This is a special case where the points have already been aligned and interpolated (smoothed) to the range boundaries.
		// Combined with the delta calculations in floatRate(), this is functionally equivalent to extendedRate() in promql/functions.go
		if isRate {
			return delta / rangeSeconds
		}
		return delta
	}

	durationToStart := float64(firstPoint.T-rangeStart) / 1000
	durationToEnd := float64(rangeEnd-lastPoint.T) / 1000

	sampledInterval := float64(lastPoint.T-firstPoint.T) / 1000
	averageDurationBetweenSamples := sampledInterval / float64(count-1)

	extrapolationThreshold := averageDurationBetweenSamples * 1.1

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

	if durationToEnd >= extrapolationThreshold {
		durationToEnd = averageDurationBetweenSamples / 2
	}

	factor := (sampledInterval + durationToStart + durationToEnd) / sampledInterval
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

func delta(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	fHead, fTail := step.Floats.UnsafePoints()
	fCount := len(fHead) + len(fTail)

	hHead, hTail := step.Histograms.UnsafePoints()
	hCount := len(hHead) + len(hTail)

	if fCount > 0 && hCount > 0 {
		// We need either at least two histograms and no floats, or at least two floats and no histograms to calculate a delta.
		// Otherwise, emit a warning and drop this sample.
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
	}

	rangeSeconds := float64(step.RangeEnd-step.RangeStart) / 1000

	if fCount >= 2 {
		val := floatDelta(fCount, fHead, fTail, step.RangeStart, step.RangeEnd, rangeSeconds, step.Anchored || step.Smoothed)
		return val, true, nil, nil
	}

	if hCount >= 2 {
		val, err := histogramDelta(hCount, hHead, hTail, step.RangeStart, step.RangeEnd, rangeSeconds, emitAnnotation)
		if err != nil {
			err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
			return 0, false, nil, err
		}
		return 0, false, val, nil
	}

	return 0, false, nil, nil
}

func floatDelta(fCount int, fHead []promql.FPoint, fTail []promql.FPoint, rangeStart int64, rangeEnd int64, rangeSeconds float64, anchoredOrSmoothed bool) float64 {
	firstPoint := fHead[0]

	var lastPoint promql.FPoint
	if len(fTail) > 0 {
		lastPoint = fTail[len(fTail)-1]
	} else {
		lastPoint = fHead[len(fHead)-1]
	}

	delta := lastPoint.F - firstPoint.F
	return calculateFloatRate(false, false, rangeStart, rangeEnd, rangeSeconds, firstPoint, lastPoint, delta, fCount, anchoredOrSmoothed)
}

func histogramDelta(hCount int, hHead []promql.HPoint, hTail []promql.HPoint, rangeStart int64, rangeEnd int64, rangeSeconds float64, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	firstPoint := hHead[0]

	var lastPoint promql.HPoint
	if len(hTail) > 0 {
		lastPoint = hTail[len(hTail)-1]
	} else {
		lastPoint = hHead[len(hHead)-1]
	}

	if firstPoint.H.UsesCustomBuckets() != lastPoint.H.UsesCustomBuckets() {
		return nil, histogram.ErrHistogramsIncompatibleSchema
	}

	delta, _, nhcbBoundsReconciled, err := lastPoint.H.Copy().Sub(firstPoint.H)
	if err != nil {
		return nil, err
	}
	if firstPoint.H.CounterResetHint != histogram.GaugeType || lastPoint.H.CounterResetHint != histogram.GaugeType {
		emitAnnotation(annotations.NewNativeHistogramNotGaugeWarning)
	}
	if nhcbBoundsReconciled {
		emitAnnotation(newSubMismatchedCustomBucketsHistogramInfo)
	}

	val := calculateHistogramRate(false, false, rangeStart, rangeEnd, rangeSeconds, firstPoint, lastPoint, delta, hCount, firstPoint.H.Count)
	return val, nil
}

func newAddMismatchedCustomBucketsHistogramInfo(_ string, expressionPosition posrange.PositionRange) error {
	return annotations.NewMismatchedCustomBucketsHistogramsInfo(expressionPosition, annotations.HistogramAdd)
}

func newSubMismatchedCustomBucketsHistogramInfo(_ string, expressionPosition posrange.PositionRange) error {
	return annotations.NewMismatchedCustomBucketsHistogramsInfo(expressionPosition, annotations.HistogramSub)
}
