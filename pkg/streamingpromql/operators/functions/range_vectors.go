// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"fmt"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

var CountOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction: DropSeriesName,
	StepFunc:               countOverTime,
}

func countOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	fPointCount := step.Floats.Count()
	hPointCount := step.Histograms.Count()

	if fPointCount == 0 && hPointCount == 0 {
		return 0, false, nil, nil
	}

	return float64(fPointCount + hPointCount), true, nil, nil
}

var LastOverTime = FunctionOverRangeVectorDefinition{
	// We want to use the input series as-is, so no need to set SeriesMetadataFunction.
	StepFunc: lastOverTime,
}

func lastOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	lastFloat, floatAvailable := step.Floats.Last()
	lastHistogram, histogramAvailable := step.Histograms.Last()

	if !floatAvailable && !histogramAvailable {
		return 0, false, nil, nil
	}

	if floatAvailable && (!histogramAvailable || lastFloat.T > lastHistogram.T) {
		return lastFloat.F, true, nil, nil
	}

	// We must make a copy of the histogram before returning it, as the ring buffer may reuse the FloatHistogram instance on subsequent steps.
	return 0, false, lastHistogram.H.Copy(), nil
}

var FirstOverTime = FunctionOverRangeVectorDefinition{
	// We want to use the input series as-is, so no need to set SeriesMetadataFunction.
	StepFunc: firstOverTime,
}

func firstOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	floatAvailable := step.Floats.Any()
	histogramAvailable := step.Histograms.Any()

	if !floatAvailable && !histogramAvailable {
		return 0, false, nil, nil
	}

	var firstFloat promql.FPoint
	if floatAvailable {
		firstFloat = step.Floats.First()
	}

	var firstHistogram promql.HPoint
	if histogramAvailable {
		firstHistogram = step.Histograms.First()
	}

	if floatAvailable && (!histogramAvailable || firstFloat.T < firstHistogram.T) {
		return firstFloat.F, true, nil, nil
	}

	// We must make a copy of the histogram before returning it, as the ring buffer may reuse the FloatHistogram instance on subsequent steps.
	return 0, false, firstHistogram.H.Copy(), nil
}

var PresentOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction: DropSeriesName,
	StepFunc:               presentOverTime,
}

func presentOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	if step.Floats.Any() || step.Histograms.Any() {
		return 1, true, nil, nil
	}

	return 0, false, nil, nil
}

var MaxOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       maxOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func maxOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	head, tail := step.Floats.UnsafePoints()

	if len(head) == 0 && len(tail) == 0 {
		return 0, false, nil, nil
	}

	if step.Histograms.Any() {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	maxSoFar := head[0].F
	head = head[1:]

	for _, p := range head {
		if p.F > maxSoFar || math.IsNaN(maxSoFar) {
			maxSoFar = p.F
		}
	}

	for _, p := range tail {
		if p.F > maxSoFar || math.IsNaN(maxSoFar) {
			maxSoFar = p.F
		}
	}

	return maxSoFar, true, nil, nil
}

var TsOfMinOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       tsOfMinOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func tsOfMinOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	cmpFunc := func(existing, next promql.FPoint) bool {
		return next.F <= existing.F || math.IsNaN(existing.F)
	}

	return tsOverTime(cmpFunc, step, emitAnnotation)
}

var TsOfMaxOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       tsOfMaxOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func tsOfMaxOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	cmpFunc := func(existing, next promql.FPoint) bool {
		return next.F >= existing.F || math.IsNaN(existing.F)
	}

	return tsOverTime(cmpFunc, step, emitAnnotation)
}

func tsOverTime(compareFn func(promql.FPoint, promql.FPoint) bool, step *types.RangeVectorStepData, emitAnnotation types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
	head, tail := step.Floats.UnsafePoints()

	if len(head) == 0 && len(tail) == 0 {
		return 0, false, nil, nil
	}

	if step.Histograms.Any() {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	best := head[0]
	head = head[1:]

	for _, p := range head {
		if compareFn(best, p) {
			best = p
		}
	}

	for _, p := range tail {
		if compareFn(best, p) {
			best = p
		}
	}

	return float64(best.T) / 1000, true, nil, nil
}

var TsOfLastOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       tsOfLastOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func tsOfLastOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	lastFloat, floatAvailable := step.Floats.Last()
	lastHistogram, histogramAvailable := step.Histograms.Last()

	if !floatAvailable && !histogramAvailable {
		return 0, false, nil, nil
	}

	if floatAvailable && histogramAvailable {
		if lastFloat.T > lastHistogram.T {
			return float64(lastFloat.T) / 1000, true, nil, nil
		}
		return float64(lastHistogram.T) / 1000, true, nil, nil
	}

	if floatAvailable {
		return float64(lastFloat.T) / 1000, true, nil, nil
	}

	return float64(lastHistogram.T) / 1000, true, nil, nil
}

var TsOfFirstOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       tsOfFirstOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func tsOfFirstOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {

	floats := step.Floats.Count()
	histograms := step.Histograms.Count()

	if floats == 0 && histograms == 0 {
		return 0, false, nil, nil
	}

	if floats > 0 && histograms > 0 {
		if step.Floats.First().T <= step.Histograms.First().T {
			return float64(step.Floats.First().T) / 1000, true, nil, nil
		}
		return float64(step.Histograms.First().T) / 1000, true, nil, nil
	}

	if floats > 0 {
		return float64(step.Floats.First().T) / 1000, true, nil, nil
	}

	return float64(step.Histograms.First().T) / 1000, true, nil, nil
}

var MinOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       minOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func minOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	head, tail := step.Floats.UnsafePoints()

	if len(head) == 0 && len(tail) == 0 {
		return 0, false, nil, nil
	}

	if step.Histograms.Any() {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	minSoFar := head[0].F
	head = head[1:]

	for _, p := range head {
		if p.F < minSoFar || math.IsNaN(minSoFar) {
			minSoFar = p.F
		}
	}

	for _, p := range tail {
		if p.F < minSoFar || math.IsNaN(minSoFar) {
			minSoFar = p.F
		}
	}

	return minSoFar, true, nil, nil
}

var SumOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       sumOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func sumOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	fHead, fTail := step.Floats.UnsafePoints()
	hHead, hTail := step.Histograms.UnsafePoints()

	haveFloats := len(fHead) > 0 || len(fTail) > 0
	haveHistograms := len(hHead) > 0 || len(hTail) > 0

	if !haveFloats && !haveHistograms {
		return 0, false, nil, nil
	}

	if haveFloats && haveHistograms {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
	}

	if haveFloats {
		return sumFloats(fHead, fTail), true, nil, nil
	}

	h, err := SumHistograms(hHead, hTail, emitAnnotation)
	return 0, false, h, err
}

func sumFloats(head, tail []promql.FPoint) float64 {
	sum, c := 0.0, 0.0

	for _, p := range head {
		sum, c = floats.KahanSumInc(p.F, sum, c)
	}

	for _, p := range tail {
		sum, c = floats.KahanSumInc(p.F, sum, c)
	}

	return sum + c
}

func SumHistograms(head, tail []promql.HPoint, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	sum := head[0].H.Copy() // We must make a copy of the histogram, as the ring buffer may reuse the FloatHistogram instance on subsequent steps.
	head = head[1:]

	counterResetSeen := false
	notCounterResetSeen := false
	nhcbBoundsReconciledSeen := false

	trackCounterReset := func(h *histogram.FloatHistogram) {
		switch h.CounterResetHint {
		case histogram.CounterReset:
			counterResetSeen = true
		case histogram.NotCounterReset:
			notCounterResetSeen = true
		}
	}

	trackCounterReset(sum)

	var compensation *histogram.FloatHistogram // Kahan compensation histogram

	accumulate := func(points []promql.HPoint) (bool, error) {
		for _, p := range points {
			trackCounterReset(p.H)

			var nhcbBoundsReconciled bool
			var err error
			compensation, _, nhcbBoundsReconciled, err = sum.KahanAdd(p.H, compensation)
			if err != nil {
				err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
				return false, err
			}
			if nhcbBoundsReconciled {
				nhcbBoundsReconciledSeen = true
			}
		}

		return true, nil
	}

	if ok, err := accumulate(head); err != nil || !ok {
		return nil, err
	}

	if ok, err := accumulate(tail); err != nil || !ok {
		return nil, err
	}

	if counterResetSeen && notCounterResetSeen {
		emitAnnotation(newAggregationCounterResetCollisionWarning)
	}
	if nhcbBoundsReconciledSeen {
		emitAnnotation(NewAggregationMismatchedCustomBucketsHistogramInfo)
	}

	// Apply Kahan compensation to get the final accurate result
	if compensation != nil {
		// Use regular Add (not KahanAdd) to apply the final compensation
		sum, _, _, err := sum.Add(compensation)
		if err != nil {
			return nil, err
		}
		return sum, nil
	}

	return sum, nil
}

func newAggregationCounterResetCollisionWarning(_ string, expressionPosition posrange.PositionRange) error {
	return annotations.NewHistogramCounterResetCollisionWarning(expressionPosition, annotations.HistogramAgg)
}

func NewAggregationMismatchedCustomBucketsHistogramInfo(_ string, expressionPosition posrange.PositionRange) error {
	return annotations.NewMismatchedCustomBucketsHistogramsInfo(expressionPosition, annotations.HistogramAgg)
}

var AvgOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       avgOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func avgOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	fHead, fTail := step.Floats.UnsafePoints()
	hHead, hTail := step.Histograms.UnsafePoints()

	haveFloats := len(fHead) > 0 || len(fTail) > 0
	haveHistograms := len(hHead) > 0 || len(hTail) > 0

	if !haveFloats && !haveHistograms {
		return 0, false, nil, nil
	}

	if haveFloats && haveHistograms {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
	}

	if haveFloats {
		return avgFloats(fHead, fTail), true, nil, nil
	}

	h, err := avgHistograms(hHead, hTail, emitAnnotation)

	if err != nil {
		err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
	}

	return 0, false, h, err
}

func avgFloats(head, tail []promql.FPoint) float64 {
	sum, c, count := 0.0, 0.0, 0.0
	avgSoFar := 0.0 // Only used for incremental calculation method.
	useIncrementalCalculation := false

	accumulate := func(points []promql.FPoint) {
		for _, p := range points {
			count++

			if !useIncrementalCalculation {
				newSum, newC := floats.KahanSumInc(p.F, sum, c)

				if count == 1 || !math.IsInf(newSum, 0) {
					// Continue using simple average calculation provided we haven't overflowed,
					// and also for first point to avoid dividing by zero below.
					sum, c = newSum, newC
					continue
				}

				// We've just hit overflow, switch to incremental calculation.
				useIncrementalCalculation = true
				avgSoFar = sum / (count - 1)
				c = c / (count - 1)
			}

			// If we get here, we've hit overflow at some point in the range.
			// Use incremental calculation method to produce more accurate results.
			if math.IsInf(avgSoFar, 0) {
				if math.IsInf(p.F, 0) && (avgSoFar > 0) == (p.F > 0) {
					// Running average is infinite and the next point is also the same infinite.
					// We already have the correct running value, so just continue.
					continue
				}
				if !math.IsInf(p.F, 0) && !math.IsNaN(p.F) {
					// Running average is infinite, and the next point is neither infinite nor NaN.
					// The running average will still be infinite after considering this point, so just continue
					// to avoid incorrectly introducing NaN below.
					continue
				}
			}

			avgSoFar, c = floats.KahanSumInc(p.F/count-(avgSoFar+c)/count, avgSoFar, c)
		}
	}

	accumulate(head)
	accumulate(tail)

	if useIncrementalCalculation {
		return avgSoFar + c
	}

	return (sum + c) / count
}

func avgHistograms(head, tail []promql.HPoint, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	avgSoFar := head[0].H.Copy() // We must make a copy of the histogram, as the ring buffer may reuse the FloatHistogram instance on subsequent steps.
	head = head[1:]
	count := 1.0

	counterResetSeen := false
	notCounterResetSeen := false
	nhcbBoundsReconciledSeen := false

	trackCounterReset := func(h *histogram.FloatHistogram) {
		switch h.CounterResetHint {
		case histogram.CounterReset:
			counterResetSeen = true
		case histogram.NotCounterReset:
			notCounterResetSeen = true
		}
	}

	trackCounterReset(avgSoFar)

	var kahanC *histogram.FloatHistogram // Kahan compensation histogram

	accumulate := func(points []promql.HPoint) error {
		for _, h := range points {
			trackCounterReset(h.H)
			count++
			q := (count - 1) / count
			if kahanC != nil {
				kahanC.Mul(q)
			}
			toAdd := h.H.Copy().Div(count)
			var nhcbBoundsReconciled bool
			var err error
			kahanC, _, nhcbBoundsReconciled, err = avgSoFar.Mul(q).KahanAdd(toAdd, kahanC)
			if err != nil {
				return err
			}
			if nhcbBoundsReconciled {
				nhcbBoundsReconciledSeen = true
			}
		}

		return nil
	}

	if err := accumulate(head); err != nil {
		return nil, err
	}

	if err := accumulate(tail); err != nil {
		return nil, err
	}

	if counterResetSeen && notCounterResetSeen {
		emitAnnotation(newAggregationCounterResetCollisionWarning)
	}

	if nhcbBoundsReconciledSeen {
		emitAnnotation(NewAggregationMismatchedCustomBucketsHistogramInfo)
	}

	// Apply Kahan compensation to get the final accurate result
	if kahanC != nil {
		// Use regular Add (not KahanAdd) to apply the final compensation
		avgSoFar, _, _, err := avgSoFar.Add(kahanC)
		if err != nil {
			return nil, err
		}
		return avgSoFar, nil
	}

	return avgSoFar, nil
}

var Changes = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction: DropSeriesName,
	StepFunc:               resetsChanges(false),
}

var Resets = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction: DropSeriesName,
	StepFunc:               resetsChanges(true),
}

func resetsChanges(isReset bool) RangeVectorStepFunction {
	return func(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, _ types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
		fHead, fTail := step.Floats.UnsafePoints()
		hHead, hTail := step.Histograms.UnsafePoints()

		if len(fHead) == 0 && len(hHead) == 0 {
			// No points, nothing to do
			return 0, false, nil, nil
		}

		count := 0.0
		var prevFloat float64
		var prevHist *histogram.FloatHistogram

		fIdx, hIdx := 0, 0

		loadFloat := func(idx int) (float64, int64, bool, int) {
			if idx < len(fHead) {
				point := fHead[idx]
				return point.F, point.T, true, idx + 1
			} else if idx-len(fHead) < len(fTail) {
				point := fTail[idx-len(fHead)]
				return point.F, point.T, true, idx + 1
			}
			return 0, 0, false, idx
		}

		loadHist := func(idx int) (*histogram.FloatHistogram, int64, bool, int) {
			if idx < len(hHead) {
				point := hHead[idx]
				return point.H, point.T, true, idx + 1
			} else if idx-len(hHead) < len(hTail) {
				point := hTail[idx-len(hHead)]
				return point.H, point.T, true, idx + 1
			}
			return nil, 0, false, idx
		}

		var fValue float64
		var hValue *histogram.FloatHistogram
		var fTime, hTime int64
		var fOk, hOk bool

		fValue, fTime, fOk, fIdx = loadFloat(fIdx)
		hValue, hTime, hOk, hIdx = loadHist(hIdx)

		if fOk && hOk {
			if fTime <= hTime {
				prevFloat = fValue
				prevHist = nil
			} else {
				prevHist = hValue
				prevFloat = 0
			}
		} else if fOk {
			prevFloat = fValue
			prevHist = nil
		} else if hOk {
			prevHist = hValue
			prevFloat = 0
		} else {
			return 0, false, nil, nil
		}

		for fOk || hOk {
			var currentFloat float64
			var currentHist *histogram.FloatHistogram

			if fOk && (!hOk || fTime <= hTime) {
				currentFloat = fValue
				currentHist = nil
				fValue, fTime, fOk, fIdx = loadFloat(fIdx)
			} else if hOk {
				currentHist = hValue
				currentFloat = 0
				hValue, hTime, hOk, hIdx = loadHist(hIdx)
			}

			if prevHist == nil {
				if currentHist == nil {
					if isReset {
						if currentFloat < prevFloat {
							count++
						}
					} else {
						if currentFloat != prevFloat && !(math.IsNaN(currentFloat) && math.IsNaN(prevFloat)) {
							count++
						}
					}
				} else {
					count++ // Type change from float to histogram
				}
			} else {
				if currentHist == nil {
					count++ // Type change from histogram to float
				} else {
					if isReset {
						if currentHist.DetectReset(prevHist) {
							count++
						}
					} else {
						if !currentHist.Equals(prevHist) {
							count++
						}
					}
				}
			}

			if currentHist == nil {
				prevFloat = currentFloat
				prevHist = nil
			} else {
				prevHist = currentHist
				prevFloat = 0
			}
		}

		return count, true, nil, nil
	}
}

var Deriv = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       deriv,
	NeedsSeriesNamesForAnnotations: true,
}

func deriv(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	fHead, fTail := step.Floats.UnsafePoints()

	if step.Floats.Any() && step.Histograms.Any() {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	if (len(fHead) + len(fTail)) < 2 {
		return 0, false, nil, nil
	}

	slope, _ := linearRegression(fHead, fTail, fHead[0].T)

	return slope, true, nil, nil
}

var PredictLinear = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       predictLinear,
	NeedsSeriesNamesForAnnotations: true,
}

func predictLinear(step *types.RangeVectorStepData, args []types.ScalarData, timeRange types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	fHead, fTail := step.Floats.UnsafePoints()

	if step.Floats.Any() && step.Histograms.Any() {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	if (len(fHead) + len(fTail)) < 2 {
		return 0, false, nil, nil
	}

	slope, intercept := linearRegression(fHead, fTail, step.StepT)
	tArg := args[0]
	duration := tArg.Samples[timeRange.PointIndex(step.StepT)].F

	return slope*duration + intercept, true, nil, nil
}

func linearRegression(head, tail []promql.FPoint, interceptTime int64) (slope, intercept float64) {
	var (
		n          float64
		sumX, cX   float64
		sumY, cY   float64
		sumXY, cXY float64
		sumX2, cX2 float64
		initY      float64
		constY     bool
	)

	initY = head[0].F
	constY = true
	accumulate := func(points []promql.FPoint, head bool) {
		for i, sample := range points {
			// Set constY to false if any new y values are encountered.
			if constY && (i > 0 || !head) && sample.F != initY {
				constY = false
			}
			n += 1.0
			x := float64(sample.T-interceptTime) / 1e3
			sumX, cX = floats.KahanSumInc(x, sumX, cX)
			sumY, cY = floats.KahanSumInc(sample.F, sumY, cY)
			sumXY, cXY = floats.KahanSumInc(x*sample.F, sumXY, cXY)
			sumX2, cX2 = floats.KahanSumInc(x*x, sumX2, cX2)
		}
	}

	accumulate(head, true)
	accumulate(tail, false)

	if constY {
		if math.IsInf(initY, 0) {
			return math.NaN(), math.NaN()
		}
		return 0, initY
	}
	sumX += cX
	sumY += cY
	sumXY += cXY
	sumX2 += cX2

	covXY := sumXY - sumX*sumY/n
	varX := sumX2 - sumX*sumX/n

	slope = covXY / varX
	intercept = sumY/n - slope*sumX/n
	return slope, intercept
}

var Irate = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       irateIdelta(true),
	NeedsSeriesNamesForAnnotations: true,
}

var Idelta = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       irateIdelta(false),
	NeedsSeriesNamesForAnnotations: true,
}

func irateIdelta(isRate bool) RangeVectorStepFunction {
	return func(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
		// We need at least two samples to calculate irate or idelta
		floatCount := step.Floats.Count()
		histogramCount := step.Histograms.Count()

		if floatCount+histogramCount < 2 {
			return 0, false, nil, nil
		}

		if floatCount < 2 && histogramCount < 2 {
			// Only two points are a float and a histogram.
			emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
			return 0, false, nil, nil
		}

		var useHistograms bool
		lastFloat, haveFloats := step.Floats.Last()
		lastHistogram, haveHistograms := step.Histograms.Last()

		if !haveHistograms {
			// Easy case: only have floats, and we know we have at least two of them.
			useHistograms = false
		} else if !haveFloats {
			// Easy case: only have histograms, and we know we have at least two of them.
			useHistograms = true
		} else if lastFloat.T > lastHistogram.T {
			// We have a mixture of histograms and floats, and the last float sample is later than the last histogram.
			// Check that the last two samples of any kind are both floats, and if so, use them to compute the result.
			if floatCount < 2 {
				emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
				return 0, false, nil, nil
			}

			secondLastFloat := step.Floats.PointAt(floatCount - 2)
			if secondLastFloat.T < lastHistogram.T {
				emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
				return 0, false, nil, nil
			}

			useHistograms = false
		} else {
			// Same as above, but using histograms to compute the final result.
			if histogramCount < 2 {
				emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
				return 0, false, nil, nil
			}

			secondLastHistogram := step.Histograms.PointAt(histogramCount - 2)
			if secondLastHistogram.T < lastFloat.T {
				emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
				return 0, false, nil, nil
			}

			useHistograms = true
		}

		if useHistograms {
			lastSample := step.Histograms.PointAt(histogramCount - 1)
			secondLastSample := step.Histograms.PointAt(histogramCount - 2)

			value, err := histogramIrateIdelta(isRate, lastSample, secondLastSample, emitAnnotation)
			return 0, false, value, err
		}

		// Use floats.
		lastSample := step.Floats.PointAt(floatCount - 1)
		secondLastSample := step.Floats.PointAt(floatCount - 2)

		value, ok := floatIrateIdelta(isRate, lastSample, secondLastSample)
		return value, ok, nil, nil
	}
}

func floatIrateIdelta(isRate bool, lastSample, secondLastSample promql.FPoint) (float64, bool) {
	var resultValue float64

	if isRate && lastSample.F < secondLastSample.F {
		// Counter reset.
		resultValue = lastSample.F
	} else {
		resultValue = lastSample.F - secondLastSample.F
	}

	sampledInterval := lastSample.T - secondLastSample.T
	if sampledInterval == 0 {
		// Avoid dividing by 0.
		return 0, false
	}

	if isRate {
		// Convert to per-second.
		resultValue /= float64(sampledInterval) / 1000
	}

	return resultValue, true
}

func histogramIrateIdelta(isRate bool, lastSample, secondLastSample promql.HPoint, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	resultValue := lastSample.H.Copy()

	if isRate && (lastSample.H.CounterResetHint == histogram.GaugeType || secondLastSample.H.CounterResetHint == histogram.GaugeType) {
		emitAnnotation(annotations.NewNativeHistogramNotCounterWarning)
	}

	if !isRate && (lastSample.H.CounterResetHint != histogram.GaugeType || secondLastSample.H.CounterResetHint != histogram.GaugeType) {
		emitAnnotation(annotations.NewNativeHistogramNotGaugeWarning)
	}

	if !isRate || !lastSample.H.DetectReset(secondLastSample.H) {
		_, _, nhcbBoundsReconciled, err := resultValue.Sub(secondLastSample.H)
		if err != nil {
			// Convert the error to an annotation, if we can.
			err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
			return nil, err
		}
		if nhcbBoundsReconciled {
			emitAnnotation(NewSubMismatchedCustomBucketsHistogramInfo)
		}
	}

	resultValue.CounterResetHint = histogram.GaugeType
	resultValue.Compact(0)

	sampledInterval := lastSample.T - secondLastSample.T
	if sampledInterval == 0 {
		// Avoid dividing by 0.
		return nil, nil
	}

	if isRate {
		// Convert to per-second.
		resultValue.Div(float64(sampledInterval) / 1000)
	}

	return resultValue, nil
}

var StddevOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       stddevStdvarOverTime(true),
	NeedsSeriesNamesForAnnotations: true,
}

var StdvarOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       stddevStdvarOverTime(false),
	NeedsSeriesNamesForAnnotations: true,
}

func stddevStdvarOverTime(isStdDev bool) RangeVectorStepFunction {
	return func(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
		head, tail := step.Floats.UnsafePoints()

		if len(head) == 0 && len(tail) == 0 {
			return 0, false, nil, nil
		}

		if step.Histograms.Any() {
			emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
		}

		count := 0.0
		mean := 0.0
		meanC := 0.0
		deviation := 0.0
		deviationC := 0.0

		accumulate := func(points []promql.FPoint) {
			for _, p := range points {
				count++
				delta := p.F - (mean + meanC)
				mean, meanC = floats.KahanSumInc(delta/count, mean, meanC)
				deviation, deviationC = floats.KahanSumInc(delta*(p.F-(mean+meanC)), deviation, deviationC)
			}
		}

		accumulate(head)
		accumulate(tail)

		result := (deviation + deviationC) / count

		if isStdDev {
			result = math.Sqrt(result)
		}

		return result, true, nil, nil
	}
}

var QuantileOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:                 DropSeriesName,
	StepFunc:                               quantileOverTime,
	NeedsSeriesNamesForAnnotations:         true,
	UseFirstArgumentPositionForAnnotations: true,
}

func quantileOverTime(step *types.RangeVectorStepData, args []types.ScalarData, timeRange types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	if !step.Floats.Any() {
		return 0, false, nil, nil
	}

	if step.Histograms.Any() {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	q := args[0].Samples[timeRange.PointIndex(step.StepT)].F
	if math.IsNaN(q) || q < 0 || q > 1 {
		emitAnnotation(func(_ string, expressionPosition posrange.PositionRange) error {
			return annotations.NewInvalidQuantileWarning(q, expressionPosition)
		})
	}

	head, tail := step.Floats.UnsafePoints()
	values, err := types.Float64SlicePool.Get(len(head)+len(tail), memoryConsumptionTracker)
	if err != nil {
		return 0, false, nil, err
	}

	defer types.Float64SlicePool.Put(&values, memoryConsumptionTracker)

	for _, p := range head {
		values = append(values, p.F)
	}

	for _, p := range tail {
		values = append(values, p.F)
	}

	return floats.Quantile(q, values), true, nil, nil
}

var DoubleExponentialSmoothing = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       doubleExponentialSmoothing,
	NeedsSeriesNamesForAnnotations: true,
}

// doubleExponentialSmoothing is similar to a weighted moving average, where
// historical data has exponentially less influence on the current data. It also
// accounts for trends in data. The smoothing factor (0 < sf < 1) affects how
// historical data will affect the current data. A lower smoothing factor
// increases the influence of historical data. The trend factor (0 < tf < 1)
// affects how trends in historical data will affect the current data. A higher
// trend factor increases the influence. of trends. Algorithm taken from
// https://en.wikipedia.org/wiki/Exponential_smoothing .
func doubleExponentialSmoothing(step *types.RangeVectorStepData, args []types.ScalarData, timeRange types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	if !step.Floats.Any() && !step.Histograms.Any() {
		return 0, false, nil, nil
	}

	smoothingFactor := args[0].Samples[timeRange.PointIndex(step.StepT)].F
	if smoothingFactor <= 0 || smoothingFactor >= 1 {
		return 0, false, nil, fmt.Errorf("invalid smoothing factor. Expected: 0 < sf < 1, got: %f", smoothingFactor)
	}

	trendFactor := args[1].Samples[timeRange.PointIndex(step.StepT)].F
	if trendFactor <= 0 || trendFactor >= 1 {
		return 0, false, nil, fmt.Errorf("invalid trend factor. Expected: 0 < tf < 1, got: %f", trendFactor)
	}

	fHead, fTail := step.Floats.UnsafePoints()

	if step.Floats.Any() && step.Histograms.Any() {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	return calculateDoubleExponentialSmoothing(fHead, fTail, smoothingFactor, trendFactor)
}

func calculateDoubleExponentialSmoothing(fHead []promql.FPoint, fTail []promql.FPoint, smoothingFactor float64, trendFactor float64) (float64, bool, *histogram.FloatHistogram, error) {
	if (len(fHead) + len(fTail)) < 2 {
		return 0, false, nil, nil
	}

	var smooth0, smooth1, estimatedTrend float64

	// Initial value of smooth1 is the first sample.
	smooth1 = fHead[0].F

	// Initial estimated trend is the difference between the first and second samples.
	// First sample will always be in the head.
	// If we have only one point in head, the second sample is the first index in tail.
	if len(fHead) == 1 {
		estimatedTrend = fTail[0].F - fHead[0].F
	} else {
		estimatedTrend = fHead[1].F - fHead[0].F
	}

	firstPointAfterInit := true
	accumulate := func(samples []promql.FPoint) {
		var x, y float64

		for _, sample := range samples {
			// Scale the raw value against the smoothing factor.
			x = smoothingFactor * sample.F

			// Scale the last smoothed value for all samples after second point regardless whether they are in head or tail.
			if firstPointAfterInit {
				firstPointAfterInit = false
			} else {
				estimatedTrend = trendFactor*(smooth1-smooth0) + (1-trendFactor)*estimatedTrend
			}
			y = (1 - smoothingFactor) * (smooth1 + estimatedTrend)

			smooth0, smooth1 = smooth1, x+y
		}
	}
	accumulate(fHead[1:])
	accumulate(fTail)

	return smooth1, true, nil, nil
}

var MadOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction:         DropSeriesName,
	StepFunc:                       madOverTime,
	NeedsSeriesNamesForAnnotations: true,
}

func madOverTime(step *types.RangeVectorStepData, _ []types.ScalarData, _ types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	head, tail := step.Floats.UnsafePoints()

	if len(head) == 0 && len(tail) == 0 {
		return 0, false, nil, nil
	}

	if step.Histograms.Any() {
		emitAnnotation(annotations.NewHistogramIgnoredInMixedRangeInfo)
	}

	values, err := types.Float64SlicePool.Get(len(head)+len(tail), memoryConsumptionTracker)
	if err != nil {
		return 0, false, nil, err
	}
	defer types.Float64SlicePool.Put(&values, memoryConsumptionTracker)

	// MAD = median( | xᵢ - median(x) | )

	for _, p := range head {
		values = append(values, p.F)
	}

	for _, p := range tail {
		values = append(values, p.F)
	}

	median := floats.Quantile(0.5, values)

	for i, p := range values {
		values[i] = math.Abs(p - median)
	}

	return floats.Quantile(0.5, values), true, nil, nil
}
