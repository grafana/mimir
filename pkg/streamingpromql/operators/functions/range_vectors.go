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
	GenerateFunc:                   sumOverTimeGenerate,
	CombineFunc:                    sumOverTimeCombine,
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

	h, err := sumHistograms(hHead, hTail, emitAnnotation)
	return 0, false, h, err
}

type sumOverTimeIntermediate struct {
	sumF       float64
	sumC       float64 // FIXME figure out how to use this in summation.
	sumH       *histogram.FloatHistogram
	hasFloat   bool
	RangeStart int64
	RangeEnd   int64
}

func sumOverTimeGenerate(step *types.RangeVectorStepData, _ float64, _ []types.ScalarData, queryRange types.QueryTimeRange, emitAnnotation types.EmitAnnotationFunc, _ *limiter.MemoryConsumptionTracker) (IntermediateResult, error) {
	f, hasFloat, h, err := sumOverTime(step, 0, nil, queryRange, emitAnnotation, nil)
	if err != nil {
		return IntermediateResult{}, err
	}
	return IntermediateResult{sumOverTimeIntermediate{sumF: f, hasFloat: hasFloat, sumH: h, RangeStart: queryRange.StartT, RangeEnd: queryRange.EndT}}, nil
}

func sumOverTimeCombine(pieces []IntermediateResult, emitAnnotation types.EmitAnnotationFunc, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (float64, bool, *histogram.FloatHistogram, error) {
	haveFloats := false
	sumF, c := 0.0, 0.0
	var sumH *histogram.FloatHistogram

	for _, ir := range pieces {
		p := ir.sumOverTime
		if p.hasFloat {
			haveFloats = true
			sumF, c = floats.KahanSumInc(p.sumF, sumF, c)
		}
		if p.sumH != nil {
			if sumH == nil {
				sumH = p.sumH.Copy()
			} else {
				if _, err := sumH.Add(p.sumH); err != nil {
					err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
					return 0, false, nil, err
				}
			}
		}
	}

	if haveFloats && sumH != nil {
		emitAnnotation(annotations.NewMixedFloatsHistogramsWarning)
		return 0, false, nil, nil
	}

	return sumF, haveFloats, sumH, nil
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

func sumHistograms(head, tail []promql.HPoint, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	sum := head[0].H.Copy() // We must make a copy of the histogram, as the ring buffer may reuse the FloatHistogram instance on subsequent steps.
	head = head[1:]

	for _, p := range head {
		if _, _, err := sum.Add(p.H); err != nil {
			err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
			return nil, err
		}
	}

	for _, p := range tail {
		if _, _, err := sum.Add(p.H); err != nil {
			err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
			return nil, err
		}
	}

	return sum, nil
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

	h, err := avgHistograms(hHead, hTail)

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

func avgHistograms(head, tail []promql.HPoint) (*histogram.FloatHistogram, error) {
	avgSoFar := head[0].H.Copy() // We must make a copy of the histogram, as the ring buffer may reuse the FloatHistogram instance on subsequent steps.
	head = head[1:]
	count := 1.0

	// Reuse these instances if we need them, to avoid allocating two FloatHistograms for every remaining histogram in the range.
	var contributionByP *histogram.FloatHistogram
	var contributionByAvgSoFar *histogram.FloatHistogram

	accumulate := func(points []promql.HPoint) error {
		for _, p := range points {
			count++

			// Make a copy of p.H, as the ring buffer may reuse the FloatHistogram instance on subsequent steps.
			if contributionByP == nil {
				contributionByP = p.H.Copy()
			} else {
				p.H.CopyTo(contributionByP)
			}

			// Make a copy of avgSoFar so we can divide it below without modifying the running total.
			if contributionByAvgSoFar == nil {
				contributionByAvgSoFar = avgSoFar.Copy()
			} else {
				avgSoFar.CopyTo(contributionByAvgSoFar)
			}

			contributionByP = contributionByP.Div(count)
			contributionByAvgSoFar = contributionByAvgSoFar.Div(count)

			change, _, err := contributionByP.Sub(contributionByAvgSoFar)
			if err != nil {
				return err
			}

			avgSoFar, _, err = avgSoFar.Add(change)
			if err != nil {
				return err
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
		_, _, err := resultValue.Sub(secondLastSample.H)
		if err != nil {
			// Convert the error to an annotation, if we can.
			err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
			return nil, err
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
