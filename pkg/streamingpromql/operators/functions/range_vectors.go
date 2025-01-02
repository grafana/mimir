// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var CountOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction: DropSeriesName,
	StepFunc:               countOverTime,
}

func countOverTime(step *types.RangeVectorStepData, _ float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
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

func lastOverTime(step *types.RangeVectorStepData, _ float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
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

func presentOverTime(step *types.RangeVectorStepData, _ float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
	if step.Floats.Any() || step.Histograms.Any() {
		return 1, true, nil, nil
	}

	return 0, false, nil, nil
}

var MaxOverTime = FunctionOverRangeVectorDefinition{
	SeriesMetadataFunction: DropSeriesName,
	StepFunc:               maxOverTime,
}

func maxOverTime(step *types.RangeVectorStepData, _ float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
	head, tail := step.Floats.UnsafePoints()

	if len(head) == 0 && len(tail) == 0 {
		return 0, false, nil, nil
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
	SeriesMetadataFunction: DropSeriesName,
	StepFunc:               minOverTime,
}

func minOverTime(step *types.RangeVectorStepData, _ float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
	head, tail := step.Floats.UnsafePoints()

	if len(head) == 0 && len(tail) == 0 {
		return 0, false, nil, nil
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

func sumOverTime(step *types.RangeVectorStepData, _ float64, emitAnnotation types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
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
		if _, err := sum.Add(p.H); err != nil {
			err = NativeHistogramErrorToAnnotation(err, emitAnnotation)
			return nil, err
		}
	}

	for _, p := range tail {
		if _, err := sum.Add(p.H); err != nil {
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

func avgOverTime(step *types.RangeVectorStepData, _ float64, emitAnnotation types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
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

			change, err := contributionByP.Sub(contributionByAvgSoFar)
			if err != nil {
				return err
			}

			avgSoFar, err = avgSoFar.Add(change)
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
	return func(step *types.RangeVectorStepData, _ float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
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
	SeriesMetadataFunction: DropSeriesName,
	StepFunc:               deriv,
}

func deriv(step *types.RangeVectorStepData, _ float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
	head, tail := step.Floats.UnsafePoints()

	if (len(head) + len(tail)) < 2 {
		return 0, false, nil, nil
	}

	slope, _ := linearRegression(head, tail, head[0].T)

	return slope, true, nil, nil
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
	StepFunc:                       irate(true),
	SeriesMetadataFunction:         DropSeriesName,
	NeedsSeriesNamesForAnnotations: true,
}

var Idelta = FunctionOverRangeVectorDefinition{
	StepFunc:                       irate(false),
	SeriesMetadataFunction:         DropSeriesName,
	NeedsSeriesNamesForAnnotations: true,
}

func irate(isRate bool) RangeVectorStepFunction {
	return func(step *types.RangeVectorStepData, rangeSeconds float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
		fHead, fTail := step.Floats.UnsafePoints()

		// We need at least two samples to calculate irate or idelta.
		if len(fHead)+len(fTail) < 2 {
			return 0, false, nil, nil
		}

		var lastSample promql.FPoint
		var previousSample promql.FPoint

		// If tails has more than two samples, we should use the last two samples from tail.
		// If tail has only one sample, the last sample is from the tail and the previous sample is last point in the head.
		// Otherwise, last two samples are all in the head.
		if len(fTail) >= 2 {
			lastSample = fTail[len(fTail)-1]
			previousSample = fTail[len(fTail)-2]
		} else if len(fTail) == 1 {
			lastSample = fTail[0]
			previousSample = fHead[len(fHead)-1]
		} else {
			lastSample = fHead[len(fHead)-1]
			previousSample = fHead[len(fHead)-2]
		}

		var resultValue float64
		if isRate && lastSample.F < previousSample.F {
			// Counter reset.
			resultValue = lastSample.F
		} else {
			resultValue = lastSample.F - previousSample.F
		}

		sampledInterval := lastSample.T - previousSample.T
		if sampledInterval == 0 {
			// Avoid dividing by 0.
			return 0, false, nil, nil
		}

		if isRate {
			// Convert to per-second.
			resultValue /= float64(sampledInterval) / 1000
		}
		return resultValue, true, nil, nil
	}
}
