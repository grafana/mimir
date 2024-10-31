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
	StepFunc:               changes,
}

func changes(step types.RangeVectorStepData, _ float64, _ types.EmitAnnotationFunc) (float64, bool, *histogram.FloatHistogram, error) {
	fHead, fTail := step.Floats.UnsafePoints(step.RangeEnd)

	haveFloats := len(fHead) > 0 || len(fTail) > 0

	if !haveFloats {
		// Prometheus' engine doesn't support histogram for `changes` function yet,
		// therefore we won't add that yet too.
		return 0, false, nil, nil
	}

	if len(fHead) == 0 && len(fTail) == 0 {
		return 0, true, nil, nil
	}

	changes := 0.0

	// Comparing the point with the point before it.
	accumulate := func(points []promql.FPoint, prev *float64) {
		for _, sample := range points {
			current := sample.F
			if current != *prev && !(math.IsNaN(current) && math.IsNaN(*prev)) {
				changes++
			}
			*prev = current
		}

	}

	// The points buffer is wrapped around, therefore we need to check changes starting from the buffer's head
	// and then continue to the tail.
	if len(fHead) > 0 && len(fTail) > 0 {
		prev := fHead[0].F
		pPrev := &prev
		accumulate(fHead, pPrev)
		accumulate(fTail, pPrev)
	}

	if len(fHead) > 0 {
		prev := fHead[0].F
		pPrev := &prev
		accumulate(fHead, pPrev)
	}
	return changes, true, nil, nil
}
