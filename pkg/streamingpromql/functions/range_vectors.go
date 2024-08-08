// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var CountOverTime = FunctionOverRangeVector{
	SeriesMetadataFunc: DropSeriesName,
	StepFunc:           countOverTime,
}

func countOverTime(step types.RangeVectorStepData, _ float64, fPoints *types.FPointRingBuffer, hPoints *types.HPointRingBuffer, _ EmitAnnotationFunc) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error) {
	fPointCount := fPoints.CountAtOrBefore(step.RangeEnd)
	hPointCount := hPoints.CountAtOrBefore(step.RangeEnd)

	if fPointCount == 0 && hPointCount == 0 {
		return 0, false, nil, nil
	}

	return float64(fPointCount + hPointCount), true, nil, nil
}

var LastOverTime = FunctionOverRangeVector{
	SeriesMetadataFunc: PassthroughSeriesMetadata,
	StepFunc:           lastOverTime,
}

func lastOverTime(step types.RangeVectorStepData, _ float64, fPoints *types.FPointRingBuffer, hPoints *types.HPointRingBuffer, _ EmitAnnotationFunc) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error) {
	lastFloat, floatAvailable := fPoints.LastAtOrBefore(step.RangeEnd)
	lastHistogram, histogramAvailable := hPoints.LastAtOrBefore(step.RangeEnd)

	if !floatAvailable && !histogramAvailable {
		return 0, false, nil, nil
	}

	if floatAvailable && (!histogramAvailable || lastFloat.T > lastHistogram.T) {
		return lastFloat.F, true, nil, nil
	}

	// We must make a copy of the histogram before returning it, as the ring buffer may reuse the FloatHistogram instance on subsequent steps.
	return 0, false, lastHistogram.H.Copy(), nil
}

var PresentOverTime = FunctionOverRangeVector{
	SeriesMetadataFunc: DropSeriesName,
	StepFunc:           presentOverTime,
}

func presentOverTime(step types.RangeVectorStepData, _ float64, fPoints *types.FPointRingBuffer, hPoints *types.HPointRingBuffer, _ EmitAnnotationFunc) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error) {
	if fPoints.AnyAtOrBefore(step.RangeEnd) || hPoints.AnyAtOrBefore(step.RangeEnd) {
		return 1, true, nil, nil
	}

	return 0, false, nil, nil
}

var MaxOverTime = FunctionOverRangeVector{
	SeriesMetadataFunc: DropSeriesName,
	StepFunc:           maxOverTime,
}

func maxOverTime(step types.RangeVectorStepData, _ float64, fPoints *types.FPointRingBuffer, _ *types.HPointRingBuffer, _ EmitAnnotationFunc) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error) {
	head, tail := fPoints.UnsafePoints(step.RangeEnd)

	if len(head) == 0 && len(tail) == 0 {
		return 0, false, nil, nil
	}

	var maxSoFar float64

	if len(head) > 0 {
		maxSoFar = head[0].F
		head = head[1:]
	} else {
		maxSoFar = tail[0].F
		tail = tail[1:]
	}

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

var MinOverTime = FunctionOverRangeVector{
	SeriesMetadataFunc: DropSeriesName,
	StepFunc:           minOverTime,
}

func minOverTime(step types.RangeVectorStepData, _ float64, fPoints *types.FPointRingBuffer, _ *types.HPointRingBuffer, _ EmitAnnotationFunc) (f float64, hasFloat bool, h *histogram.FloatHistogram, err error) {
	head, tail := fPoints.UnsafePoints(step.RangeEnd)

	if len(head) == 0 && len(tail) == 0 {
		return 0, false, nil, nil
	}

	var minSoFar float64

	if len(head) > 0 {
		minSoFar = head[0].F
		head = head[1:]
	} else {
		minSoFar = tail[0].F
		tail = tail[1:]
	}

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
