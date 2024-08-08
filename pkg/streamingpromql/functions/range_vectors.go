// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
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
