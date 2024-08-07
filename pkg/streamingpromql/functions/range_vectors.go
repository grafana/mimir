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
