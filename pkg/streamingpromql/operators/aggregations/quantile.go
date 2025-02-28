// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"math"
	"unsafe"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/pool"
)

type QuantileAggregationGroup struct {
	qGroups []qGroup // A group per point in time
}

type qGroup struct {
	points []float64 // All of the floats for this group of series at a point in time
}

const maxExpectedQuantileGroups = 64 // There isn't much science to this

var qGroupPool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(maxExpectedQuantileGroups, func(size int) []qGroup {
		return make([]qGroup, 0, size)
	}),
	uint64(unsafe.Sizeof(qGroup{})),
	false,
	nil,
)

func (q *QuantileAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc types.EmitAnnotationFunc, remainingSeriesInGroup uint) error {
	defer types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)

	if len(data.Histograms) > 0 {
		emitAnnotationFunc(func(_ string, expressionPosition posrange.PositionRange) error {
			return annotations.NewHistogramIgnoredInAggregationInfo("quantile", expressionPosition)
		})
	}

	if len(data.Floats) == 0 {
		// Nothing to do
		return nil
	}

	var err error
	if q.qGroups == nil {
		q.qGroups, err = qGroupPool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		q.qGroups = q.qGroups[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		idx := timeRange.PointIndex(p.T)

		if q.qGroups[idx].points == nil {
			q.qGroups[idx].points, err = types.Float64SlicePool.Get(int(remainingSeriesInGroup), memoryConsumptionTracker)
			if err != nil {
				return err
			}
		}
		q.qGroups[idx].points = append(q.qGroups[idx].points, p.F)
	}

	return nil
}

func (q *QuantileAggregationGroup) ComputeOutputSeries(param types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitParamAnnotationFunc emitParamAnnotationFunc) (types.InstantVectorSeriesData, bool, error) {
	quantilePoints, err := types.FPointSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, false, err
	}

	for i, qGroup := range q.qGroups {
		if qGroup.points == nil {
			// No series have any points at this time step, so nothing to output
			continue
		}
		p := param.Samples[i].F
		if math.IsNaN(p) || p < 0 || p > 1 {
			emitParamAnnotationFunc(p, annotations.NewInvalidQuantileWarning)
		}
		t := timeRange.StartT + int64(i)*timeRange.IntervalMilliseconds
		f := functions.Quantile(p, qGroup.points)
		quantilePoints = append(quantilePoints, promql.FPoint{T: t, F: f})
		types.Float64SlicePool.Put(qGroup.points, memoryConsumptionTracker)
		q.qGroups[i].points = nil
	}

	qGroupPool.Put(q.qGroups, memoryConsumptionTracker)
	return types.InstantVectorSeriesData{Floats: quantilePoints}, false, nil
}
