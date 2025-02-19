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
	heaps []heap // A heap per point in time
}

type heap struct {
	points []float64 // All of the floats for this group series at a point in time
}

const maxExpectedQuantileGroups = 64 // There isn't much science to this

var heapPool = types.NewLimitingBucketedPool(
	pool.NewBucketedPool(maxExpectedQuantileGroups, func(size int) []heap {
		return make([]heap, 0, size)
	}),
	uint64(unsafe.Sizeof(heap{})),
	true,
	nil,
)

func (q *QuantileAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc types.EmitAnnotationFunc, remainingSeriesToGroup uint) error {
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
	if q.heaps == nil {
		q.heaps, err = heapPool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		q.heaps = q.heaps[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		idx := timeRange.PointIndex(p.T)

		if q.heaps[idx].points == nil {
			q.heaps[idx].points, err = types.Float64SlicePool.Get(int(remainingSeriesToGroup), memoryConsumptionTracker)
			if err != nil {
				return err
			}
		}
		q.heaps[idx].points = append(q.heaps[idx].points, p.F)
	}

	return nil
}

func (q *QuantileAggregationGroup) ComputeOutputSeries(param types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationParamFunc types.EmitAnnotationParamFunc) (types.InstantVectorSeriesData, bool, error) {
	hasMixedData := false
	quantilePoints, err := types.FPointSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, hasMixedData, err
	}

	for i, heap := range q.heaps {
		if heap.points == nil {
			// No series have any points at this time step, so nothing to output
			continue
		}
		p := param.Samples[i].F
		if math.IsNaN(p) || p < 0 || p > 1 {
			emitAnnotationParamFunc(p, annotations.NewInvalidQuantileWarning)
		}
		t := timeRange.StartT + int64(i)*timeRange.IntervalMilliseconds
		f := functions.Quantile(p, heap.points)
		quantilePoints = append(quantilePoints, promql.FPoint{T: t, F: f})
		types.Float64SlicePool.Put(heap.points, memoryConsumptionTracker)
		heap.points = nil
	}

	heapPool.Put(q.heaps, memoryConsumptionTracker)
	return types.InstantVectorSeriesData{Floats: quantilePoints}, hasMixedData, nil
}
