// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// count represents whether this aggregation is `count` (true), or `group` (false)
func NewCountGroupAggregationGroup(count bool) *CountGroupAggregationGroup {
	g := &CountGroupAggregationGroup{}
	if count {
		g.accumulatePoint = g.countAccumulatePoint
	} else {
		g.accumulatePoint = g.groupAccumulatePoint
	}
	return g
}

type CountGroupAggregationGroup struct {
	values []float64

	accumulatePoint func(idx int64)
}

func (g *CountGroupAggregationGroup) countAccumulatePoint(idx int64) {
	g.values[idx]++
}

func (g *CountGroupAggregationGroup) groupAccumulatePoint(idx int64) {
	g.values[idx] = 1
}

func (g *CountGroupAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ functions.EmitAnnotationFunc) error {
	if (len(data.Floats) > 0 || len(data.Histograms) > 0) && g.values == nil {
		var err error
		// First series with values for this group, populate it.
		g.values, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}
		g.values = g.values[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		g.accumulatePoint(timeRange.PointIdx(p.T))
	}

	for _, p := range data.Histograms {
		g.accumulatePoint(timeRange.PointIdx(p.T))
	}

	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	return nil
}

func (g *CountGroupAggregationGroup) ComputeOutputSeries(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error) {
	floatPointCount := 0
	for _, fv := range g.values {
		if fv > 0 {
			floatPointCount++
		}
	}
	var floatPoints []promql.FPoint
	var err error
	if floatPointCount > 0 {
		floatPoints, err = types.FPointSlicePool.Get(floatPointCount, memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, false, err
		}

		for i, fv := range g.values {
			if fv > 0 {
				t := timeRange.StartT + int64(i)*timeRange.IntervalMs
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: fv})
			}
		}
	}

	types.Float64SlicePool.Put(g.values, memoryConsumptionTracker)

	return types.InstantVectorSeriesData{Floats: floatPoints}, false, nil
}
