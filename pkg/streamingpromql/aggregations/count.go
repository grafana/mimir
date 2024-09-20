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

type CountAggregationGroup struct {
	floatValues []float64
}

func (g *CountAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ functions.EmitAnnotationFunc) error {
	if (len(data.Floats) > 0 || len(data.Histograms) > 0) && g.floatValues == nil {
		var err error
		// First series with values for this group, populate it.
		g.floatValues, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		if err != nil {
			return err
		}
		g.floatValues = g.floatValues[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		idx := (p.T - timeRange.StartT) / timeRange.IntervalMs
		g.floatValues[idx]++
	}

	for _, p := range data.Histograms {
		idx := (p.T - timeRange.StartT) / timeRange.IntervalMs
		g.floatValues[idx]++
	}

	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	return nil
}

func (g *CountAggregationGroup) ComputeOutputSeries(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error) {
	floatPointCount := 0
	for _, fv := range g.floatValues {
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

		for i, fv := range g.floatValues {
			if fv > 0 {
				t := timeRange.StartT + int64(i)*timeRange.IntervalMs
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: fv})
			}
		}
	}

	types.Float64SlicePool.Put(g.floatValues, memoryConsumptionTracker)

	return types.InstantVectorSeriesData{Floats: floatPoints}, false, nil
}
