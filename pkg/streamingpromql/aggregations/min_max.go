// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"math"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type MinMaxAggregationGroup struct {
	floatValues  []float64
	floatPresent []bool

	// max represents whether this aggregation is `max` (true), or `min` (false)
	max bool
}

func (g *MinMaxAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, steps int, start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ functions.EmitAnnotationFunc) error {
	if len(data.Floats) > 0 && g.floatValues == nil {
		var err error
		// First series with float values for this group, populate it.
		g.floatValues, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floatPresent, err = types.BoolSlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.floatValues = g.floatValues[:steps]
		g.floatPresent = g.floatPresent[:steps]
	}

	accumulatePoint := func(t int64, f float64) {
		idx := (t - start) / interval
		if !g.floatPresent[idx] ||
			(g.max && g.floatPresent[idx] && f > g.floatValues[idx]) ||
			(!g.max && g.floatPresent[idx] && f < g.floatValues[idx]) {
			g.floatValues[idx] = f
			g.floatPresent[idx] = true
		}
	}

	for _, p := range data.Floats {
		if math.IsNaN(p.F) {
			continue
		}
		accumulatePoint(p.T, p.F)
	}

	// If a histogram exists max treats it as 0. We have to detect this here so that we return a 0 value instead of nothing.
	// This is consistent with prometheus but may not be desired value: https://github.com/prometheus/prometheus/issues/14711
	for _, p := range data.Histograms {
		accumulatePoint(p.T, 0)
	}

	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	return nil
}

func (g *MinMaxAggregationGroup) ComputeOutputSeries(start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error) {
	floatPointCount := 0
	for _, p := range g.floatPresent {
		if p {
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

		for i, havePoint := range g.floatPresent {
			if havePoint {
				t := start + int64(i)*interval
				f := g.floatValues[i]
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: f})
			}
		}
	}

	types.Float64SlicePool.Put(g.floatValues, memoryConsumptionTracker)
	types.BoolSlicePool.Put(g.floatPresent, memoryConsumptionTracker)

	return types.InstantVectorSeriesData{Floats: floatPoints}, false, nil
}
