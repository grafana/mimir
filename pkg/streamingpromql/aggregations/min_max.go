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

	accumulatePoint func(idx int64, f float64)
}

// max represents whether this aggregation is `max` (true), or `min` (false)
func NewMinMaxAggregationGroup(max bool) *MinMaxAggregationGroup {
	g := &MinMaxAggregationGroup{}
	if max {
		g.accumulatePoint = g.maxAccumulatePoint
	} else {
		g.accumulatePoint = g.minAccumulatePoint
	}
	return g
}

func (g *MinMaxAggregationGroup) maxAccumulatePoint(idx int64, f float64) {
	if !g.floatPresent[idx] || g.floatPresent[idx] && f > g.floatValues[idx] {
		g.floatValues[idx] = f
		g.floatPresent[idx] = true
	}
}

func (g *MinMaxAggregationGroup) minAccumulatePoint(idx int64, f float64) {
	if !g.floatPresent[idx] || g.floatPresent[idx] && f < g.floatValues[idx] {
		g.floatValues[idx] = f
		g.floatPresent[idx] = true
	}
}

func (g *MinMaxAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, steps int, start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ functions.EmitAnnotationFunc) error {
	if (len(data.Floats) > 0 || len(data.Histograms) > 0) && g.floatValues == nil {
		// Even if we have histograms, we have to populate the float slices, as we'll treat histograms as if they have value 0.
		// This is consistent with Prometheus but may not be the desired value: https://github.com/prometheus/prometheus/issues/14711

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

	for _, p := range data.Floats {
		if math.IsNaN(p.F) {
			continue
		}
		idx := (p.T - start) / interval
		g.accumulatePoint(idx, p.F)
	}

	// If a histogram exists max treats it as 0. We have to detect this here so that we return a 0 value instead of nothing.
	// This is consistent with Prometheus but may not be the desired value: https://github.com/prometheus/prometheus/issues/14711
	for _, p := range data.Histograms {
		idx := (p.T - start) / interval
		g.accumulatePoint(idx, 0)
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
