// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"math"

	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type MinMaxAggregationGroup struct {
	floatValues  []float64
	floatPresent []bool

	accumulatePoint func(idx int64, f float64)
	isMax           bool
}

// max represents whether this aggregation is `max` (true), or `min` (false)
func NewMinMaxAggregationGroup(max bool) *MinMaxAggregationGroup {
	g := &MinMaxAggregationGroup{isMax: max}
	if max {
		g.accumulatePoint = g.maxAccumulatePoint
	} else {
		g.accumulatePoint = g.minAccumulatePoint
	}
	return g
}

func (g *MinMaxAggregationGroup) maxAccumulatePoint(idx int64, f float64) {
	// We return a NaN only if there are no other values to return
	if !g.floatPresent[idx] || f > g.floatValues[idx] || math.IsNaN(g.floatValues[idx]) {
		g.floatValues[idx] = f
		g.floatPresent[idx] = true
	}
}

func (g *MinMaxAggregationGroup) minAccumulatePoint(idx int64, f float64) {
	// We return a NaN only if there are no other values to return
	if !g.floatPresent[idx] || f < g.floatValues[idx] || math.IsNaN(g.floatValues[idx]) {
		g.floatValues[idx] = f
		g.floatPresent[idx] = true
	}
}

func (g *MinMaxAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotation types.EmitAnnotationFunc) error {
	// Native histograms are ignored for min and max.
	if len(data.Histograms) > 0 {
		emitAnnotation(func(_ string, expressionPosition posrange.PositionRange) error {
			name := "min"

			if g.isMax {
				name = "max"
			}

			return annotations.NewHistogramIgnoredInAggregationInfo(name, expressionPosition)
		})
	}

	if len(data.Floats) > 0 && g.floatValues == nil {
		var err error
		// First series with float values for this group, populate it.
		g.floatValues, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floatPresent, err = types.BoolSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.floatValues = g.floatValues[:timeRange.StepCount]
		g.floatPresent = g.floatPresent[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		idx := timeRange.PointIndex(p.T)
		g.accumulatePoint(idx, p.F)
	}

	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	return nil
}

func (g *MinMaxAggregationGroup) ComputeOutputSeries(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error) {
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
				t := timeRange.StartT + int64(i)*timeRange.IntervalMilliseconds
				f := g.floatValues[i]
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: f})
			}
		}
	}

	types.Float64SlicePool.Put(g.floatValues, memoryConsumptionTracker)
	types.BoolSlicePool.Put(g.floatPresent, memoryConsumptionTracker)

	return types.InstantVectorSeriesData{Floats: floatPoints}, false, nil
}
