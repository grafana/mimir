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

type StddevAggregationGroup struct {
	floats     []float64
	floatMeans []float64

	// Keeps track of how many samples we have encountered thus far for the group at this point
	// This is necessary to do per point (instead of just counting the input series) as a series may have
	// stale or non-existent values that are not added towards the count.
	// We use float64 instead of uint64 so that we can reuse the float pool and don't have to retype on each division.
	groupSeriesCounts []float64
}

func (g *StddevAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, _ functions.EmitAnnotationFunc) error {
	var err error

	// Native histograms are ignored for stddev
	if len(data.Floats) > 0 && g.floats == nil {
		// First series with float values for this group, populate it.
		g.floats, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.floatMeans, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.groupSeriesCounts, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floats = g.floats[:timeRange.StepCount]
		g.floatMeans = g.floatMeans[:timeRange.StepCount]
		g.groupSeriesCounts = g.groupSeriesCounts[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		idx := (p.T - timeRange.StartT) / timeRange.IntervalMs

		g.groupSeriesCounts[idx]++
		delta := p.F - g.floatMeans[idx]
		g.floatMeans[idx] += delta / g.groupSeriesCounts[idx]
		g.floats[idx] += delta * (p.F - g.floatMeans[idx])
	}

	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	return nil
}

func (g *StddevAggregationGroup) ComputeOutputSeries(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error) {
	floatPointCount := 0
	for _, sc := range g.groupSeriesCounts {
		if sc > 0 {
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

		for i, sc := range g.groupSeriesCounts {
			if sc > 0 {
				t := timeRange.StartT + int64(i)*timeRange.IntervalMs
				f := math.Sqrt(g.floats[i] / g.groupSeriesCounts[i])
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: f})
			}
		}
	}

	types.Float64SlicePool.Put(g.floats, memoryConsumptionTracker)
	types.Float64SlicePool.Put(g.floatMeans, memoryConsumptionTracker)
	types.Float64SlicePool.Put(g.groupSeriesCounts, memoryConsumptionTracker)

	return types.InstantVectorSeriesData{Floats: floatPoints}, false, nil
}
