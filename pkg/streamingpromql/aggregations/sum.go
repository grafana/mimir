// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type SumAggregationGroup struct {
	// Sum, presence, and histograms for each step.
	floatSums              []float64
	floatCompensatingMeans []float64 // Mean, or "compensating value" for Kahan summation.
	floatPresent           []bool
	histogramSums          []*histogram.FloatHistogram
	histogramPointCount    int
}

func (g *SumAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, steps int, start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc functions.EmitAnnotationFunc) (bool, error) {
	var err error
	if len(data.Floats) > 0 && g.floatSums == nil {
		// First series with float values for this group, populate it.
		g.floatSums, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}

		g.floatCompensatingMeans, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}

		g.floatPresent, err = types.BoolSlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}
		g.floatSums = g.floatSums[:steps]
		g.floatCompensatingMeans = g.floatCompensatingMeans[:steps]
		g.floatPresent = g.floatPresent[:steps]
	}

	if len(data.Histograms) > 0 && g.histogramSums == nil {
		// First series with histogram values for this group, populate it.
		g.histogramSums, err = types.HistogramSlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return false, err
		}
		g.histogramSums = g.histogramSums[:steps]
	}

	haveMixedFloatsAndHistograms := false
	removeConflictingPoint := func(idx int64) {
		haveMixedFloatsAndHistograms = true
		g.floatPresent[idx] = false
		g.histogramSums[idx] = nil
		g.histogramPointCount--
	}

	for _, p := range data.Floats {
		idx := (p.T - start) / interval
		// Check that a NH doesn't already exist at this point. If both exist, the vector is removed.
		if g.histogramSums != nil && g.histogramSums[idx] != nil {
			removeConflictingPoint(idx)
			continue
		}
		g.floatSums[idx], g.floatCompensatingMeans[idx] = floats.KahanSumInc(p.F, g.floatSums[idx], g.floatCompensatingMeans[idx])
		g.floatPresent[idx] = true
	}

	var lastUncopiedHistogram *histogram.FloatHistogram

	for _, p := range data.Histograms {
		idx := (p.T - start) / interval

		// Check that a float doesn't already exist at this point. If both exist, the vector is removed.
		if g.floatPresent != nil && g.floatPresent[idx] {
			removeConflictingPoint(idx)
			continue
		}

		if g.histogramSums[idx] == invalidCombinationOfHistograms {
			// We've already seen an invalid combination of histograms at this timestamp. Ignore this point.
			continue
		}

		if g.histogramSums[idx] != nil {
			g.histogramSums[idx], err = g.histogramSums[idx].Add(p.H)
			if err != nil {
				// Unable to add histograms together (likely due to invalid combination of histograms). Make sure we don't emit a sample at this timestamp.
				g.histogramSums[idx] = invalidCombinationOfHistograms
				g.histogramPointCount--

				if err := functions.NativeHistogramErrorToAnnotation(err, emitAnnotationFunc); err != nil {
					// Unknown error: we couldn't convert the error to an annotation. Give up.
					return false, err
				}
			}
		} else if lastUncopiedHistogram == p.H {
			// We've already used this histogram for a previous point due to lookback.
			// Make a copy of it so we don't modify the other point.
			g.histogramSums[idx] = p.H.Copy()
			g.histogramPointCount++
		} else {
			// This is the first time we have seen this histogram.
			// It is safe to store it and modify it later without copying, as we'll make copies above if the same histogram is used for subsequent points.
			g.histogramSums[idx] = p.H
			g.histogramPointCount++
			lastUncopiedHistogram = p.H
		}
	}

	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	return haveMixedFloatsAndHistograms, nil
}

func (g *SumAggregationGroup) ComputeOutputSeries(start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	var floatPoints []promql.FPoint
	var err error

	floatPointCount := 0
	for _, p := range g.floatPresent {
		if p {
			floatPointCount++
		}
	}

	if floatPointCount > 0 {
		floatPoints, err = types.FPointSlicePool.Get(floatPointCount, memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		for i, havePoint := range g.floatPresent {
			if havePoint {
				t := start + int64(i)*interval
				f := g.floatSums[i] + g.floatCompensatingMeans[i]
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: f})
			}
		}
	}

	var histogramPoints []promql.HPoint
	if g.histogramPointCount > 0 {
		histogramPoints, err = types.HPointSlicePool.Get(g.histogramPointCount, memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		for i, h := range g.histogramSums {
			if h != nil && h != invalidCombinationOfHistograms {
				t := start + int64(i)*interval
				histogramPoints = append(histogramPoints, promql.HPoint{T: t, H: g.histogramSums[i].Compact(0)})
			}
		}
	}

	types.Float64SlicePool.Put(g.floatSums, memoryConsumptionTracker)
	types.Float64SlicePool.Put(g.floatCompensatingMeans, memoryConsumptionTracker)
	types.BoolSlicePool.Put(g.floatPresent, memoryConsumptionTracker)
	types.HistogramSlicePool.Put(g.histogramSums, memoryConsumptionTracker)

	return types.InstantVectorSeriesData{Floats: floatPoints, Histograms: histogramPoints}, nil
}
