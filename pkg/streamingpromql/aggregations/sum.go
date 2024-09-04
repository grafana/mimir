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
	floatSums               []float64
	floatCompensatingValues []float64 // Compensation value for Kahan summation.
	floatPresent            []bool
	histogramSums           []*histogram.FloatHistogram
	histogramPointCount     int
}

func (g *SumAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, steps int, start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc functions.EmitAnnotationFunc) error {
	var err error
	if len(data.Floats) > 0 && g.floatSums == nil {
		// First series with float values for this group, populate it.
		g.floatSums, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floatCompensatingValues, err = types.Float64SlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floatPresent, err = types.BoolSlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.floatSums = g.floatSums[:steps]
		g.floatCompensatingValues = g.floatCompensatingValues[:steps]
		g.floatPresent = g.floatPresent[:steps]
	}

	if len(data.Histograms) > 0 && g.histogramSums == nil {
		// First series with histogram values for this group, populate it.
		g.histogramSums, err = types.HistogramSlicePool.Get(steps, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.histogramSums = g.histogramSums[:steps]
	}

	for _, p := range data.Floats {
		idx := (p.T - start) / interval
		g.floatSums[idx], g.floatCompensatingValues[idx] = floats.KahanSumInc(p.F, g.floatSums[idx], g.floatCompensatingValues[idx])
		g.floatPresent[idx] = true
	}

	var lastUncopiedHistogram *histogram.FloatHistogram

	for _, p := range data.Histograms {
		idx := (p.T - start) / interval

		if g.histogramSums[idx] == invalidCombinationOfHistograms {
			// We've already seen an invalid combination of histograms at this timestamp. Ignore this point.
			continue
		}

		if g.histogramSums[idx] == nil {
			if lastUncopiedHistogram == p.H {
				// We've already used this histogram for a previous point due to lookback.
				// Make a copy of it so we don't modify the other point.
				g.histogramSums[idx] = p.H.Copy()
				g.histogramPointCount++
				continue
			}
			// This is the first time we have seen this histogram.
			// It is safe to store it and modify it later without copying, as we'll make copies above if the same histogram is used for subsequent points.
			g.histogramSums[idx] = p.H
			g.histogramPointCount++
			lastUncopiedHistogram = p.H
			continue
		}

		g.histogramSums[idx], err = g.histogramSums[idx].Add(p.H)
		if err != nil {
			// Unable to add histograms together (likely due to invalid combination of histograms). Make sure we don't emit a sample at this timestamp.
			g.histogramSums[idx] = invalidCombinationOfHistograms
			g.histogramPointCount--

			if err := functions.NativeHistogramErrorToAnnotation(err, emitAnnotationFunc); err != nil {
				// Unknown error: we couldn't convert the error to an annotation. Give up.
				return err
			}
		}
	}

	types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	return nil
}

// reconcileAndCountFloatPoints will return the number of points with a float present.
// It also takes the opportunity whilst looping through the floats to check if there
// is a conflicting Histogram present. If both are present, an empty vector should
// be returned. So this method removes the float+histogram where they conflict.
func (g *SumAggregationGroup) reconcileAndCountFloatPoints() (int, bool) {
	// It would be possible to calculate the number of points when constructing
	// the series groups. However, it requires checking each point at each input
	// series which is more costly than looping again here and just checking each
	// point of the already grouped series.
	// See: https://github.com/grafana/mimir/pull/8442
	// We also take two different approaches here: One with extra checks if we
	// have both Floats and Histograms present, and one without these checks
	// so we don't have to do it at every point.
	floatPointCount := 0
	haveMixedFloatsAndHistograms := false
	if len(g.floatPresent) > 0 && len(g.histogramSums) > 0 {
		for idx, present := range g.floatPresent {
			if present {
				if g.histogramSums[idx] != nil {
					// If a mix of histogram samples and float samples, the corresponding vector element is removed from the output vector entirely
					// and a warning annotation is emitted.
					g.floatPresent[idx] = false
					g.histogramSums[idx] = nil
					g.histogramPointCount--

					haveMixedFloatsAndHistograms = true
				} else {
					floatPointCount++
				}
			}
		}
	} else {
		for _, p := range g.floatPresent {
			if p {
				floatPointCount++
			}
		}
	}
	return floatPointCount, haveMixedFloatsAndHistograms
}

func (g *SumAggregationGroup) ComputeOutputSeries(start int64, interval int64, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error) {
	floatPointCount, hasMixedData := g.reconcileAndCountFloatPoints()
	var floatPoints []promql.FPoint
	var err error
	if floatPointCount > 0 {
		floatPoints, err = types.FPointSlicePool.Get(floatPointCount, memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, hasMixedData, err
		}

		for i, havePoint := range g.floatPresent {
			if havePoint {
				t := start + int64(i)*interval
				f := g.floatSums[i] + g.floatCompensatingValues[i]
				floatPoints = append(floatPoints, promql.FPoint{T: t, F: f})
			}
		}
	}

	var histogramPoints []promql.HPoint
	if g.histogramPointCount > 0 {
		histogramPoints, err = types.HPointSlicePool.Get(g.histogramPointCount, memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, hasMixedData, err
		}

		for i, h := range g.histogramSums {
			if h != nil && h != invalidCombinationOfHistograms {
				t := start + int64(i)*interval
				histogramPoints = append(histogramPoints, promql.HPoint{T: t, H: h.Compact(0)})
			}
		}
	}

	types.Float64SlicePool.Put(g.floatSums, memoryConsumptionTracker)
	types.Float64SlicePool.Put(g.floatCompensatingValues, memoryConsumptionTracker)
	types.BoolSlicePool.Put(g.floatPresent, memoryConsumptionTracker)
	types.HistogramSlicePool.Put(g.histogramSums, memoryConsumptionTracker)

	return types.InstantVectorSeriesData{Floats: floatPoints, Histograms: histogramPoints}, hasMixedData, nil
}
