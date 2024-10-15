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

func (g *SumAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc functions.EmitAnnotationFunc) error {
	defer types.PutInstantVectorSeriesData(data, memoryConsumptionTracker)
	if len(data.Floats) == 0 && len(data.Histograms) == 0 {
		// Nothing to do
		return nil
	}

	err := g.accumulateFloats(data, timeRange, memoryConsumptionTracker)
	if err != nil {
		return err
	}
	err = g.accumulateHistograms(data, timeRange, memoryConsumptionTracker, emitAnnotationFunc)
	if err != nil {
		return err
	}

	return nil
}

func (g *SumAggregationGroup) accumulateFloats(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) error {
	var err error

	if len(data.Floats) > 0 && g.floatSums == nil {
		// First series with float values for this group, populate it.
		g.floatSums, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floatCompensatingValues, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floatPresent, err = types.BoolSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.floatSums = g.floatSums[:timeRange.StepCount]
		g.floatCompensatingValues = g.floatCompensatingValues[:timeRange.StepCount]
		g.floatPresent = g.floatPresent[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		idx := timeRange.PointIndex(p.T)
		g.floatSums[idx], g.floatCompensatingValues[idx] = floats.KahanSumInc(p.F, g.floatSums[idx], g.floatCompensatingValues[idx])
		g.floatPresent[idx] = true
	}

	return nil
}

func (g *SumAggregationGroup) accumulateHistograms(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker, emitAnnotationFunc functions.EmitAnnotationFunc) error {
	var err error

	if len(data.Histograms) > 0 && g.histogramSums == nil {
		// First series with histogram values for this group, populate it.
		g.histogramSums, err = types.HistogramSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.histogramSums = g.histogramSums[:timeRange.StepCount]
	}

	for inputIdx, p := range data.Histograms {
		outputIdx := timeRange.PointIndex(p.T)

		if g.histogramSums[outputIdx] == invalidCombinationOfHistograms {
			// We've already seen an invalid combination of histograms at this timestamp. Ignore this point.
			continue
		}

		if g.histogramSums[outputIdx] == nil {
			// First sample for this output point, retain the histogram as-is.
			g.histogramSums[outputIdx] = p.H
			g.histogramPointCount++

			// Ensure the FloatHistogram instance is not reused when the HPoint slice data.Histograms is reused.
			data.Histograms[inputIdx].H = nil

			continue
		}

		g.histogramSums[outputIdx], err = g.histogramSums[outputIdx].Add(p.H)
		if err != nil {
			// Unable to add histograms together (likely due to invalid combination of histograms). Make sure we don't emit a sample at this timestamp.
			g.histogramSums[outputIdx] = invalidCombinationOfHistograms
			g.histogramPointCount--

			if err := functions.NativeHistogramErrorToAnnotation(err, emitAnnotationFunc); err != nil {
				// Unknown error: we couldn't convert the error to an annotation. Give up.
				return err
			}
		}
	}

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

func (g *SumAggregationGroup) ComputeOutputSeries(timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error) {
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
				t := timeRange.StartT + int64(i)*timeRange.IntervalMilliseconds
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
				t := timeRange.StartT + int64(i)*timeRange.IntervalMilliseconds
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
