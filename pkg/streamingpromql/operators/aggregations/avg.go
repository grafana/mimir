// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type AvgAggregationGroup struct {
	floats                      []float64
	floatMeans                  []float64
	floatCompensatingMeans      []float64 // Mean, or "compensating value" for Kahan summation.
	incrementalMeans            []bool    // True after reverting to incremental calculation of the mean value.
	floatPresent                []bool
	histograms                  []*histogram.FloatHistogram
	histogramCompensatingValues []*histogram.FloatHistogram // Compensation histograms for Kahan summation.
	histogramPointCount         int

	// Keeps track of how many samples we have encountered thus far for the group at this point
	// This is necessary to do per point (instead of just counting the input series) as a series may have
	// stale or non-existent values that are not added towards the count.
	// We use float64 instead of uint64 so that we can reuse the float pool and don't have to retype on each division.
	groupSeriesCounts            []float64
	histogramCounterResetTracker *histogramCounterResetTracker

	temporaryHistogram *histogram.FloatHistogram // Used to avoid unnecessary allocations when mutating the provided data is not allowed
}

func (g *AvgAggregationGroup) AccumulateSeries(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, emitAnnotation types.EmitAnnotationFunc, _ uint, mutatingDataAllowed bool) error {
	if len(data.Floats) == 0 && len(data.Histograms) == 0 {
		// Nothing to do
		return nil
	}

	var err error

	if g.groupSeriesCounts == nil {
		g.groupSeriesCounts, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.groupSeriesCounts = g.groupSeriesCounts[:timeRange.StepCount]
	}

	err = g.accumulateFloats(data, timeRange, memoryConsumptionTracker)
	if err != nil {
		return err
	}
	err = g.accumulateHistograms(data, timeRange, memoryConsumptionTracker, emitAnnotation, mutatingDataAllowed)
	if err != nil {
		return err
	}

	return nil
}

func (g *AvgAggregationGroup) accumulateFloats(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) error {
	var err error
	if len(data.Floats) > 0 && g.floats == nil {
		// First series with float values for this group, populate it.
		g.floats, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floatCompensatingMeans, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floatPresent, err = types.BoolSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}

		g.floats = g.floats[:timeRange.StepCount]
		g.floatCompensatingMeans = g.floatCompensatingMeans[:timeRange.StepCount]
		g.floatPresent = g.floatPresent[:timeRange.StepCount]
	}

	for _, p := range data.Floats {
		idx := timeRange.PointIndex(p.T)
		g.groupSeriesCounts[idx]++
		if !g.floatPresent[idx] {
			// The first point is just taken as the value
			g.floats[idx] = p.F
			g.floatPresent[idx] = true
			continue
		}

		if g.incrementalMeans == nil || !g.incrementalMeans[idx] {
			newV, newC := floats.KahanSumInc(p.F, g.floats[idx], g.floatCompensatingMeans[idx])
			if !math.IsInf(newV, 0) {
				// The sum doesn't overflow, so we propagate it to the
				// group struct and continue with the regular
				// calculation of the mean value.
				g.floats[idx], g.floatCompensatingMeans[idx] = newV, newC
				continue
			}
			// If we are here, we know that the sum _would_ overflow. So
			// instead of continuing to sum up, we revert to incremental
			// calculation of the mean value from here on.
			if g.floatMeans == nil {
				g.floatMeans, err = types.Float64SlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
				if err != nil {
					return err
				}
				g.floatMeans = g.floatMeans[:timeRange.StepCount]
			}
			if g.incrementalMeans == nil {
				// First time we are using an incremental mean. Track which samples will be incremental.
				g.incrementalMeans, err = types.BoolSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
				if err != nil {
					return err
				}
				g.incrementalMeans = g.incrementalMeans[:timeRange.StepCount]
			}
			g.incrementalMeans[idx] = true
			g.floatMeans[idx] = g.floats[idx] / (g.groupSeriesCounts[idx] - 1)
			g.floatCompensatingMeans[idx] /= g.groupSeriesCounts[idx] - 1
		}
		if math.IsInf(g.floatMeans[idx], 0) {
			if math.IsInf(p.F, 0) && (g.floatMeans[idx] > 0) == (p.F > 0) {
				// The `floatMean` and `s.F` values are `Inf` of the same sign.  They
				// can't be subtracted, but the value of `floatMean` is correct
				// already.
				continue
			}
			if !math.IsInf(p.F, 0) && !math.IsNaN(p.F) {
				// At this stage, the mean is an infinite. If the added
				// value is neither an Inf or a Nan, we can keep that mean
				// value.
				// This is required because our calculation below removes
				// the mean value, which would look like Inf += x - Inf and
				// end up as a NaN.
				continue
			}
		}
		currentMean := g.floatMeans[idx] + g.floatCompensatingMeans[idx]
		g.floatMeans[idx], g.floatCompensatingMeans[idx] = floats.KahanSumInc(
			p.F/g.groupSeriesCounts[idx]-currentMean/g.groupSeriesCounts[idx],
			g.floatMeans[idx],
			g.floatCompensatingMeans[idx],
		)
	}
	return nil
}

func (g *AvgAggregationGroup) accumulateHistograms(data types.InstantVectorSeriesData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker, emitAnnotation types.EmitAnnotationFunc, mutatingDataAllowed bool) error {
	var err error
	if len(data.Histograms) > 0 && g.histograms == nil {
		// First series with histogram values for this group, populate it.
		g.histograms, err = types.HistogramSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.histograms = g.histograms[:timeRange.StepCount]
		g.histogramCompensatingValues, err = types.HistogramSlicePool.Get(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
		g.histogramCompensatingValues = g.histogramCompensatingValues[:timeRange.StepCount]
		g.histogramCounterResetTracker, err = newHistogramCounterResetTracker(timeRange.StepCount, memoryConsumptionTracker)
		if err != nil {
			return err
		}
	}

	for inputIdx, p := range data.Histograms {
		outputIdx := timeRange.PointIndex(p.T)
		g.groupSeriesCounts[outputIdx]++

		if g.histograms[outputIdx] == invalidCombinationOfHistograms {
			// We've already seen an invalid combination of histograms at this timestamp. Ignore this point.
			continue
		}

		if g.histograms[outputIdx] == nil {
			// First sample for this output point, retain the histogram as-is if we can.
			if mutatingDataAllowed {
				g.histograms[outputIdx] = p.H

				// Ensure the FloatHistogram instance is not reused when the HPoint slice data.Histograms is reused.
				data.Histograms[inputIdx].H = nil
			} else {
				g.histograms[outputIdx] = p.H.Copy()
			}

			g.histogramPointCount++
			g.histogramCounterResetTracker.init(outputIdx, p.H.CounterResetHint)

			continue
		}

		if g.histogramCounterResetTracker.checkCounterResetConflicts(outputIdx, p.H.CounterResetHint) {
			emitAnnotation(newAggregationCounterResetCollisionWarning)
		}

		if mutatingDataAllowed {
			g.temporaryHistogram = p.H
		} else if g.temporaryHistogram == nil {
			g.temporaryHistogram = p.H.Copy()
		} else {
			p.H.CopyTo(g.temporaryHistogram)
		}

		// For incremental mean calculation with Kahan summation:
		// mean_new = mean * ((count-1)/count) + p.H/count
		// Both mean and compensation must be scaled by (count-1)/count
		q := (g.groupSeriesCounts[outputIdx] - 1) / g.groupSeriesCounts[outputIdx]

		// Scale the compensation
		if g.histogramCompensatingValues[outputIdx] != nil {
			g.histogramCompensatingValues[outputIdx].Mul(q)
		}

		// Prepare the increment: p.H/count
		g.temporaryHistogram.Div(g.groupSeriesCounts[outputIdx])

		// Compute: mean * q + (p.H/count) with Kahan summation
		// Note: Mul modifies in place and returns the result, so we can chain it with KahanAdd
		var nhcbBoundsReconciled bool
		g.histogramCompensatingValues[outputIdx], _, nhcbBoundsReconciled, err = g.histograms[outputIdx].Mul(q).KahanAdd(g.temporaryHistogram, g.histogramCompensatingValues[outputIdx])
		if err != nil {
			// Unable to add histograms together (likely due to invalid combination of histograms). Make sure we don't emit a sample at this timestamp.
			g.histograms[outputIdx] = invalidCombinationOfHistograms
			g.histogramPointCount--

			if err := functions.NativeHistogramErrorToAnnotation(err, emitAnnotation); err != nil {
				// Unknown error: we couldn't convert the error to an annotation. Give up.
				return err
			}
			continue
		}

		if nhcbBoundsReconciled {
			emitAnnotation(newAggregationMismatchedCustomBucketsHistogramInfo)
		}
	}
	return nil
}

// reconcileAndCountFloatPoints will return the number of points with a float present.
// It also takes the opportunity whilst looping through the floats to check if there
// is a conflicting Histogram present. If both are present, an empty vector should
// be returned. So this method removes the float+histogram where they conflict.
func (g *AvgAggregationGroup) reconcileAndCountFloatPoints() (int, bool) {
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
	if len(g.floatPresent) > 0 && len(g.histograms) > 0 {
		for idx, present := range g.floatPresent {
			if present {
				if g.histograms[idx] != nil {
					// If a mix of histogram samples and float samples, the corresponding vector element is removed from the output vector entirely
					// and a warning annotation is emitted.
					g.floatPresent[idx] = false
					g.histograms[idx] = nil
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

func (g *AvgAggregationGroup) ComputeOutputSeries(_ types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (types.InstantVectorSeriesData, bool, error) {
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
				var f float64
				if g.incrementalMeans != nil && g.incrementalMeans[i] {
					f = g.floatMeans[i] + g.floatCompensatingMeans[i]
				} else {
					f = (g.floats[i] + g.floatCompensatingMeans[i]) / g.groupSeriesCounts[i]
				}
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

		for i, h := range g.histograms {
			if h != nil && h != invalidCombinationOfHistograms {
				t := timeRange.StartT + int64(i)*timeRange.IntervalMilliseconds

				// The histogram already contains the mean (computed incrementally), just apply compensation
				if g.histogramCompensatingValues[i] != nil {
					// Add the compensation to get the final accurate result
					h, _, _, err = h.Add(g.histogramCompensatingValues[i])
					if err != nil {
						return types.InstantVectorSeriesData{}, hasMixedData, err
					}
				}

				histogramPoints = append(histogramPoints, promql.HPoint{T: t, H: h.Compact(0)})

				// Remove histogram from slice to ensure it's not mutated when the slice is reused.
				g.histograms[i] = nil
			}
		}
	}

	return types.InstantVectorSeriesData{Floats: floatPoints, Histograms: histogramPoints}, hasMixedData, nil
}

func (g *AvgAggregationGroup) Close(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) {
	types.Float64SlicePool.Put(&g.floats, memoryConsumptionTracker)
	types.Float64SlicePool.Put(&g.floatMeans, memoryConsumptionTracker)
	types.Float64SlicePool.Put(&g.floatCompensatingMeans, memoryConsumptionTracker)
	types.BoolSlicePool.Put(&g.incrementalMeans, memoryConsumptionTracker)
	types.BoolSlicePool.Put(&g.floatPresent, memoryConsumptionTracker)
	types.HistogramSlicePool.Put(&g.histograms, memoryConsumptionTracker)
	types.HistogramSlicePool.Put(&g.histogramCompensatingValues, memoryConsumptionTracker)
	types.Float64SlicePool.Put(&g.groupSeriesCounts, memoryConsumptionTracker)
	if g.histogramCounterResetTracker != nil {
		g.histogramCounterResetTracker.close()
		g.histogramCounterResetTracker = nil
	}
}
