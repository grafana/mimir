// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"errors"
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/floats"
	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func HistogramAvg(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	fPoints, err := types.FPointSlicePool.Get(len(seriesData.Histograms), memoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: fPoints,
	}

	for _, histogram := range seriesData.Histograms {
		data.Floats = append(data.Floats, promql.FPoint{
			T: histogram.T,
			F: histogram.H.Sum / histogram.H.Count,
		})
	}

	types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)

	return data, nil
}

func HistogramCount(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	fPoints, err := types.FPointSlicePool.Get(len(seriesData.Histograms), memoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: fPoints,
	}

	for _, histogram := range seriesData.Histograms {
		data.Floats = append(data.Floats, promql.FPoint{
			T: histogram.T,
			F: histogram.H.Count,
		})
	}

	types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)

	return data, nil
}

func HistogramFraction(seriesData types.InstantVectorSeriesData, scalarArgsData []types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	fPoints, err := types.FPointSlicePool.Get(len(seriesData.Histograms), memoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: fPoints,
	}

	lower := scalarArgsData[0]
	upper := scalarArgsData[1]

	for _, histogram := range seriesData.Histograms {
		// Scalars are guaranteed to have a point for each step in the query.
		idx := timeRange.PointIndex(histogram.T)
		lowerVal := lower.Samples[idx].F
		upperVal := upper.Samples[idx].F

		data.Floats = append(data.Floats, promql.FPoint{
			T: histogram.T,
			F: histogramFraction(lowerVal, upperVal, histogram.H),
		})
	}

	types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)

	return data, nil
}

func HistogramStdDevStdVar(isStdDev bool) InstantVectorSeriesFunction {
	// returns either the standard deviation, or standard variance of a native histogram.
	// Float values are ignored.
	return func(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
		fPoints, err := types.FPointSlicePool.Get(len(seriesData.Histograms), memoryConsumptionTracker)
		if err != nil {
			return types.InstantVectorSeriesData{}, err
		}

		data := types.InstantVectorSeriesData{
			Floats: fPoints,
		}

		for _, histogram := range seriesData.Histograms {
			mean := histogram.H.Sum / histogram.H.Count
			var variance, cVariance float64
			it := histogram.H.AllBucketIterator()
			for it.Next() {
				bucket := it.At()
				if bucket.Count == 0 {
					continue
				}
				var val float64
				if bucket.Lower <= 0 && 0 <= bucket.Upper {
					val = 0
				} else {
					val = math.Sqrt(bucket.Upper * bucket.Lower)
					if bucket.Upper < 0 {
						val = -val
					}
				}
				delta := val - mean
				variance, cVariance = floats.KahanSumInc(bucket.Count*delta*delta, variance, cVariance)
			}
			variance += cVariance
			variance /= histogram.H.Count
			if isStdDev {
				variance = math.Sqrt(variance)
			}

			data.Floats = append(data.Floats, promql.FPoint{
				T: histogram.T,
				F: variance,
			})
		}

		types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)

		return data, nil
	}
}

func HistogramSum(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	floats, err := types.FPointSlicePool.Get(len(seriesData.Histograms), memoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: floats,
	}

	for _, histogram := range seriesData.Histograms {
		data.Floats = append(data.Floats, promql.FPoint{
			T: histogram.T,
			F: histogram.H.Sum,
		})
	}

	types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)

	return data, nil
}

func NativeHistogramErrorToAnnotation(err error, emitAnnotation types.EmitAnnotationFunc) error {
	if err == nil {
		return nil
	}

	if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
		emitAnnotation(annotations.NewMixedExponentialCustomHistogramsWarning)
		return nil
	}

	if errors.Is(err, histogram.ErrHistogramsIncompatibleBounds) {
		emitAnnotation(annotations.NewIncompatibleCustomBucketsHistogramsWarning)
		return nil
	}

	return err
}
