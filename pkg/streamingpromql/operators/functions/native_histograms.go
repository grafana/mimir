// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"errors"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func HistogramAvg(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
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
			F: histogram.H.Sum / histogram.H.Count,
		})
	}

	types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)

	return data, nil
}

func HistogramCount(seriesData types.InstantVectorSeriesData, _ []types.ScalarData, _ types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
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
			F: histogram.H.Count,
		})
	}

	types.PutInstantVectorSeriesData(seriesData, memoryConsumptionTracker)

	return data, nil
}

func HistogramFraction(seriesData types.InstantVectorSeriesData, scalarArgsData []types.ScalarData, timeRange types.QueryTimeRange, memoryConsumptionTracker *limiting.MemoryConsumptionTracker) (types.InstantVectorSeriesData, error) {
	floats, err := types.FPointSlicePool.Get(len(seriesData.Histograms), memoryConsumptionTracker)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: floats,
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
