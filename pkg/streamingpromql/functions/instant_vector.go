// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"math"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/prometheus/prometheus/promql"
)

type SeriesMetadataFunction func(seriesMetadata []types.SeriesMetadata, pool *pooling.LimitingPool) ([]types.SeriesMetadata, error)

func DropSeriesName(seriesMetadata []types.SeriesMetadata, _ *pooling.LimitingPool) ([]types.SeriesMetadata, error) {
	for i := range seriesMetadata {
		seriesMetadata[i].Labels = seriesMetadata[i].Labels.DropMetricName()
	}

	return seriesMetadata, nil
}

type InstantVectorFunction func(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error)

func TransformationFunc(transform func(f float64) float64) InstantVectorFunction {
	return func(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
		for i := range seriesData.Floats {
			seriesData.Floats[i].F = transform(seriesData.Floats[i].F)
		}

		return seriesData, nil
	}
}

var Acos = TransformationFunc(math.Acos)

func HistogramCount(series types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
	floats, err := pool.GetFPointSlice(len(series.Histograms))
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: floats,
	}
	for _, Histogram := range series.Histograms {
		data.Floats = append(data.Floats, promql.FPoint{
			T: Histogram.T,
			F: Histogram.H.Count,
		})
	}
	return data, nil
}

func HistogramSum(series types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
	floats, err := pool.GetFPointSlice(len(series.Histograms))
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: floats,
	}
	for _, Histogram := range series.Histograms {
		data.Floats = append(data.Floats, promql.FPoint{
			T: Histogram.T,
			F: Histogram.H.Sum,
		})
	}
	return data, nil
}
