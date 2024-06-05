// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func HistogramCount(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
	floats, err := pool.GetFPointSlice(len(seriesData.Histograms))
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: floats,
	}
	for _, Histogram := range seriesData.Histograms {
		data.Floats = append(data.Floats, promql.FPoint{
			T: Histogram.T,
			F: Histogram.H.Count,
		})
	}
	pool.PutInstantVectorSeriesData(seriesData)
	return data, nil
}

func HistogramSum(seriesData types.InstantVectorSeriesData, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
	floats, err := pool.GetFPointSlice(len(seriesData.Histograms))
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	data := types.InstantVectorSeriesData{
		Floats: floats,
	}
	for _, Histogram := range seriesData.Histograms {
		data.Floats = append(data.Floats, promql.FPoint{
			T: Histogram.T,
			F: Histogram.H.Sum,
		})
	}
	pool.PutInstantVectorSeriesData(seriesData)
	return data, nil
}
