// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
)

// InstantVectorFunction performs a histogram_count over a range vector.
type InstantVectorFunction struct {
	Inner InstantVectorOperator
	Pool  *pooling.LimitingPool

	Args parser.Expressions
	Func functionCall
}

var _ InstantVectorOperator = &InstantVectorFunction{}

func (m *InstantVectorFunction) SeriesMetadata(ctx context.Context) ([]types.SeriesMetadata, error) {
	metadata, err := m.Inner.SeriesMetadata(ctx)
	if err != nil {
		return nil, err
	}

	lb := labels.NewBuilder(labels.EmptyLabels())
	for i := range metadata {
		metadata[i].Labels = dropMetricName(metadata[i].Labels, lb)
	}

	return metadata, nil
}

func (m *InstantVectorFunction) NextSeries(ctx context.Context) (types.InstantVectorSeriesData, error) {
	series, err := m.Inner.NextSeries(ctx)
	if err != nil {
		return types.InstantVectorSeriesData{}, err
	}

	return m.Func(series, m.Args, m.Pool)
}

func (m *InstantVectorFunction) Close() {
	m.Inner.Close()
}

type functionCall func(seriesData types.InstantVectorSeriesData, args parser.Expressions, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error)

var InstantVectorFunctionCalls = map[string]functionCall{
	"histogram_count": histogramCount,
}

func histogramCount(series types.InstantVectorSeriesData, _ parser.Expressions, pool *pooling.LimitingPool) (types.InstantVectorSeriesData, error) {
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
