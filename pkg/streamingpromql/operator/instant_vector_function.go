// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package operator

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
)

// InstantVectorFunction performs a histogram_count over a range vector.
type InstantVectorFunction struct {
	Inner InstantVectorOperator
}

var _ InstantVectorOperator = &InstantVectorFunction{}

func (m *InstantVectorFunction) SeriesMetadata(ctx context.Context) ([]SeriesMetadata, error) {
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

func (m *InstantVectorFunction) NextSeries(ctx context.Context) (InstantVectorSeriesData, error) {
	series, err := m.Inner.NextSeries(ctx)
	if err != nil {
		return InstantVectorSeriesData{}, err
	}
	data := InstantVectorSeriesData{
		Floats: GetFPointSlice(len(series.Histograms)),
	}
	for _, Histogram := range series.Histograms {
		data.Floats = append(data.Floats, promql.FPoint{
			T: Histogram.T,
			F: Histogram.H.Count,
		})
	}
	return data, nil
}

func (m *InstantVectorFunction) Close() {
	m.Inner.Close()
}
