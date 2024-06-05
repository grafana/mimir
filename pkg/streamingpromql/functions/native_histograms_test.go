// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestHistogramCount(t *testing.T) {
	pool := pooling.NewLimitingPool(0)

	seriesData := types.InstantVectorSeriesData{
		Histograms: []promql.HPoint{
			{T: 1, H: &histogram.FloatHistogram{Count: 10}},
			{T: 2, H: &histogram.FloatHistogram{Count: 20}},
		},
	}

	expected := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{T: 1, F: 10},
			{T: 2, F: 20},
		},
	}

	modifiedSeriesData, err := HistogramCount(seriesData, pool)
	require.NoError(t, err)
	require.Equal(t, expected, modifiedSeriesData)
}

func TestHistogramSum(t *testing.T) {
	pool := pooling.NewLimitingPool(0)

	seriesData := types.InstantVectorSeriesData{
		Histograms: []promql.HPoint{
			{T: 1, H: &histogram.FloatHistogram{Sum: 100.5}},
			{T: 2, H: &histogram.FloatHistogram{Sum: 200.75}},
		},
	}

	expected := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{T: 1, F: 100.5},
			{T: 2, F: 200.75},
		},
	}

	modifiedSeriesData, err := HistogramSum(seriesData, pool)
	require.NoError(t, err)
	require.Equal(t, expected, modifiedSeriesData)
}
