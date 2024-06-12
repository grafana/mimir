// SPDX-License-Identifier: AGPL-3.0-only

package functions

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/pooling"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestAcos(t *testing.T) {
	pool := pooling.NewLimitingPool(0, nil)

	seriesData := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{F: 1.0},
			{F: 0.5},
			{F: 0.0},
		},
		Histograms: []promql.HPoint{
			{H: &histogram.FloatHistogram{Count: 1, Sum: 2}},
		},
	}

	expected := types.InstantVectorSeriesData{
		Floats: []promql.FPoint{
			{F: math.Acos(1.0)},
			{F: math.Acos(0.5)},
			{F: math.Acos(0.0)},
		},
		Histograms: nil, // Histograms should be dropped
	}

	modifiedSeriesData, err := Acos(seriesData, pool)
	require.NoError(t, err)
	require.Equal(t, expected, modifiedSeriesData)
}
