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

func TestClampFactory(t *testing.T) {
	pool := pooling.NewLimitingPool(0)

	tests := []struct {
		name     string
		min      float64
		max      float64
		input    types.InstantVectorSeriesData
		expected types.InstantVectorSeriesData
	}{
		{
			name: "Normal case",
			min:  0.5,
			max:  1.5,
			input: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{F: 0.0},
					{F: 1.0},
					{F: 2.0},
				},
				Histograms: []promql.HPoint{
					{H: &histogram.FloatHistogram{Count: 1, Sum: 2}},
				},
			},
			expected: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{F: 0.5},
					{F: 1.0},
					{F: 1.5},
				},
				Histograms: nil,
			},
		},
		{
			name: "Min greater than max",
			min:  2.0,
			max:  1.0,
			input: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{F: 0.0},
					{F: 1.0},
					{F: 2.0},
				},
			},
			expected: types.InstantVectorSeriesData{
				Floats: nil,
			},
		},
		{
			name: "NaN handling",
			min:  math.NaN(),
			max:  1.0,
			input: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{F: 0.0},
					{F: 1.0},
					{F: 2.0},
				},
			},
			expected: types.InstantVectorSeriesData{
				Floats: []promql.FPoint{
					{F: math.NaN()},
					{F: math.NaN()},
					{F: math.NaN()},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clampFunc := ClampFactory(tt.min, tt.max)
			modifiedSeriesData, err := clampFunc(tt.input, pool)
			require.NoError(t, err)

			require.Equal(t, len(tt.expected.Floats), len(modifiedSeriesData.Floats))
			for i := range tt.expected.Floats {
				if math.IsNaN(tt.expected.Floats[i].F) {
					require.True(t, math.IsNaN(modifiedSeriesData.Floats[i].F))
				} else {
					require.Equal(t, tt.expected.Floats[i].F, modifiedSeriesData.Floats[i].F)
				}
			}
			require.Nil(t, modifiedSeriesData.Histograms)
		})
	}
}
