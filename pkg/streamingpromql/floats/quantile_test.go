// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/quantile.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

// NOTE(jhesketh): This is copied from Prometheus alongside quantile.go

package floats

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuantile(t *testing.T) {
	tests := []struct {
		name     string
		q        float64
		values   []float64
		expected float64
	}{
		{"Empty slice", 0.5, []float64{}, math.NaN()},
		{"NaN quantile", math.NaN(), []float64{1, 2, 3}, math.NaN()},
		{"Quantile below zero", -0.1, []float64{1, 2, 3}, math.Inf(-1)},
		{"Quantile above one", 1.1, []float64{1, 2, 3}, math.Inf(1)},
		{"Median (odd number of elements)", 0.5, []float64{3, 1, 2}, 2},
		{"Median (even number of elements)", 0.5, []float64{4, 1, 2, 3}, 2.5},
		{"First quartile", 0.25, []float64{1, 2, 3, 4}, 1.75},
		{"Third quartile", 0.75, []float64{1, 2, 3, 4}, 3.25},
		{"Exact quantile at 0", 0, []float64{10, 20, 30}, 10},
		{"Exact quantile at 1", 1, []float64{10, 20, 30}, 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Quantile(tt.q, append([]float64(nil), tt.values...))
			if math.IsNaN(tt.expected) {
				// Can't compare NaN's directly
				require.True(t, math.IsNaN(got), "expected NaN, but got %d", got)
			} else {
				require.Equal(t, tt.expected, got, "expected result %d, but got %d", tt.expected, got)
			}
		})
	}
}
