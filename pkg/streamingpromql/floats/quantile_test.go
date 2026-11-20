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
	smallestSubnormal := math.SmallestNonzeroFloat64
	largestSubnormal := math.Nextafter(math.Ldexp(1, -1022), 0)
	smallestNormal := math.Ldexp(1, -1022)
	negativeZero := math.Copysign(0, -1)
	maxFloat := math.MaxFloat64
	previousMaxFloat := math.Nextafter(maxFloat, 0)

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

		// Interpolating between identical values must return that value exactly, without rounding error or underflow.
		{"Smallest positive subnormal", 0.5, []float64{smallestSubnormal, smallestSubnormal}, smallestSubnormal},
		{"Three positive subnormal units", 0.5, []float64{math.Float64frombits(3), math.Float64frombits(3)}, math.Float64frombits(3)},
		{"Smallest negative subnormal", 0.5, []float64{-smallestSubnormal, -smallestSubnormal}, -smallestSubnormal},
		{"Three negative subnormal units", 0.5, []float64{-math.Float64frombits(3), -math.Float64frombits(3)}, -math.Float64frombits(3)},
		{"Largest positive subnormal", 0.5, []float64{largestSubnormal, largestSubnormal}, largestSubnormal},
		{"Largest negative subnormal", 0.5, []float64{-largestSubnormal, -largestSubnormal}, -largestSubnormal},
		{"Smallest positive normal", 0.5, []float64{smallestNormal, smallestNormal}, smallestNormal},
		{"Smallest negative normal", 0.5, []float64{-smallestNormal, -smallestNormal}, -smallestNormal},
		{"Identical ordinary values", 0.5, []float64{3, 3}, 3},
		{"Identical ordinary values with rounding error from interpolation", 0.3, []float64{0.1, 0.1}, 0.1},
		{"Positive zero", 0.5, []float64{0, 0}, 0},
		{"Negative zero", 0.5, []float64{negativeZero, negativeZero}, negativeZero},
		{"Mixed signed zero", 0.5, []float64{negativeZero, 0}, 0},
		{"Equal positive infinity", 0.5, []float64{math.Inf(1), math.Inf(1)}, math.Inf(1)},
		{"Equal negative infinity", 0.5, []float64{math.Inf(-1), math.Inf(-1)}, math.Inf(-1)},
		{"Mixed infinities", 0.5, []float64{math.Inf(-1), math.Inf(1)}, math.NaN()},
		{"Equal NaNs", 0.5, []float64{math.NaN(), math.NaN()}, math.NaN()},
		{"NaN and finite value", 0.5, []float64{math.NaN(), 1}, math.NaN()},
		{"Opposite finite extremes", 0.5, []float64{-maxFloat, maxFloat}, 0},
		{"Adjacent finite values near maximum", 0.5, []float64{previousMaxFloat, maxFloat}, previousMaxFloat},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Quantile(tt.q, append([]float64(nil), tt.values...))
			if math.IsNaN(tt.expected) {
				// Can't compare NaN's directly
				require.True(t, math.IsNaN(got), "expected NaN, but got %d", got)
			} else {
				// Compare bits, because require.Equal treats negative zero and positive zero as equal.
				require.Equal(t, math.Float64bits(tt.expected), math.Float64bits(got), "expected result %v, but got %v", tt.expected, got)
			}
		})
	}
}
