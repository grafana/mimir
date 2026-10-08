// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/util/kahansum/kahansum.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package floats

import "math"

// isInf reports whether f is positive or negative infinity.
// It avoids math.IsInf to prevent inflating KahanSumInc's inlining cost.
func isInf(f float64) bool {
	return f > math.MaxFloat64 || f < -math.MaxFloat64
}

// KahanSumInc computes the sum of inc and sum using the Kahan summation algorithm.
func KahanSumInc(inc, sum, c float64) (newSum, newC float64) {
	// Kahan summation is sensitive to float rounding. Go permits fusing float operations
	// (e.g. fused multiply-add), which gave less accurate results when this function was inlined
	// (see https://github.com/prometheus/prometheus/pull/16895).
	// Explicit conversions force rounding to float64 precision at the call boundary, which forbids
	// fusing across it without the cost of //go:noinline (https://go.dev/ref/spec#Floating_point_operators).
	// The following conversions are not no-ops.
	inc = float64(inc)
	sum = float64(sum)
	c = float64(c)

	t := sum + inc
	switch {
	case isInf(t):
		c = 0

	// Using Neumaier improvement, swap if next term larger than sum.
	case math.Abs(sum) >= math.Abs(inc):
		c += (sum - t) + inc
	default:
		c += (inc - t) + sum
	}

	t = float64(t)
	c = float64(c)
	return t, c
}
