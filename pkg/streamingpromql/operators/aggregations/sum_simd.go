// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.simd

package aggregations

import (
	"math"
	"simd"
)

// accumulateFloat64sSIMD preserves contributing-series order within each lane.
// Accumulator slices must cover values, and all three slices must be disjoint.
func accumulateFloat64sSIMD(values, sums, compensations []float64) {
	// Capped state slices let the compiler prove the vector windows are in bounds.
	sums = sums[:len(values):len(values)]
	compensations = compensations[:len(values):len(values)]

	var zero simd.Float64s
	vlen := zero.Len()
	inf := simd.BroadcastFloat64s(math.Inf(1))
	var i int
	for ; i <= len(values)-vlen; i += vlen {
		v := simd.LoadFloat64s(values[i : i+vlen])
		s := simd.LoadFloat64s(sums[i : i+vlen])
		c := simd.LoadFloat64s(compensations[i : i+vlen])
		t := s.Add(v)
		sumIsBigger := s.Abs().GreaterEqual(v.Abs())
		hi := s.IfElse(sumIsBigger, v)
		lo := v.IfElse(sumIsBigger, s)
		c = c.Add(hi.Sub(t).Add(lo))
		// AndNot avoids the extra mask inversion emitted for Masked(NotEqual) on arm64.
		infinite := t.Abs().Equal(inf).ToInt64s().ToBits()
		c = c.ToBits().AndNot(infinite).BitsToFloat64()
		t.Store(sums[i : i+vlen])
		c.Store(compensations[i : i+vlen])
	}
	if i < len(values) {
		// A scalar tail avoids padded lanes and the emulated partial-load count discrepancy.
		accumulateFloat64sScalar(values[i:], sums[i:], compensations[i:])
	}
}
