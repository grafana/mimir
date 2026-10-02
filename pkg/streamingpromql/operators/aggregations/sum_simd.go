// SPDX-License-Identifier: AGPL-3.0-only

//go:build goexperiment.simd

package aggregations

import (
	"math"
	"simd"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func (g *SumAggregationGroup) accumulateFloatPoints(points []promql.FPoint, timeRange types.QueryTimeRange) {
	if len(points) == 0 {
		return
	}

	first := int(timeRange.PointIndex(points[0].T))
	end := first + len(points)
	// Sorted, unique evaluation-grid timestamps make the endpoints sufficient to detect gaps.
	// If gaps detected fall back to no-SIMD implementation, that walks over all points.
	if points[len(points)-1].T != timeRange.IndexTime(int64(end-1)) {
		g.accumulateFloatsFallback(points, timeRange)
		return
	}

	sums := g.floatSums[first:end]
	compensations := g.floatCompensatingValues[first:end]
	floatPresent := g.floatPresent[first:end]
	// 32 values amortize packing and kernel dispatch within 256 stack bytes.
	var buf [32]float64
	for offset := 0; offset < len(points); offset += len(buf) {
		n := min(len(buf), len(points)-offset)
		// Matching block lengths let the compiler eliminate per-element bounds checks.
		ps := points[offset : offset+n]
		vals := buf[:n]
		fps := floatPresent[offset : offset+n]
		for i, point := range ps {
			vals[i] = point.F
			fps[i] = true
		}
		accumulateFloat64sSIMD(vals, sums[offset:offset+n], compensations[offset:offset+n])
	}
}

// accumulateFloat64sSIMD preserves contributing-series order within each lane.
// Accumulator slices must cover values, and all three slices must be disjoint.
func accumulateFloat64sSIMD(vals, sums, compensations []float64) {
	// Capped state slices let the compiler prove the vector windows are in bounds.
	sums = sums[:len(vals):len(vals)]
	compensations = compensations[:len(vals):len(vals)]

	var zero simd.Float64s
	vlen := zero.Len()
	inf := simd.BroadcastFloat64s(math.Inf(1))
	var i int
	for ; i <= len(vals)-vlen; i += vlen {
		v := simd.LoadFloat64s(vals[i : i+vlen])
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
	if i < len(vals) {
		// A scalar tail avoids padded lanes and the emulated partial-load count discrepancy.
		accumulateFloat64sScalar(vals[i:], sums[i:], compensations[i:])
	}
}

// accumulateFloat64sScalar loads operands from memory, so the multiply-add fusion
// that requires floats.KahanSumInc's noinline guard cannot occur here.
// Accumulator slices must cover values, and all three slices must be disjoint.
func accumulateFloat64sScalar(vals, sums, compensations []float64) {
	sums = sums[:len(vals)]
	compensations = compensations[:len(vals)]

	for i, v := range vals {
		s, c := sums[i], compensations[i]
		t := s + v
		switch {
		case math.IsInf(t, 0):
			c = 0
		case math.Abs(s) >= math.Abs(v):
			c += (s - t) + v
		default:
			c += (v - t) + s
		}
		sums[i], compensations[i] = t, c
	}
}
