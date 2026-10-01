// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package aggregations

import (
	"math"

	"github.com/prometheus/prometheus/promql"
)

// accumulateFloat64sScalar loads operands from memory, so the multiply-add fusion
// that requires floats.KahanSumInc's noinline guard cannot occur here.
// Accumulator slices must cover values, and all three slices must be disjoint.
func accumulateFloat64sScalar(values, sums, compensations []float64) {
	sums = sums[:len(values)]
	compensations = compensations[:len(values)]

	for i, v := range values {
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

func packFloat64s(values []float64, points []promql.FPoint) {
	values = values[:len(points)]
	for i, p := range points {
		values[i] = p.F
	}
}
