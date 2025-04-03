// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/quantile.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package floats

import (
	"math"
	"slices"
)

// Quantile calculates the given quantile of a vector of samples.
//
// values will be sorted in place.
// If values has zero elements, NaN is returned.
// If q==NaN, NaN is returned.
// If q<0, -Inf is returned.
// If q>1, +Inf is returned.
//
// This is largely copied from github.com/prometheus/prometheus/promql/quantile.go
// but modified to take a slice of float64's instead of their vectorByValueHeap.
func Quantile(q float64, values []float64) float64 {
	if len(values) == 0 || math.IsNaN(q) {
		return math.NaN()
	}
	if q < 0 {
		return math.Inf(-1)
	}
	if q > 1 {
		return math.Inf(+1)
	}
	slices.Sort(values)

	n := float64(len(values))
	// When the quantile lies between two samples,
	// we use a weighted average of the two samples.
	rank := q * (n - 1)

	lowerIndex := math.Max(0, math.Floor(rank))
	upperIndex := math.Min(n-1, lowerIndex+1)

	weight := rank - math.Floor(rank)
	return values[int(lowerIndex)]*(1-weight) + values[int(upperIndex)]*weight
}
