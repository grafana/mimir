// SPDX-License-Identifier: AGPL-3.0-only

package batch

import (
	"math"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
)

// The functions in this file decide which of two samples with the same timestamp to keep. The querier uses them
// both when it merges the chunks of a series (decideTimestampConflict) and when it merges the same series from
// different sources (querier.deterministicSeriesMerge), so that the result does not depend on where the two samples
// meet. Each function returns true if the left sample is preferred.
//
// Together they are a strict weak ordering of the samples, so the sample that is kept does not depend on the order
// of the inputs.

// PreferValueType decides between two samples of different types: a float histogram is preferred over a histogram,
// and a histogram over a float. If lt and rt are the same, decided is false, and the caller must compare the values.
func PreferValueType(lt, rt chunkenc.ValueType) (preferLeft, decided bool) {
	switch {
	case lt == rt:
		return false, false
	case lt == chunkenc.ValFloatHistogram || rt == chunkenc.ValFloatHistogram:
		return lt == chunkenc.ValFloatHistogram, true
	case lt == chunkenc.ValHistogram || rt == chunkenc.ValHistogram:
		return lt == chunkenc.ValHistogram, true
	default:
		// We should never reach this point.
		return true, true
	}
}

// PreferFloat returns true if l is preferred over r: the higher value is preferred.
//
// A NaN is lower than any other value, and two NaNs are ordered by their bits. With only l > r, a NaN would be
// neither lower nor higher than any other value, so the sample that is kept would depend on the order of the inputs.
func PreferFloat(l, r float64) bool {
	lNaN, rNaN := math.IsNaN(l), math.IsNaN(r)
	switch {
	case lNaN && rNaN:
		return math.Float64bits(l) > math.Float64bits(r)
	case lNaN || rNaN:
		return rNaN
	default:
		return l > r
	}
}

// PreferHistogram returns true if l is preferred over r: the higher count is preferred, and then the higher sum.
func PreferHistogram(l, r *histogram.Histogram) bool {
	if l.Count != r.Count {
		return l.Count > r.Count
	}
	return PreferFloat(l.Sum, r.Sum)
}

// PreferFloatHistogram returns true if l is preferred over r: the higher count is preferred, and then the higher sum.
func PreferFloatHistogram(l, r *histogram.FloatHistogram) bool {
	if l.Count != r.Count {
		return PreferFloat(l.Count, r.Count)
	}
	return PreferFloat(l.Sum, r.Sum)
}
