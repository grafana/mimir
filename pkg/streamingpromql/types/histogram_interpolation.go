// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package types

import (
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"
)

// InterpolateHistograms linearly interpolates between two histograms (h1 at t1, h2 at t2) and
// returns the histogram value at time t. When isCounter is true and h2.DetectReset(h1) returns
// true, the counter is modelled as restarting from zero, so the result is h2 scaled by the
// fraction (t-t1)/(t2-t1). Returns an error when the two histograms have incompatible schemas
// (mixing exponential and custom buckets). When a Sub or Add reconciles mismatched custom-bucket
// bounds, emitNHCBReconciledAnnotation is called with the corresponding operation so the caller can
// surface the matching info annotation. The returned histogram is always a freshly allocated instance; h1 and h2
// are never returned as-is, even when t == t1 or t == t2. Mirrors interpolateHistograms in upstream
// Prometheus.
func InterpolateHistograms(h1 *histogram.FloatHistogram, t1 int64, h2 *histogram.FloatHistogram, t2, t int64, isCounter bool, emitNHCBReconciledAnnotation func(annotations.HistogramOperation)) (*histogram.FloatHistogram, error) {
	if t == t1 {
		return h1.Copy(), nil
	}
	if t == t2 {
		return h2.Copy(), nil
	}
	// Exponential and custom-bucket histograms cannot be interpolated. Surface this as
	// ErrHistogramsIncompatibleSchema before the counter-reset path below, so a DetectReset that
	// happens to return true cannot mask it. Mirrors Prometheus's smoothSeries, which checks
	// UsesCustomBuckets before interpolating.
	if h1.UsesCustomBuckets() != h2.UsesCustomBuckets() {
		return nil, histogram.ErrHistogramsIncompatibleSchema
	}
	fraction := float64(t-t1) / float64(t2-t1)
	if isCounter && h2.DetectReset(h1) {
		return h2.Copy().Mul(fraction), nil
	}
	result := h2.Copy()
	if _, _, nhcbReconciled, err := result.Sub(h1); err != nil {
		return nil, err
	} else if nhcbReconciled {
		emitNHCBReconciledAnnotation(annotations.HistogramSub)
	}
	result.Mul(fraction)
	if _, _, nhcbReconciled, err := result.Add(h1); err != nil {
		return nil, err
	} else if nhcbReconciled {
		emitNHCBReconciledAnnotation(annotations.HistogramAdd)
	}
	return result, nil
}
