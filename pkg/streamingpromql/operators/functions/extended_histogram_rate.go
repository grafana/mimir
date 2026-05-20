// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"errors"
	"sort"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// interpolateHistograms performs linear interpolation between two histogram points h1 (at t1)
// and h2 (at t2) and returns the histogram interpolated at time t. If isCounter is true and
// h2.DetectReset(h1) returns true, the counter is modeled as starting from zero, so the result
// is h2 scaled by the fraction (t - t1) / (t2 - t1). NHCB bucket-bounds reconciliation warnings
// are emitted via emitAnnotation. Mirrors interpolateHistograms in upstream Prometheus.
func interpolateHistograms(h1 *histogram.FloatHistogram, t1 int64, h2 *histogram.FloatHistogram, t2, t int64, isCounter bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	if t == t1 {
		return h1.Copy(), nil
	}
	if t == t2 {
		return h2.Copy(), nil
	}
	fraction := float64(t-t1) / float64(t2-t1)

	if isCounter && h2.DetectReset(h1) {
		return h2.Copy().Mul(fraction), nil
	}

	// Result = H1 + (H2 - H1) * fraction.
	result := h2.Copy()
	_, _, nhcbReconciled, err := result.Sub(h1)
	if err != nil {
		return nil, err
	}
	if nhcbReconciled {
		emitAnnotation(NewSubMismatchedCustomBucketsHistogramInfo)
	}
	result.Mul(fraction)
	_, _, nhcbReconciled, err = result.Add(h1)
	if err != nil {
		return nil, err
	}
	if nhcbReconciled {
		emitAnnotation(NewAddMismatchedCustomBucketsHistogramInfo)
	}
	return result, nil
}

// pickOrInterpolateLeftHistogram returns the histogram at the left boundary of the range.
// If interpolation is needed (smoothed and the first sample is before rangeStart), it returns
// the interpolated histogram at rangeStart; otherwise it returns a copy of the first sample's
// histogram.
func pickOrInterpolateLeftHistogram(hists []promql.HPoint, first int, rangeStart int64, smoothed, isCounter bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	if smoothed && hists[first].T < rangeStart {
		return interpolateHistograms(hists[first].H, hists[first].T, hists[first+1].H, hists[first+1].T, rangeStart, isCounter, emitAnnotation)
	}
	return hists[first].H.Copy(), nil
}

// pickOrInterpolateRightHistogram returns the histogram at the right boundary of the range.
// If interpolation is needed (smoothed and the last sample is after rangeEnd), it returns the
// interpolated histogram at rangeEnd; otherwise it returns a copy of the last sample's histogram.
func pickOrInterpolateRightHistogram(hists []promql.HPoint, last int, rangeEnd int64, smoothed, isCounter bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	if smoothed && last > 0 && hists[last].T > rangeEnd {
		return interpolateHistograms(hists[last-1].H, hists[last-1].T, hists[last].H, hists[last].T, rangeEnd, isCounter, emitAnnotation)
	}
	return hists[last].H.Copy(), nil
}

// annosFromInterpolationError translates an error returned by interpolateHistograms (via
// pickOrInterpolate*Histogram) into the appropriate annotation. Unknown errors are left for the
// caller to surface.
func annosFromInterpolationError(err error, emitAnnotation types.EmitAnnotationFunc) {
	if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
		emitAnnotation(annotations.NewMixedExponentialCustomHistogramsWarning)
	}
}

// addHistogramWithAnnotations adds other into base in place, translating histogram errors and
// bucket-bounds reconciliations into annotations. Returns false if the operation failed.
func addHistogramWithAnnotations(base, other *histogram.FloatHistogram, emitAnnotation types.EmitAnnotationFunc) bool {
	_, _, nhcbBoundsReconciled, err := base.Add(other)
	if err != nil {
		if errors.Is(err, histogram.ErrHistogramsIncompatibleSchema) {
			emitAnnotation(annotations.NewMixedExponentialCustomHistogramsWarning)
		}
		return false
	}
	if nhcbBoundsReconciled {
		emitAnnotation(NewAddMismatchedCustomBucketsHistogramInfo)
	}
	return true
}

// validateHistogramRange checks all histogram samples in h for schema consistency and
// counter-type hints. It returns false (and emits MixedExponentialCustomHistogramsWarning)
// when exponential and custom buckets are mixed. It emits NativeHistogramNotCounterWarning for
// any sample carrying a gauge hint while isCounter is true.
func validateHistogramRange(h []promql.HPoint, isCounter bool, emitAnnotation types.EmitAnnotationFunc) bool {
	usingCustomBuckets := h[0].H.UsesCustomBuckets()
	for _, p := range h {
		if p.H.UsesCustomBuckets() != usingCustomBuckets {
			emitAnnotation(annotations.NewMixedExponentialCustomHistogramsWarning)
			return false
		}
		if isCounter && p.H.CounterResetHint == histogram.GaugeType {
			emitAnnotation(annotations.NewNativeHistogramNotCounterWarning)
		}
	}
	return true
}

// correctForCounterResetsHistogram accumulates counter-reset corrections between
// firstSampleIndex and lastSampleIndex in h, using left and right as the boundary values. It
// mirrors correctForCounterResets for float samples. Returns the accumulated correction
// (nil if none) and false if combining histograms failed.
func correctForCounterResetsHistogram(h []promql.HPoint, firstSampleIndex, lastSampleIndex int, left, right *histogram.FloatHistogram, rangeStart int64, smoothed bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, bool) {
	// firstSampleIndex is represented by left, so the loop starts one beyond.
	first := firstSampleIndex + 1
	prev := left
	if smoothed && h[firstSampleIndex].T < rangeStart && h[firstSampleIndex+1].H.DetectReset(h[firstSampleIndex].H) {
		// The left interpolation spanned the reset between h[firstSampleIndex] and
		// h[firstSampleIndex+1]. That reset is already accounted for, so skip
		// h[firstSampleIndex+1] from the loop and use it as the comparison anchor for any
		// reset that immediately follows.
		prev = h[firstSampleIndex+1].H
		first++
	}
	// lastSampleIndex is always excluded: right is either a direct copy of h[lastSampleIndex]
	// or an interpolation that inherits its CounterResetHint. Including h[lastSampleIndex] in
	// the loop would make right.DetectReset self-detect on the same hint. The final
	// right.DetectReset(prev) below handles the right-boundary reset safely once
	// h[lastSampleIndex] is not prev.
	last := lastSampleIndex - 1

	// first > last+1 when there is nothing between the two boundary samples to check. This
	// happens when firstSampleIndex == lastSampleIndex (single-sample window), or when the
	// smoothed left interpolation already spanned the only reset interval
	// (lastSampleIndex == firstSampleIndex+1 and first was incremented above). Both
	// boundaries were interpolated from the same reset segment, so there is nothing more to
	// correct.
	if first > last+1 {
		return nil, true
	}

	var correction *histogram.FloatHistogram

	addCorrection := func(h *histogram.FloatHistogram) bool {
		if correction == nil {
			correction = h.Copy()
			return true
		}
		return addHistogramWithAnnotations(correction, h, emitAnnotation)
	}

	for _, p := range h[first : last+1] {
		if p.H.DetectReset(prev) {
			if !addCorrection(prev) {
				return nil, false
			}
		}
		prev = p.H
	}
	if right.DetectReset(prev) {
		if !addCorrection(prev) {
			return nil, false
		}
	}
	return correction, true
}

// extendedHistogramRate computes rate/increase/delta for histograms under the anchored or
// smoothed modifier. It mirrors extendedHistogramRate in upstream Prometheus: it picks (or
// interpolates) histogram values at the range boundaries, subtracts the left value from the
// right value, accumulates any counter-reset correction across the interior samples (when
// isCounter is true), and divides by the range duration when isRate is true.
//
// The hExtended slice contains the histogram samples in the extended look-back/look-ahead
// window provided by the range vector selector. originalRangeStart and originalRangeEnd
// delimit the original (non-extended) window the user requested.
func extendedHistogramRate(hExtended []promql.HPoint, originalRangeStart, originalRangeEnd int64, rangeSeconds float64, isCounter, isRate, smoothed bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	if len(hExtended) == 0 {
		return nil, nil
	}
	lastSampleIndex := len(hExtended) - 1
	firstSampleIndex := sort.Search(lastSampleIndex, func(i int) bool { return hExtended[i].T > originalRangeStart }) - 1
	if firstSampleIndex < 0 {
		firstSampleIndex = 0
	}
	if smoothed {
		lastSampleIndex = sort.Search(lastSampleIndex, func(i int) bool { return hExtended[i].T >= originalRangeEnd })
	} else if hExtended[lastSampleIndex].T > originalRangeEnd {
		// fillBuffer always appends the first sample >= rangeEnd as the right-neighbour anchor,
		// but the anchored modifier (unlike smoothed) does not interpolate at rangeEnd, so the
		// trailing post-rangeEnd sample must not be used as the right boundary.
		if lastSampleIndex == 0 {
			return nil, nil
		}
		lastSampleIndex--
	}

	if hExtended[lastSampleIndex].T <= originalRangeStart {
		return nil, nil
	}
	if smoothed && hExtended[firstSampleIndex].T > originalRangeEnd {
		return nil, nil
	}

	if !validateHistogramRange(hExtended[firstSampleIndex:lastSampleIndex+1], isCounter, emitAnnotation) {
		return nil, nil
	}

	left, err := pickOrInterpolateLeftHistogram(hExtended, firstSampleIndex, originalRangeStart, smoothed, isCounter, emitAnnotation)
	if err != nil {
		annosFromInterpolationError(err, emitAnnotation)
		return nil, nil
	}
	right, err := pickOrInterpolateRightHistogram(hExtended, lastSampleIndex, originalRangeEnd, smoothed, isCounter, emitAnnotation)
	if err != nil {
		annosFromInterpolationError(err, emitAnnotation)
		return nil, nil
	}

	if !isCounter && (left.CounterResetHint != histogram.GaugeType || right.CounterResetHint != histogram.GaugeType) {
		emitAnnotation(annotations.NewNativeHistogramNotGaugeWarning)
	}

	// Copy right before subtracting left so that correctForCounterResetsHistogram can still
	// call right.DetectReset against the original boundary value rather than (right - left).
	delta := right.Copy()
	if _, _, nhcbReconciled, err := delta.Sub(left); err != nil {
		annosFromInterpolationError(err, emitAnnotation)
		return nil, nil
	} else if nhcbReconciled {
		emitAnnotation(NewSubMismatchedCustomBucketsHistogramInfo)
	}

	if isCounter {
		correction, ok := correctForCounterResetsHistogram(hExtended, firstSampleIndex, lastSampleIndex, left, right, originalRangeStart, smoothed, emitAnnotation)
		if !ok {
			return nil, nil
		}
		if correction != nil {
			if !addHistogramWithAnnotations(delta, correction, emitAnnotation) {
				return nil, nil
			}
		}
	}

	if isRate {
		delta.Div(rangeSeconds)
	}
	delta.CounterResetHint = histogram.GaugeType
	return delta.Compact(0), nil
}
