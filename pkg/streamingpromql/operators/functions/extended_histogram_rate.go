// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/functions.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package functions

import (
	"sort"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/util/annotations"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// interpolateHistograms performs linear interpolation between two histogram points h1 (at t1)
// and h2 (at t2) and returns the histogram interpolated at time t. It delegates to
// types.InterpolateHistograms, translating the reconciliation operation into the matching
// annotation generator. The caller guarantees schema consistency via validateHistogramRange, so
// the schema check inside InterpolateHistograms is a no-op for this path.
func interpolateHistograms(h1 *histogram.FloatHistogram, t1 int64, h2 *histogram.FloatHistogram, t2, t int64, isCounter bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	return types.InterpolateHistograms(h1, t1, h2, t2, t, isCounter, func(op annotations.HistogramOperation) {
		switch op {
		case annotations.HistogramSub:
			emitAnnotation(NewSubMismatchedCustomBucketsHistogramInfo)
		case annotations.HistogramAdd:
			emitAnnotation(NewAddMismatchedCustomBucketsHistogramInfo)
		}
	})
}

// pickOrInterpolateLeftHistogram returns the histogram at the left boundary of the range.
// If interpolation is needed (smoothed and the first sample is before rangeStart), it returns
// the interpolated histogram at rangeStart; otherwise it returns a copy of the first sample's
// histogram.
func pickOrInterpolateLeftHistogram(view *types.HPointRingBufferView, first int, rangeStart int64, smoothed, isCounter bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	firstPoint := view.PointAt(first)
	if smoothed && firstPoint.T < rangeStart {
		next := view.PointAt(first + 1)
		return interpolateHistograms(firstPoint.H, firstPoint.T, next.H, next.T, rangeStart, isCounter, emitAnnotation)
	}
	return firstPoint.H.Copy(), nil
}

// pickOrInterpolateRightHistogram returns the histogram at the right boundary of the range.
// If interpolation is needed (smoothed and the last sample is after rangeEnd), it returns the
// interpolated histogram at rangeEnd; otherwise it returns a copy of the last sample's histogram.
func pickOrInterpolateRightHistogram(view *types.HPointRingBufferView, last int, rangeEnd int64, smoothed, isCounter bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	lastPoint := view.PointAt(last)
	if smoothed && last > 0 && lastPoint.T > rangeEnd {
		prev := view.PointAt(last - 1)
		return interpolateHistograms(prev.H, prev.T, lastPoint.H, lastPoint.T, rangeEnd, isCounter, emitAnnotation)
	}
	return lastPoint.H.Copy(), nil
}

// addHistogramWithAnnotations adds other into base in place, translating bucket-bounds
// reconciliations into info annotations. It returns ok=false when the addition failed: err is nil
// when the failure was converted into an annotation (incompatible schemas), and non-nil when the
// error is unexpected and must be surfaced by the caller.
func addHistogramWithAnnotations(base, other *histogram.FloatHistogram, emitAnnotation types.EmitAnnotationFunc) (bool, error) {
	_, _, nhcbBoundsReconciled, err := base.Add(other)
	if err != nil {
		return false, NativeHistogramErrorToAnnotation(err, emitAnnotation)
	}
	if nhcbBoundsReconciled {
		emitAnnotation(NewAddMismatchedCustomBucketsHistogramInfo)
	}
	return true, nil
}

// subHistogramWithAnnotations subtracts other from base in place, translating bucket-bounds
// reconciliations into info annotations. It returns ok=false when the subtraction failed: err is
// nil when the failure was converted into an annotation (incompatible schemas), and non-nil when
// the error is unexpected and must be surfaced by the caller.
func subHistogramWithAnnotations(base, other *histogram.FloatHistogram, emitAnnotation types.EmitAnnotationFunc) (bool, error) {
	_, _, nhcbBoundsReconciled, err := base.Sub(other)
	if err != nil {
		return false, NativeHistogramErrorToAnnotation(err, emitAnnotation)
	}
	if nhcbBoundsReconciled {
		emitAnnotation(NewSubMismatchedCustomBucketsHistogramInfo)
	}
	return true, nil
}

// validateHistogramRange checks the histogram samples in view between first and last (inclusive)
// for schema consistency and counter-type hints. It returns false (and emits
// MixedExponentialCustomHistogramsWarning) when exponential and custom buckets are mixed. It emits
// NativeHistogramNotCounterWarning for any sample carrying a gauge hint while isCounter is true.
func validateHistogramRange(view *types.HPointRingBufferView, first, last int, isCounter bool, emitAnnotation types.EmitAnnotationFunc) bool {
	usingCustomBuckets := view.PointAt(first).H.UsesCustomBuckets()
	for i := first; i <= last; i++ {
		p := view.PointAt(i)
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
// firstSampleIndex and lastSampleIndex in view, using left and right as the boundary values. It
// mirrors correctForCounterResets for float samples. Returns the accumulated correction
// (nil if none) and false if combining histograms failed.
func correctForCounterResetsHistogram(view *types.HPointRingBufferView, firstSampleIndex, lastSampleIndex int, left, right *histogram.FloatHistogram, rangeStart int64, smoothed bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, bool, error) {
	// firstSampleIndex is represented by left, so the loop starts one beyond.
	firstToCheck := firstSampleIndex + 1
	prev := left
	if firstPoint := view.PointAt(firstSampleIndex); smoothed && firstPoint.T < rangeStart {
		next := view.PointAt(firstSampleIndex + 1)
		if next.H.DetectReset(firstPoint.H) {
			// There is a reset somewhere between the point just outside the range
			// (view.PointAt(firstSampleIndex)) and the point just inside it
			// (view.PointAt(firstSampleIndex+1)). That reset is already accounted for by the left
			// interpolation, so skip view.PointAt(firstSampleIndex+1) from the loop and use it as
			// the comparison anchor for any reset that immediately follows.
			prev = next.H
			firstToCheck++
		}
	}
	// lastSampleIndex is always excluded: right is either a direct copy of
	// view.PointAt(lastSampleIndex) or an interpolation that inherits its CounterResetHint.
	// Including view.PointAt(lastSampleIndex) in the loop would make right.DetectReset self-detect
	// on the same hint. The final right.DetectReset(prev) below handles the right-boundary reset
	// safely once view.PointAt(lastSampleIndex) is not prev.
	lastToCheck := lastSampleIndex - 1

	// firstToCheck > lastToCheck+1 when there is nothing between the two boundary samples to check.
	// This happens when firstSampleIndex == lastSampleIndex (single-sample window), or when the
	// smoothed left interpolation already spanned the only reset interval
	// (lastSampleIndex == firstSampleIndex+1 and firstToCheck was incremented above). Both
	// boundaries were interpolated from the same reset segment, so there is nothing more to
	// correct.
	if firstToCheck > lastToCheck+1 {
		return nil, true, nil
	}

	var correction *histogram.FloatHistogram

	addCorrection := func(h *histogram.FloatHistogram) (bool, error) {
		if correction == nil {
			correction = h.Copy()
			return true, nil
		}
		return addHistogramWithAnnotations(correction, h, emitAnnotation)
	}

	for i := firstToCheck; i <= lastToCheck; i++ {
		p := view.PointAt(i)
		if p.H.DetectReset(prev) {
			if ok, err := addCorrection(prev); !ok {
				return nil, false, err
			}
		}
		prev = p.H
	}
	if right.DetectReset(prev) {
		if ok, err := addCorrection(prev); !ok {
			return nil, false, err
		}
	}
	return correction, true, nil
}

// extendedHistogramRate computes rate/increase/delta for histograms under the anchored or
// smoothed modifier. It mirrors extendedHistogramRate in upstream Prometheus: it picks (or
// interpolates) histogram values at the range boundaries, subtracts the left value from the
// right value, accumulates any counter-reset correction across the interior samples (when
// isCounter is true), and divides by the range duration when isRate is true.
//
// The view contains the histogram samples in the extended look-back/look-ahead window provided by
// the range vector selector. originalRangeStart and originalRangeEnd delimit the original
// (non-extended) window the user requested.
func extendedHistogramRate(view *types.HPointRingBufferView, originalRangeStart, originalRangeEnd int64, rangeSeconds float64, isCounter, isRate, smoothed bool, emitAnnotation types.EmitAnnotationFunc) (*histogram.FloatHistogram, error) {
	count := view.Count()
	if count == 0 {
		return nil, nil
	}
	lastSampleIndex := count - 1
	// firstSampleIndex is the last sample at or before originalRangeStart (the left boundary
	// neighbour). This is not necessarily index 0 or 1: the selector extends the look-back by the
	// full LookbackDelta (see rangeStart adjustment in range_vector_selector.go), so the view
	// retains every sample within LookbackDelta before originalRangeStart.
	// Note that this is different to the handling of floats in range_vector_selector where the view
	// used by the functions is already trimmed to the boundary points.
	// A binary search matches upstream Prometheus's extendedHistogramRate.
	firstSampleIndex := sort.Search(lastSampleIndex, func(i int) bool { return view.PointAt(i).T > originalRangeStart }) - 1
	if firstSampleIndex < 0 {
		firstSampleIndex = 0
	}
	if smoothed {
		// Smoothed extends the look-ahead by the full LookbackDelta (see rangeEnd adjustment in
		// range_vector_selector.go), so the view commonly holds several samples past
		// originalRangeEnd. lastSampleIndex is set to the first sample at or after originalRangeEnd
		// (the right boundary neighbour used to interpolate at originalRangeEnd); a binary search
		// keeps this O(log n), matching upstream Prometheus's extendedHistogramRate.
		lastSampleIndex = sort.Search(lastSampleIndex, func(i int) bool { return view.PointAt(i).T >= originalRangeEnd })
	} else if view.PointAt(lastSampleIndex).T > originalRangeEnd {
		// fillBuffer always appends the first sample >= rangeEnd as the right-neighbour anchor,
		// but the anchored modifier (unlike smoothed) does not interpolate at rangeEnd, so the
		// trailing post-rangeEnd sample must not be used as the right boundary.
		if lastSampleIndex == 0 {
			return nil, nil
		}
		lastSampleIndex--
	}

	if view.PointAt(lastSampleIndex).T <= originalRangeStart {
		return nil, nil
	}
	if smoothed && view.PointAt(firstSampleIndex).T > originalRangeEnd {
		return nil, nil
	}

	if !validateHistogramRange(view, firstSampleIndex, lastSampleIndex, isCounter, emitAnnotation) {
		return nil, nil
	}

	left, err := pickOrInterpolateLeftHistogram(view, firstSampleIndex, originalRangeStart, smoothed, isCounter, emitAnnotation)
	if err != nil {
		return nil, NativeHistogramErrorToAnnotation(err, emitAnnotation)
	}
	right, err := pickOrInterpolateRightHistogram(view, lastSampleIndex, originalRangeEnd, smoothed, isCounter, emitAnnotation)
	if err != nil {
		return nil, NativeHistogramErrorToAnnotation(err, emitAnnotation)
	}

	if !isCounter && (left.CounterResetHint != histogram.GaugeType || right.CounterResetHint != histogram.GaugeType) {
		emitAnnotation(annotations.NewNativeHistogramNotGaugeWarning)
	}

	// Accumulate the counter-reset correction before subtracting left from right, because it
	// needs right.DetectReset against the original boundary value rather than (right - left).
	// Doing it first lets us subtract into right in place (it is always a freshly-owned copy
	// from pickOrInterpolateRightHistogram) and avoid an extra Copy.
	var correction *histogram.FloatHistogram
	if isCounter {
		var ok bool
		correction, ok, err = correctForCounterResetsHistogram(view, firstSampleIndex, lastSampleIndex, left, right, originalRangeStart, smoothed, emitAnnotation)
		if !ok {
			return nil, err
		}
	}

	delta := right
	if ok, err := subHistogramWithAnnotations(delta, left, emitAnnotation); !ok {
		return nil, err
	}
	if correction != nil {
		if ok, err := addHistogramWithAnnotations(delta, correction, emitAnnotation); !ok {
			return nil, err
		}
	}

	if isRate {
		delta.Div(rangeSeconds)
	}
	delta.CounterResetHint = histogram.GaugeType
	return delta.Compact(0), nil
}
