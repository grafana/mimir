// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type undoAction int

const (
	none undoAction = iota
	remove
	replace
)

// AnchoredExtensionMetadata tracks the first and last points considered in a range extension.
// It also tracks modifications made to the buffer, such as synthetic boundary points and head/tail trimming.
// A function is provided to undo these changes on a given buffer.
type AnchoredExtensionMetadata struct {
	// The original first point in the set of points considered for the anchored extension. This may not be the first point in the overall buffer.
	first promql.FPoint
	// The original last point in the set of points considered for the anchored extension. This may not be the last point in the overall buffer.
	last promql.FPoint
	// If the buffer has a trailing point which is outside consideration for the anchored extension it is stored here so it can be restored.
	excludedLast promql.FPoint
	// Flag indicating that the point in excludedLast needs to be re-inserted to the fail of the buffer
	restoreExcludedLast bool
	// If the buffer has leading point which is outside consideration for the anchored extension it is stored here so it can be restored.
	// This can occur when there is a single point in the extended look-back. The point can not be used since it is not after the rangeStart,
	// but we can not delete it from the buffer since it may be usable in the next step when additional points are pulled in.
	excludedFirst        promql.FPoint
	restoreExcludedFirst bool
	// The actions required to undo synthetic head and tail modifications
	undoTailModifications undoAction
	undoHeadModifications undoAction
}

// UndoChanges will restore the buffer to its original points.
func (m *AnchoredExtensionMetadata) UndoChanges(buff *types.FPointRingBuffer) error {

	switch m.undoTailModifications {
	case none:
		// nothing to be done
	case remove:
		buff.RemoveLast()
	case replace:
		if err := buff.ReplaceTail(m.last); err != nil {
			return err
		}
	}

	switch m.undoHeadModifications {
	case none:
		// nothing to be done
	case remove:
		buff.RemoveFirst()
	case replace:
		if err := buff.ReplaceHead(m.first); err != nil {
			return err
		}
	}

	if m.restoreExcludedLast {
		if err := buff.Append(m.excludedLast); err != nil {
			return err
		}
	}

	if m.restoreExcludedFirst {
		if err := buff.InsertHeadPoint(m.excludedFirst); err != nil {
			return err
		}
	}
	return nil
}

type SmoothedPoints struct {
	smoothedHeadSet bool
	smoothedTailSet bool
	smoothedHead    promql.FPoint
	smoothedTail    promql.FPoint
}

// ApplyRangeAnchoring modifies the given buffer to both trim it to the desired range and to
// anchor points at the start and end of the specified range.
//
// A synthetic (anchored) point is placed at both rangeStart and rangeEnd (if points do not already exist on these boundaries):
//
//   - Start value: taken from the last point with T <= rangeStart,
//     or, if none exists, the first point with T > rangeStart.
//   - End value:   taken from the first point with T >= rangeEnd,
//     or, if none exists, the last point with T < rangeEnd.
//
// The given buffer has had some preparation.
// The given buffer may be empty.
// The buffer may have 0 or more points before the rangeStart, but it is assumed these are all after the extended range start.
// The buffer may have 0 or 1 point after the rangeEnd. It is possible for this point to be after the extended range end.
//
// The given buffer is updated to discard un-necessary leading or trailing points, and to ensure the anchored points are set on the boundary.
//
// Because this buffer may be re-used between steps, details of the modifications are returned which allows
// a caller to undo these modifications.
//
// This implementation is based on extendFloats() from promql/engine.go.
func ApplyRangeAnchoring(buff *types.FPointRingBuffer, rangeStart, rangeEnd, extendedRangeEnd int64) (AnchoredExtensionMetadata, error) {
	e := AnchoredExtensionMetadata{}

	if buff.Count() == 0 {
		return e, nil
	}

	// Left trim the float buffer to ensure we have at most 1 point <= rangeStart
	// Note that we do delete the point from the buffer, we instead record the modification
	// so it can be undone for the next step.
	for buff.Count() > 1 && buff.PointAt(1).T <= rangeStart {
		e.excludedFirst = buff.PointAt(0)
		e.restoreExcludedFirst = true
		buff.RemoveFirst()
	}

	e.first = buff.PointAt(0)
	e.last = buff.Last()

	// Discard any trailing point which is outside the extended range end.
	// Note that we only expect at most 1 point to be found here.
	if e.last.T > extendedRangeEnd {
		e.excludedLast = e.last
		e.restoreExcludedLast = true
		buff.RemoveLast()
		if buff.Count() == 0 {
			return e, nil
		}
		e.last = buff.Last()
	}

	// A single point can be within the extended look-back, but we discard it because
	// we need at least one point within the original range to proceed.
	// Note that this may replace a point already stored in excludedFirst.
	// This is ok, as we only require one point before the rangeStart and the
	// next step will have a rangeStart greater than this rangeStart.
	if buff.Count() == 1 && e.first.T <= rangeStart {
		e.excludedFirst = e.first
		e.restoreExcludedFirst = true
		buff.RemoveFirst()
		return e, nil
	}

	// Add synthetic or clamp start boundary
	if e.first.T > rangeStart {
		if err := buff.InsertHeadPoint(promql.FPoint{T: rangeStart, F: e.first.F}); err != nil {
			return e, err
		}
		e.undoHeadModifications = remove

	} else if e.first.T < rangeStart {
		if err := buff.ReplaceHead(promql.FPoint{T: rangeStart, F: e.first.F}); err != nil {
			return e, err
		}
		e.undoHeadModifications = replace
	}

	// Add synthetic or clamp end boundary
	if e.last.T < rangeEnd {
		if err := buff.Append(promql.FPoint{T: rangeEnd, F: e.last.F}); err != nil {
			return e, err
		}
		e.undoTailModifications = remove

	} else if e.last.T > rangeEnd {
		if err := buff.ReplaceTail(promql.FPoint{T: rangeEnd, F: e.last.F}); err != nil {
			return e, err
		}
		e.undoTailModifications = replace
	}

	return e, nil
}

// ConvertExtendedPointsToSmoothed modifies a buffer to adjust boundary points to be smoothed.
//
// This given buffer should have already been passed through ApplyRangeAnchoring. The given
// anchoredMetadata should be the result of calling ApplyRangeAnchoring.
//
// Synthetic points on the boundaries will be re-calculated using interpolation:
//
//   - Start value: interpolated between the last point with T < rangeStart and
//     the first point with T > rangeStart, or taken directly from
//     the first point with T >= rangeStart.
//   - End value:   interpolated between the last point with T < rangeEnd and
//     the first point with T >= rangeEnd, or taken directly from
//     the last point with T <= rangeEnd.
//
// When an interpolated boundary point is used, an additional interpolated
// value—applying counter-reset compensation—is written into the provided
// smoothedHead or smoothedTail. If the returned range is later used by
// rate() or increase(), these alternate compensated boundary points must be
// substituted for the default smoothed ones.
//
// The modified buffer includes all points within the specified range, along
// with the added (interpolated or direct) boundary points.
//
// This implementation is based on extendFloats() from promql/engine.go.
func ConvertExtendedPointsToSmoothed(anchoredMetadata AnchoredExtensionMetadata, buff *types.FPointRingBuffer, rangeStart, rangeEnd int64, calculateCounterAdjustedPoints bool) (SmoothedPoints, error) {

	smoothedPoints := SmoothedPoints{}

	// Smoothing has 2 special cases.
	// Firstly, the values on the boundaries are replaced with an interpolated values - thereby smoothing the value to reflect the values of the points before/after the boundary
	//
	// Secondly, to ensure rate/increase return the correct values, we need to calculate and store alternate points in addition to the interpolated points.
	// These alternate points ensure that rate/increase don't incorrectly detect counter resets at the beginning and end of the range, and will be stored
	// alongside the resulting vector so that the rate/increase function handler can utilise these values.
	//
	// This is done regardless of this selector being wrapped in rate/increase.

	var interpolatedBoundaryValue float64
	if anchoredMetadata.first.T < rangeStart {
		pointAfterRangeStart := buff.PointAt(1)

		if calculateCounterAdjustedPoints {
			interpolatedBoundaryValue, smoothedPoints.smoothedHead.F = interpolateCombined(anchoredMetadata.first, pointAfterRangeStart, rangeStart, true)
			smoothedPoints.smoothedHead.T = rangeStart
			smoothedPoints.smoothedHeadSet = true
		} else {
			interpolatedBoundaryValue = interpolate(anchoredMetadata.first, pointAfterRangeStart, rangeStart)
		}

		if err := buff.ReplaceValueAtPos(0, interpolatedBoundaryValue); err != nil {
			return smoothedPoints, err
		}
	}

	if anchoredMetadata.last.T > rangeEnd {
		pointBeforeRangeEnd := buff.PointAt(buff.Count() - 2)

		if calculateCounterAdjustedPoints {
			interpolatedBoundaryValue, smoothedPoints.smoothedTail.F = interpolateCombined(pointBeforeRangeEnd, anchoredMetadata.last, rangeEnd, false)
			smoothedPoints.smoothedTail.T = rangeEnd
			smoothedPoints.smoothedTailSet = true
		} else {
			interpolatedBoundaryValue = interpolate(pointBeforeRangeEnd, anchoredMetadata.last, rangeEnd)
		}
		if err := buff.ReplaceValueAtPos(buff.Count()-1, interpolatedBoundaryValue); err != nil {
			return smoothedPoints, err
		}
	}

	return smoothedPoints, nil
}

// interpolateCombined performs linear interpolation between two points.
// 2 floats are returned. The first is treating the points as not being counters,
// and the second assumes the points are counters and adjusts for a counter reset.
// This has been adapted from interpolate() in promql/functions.go
func interpolateCombined(p1, p2 promql.FPoint, t int64, leftEdge bool) (float64, float64) {
	y1 := p1.F
	y2 := p2.F

	notCounter := y1 + (y2-y1)*float64(t-p1.T)/float64(p2.T-p1.T)
	asCounter := notCounter

	if y2 < y1 {
		if leftEdge {
			y1 = 0
		} else {
			y2 += y1
		}
		asCounter = y1 + (y2-y1)*float64(t-p1.T)/float64(p2.T-p1.T)
	}

	return notCounter, asCounter
}

func interpolate(p1, p2 promql.FPoint, t int64) float64 {
	y1 := p1.F
	y2 := p2.F

	return y1 + (y2-y1)*float64(t-p1.T)/float64(p2.T-p1.T)
}
