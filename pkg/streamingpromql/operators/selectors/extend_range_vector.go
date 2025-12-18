// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"slices"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type undoAction int

const (
	none undoAction = iota
	remove
	replace
)

// AnchoredExtensionMetadata tracks the original first and last points considered in a range extension.
// It also tracks tail modifications (points added or replaced), so that these can be undone to restore
// a given buffer to its original (non-synthetic) points.
type AnchoredExtensionMetadata struct {
	first promql.FPoint
	last  promql.FPoint

	// action we need to take on the tail to undo modifications
	undoTailModifications undoAction
}

// UndoSyntheticPoints will undo a tail synthetic point and restore the buffer to original points.
// Note that head modifications are not undone in this function.
func (m *AnchoredExtensionMetadata) UndoSyntheticPoints(buff *types.FPointRingBuffer) error {

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
	return nil
}

type SmoothedPoints struct {
	smoothedHeadSet bool
	smoothedTailSet bool
	smoothedHead    promql.FPoint
	smoothedTail    promql.FPoint
}

// NewExtendedPointsForAnchored prepares a buffer with points adjusted to include
// anchored boundary points at the start and end of the specified range.
//
// A synthetic point is placed at both rangeStart and rangeEnd (if points do not already exist on these boundaries):
//
//   - Start value: taken from the last point with T <= rangeStart,
//     or, if none exists, the first point with T > rangeStart.
//   - End value:   taken from the first point with T >= rangeEnd,
//     or, if none exists, the last point with T < rangeEnd.
//
// The provided view has already prepared the input range to include a point before the rangeStart,
// and a point after the rangeEnd (smoothed only).
//
// Note that these points outside the original range are only provided if there is no existing point
// already on the range boundary and the points are within a defined lookback/lookahead window.
//
// The given buffer is updated to include all points within the range, along with the added/modified boundary points.
//
// Note that it is assumed that the given buff has already been pruned to remove all points less than or equal to the rangeStart.
//
// Because this buffer may be re-used between steps, details of the synthetic modifications are returned which allows
// a caller to undo the synthetic modifications.
//
// Note that synthetic head modifications are not tracked since they will be discarded in the next step iteration as the buffer
// will be trimmed to remove points before the new steps rangeStart.
//
// This implementation is based on extendFloats() from promql/engine.go.
func NewExtendedPointsForAnchored(buff *types.FPointRingBuffer, view *types.FPointRingBufferView, rangeStart, rangeEnd int64) (AnchoredExtensionMetadata, error) {

	head, tail := view.UnsafePoints()

	e := AnchoredExtensionMetadata{}
	e.first = head[0]

	// Note - the given buff has already been pruned of samples with a T <= rangeStart.
	// We do not need to record modifications for synthetic head points, as these will be discarded in the next step
	// iteration since they will be before the next steps rangeStart.
	// Modifications for added or modified tail points are recorded.

	// Add synthetic or clamp start boundary
	if e.first.T == rangeStart {
		if err := buff.InsertHeadPoint(e.first); err != nil {
			return e, err
		}

	} else if e.first.T > rangeStart {
		// Note - we do these inserts in reverse order so the point with the T=rangeStart will be the head of the buffer.
		// This extra check is to ensure that this point does not already exist in the buffer
		if buff.Count() == 0 || buff.TimeAt(0) > e.first.T {
			if err := buff.InsertHeadPoint(e.first); err != nil {
				return e, err
			}
		}
		if err := buff.InsertHeadPoint(promql.FPoint{T: rangeStart, F: e.first.F}); err != nil {
			return e, err
		}

	} else {
		if err := buff.InsertHeadPoint(promql.FPoint{T: rangeStart, F: e.first.F}); err != nil {
			return e, err
		}
	}

	// scan past any given points which have a timestamp less than the last point in the buffer
	indexFunc := func(p promql.FPoint) bool { return p.T > buff.TimeAt(buff.Count()-1) }
	hIdx := slices.IndexFunc(head[1:], indexFunc)
	tIdx := slices.IndexFunc(tail, indexFunc)

	// Copy the remaining points into the buffer.
	if hIdx >= 0 {
		if err := buff.AppendSlice(head[hIdx+1:]); err != nil {
			return e, err
		}
	}
	if tIdx >= 0 {
		if err := buff.AppendSlice(tail[tIdx:]); err != nil {
			return e, err
		}
	}

	// We could use either the view.Last() or the last point in the buffer.
	// Even if the view contains only a single point and only point in the buffer is the synthetic head, that's fine.
	// In that case, e.last.T will be less than rangeEnd, and we only care about the last F value,
	// which is the same regardless of the point being the original or synthetic.
	e.last = buff.Last()

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
// This given buffer should have already been passed through NewExtendedPointsForAnchored. The given
// anchoredMetadata should be the result of calling NewExtendedPointsForAnchored.
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
func ConvertExtendedPointsToSmoothed(anchoredMetadata AnchoredExtensionMetadata, buff *types.FPointRingBuffer, rangeStart, rangeEnd int64) (SmoothedPoints, error) {

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
		interpolatedBoundaryValue, smoothedPoints.smoothedHead.F = interpolateCombined(anchoredMetadata.first, pointAfterRangeStart, rangeStart, true)
		smoothedPoints.smoothedHead.T = rangeStart
		smoothedPoints.smoothedHeadSet = true
		if err := buff.ReplaceValueAtPos(0, interpolatedBoundaryValue); err != nil {
			return smoothedPoints, err
		}
	}

	if anchoredMetadata.last.T > rangeEnd {
		pointBeforeRangeEnd := buff.PointAt(buff.Count() - 2)
		interpolatedBoundaryValue, smoothedPoints.smoothedTail.F = interpolateCombined(pointBeforeRangeEnd, anchoredMetadata.last, rangeEnd, false)
		smoothedPoints.smoothedTail.T = rangeEnd
		smoothedPoints.smoothedTailSet = true
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
