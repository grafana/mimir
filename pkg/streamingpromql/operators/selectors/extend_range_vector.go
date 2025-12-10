// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type SmoothedPoints struct {
	points          []promql.FPoint
	smoothedHeadSet bool
	smoothedTailSet bool
	smoothedHead    promql.FPoint
	smoothedTail    promql.FPoint
}

type ExtendedPoints struct {
	points []promql.FPoint
	first  promql.FPoint
	last   promql.FPoint
}

// NewExtendedPointsForAnchored returns a slice of points adjusted to include
// anchored boundary points at the start and end of the specified range.
//
// A synthetic point is placed at both rangeStart and rangeEnd:
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
// The returned slice includes all points within the range, along with the added/modified boundary points.
//
// This implementation is based on extendFloats() from promql/engine.go.
func NewExtendedPointsForAnchored(view *types.FPointRingBufferView, rangeStart, rangeEnd int64, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (ExtendedPoints, error) {
	head, tail := view.UnsafePoints()
	count := len(head) + len(tail)

	// We need a new buffer to store the extended points
	// The caller is responsible for releasing this slice back to the slices pool
	buff, err := types.FPointSlicePool.Get(count+2, memoryConsumptionTracker)
	if err != nil {
		return ExtendedPoints{}, err
	}

	e := ExtendedPoints{}
	e.first = head[0]

	// Add synthetic or clamp start boundary
	if e.first.T == rangeStart {
		buff = append(buff, e.first)
	} else if e.first.T > rangeStart {
		buff = append(buff, promql.FPoint{T: rangeStart, F: e.first.F})
		buff = append(buff, e.first)
	} else {
		buff = append(buff, promql.FPoint{T: rangeStart, F: e.first.F})
	}

	buff = append(buff, head[1:]...)
	buff = append(buff, tail...)

	// Add synthetic or clamp end boundary
	lastIdx := len(buff) - 1
	e.last = buff[lastIdx]
	if e.last.T < rangeEnd {
		buff = append(buff, promql.FPoint{T: rangeEnd, F: e.last.F})
	} else if e.last.T > rangeEnd {
		buff[lastIdx].T = rangeEnd
	}

	e.points = buff

	return e, nil
}

// NewExtendedPointsForSmoothed returns a slice of points adjusted to include
// smoothed boundary points at the start and end of the specified range.
//
// As with NewExtendedPointsForAnchored, synthetic points are placed at both
// rangeStart and rangeEnd. However, the boundary values are determined using
// interpolation:
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
// The returned slice includes all points within the specified range, along
// with the added (interpolated or direct) boundary points.
//
// This implementation is based on extendFloats() from promql/engine.go.
func NewExtendedPointsForSmoothed(view *types.FPointRingBufferView, rangeStart, rangeEnd int64, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) (SmoothedPoints, error) {
	extendedPoints, err := NewExtendedPointsForAnchored(view, rangeStart, rangeEnd, memoryConsumptionTracker)
	if err != nil {
		return SmoothedPoints{}, err
	}

	smoothedPoints := SmoothedPoints{
		points: extendedPoints.points,
	}

	// Smoothing has 2 special cases.
	// Firstly, the values on the boundaries are replaced with an interpolated values - thereby smoothing the value to reflect the values of the points before/after the boundary
	//
	// Secondly, to ensure rate/increase return the correct values, we need to calculate and store alternate points in addition to the interpolated points.
	// These alternate points ensure that rate/increase don't incorrectly detect counter resets at the beginning and end of the range, and will be stored
	// alongside the resulting vector so that the rate/increase function handler can utilise these values.
	//
	// This is done regardless of this selector being wrapped in rate/increase.
	if len(extendedPoints.points) > 1 {
		if extendedPoints.first.T < rangeStart {
			extendedPoints.points[0].F, smoothedPoints.smoothedHead.F = interpolateCombined(extendedPoints.first, extendedPoints.points[1], rangeStart, true)
			smoothedPoints.smoothedHead.T = rangeStart
			smoothedPoints.smoothedHeadSet = true
		}

		if extendedPoints.last.T > rangeEnd {
			extendedPoints.points[len(extendedPoints.points)-1].F, smoothedPoints.smoothedTail.F = interpolateCombined(extendedPoints.points[len(extendedPoints.points)-2], extendedPoints.last, rangeEnd, false)
			smoothedPoints.smoothedTail.T = rangeEnd
			smoothedPoints.smoothedTailSet = true
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
