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

// extendRangeVectorPoints will return a slice of points which has been adjusted to have anchored/smoothed points on the bounds of the given range.
// This is used with the anchored/smoothed range query modifiers.
// This implementation is based on extendFloats() found in promql/engine.go
// The function also returns bool's indicating if alternate counter reset compensating interpolated head & tail points have been pre-calculated for the boundaries.
func extendRangeVectorPoints(view *types.FPointRingBufferView, rangeStart, rangeEnd int64, smoothed bool, smoothedHead, smoothedTail *promql.FPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]promql.FPoint, bool, bool, error) {

	head, tail := view.UnsafePoints()
	count := len(head) + len(tail)

	// We need a new buffer to store the extended points
	// The caller is responsible for releasing this slice back to the slices pool
	buff, err := types.FPointSlicePool.Get(count+2, memoryConsumptionTracker)
	if err != nil {
		return nil, false, false, err
	}

	first := head[0]

	// Add synthetic or clamp start boundary
	if first.T == rangeStart {
		buff = append(buff, first)
	} else if first.T > rangeStart {
		buff = append(buff, promql.FPoint{T: rangeStart, F: first.F})
		buff = append(buff, first)
	} else {
		buff = append(buff, promql.FPoint{T: rangeStart, F: first.F})
	}

	buff = append(buff, head[1:]...)
	buff = append(buff, tail...)

	// Add synthetic or clamp end boundary
	lastIdx := len(buff) - 1
	last := buff[lastIdx]
	if last.T < rangeEnd {
		buff = append(buff, promql.FPoint{T: rangeEnd, F: last.F})
	} else if last.T > rangeEnd {
		buff[lastIdx].T = rangeEnd
	}

	// Smoothing has 2 special cases.
	// Firstly, the values on the boundaries are replaced with an interpolated values - there by smoothing the value to reflect the time of the point before/after the boundary
	// Secondly, if vector will be used in a rate/increase function then the boundary points must be calculated differently to consider the value as a counter.
	// These alternate points will be stored alongside the resulting vector so that the rate/increase function handler can utilise these values.
	smoothedBasisForHeadPointSet := false
	smoothedBasisForTailPointSet := false

	if smoothed && len(buff) > 1 {
		if first.T < rangeStart {
			buff[0].F, smoothedHead.F = interpolateCombined(first, buff[1], rangeStart, true)
			smoothedHead.T = rangeStart
			smoothedBasisForHeadPointSet = true
		}

		if last.T > rangeEnd {
			buff[len(buff)-1].F, smoothedTail.F = interpolateCombined(buff[len(buff)-2], last, rangeEnd, false)
			smoothedTail.T = rangeEnd
			smoothedBasisForTailPointSet = true
		}
	}

	return buff, smoothedBasisForHeadPointSet, smoothedBasisForTailPointSet, nil
}

// interpolate performs linear interpolation between two points.
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
