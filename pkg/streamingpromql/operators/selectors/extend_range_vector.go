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
func extendRangeVectorPoints(view *types.FPointRingBufferView, rangeStart, rangeEnd int64, smoothed bool, smoothedHead, smoothedTail *promql.FPoint, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]promql.FPoint, byte, error) {

	head, tail := view.UnsafePoints()
	count := len(head) + len(tail)

	// We need a new buffer to store the extended points
	// The caller is responsible for releasing this slice back to the slices pool
	buff, err := types.FPointSlicePool.Get(count+2, memoryConsumptionTracker)
	if err != nil {
		return nil, 0, err
	}

	first := head[0]

	// Add synthetic start boundary
	if first.T == rangeStart {
		buff = append(buff, first)
	} else {
		buff = append(buff, promql.FPoint{T: rangeStart, F: first.F})
		if first.T > rangeStart {
			buff = append(buff, first)
		}
	}

	buff = append(buff, head[1:]...)
	buff = append(buff, tail...)

	// Add or clamp final boundary
	last := buff[len(buff)-1]
	if last.T < rangeEnd {
		buff = append(buff, promql.FPoint{T: rangeEnd, F: last.F})
	} else if last.T > rangeEnd {
		buff[len(buff)-1].T = rangeEnd
	}

	// Smoothing has 2 special cases.
	// Firstly, the values on the boundaries are replaced with an interpolated values - there by smoothing the value to reflect the time of the point before/after the boundary
	// Secondly, if vector will be used in a rate/increase function then the boundary points must be calculated differently to consider the value as a counter.
	// These alternate points will be stored alongside the resulting vector so that the rate/increase function handler can utilise these values.
	smoothedPointSetMask := uint8(0)
	if smoothed && len(buff) > 1 {
		if first.T < rangeStart {
			buff[0].F = interpolate(first, buff[1], rangeStart, false, true)
			smoothedHead.T = rangeStart
			smoothedHead.F = interpolate(first, buff[1], rangeStart, true, true)
			smoothedPointSetMask |= 1 << 0
		}

		if last.T > rangeEnd {
			prev := buff[len(buff)-2]
			buff[len(buff)-1].F = interpolate(prev, last, rangeEnd, false, false)
			smoothedTail.T = rangeEnd
			smoothedTail.F = interpolate(prev, last, rangeEnd, true, false)
			smoothedPointSetMask |= 1 << 1
		}
	}

	return buff, smoothedPointSetMask, nil
}

// interpolate performs linear interpolation between two points.
// If isCounter is true and there is a counter reset:
// - on the left edge, it sets the value to 0.
// - on the right edge, it adds the left value to the right value.
// It then calculates the interpolated value at the given timestamp.
// This has been adapted from interpolate() in promql/functions.go
func interpolate(p1, p2 promql.FPoint, t int64, isCounter, leftEdge bool) float64 {
	y1 := p1.F
	y2 := p2.F

	if isCounter && y2 < y1 {
		if leftEdge {
			y1 = 0
		} else {
			y2 += y1
		}
	}

	return y1 + (y2-y1)*float64(t-p1.T)/float64(p2.T-p1.T)
}
