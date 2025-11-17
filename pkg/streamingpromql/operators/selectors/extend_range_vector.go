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
func extendRangeVectorPoints(it *types.FPointRingBufferViewIterator, rangeStart, rangeEnd int64, smoothed bool, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]promql.FPoint, *promql.FPoint, *promql.FPoint, error) {

	// We need a new buffer to store the extended points
	// The caller is responsible for releasing this slice back to the slices pool
	buff, err := types.FPointSlicePool.Get(it.Count()+2, memoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, err
	}
	
	// Find the last point before the rangeStart, or the first point >= rangeStart
	first := it.Seek(rangeStart)

	// Use this first value as the range boundary value
	buff = append(buff, promql.FPoint{T: rangeStart, F: first.F})

	// Accumulate the points <= rangeEnd into the buffer.
	// Note - if the first.T > rangeStart, it will also be accumulated into buff as the 2nd point in the buffer.
	buff = it.CopyRemainingPointsTo(rangeEnd, buff)
	last := it.At()

	if last.T != rangeEnd {
		// Use the last point >= rangeEnd, or the point immediately preceding as the value for the end boundary
		buff = append(buff, promql.FPoint{T: rangeEnd, F: last.F})
	}

	// Smoothing has 2 special cases.
	// Firstly, the values on the boundaries are replaced with an interpolated values - there by smoothing the value to reflect the time of the point before/after the boundary
	// Secondly, if vector will be used in a rate/increase function then the boundary points must be calculated differently to consider the value as a counter.
	// These alternate points will be stored alongside the resulting vector so that the rate/increase function handler can utilise these values.
	var smoothedHead *promql.FPoint
	var smoothedTail *promql.FPoint
	if smoothed && len(buff) > 1 {
		if first.T < rangeStart {
			buff[0].F = interpolate(first, buff[1], rangeStart, false, true)
			smoothedHead = &promql.FPoint{T: rangeStart, F: interpolate(first, buff[1], rangeStart, true, true)}
		}

		if last.T > rangeEnd {
			buff[len(buff)-1].F = interpolate(buff[len(buff)-2], last, rangeEnd, false, false)
			smoothedTail = &promql.FPoint{T: rangeEnd, F: interpolate(buff[len(buff)-2], last, rangeEnd, true, false)}
		}
	}

	return buff, smoothedHead, smoothedTail, nil
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
