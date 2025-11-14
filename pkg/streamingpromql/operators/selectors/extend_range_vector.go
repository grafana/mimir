// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package selectors

import (
	"fmt"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/limiter"
)

type fPointRingBufferViewIterator struct {
	idx  int
	view *types.FPointRingBufferView
}

func (i *fPointRingBufferViewIterator) hasNext() bool {
	return i.idx < i.view.Count()
}

// next moves the iterator forward, returning the next point.
// This function will panic if moving the iterator would result in an index out of bounds.
func (i *fPointRingBufferViewIterator) next() promql.FPoint {
	if i.idx >= i.view.Count() {
		panic(fmt.Sprintf("next(): out of range, requested index %v but have length %v", i.idx, i.view.Count()))
	}
	p := i.view.PointAt(i.idx)
	i.idx++
	return p
}

func (i *fPointRingBufferViewIterator) at() promql.FPoint {
	return i.view.PointAt(i.idx)
}

// next moves the iterator backwards, returning the previous point.
// This function will panic if moving the iterator would result in an index out of bounds.
func (i *fPointRingBufferViewIterator) prev() promql.FPoint {
	if i.idx <= 0 {
		panic(fmt.Sprintf("prev(): out of range, requested index %v", i.idx-1))
	}
	i.idx--
	return i.view.PointAt(i.idx)
}

func (i *fPointRingBufferViewIterator) advance() {
	if i.idx < i.view.Count() {
		i.idx++
	}
}

func (i *fPointRingBufferViewIterator) reverse() {
	if i.idx > 0 {
		i.idx--
	}
}

// peek returns the next point, but does not move the iterator forward.
// This function will panic if this look ahead would result in an index out of bounds.
func (i *fPointRingBufferViewIterator) peek() promql.FPoint {
	if i.idx >= i.view.Count() {
		panic(fmt.Sprintf("peek(): out of range, requested index %v but have length %v", i.idx, i.view.Count()))
	}
	return i.view.PointAt(i.idx)
}

// seek will return the point which is closest to being <= time, or the first point after this time.
// The iterator will be positioned to the first point > time.
func (i *fPointRingBufferViewIterator) seek(time int64) promql.FPoint {
	var first *promql.FPoint

	for i.hasNext() {
		next := i.peek()

		if next.T < time {
			first = &next
			i.advance()
			continue
		}

		if next.T == time {
			i.advance()
			return next
		}

		if first != nil {
			return *first
		}

		return next
	}

	return *first
}

// copyRemainingPointsTo will accumulate all points <= time into the given buff.
// The iterator will be positioned at the first point which is >= time.
// If there is no point >= time, then the iterator is positioned at the last point < time.
func (i *fPointRingBufferViewIterator) copyRemainingPointsTo(time int64, buff []promql.FPoint) []promql.FPoint {
	for i.hasNext() {
		next := i.next()

		if next.T <= time {
			buff = append(buff, next)

			if next.T == time {
				// move the iterator back so that the the at() call will return this 'next' point
				i.reverse()
				return buff
			}

		} else {
			// This is the first point to be >= rangeEnd
			break
		}
	}

	// move the iterator back so that the the at() call will return this last point which caused our loop to exit
	i.reverse()
	return buff
}

// extendRangeVectorPoints will return a slice of points which has been adjusted to have anchored/smoothed points on the bounds of the given range.
// This is used with the anchored/smoothed range query modifiers.
// This implementation is based on extendFloats() found in promql/engine.go
func extendRangeVectorPoints(view *types.FPointRingBufferView, rangeStart, rangeEnd int64, smoothed bool, memoryConsumptionTracker *limiter.MemoryConsumptionTracker) ([]promql.FPoint, *promql.FPoint, *promql.FPoint, error) {

	// no points to consider!
	if !view.Any() {
		return nil, nil, nil, nil
	}

	// ignore ok as we already tested that we have points
	lastInView, _ := view.Last()

	// No points were found within the original range.
	// If we only find points prior to the start of the original range then no points are returned.
	if lastInView.T <= rangeStart {
		return nil, nil, nil, nil
	}

	// We need a new buffer to store the extended points
	// The caller is responsible for releasing this slice back to the slices pool
	buff, err := types.FPointSlicePool.Get(view.Count()+2, memoryConsumptionTracker)
	if err != nil {
		return nil, nil, nil, err
	}

	it := fPointRingBufferViewIterator{view: view}

	// Find the last point before the rangeStart, or the first point >= rangeStart
	first := it.seek(rangeStart)

	// Use this first value as the range boundary value
	buff = append(buff, promql.FPoint{T: rangeStart, F: first.F})

	// Accumulate the points <= rangeEnd into the buffer.
	// Note - if the first.T > rangeStart, it will also be accumulated into buff as the 2nd point in the buffer.
	buff = it.copyRemainingPointsTo(rangeEnd, buff)
	last := it.at()

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
