// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
)

// FPointRingBuffer and HPointRingBuffer are nearly identical, but exist for each
// specific point type.
// The code is duplicated due to the cost of indirection in golang.
// The tests are combined to ensure consistency. We may wish to consider codegen
// or similar in the future if we end up with more data types etc.
// (see: https://github.com/grafana/mimir/pull/8508#discussion_r1654668995)

type HPointRingBuffer struct {
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
	points                   []promql.HPoint
	firstIndex               int // Index into 'points' of first point in this buffer.
	size                     int // Number of points in this buffer.
}

func NewHPointRingBuffer(memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *HPointRingBuffer {
	return &HPointRingBuffer{memoryConsumptionTracker: memoryConsumptionTracker}
}

// DiscardPointsBefore discards all points in this buffer with timestamp less than t.
func (b *HPointRingBuffer) DiscardPointsBefore(t int64) {
	for b.size > 0 && b.points[b.firstIndex].T < t {
		b.firstIndex++
		b.size--

		if b.firstIndex >= len(b.points) {
			b.firstIndex = 0
		}
	}

	if b.size == 0 {
		b.firstIndex = 0
	}
}

// UnsafePoints returns slices of the points in this buffer, including only points with timestamp less than or equal to maxT.
// Either or both slice could be empty.
// Callers must not modify the values in the returned slices or return them to a pool.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of this buffer.
// The returned slices, and the FloatHistogram instances in the returned slices, are no longer valid if this buffer is modified
// (eg. a point is added, or the buffer is reset or closed).
//
// FIXME: the fact we have to expose this is a bit gross, but the overhead of calling a function with ForEach is terrible.
// Perhaps we can use range-over function iterators (https://go.dev/wiki/RangefuncExperiment) once this is not experimental?
func (b *HPointRingBuffer) UnsafePoints(maxT int64) (head []promql.HPoint, tail []promql.HPoint) {
	size := b.size

	for size > 0 && b.points[(b.firstIndex+size-1)%len(b.points)].T > maxT {
		size--
	}

	endOfHeadSegment := b.firstIndex + size

	if endOfHeadSegment > len(b.points) {
		// Need to wrap around.
		endOfTailSegment := endOfHeadSegment % len(b.points)
		endOfHeadSegment = len(b.points)
		return b.points[b.firstIndex:endOfHeadSegment], b.points[0:endOfTailSegment]
	}

	return b.points[b.firstIndex:endOfHeadSegment], nil
}

// CopyPoints returns a single slice of the points in this buffer, including only points with timestamp less than or equal to maxT.
// Callers may modify the values in the returned slice, and should return the slice to the pool by calling
// PutHPointSlice when it is no longer needed.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of this buffer.
// In addition to copying the points, CopyPoints will also duplicate the FloatHistogram values
// so that they are also safe to modify and use after calling Close.
func (b *HPointRingBuffer) CopyPoints(maxT int64) ([]promql.HPoint, error) {
	if b.size == 0 {
		return nil, nil
	}

	head, tail := b.UnsafePoints(maxT)
	combined, err := getHPointSliceForRingBuffer(len(head)+len(tail), b.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	combine := func(p []promql.HPoint) {
		for i := range p {
			combined = append(combined,
				promql.HPoint{
					T: p[i].T,
					H: p[i].H.Copy(),
				},
			)
		}
	}

	combine(head)
	combine(tail)

	return combined, nil
}

// ForEach calls f for each point in this buffer.
func (b *HPointRingBuffer) ForEach(f func(p promql.HPoint)) {
	if b.size == 0 {
		return
	}

	lastIndexPlusOne := b.firstIndex + b.size

	if lastIndexPlusOne > len(b.points) {
		lastIndexPlusOne = len(b.points)
	}

	for i := b.firstIndex; i < lastIndexPlusOne; i++ {
		f(b.points[i])
	}

	if b.firstIndex+b.size < len(b.points) {
		// Don't need to wrap around to start of buffer.
		return
	}

	for i := 0; i < (b.firstIndex+b.size)%len(b.points); i++ {
		f(b.points[i])
	}
}

// Append adds p to this buffer, expanding it if required.
// If this buffer is non-empty, p.T must be greater than or equal to the
// timestamp of the last point in the buffer.
func (b *HPointRingBuffer) Append(p promql.HPoint) error {
	hPoint, err := b.NextPoint()
	if err != nil {
		return err
	}
	hPoint.T = p.T
	hPoint.H = p.H
	return nil
}

// NextPoint gets the next point in this buffer, expanding it if required.
// The returned point's timestamp (HPoint.T) must be set to greater than or equal
// to the timestamp of the last point in the buffer before further methods
// are called on this buffer (with the exception of RemoveLastPoint, Reset or Close).
//
// This method allows reusing an existing HPoint in this buffer where possible,
// reducing the number of FloatHistograms allocated.
func (b *HPointRingBuffer) NextPoint() (*promql.HPoint, error) {
	if b.size == len(b.points) {
		// Create a new slice, copy the elements from the current slice.
		newSize := b.size * 2
		if newSize == 0 {
			newSize = 2
		}

		newSlice, err := getHPointSliceForRingBuffer(newSize, b.memoryConsumptionTracker)
		if err != nil {
			return nil, err
		}

		newSlice = newSlice[:cap(newSlice)]
		pointsAtEnd := b.size - b.firstIndex
		copy(newSlice, b.points[b.firstIndex:])
		copy(newSlice[pointsAtEnd:], b.points[:b.firstIndex])

		// We must clear b.points before returning it to the pool, as the current query could continue using the
		// FloatHistogram instances it contains a reference to, but a later user of b.points may otherwise reuse
		// those instances instead of creating new FloatHistograms.
		clear(b.points)

		putHPointSliceForRingBuffer(b.points, b.memoryConsumptionTracker)
		b.points = newSlice
		b.firstIndex = 0
	}

	nextIndex := (b.firstIndex + b.size) % len(b.points)
	b.size++
	return &b.points[nextIndex], nil
}

// RemoveLastPoint removes the last point that was allocated.
// This is used for when NextPoint allocates a point that is then unused and
// needs to be returned to the ring buffer.
// This occurs when a histogram point has a stale marker.
// It panics if the buffer is empty.
func (b *HPointRingBuffer) RemoveLastPoint() {
	if b.size == 0 {
		panic("There are no points to remove")
	}

	b.size--
	if b.size == 0 {
		b.firstIndex = 0
	}
}

// Reset clears the contents of this buffer.
func (b *HPointRingBuffer) Reset() {
	b.firstIndex = 0
	b.size = 0
}

// Close releases any resources associated with this buffer.
func (b *HPointRingBuffer) Close() {
	putHPointSliceForRingBuffer(b.points, b.memoryConsumptionTracker)
	b.points = nil
}

// First returns the first point in this buffer.
// It panics if the buffer is empty.
func (b *HPointRingBuffer) First() promql.HPoint {
	if b.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return b.points[b.firstIndex]
}

// LastAtOrBefore returns the last point in this buffer with timestamp less than or equal to maxT.
// It returns false if there is no point satisfying this requirement.
func (b *HPointRingBuffer) LastAtOrBefore(maxT int64) (promql.HPoint, bool) {
	size := b.size

	for size > 0 {
		p := b.points[(b.firstIndex+size-1)%len(b.points)]

		if p.T <= maxT {
			return p, true
		}

		size--
	}

	return promql.HPoint{}, false
}

// CountAtOrBefore returns the number of points in this ring buffer with timestamp less than or equal to maxT.
func (b *HPointRingBuffer) CountAtOrBefore(maxT int64) int {
	count := b.size

	for count > 0 {
		p := b.points[(b.firstIndex+count-1)%len(b.points)]

		if p.T <= maxT {
			return count
		}

		count--
	}

	return count
}

// AnyAtOrBefore returns true if this ring buffer contains any points with timestamp less than or equal to maxT.
func (b *HPointRingBuffer) AnyAtOrBefore(maxT int64) bool {
	if b.size == 0 {
		return false
	}

	return b.points[b.firstIndex].T <= maxT
}

// These hooks exist so we can override them during unit tests.
var getHPointSliceForRingBuffer = HPointSlicePool.Get
var putHPointSliceForRingBuffer = HPointSlicePool.Put
