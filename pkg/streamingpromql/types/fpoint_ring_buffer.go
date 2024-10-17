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

type FPointRingBuffer struct {
	memoryConsumptionTracker *limiting.MemoryConsumptionTracker
	points                   []promql.FPoint
	firstIndex               int // Index into 'points' of first point in this buffer.
	size                     int // Number of points in this buffer.
}

func NewFPointRingBuffer(memoryConsumptionTracker *limiting.MemoryConsumptionTracker) *FPointRingBuffer {
	return &FPointRingBuffer{memoryConsumptionTracker: memoryConsumptionTracker}
}

// DiscardPointsBefore discards all points in this buffer with timestamp less than t.
func (b *FPointRingBuffer) DiscardPointsBefore(t int64) {
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
// The returned slices are no longer valid if this buffer is modified (eg. a point is added, or the buffer is reset or closed).
//
// FIXME: the fact we have to expose this is a bit gross, but the overhead of calling a function with ForEach is terrible.
// Perhaps we can use range-over function iterators (https://go.dev/wiki/RangefuncExperiment) once this is not experimental?
func (b *FPointRingBuffer) UnsafePoints(maxT int64) (head []promql.FPoint, tail []promql.FPoint) {
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
// PutFPointSlice when it is no longer needed.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of this buffer.
func (b *FPointRingBuffer) CopyPoints(maxT int64) ([]promql.FPoint, error) {
	if b.size == 0 {
		return nil, nil
	}

	head, tail := b.UnsafePoints(maxT)
	combined, err := getFPointSliceForRingBuffer(len(head)+len(tail), b.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	combined = append(combined, head...)
	combined = append(combined, tail...)

	return combined, nil
}

// ForEach calls f for each point in this buffer.
func (b *FPointRingBuffer) ForEach(f func(p promql.FPoint)) {
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
func (b *FPointRingBuffer) Append(p promql.FPoint) error {
	if b.size == len(b.points) {
		// Create a new slice, copy the elements from the current slice.
		newSize := b.size * 2
		if newSize == 0 {
			newSize = 2
		}

		newSlice, err := getFPointSliceForRingBuffer(newSize, b.memoryConsumptionTracker)
		if err != nil {
			return err
		}

		newSlice = newSlice[:cap(newSlice)]
		pointsAtEnd := b.size - b.firstIndex
		copy(newSlice, b.points[b.firstIndex:])
		copy(newSlice[pointsAtEnd:], b.points[:b.firstIndex])

		putFPointSliceForRingBuffer(b.points, b.memoryConsumptionTracker)
		b.points = newSlice
		b.firstIndex = 0
	}

	nextIndex := (b.firstIndex + b.size) % len(b.points)
	b.points[nextIndex] = p
	b.size++
	return nil
}

// Reset clears the contents of this buffer.
func (b *FPointRingBuffer) Reset() {
	b.firstIndex = 0
	b.size = 0
}

// Use replaces the contents of this buffer with s.
// The points in s must be in time order, not contain duplicate timestamps and start at index 0.
// s will be modified in place when the buffer is modified, and callers should not modify s after calling Use.
// s will be returned to the pool when Close is called, Use is called again, or the buffer needs to expand, so callers
// should not return s to the pool themselves.
func (b *FPointRingBuffer) Use(s []promql.FPoint) {
	putFPointSliceForRingBuffer(b.points, b.memoryConsumptionTracker)

	b.points = s
	b.firstIndex = 0
	b.size = len(s)
}

// Close releases any resources associated with this buffer.
func (b *FPointRingBuffer) Close() {
	putFPointSliceForRingBuffer(b.points, b.memoryConsumptionTracker)
	b.points = nil
}

// First returns the first point in this buffer.
// It panics if the buffer is empty.
func (b *FPointRingBuffer) First() promql.FPoint {
	if b.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return b.points[b.firstIndex]
}

// LastAtOrBefore returns the last point in this buffer with timestamp less than or equal to maxT.
// It returns false if there is no point satisfying this requirement.
func (b *FPointRingBuffer) LastAtOrBefore(maxT int64) (promql.FPoint, bool) {
	size := b.size

	for size > 0 {
		p := b.points[(b.firstIndex+size-1)%len(b.points)]

		if p.T <= maxT {
			return p, true
		}

		size--
	}

	return promql.FPoint{}, false
}

// CountAtOrBefore returns the number of points in this ring buffer with timestamp less than or equal to maxT.
func (b *FPointRingBuffer) CountAtOrBefore(maxT int64) int {
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
func (b *FPointRingBuffer) AnyAtOrBefore(maxT int64) bool {
	if b.size == 0 {
		return false
	}

	return b.points[b.firstIndex].T <= maxT
}

// These hooks exist so we can override them during unit tests.
var getFPointSliceForRingBuffer = FPointSlicePool.Get
var putFPointSliceForRingBuffer = FPointSlicePool.Put
