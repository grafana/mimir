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

// ViewUntilSearchingForwards returns a view into this buffer, including only points with timestamps less than or equal to maxT.
// ViewUntilSearchingForwards examines the points in the buffer starting from the front of the buffer, so is preferred over
// ViewUntilSearchingBackwards if it is expected that there are many points with timestamp greater than maxT, and few points with
// earlier timestamps.
// existing is an existing view instance for this buffer that is reused if provided. It can be nil.
// The returned view is no longer valid if this buffer is modified (eg. a point is added, or the buffer is reset or closed).
func (b *FPointRingBuffer) ViewUntilSearchingForwards(maxT int64, existing *FPointRingBufferView) *FPointRingBufferView {
	if existing == nil {
		existing = &FPointRingBufferView{buffer: b}
	}

	size := 0

	for size < b.size && b.pointAt(size).T <= maxT {
		size++
	}

	existing.size = size
	return existing
}

// ViewUntilSearchingBackwards is like ViewUntilSearchingForwards, except it examines the points from the end of the buffer, so
// is preferred over ViewUntilSearchingForwards if it is expected that only a few of the points will have timestamp greater than maxT.
func (b *FPointRingBuffer) ViewUntilSearchingBackwards(maxT int64, existing *FPointRingBufferView) *FPointRingBufferView {
	if existing == nil {
		existing = &FPointRingBufferView{buffer: b}
	}

	nextPositionToCheck := b.size - 1

	for nextPositionToCheck >= 0 && b.pointAt(nextPositionToCheck).T > maxT {
		nextPositionToCheck--
	}

	existing.size = nextPositionToCheck + 1
	return existing
}

// pointAt returns the point at index 'position'.
func (b *FPointRingBuffer) pointAt(position int) promql.FPoint {
	return b.points[(b.firstIndex+position)%len(b.points)]
}

// Reset clears the contents of this buffer, but retains the underlying point slice for future reuse.
func (b *FPointRingBuffer) Reset() {
	b.firstIndex = 0
	b.size = 0
}

// Release clears the contents of this buffer and releases the underlying point slice.
// The buffer can be used again and will acquire a new slice when required.
func (b *FPointRingBuffer) Release() {
	b.Reset()
	putFPointSliceForRingBuffer(b.points, b.memoryConsumptionTracker)
	b.points = nil
}

// Use replaces the contents of this buffer with s.
// The points in s must be in time order, not contain duplicate timestamps and start at index 0.
// s will be modified in place when the buffer is modified, and callers should not modify s after passing it off to the ring buffer via Use.
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

type FPointRingBufferView struct {
	buffer *FPointRingBuffer
	size   int
}

// UnsafePoints returns slices of the points in this buffer view.
// Either or both slice could be empty.
// Callers must not modify the values in the returned slices or return them to a pool.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of the buffer.
// The returned slices are no longer valid if this buffer is modified (eg. a point is added, or the buffer is reset or closed).
//
// FIXME: the fact we have to expose this is a bit gross, but the overhead of calling a function with ForEach is terrible.
// Perhaps we can use range-over function iterators (https://go.dev/wiki/RangefuncExperiment) once this is not experimental?
func (v FPointRingBufferView) UnsafePoints() (head []promql.FPoint, tail []promql.FPoint) {
	if v.size == 0 {
		return nil, nil
	}

	endOfHeadSegment := v.buffer.firstIndex + v.size

	if endOfHeadSegment > len(v.buffer.points) {
		// Need to wrap around.
		endOfTailSegment := endOfHeadSegment - len(v.buffer.points)
		endOfHeadSegment = len(v.buffer.points)
		return v.buffer.points[v.buffer.firstIndex:endOfHeadSegment], v.buffer.points[0:endOfTailSegment]
	}

	return v.buffer.points[v.buffer.firstIndex:endOfHeadSegment], nil
}

// CopyPoints returns a single slice of the points in this buffer view.
// Callers may modify the values in the returned slice, and should return the slice to the pool by calling
// PutFPointSlice when it is no longer needed.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of this buffer.
func (v FPointRingBufferView) CopyPoints() ([]promql.FPoint, error) {
	if v.size == 0 {
		return nil, nil
	}

	head, tail := v.UnsafePoints()
	combined, err := getFPointSliceForRingBuffer(len(head)+len(tail), v.buffer.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	combined = append(combined, head...)
	combined = append(combined, tail...)

	return combined, nil
}

// ForEach calls f for each point in this buffer view.
func (v FPointRingBufferView) ForEach(f func(p promql.FPoint)) {
	for i := 0; i < v.size; i++ {
		f(v.buffer.pointAt(i))
	}
}

// First returns the first point in this ring buffer view.
// It panics if the buffer is empty.
func (v FPointRingBufferView) First() promql.FPoint {
	if v.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return v.buffer.points[v.buffer.firstIndex]
}

// Last returns the last point in this ring buffer view.
// It returns false if the view is empty.
func (v FPointRingBufferView) Last() (promql.FPoint, bool) {
	if v.size == 0 {
		return promql.FPoint{}, false
	}

	return v.buffer.pointAt(v.size - 1), true
}

// Count returns the number of points in this ring buffer view.
func (v FPointRingBufferView) Count() int {
	return v.size
}

// Any returns true if this ring buffer view contains any points.
func (v FPointRingBufferView) Any() bool {
	return v.size != 0
}

// These hooks exist so we can override them during unit tests.
var getFPointSliceForRingBuffer = FPointSlicePool.Get
var putFPointSliceForRingBuffer = FPointSlicePool.Put
