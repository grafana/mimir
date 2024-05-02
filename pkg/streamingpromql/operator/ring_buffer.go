// SPDX-License-Identifier: AGPL-3.0-only

package operator

import "github.com/prometheus/prometheus/promql"

type RingBuffer struct {
	points     []promql.FPoint
	firstIndex int // Index into 'points' of first point in this buffer.
	size       int // Number of points in this buffer.
}

// DiscardPointsBefore discards all points in this buffer with timestamp less than t.
func (b *RingBuffer) DiscardPointsBefore(t int64) {
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

// HeadAndTail returns slices of the points in this buffer.
// Either or both slice could be empty.
// Callers must not modify the values in the returned slices or return them to a pool.
// Calling HeadAndTail is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice.
//
// FIXME: the fact we have to expose this is a bit gross, but the overhead of calling a function with ForEach is terrible.
// Perhaps we can use range-over function iterators (https://go.dev/wiki/RangefuncExperiment) once this is not experimental?
func (b *RingBuffer) HeadAndTail() ([]promql.FPoint, []promql.FPoint) {
	endOfTailSegment := b.firstIndex + b.size

	if endOfTailSegment > len(b.points) {
		// Need to wrap around.
		endOfHeadSegment := endOfTailSegment % len(b.points)
		endOfTailSegment = len(b.points)
		return b.points[b.firstIndex:endOfTailSegment], b.points[0:endOfHeadSegment]
	}

	return b.points[b.firstIndex:endOfTailSegment], nil
}

// CopyPoints returns a single slice of the points in this buffer.
// Callers may modify the values in the returned slice, and should return the slice to the pool by calling
// PutFPointSlice when it is no longer needed.
// Calling HeadAndTail is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice.
func (b *RingBuffer) CopyPoints() []promql.FPoint {
	if b.size == 0 {
		return nil
	}

	head, tail := b.HeadAndTail()
	combined := GetFPointSlice(len(head) + len(tail))
	combined = append(combined, head...)
	combined = append(combined, tail...)

	return combined
}

// ForEach calls f for each point in this buffer.
func (b *RingBuffer) ForEach(f func(p promql.FPoint)) {
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
func (b *RingBuffer) Append(p promql.FPoint) {
	if b.size == len(b.points) {
		// Create a new slice, copy the elements from the current slice.
		newSize := b.size * 2
		if newSize == 0 {
			newSize = 2
		}

		newSlice := GetFPointSlice(newSize)
		newSlice = newSlice[:cap(newSlice)]
		pointsAtEnd := b.size - b.firstIndex
		copy(newSlice, b.points[b.firstIndex:])
		copy(newSlice[pointsAtEnd:], b.points[:b.firstIndex])

		PutFPointSlice(b.points)
		b.points = newSlice
		b.firstIndex = 0
	}

	nextIndex := (b.firstIndex + b.size) % len(b.points)
	b.points[nextIndex] = p
	b.size++
}

// Reset clears the contents of this buffer.
func (b *RingBuffer) Reset() {
	b.firstIndex = 0
	b.size = 0
}

// Close releases any resources associated with this buffer.
func (b *RingBuffer) Close() {
	b.Reset()
	PutFPointSlice(b.points)
	b.points = nil
}

// First returns the first point in this buffer.
// It panics if the buffer is empty.
func (b *RingBuffer) First() promql.FPoint {
	if b.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return b.points[b.firstIndex]
}

// Last returns the last point in this buffer.
// It panics if the buffer is empty.
func (b *RingBuffer) Last() promql.FPoint {
	if b.size == 0 {
		panic("Can't get last element of empty buffer")
	}

	return b.points[(b.firstIndex+b.size-1)%len(b.points)]
}
