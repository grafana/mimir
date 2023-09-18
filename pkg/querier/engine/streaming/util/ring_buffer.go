// SPDX-License-Identifier: AGPL-3.0-only

package util // TODO: find a better package name

import "github.com/prometheus/prometheus/promql"

type RingBuffer struct {
	points     []promql.FPoint
	firstIndex int
	size       int
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

	// TODO: if we now have an empty buffer, reset firstIndex to 0?
}

// Points returns slices of the points in this buffer.
// Either or both slice could be empty.
// Callers must not modify the values in the returned slices.
// FIXME: the fact we have to expose this is a bit gross, but the overhead of calling a function with ForEach is terrible.
func (b *RingBuffer) Points() ([]promql.FPoint, []promql.FPoint) {
	endOfTailSegment := b.firstIndex + b.size

	if endOfTailSegment > len(b.points) {
		// Need to wrap around.
		endOfHeadSegment := endOfTailSegment % len(b.points)
		endOfTailSegment = len(b.points)
		return b.points[b.firstIndex:endOfTailSegment], b.points[0:endOfHeadSegment]
	} else {
		return b.points[b.firstIndex:endOfTailSegment], nil
	}
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
		// TODO: is there a better resizing strategy? Guess expected number of points based on expected time interval + step between points we've seen so far?
		// TODO: pool slices
		newSize := b.size * 2
		if newSize == 0 {
			newSize = 2
		}

		newSlice := make([]promql.FPoint, newSize)
		pointsAtEnd := b.size - b.firstIndex
		copy(newSlice, b.points[b.firstIndex:])
		copy(newSlice[pointsAtEnd:], b.points[:b.firstIndex])

		b.points = newSlice
		b.firstIndex = 0
	}

	nextIndex := (b.firstIndex + b.size) % len(b.points)
	b.points[nextIndex] = p
	b.size++
}

func (b *RingBuffer) Reset() {
	b.firstIndex = 0
	b.size = 0
}

func (b *RingBuffer) First() promql.FPoint {
	if b.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return b.points[b.firstIndex]
}

func (b *RingBuffer) Last() promql.FPoint {
	if b.size == 0 {
		panic("Can't get last element of empty buffer")
	}

	return b.points[(b.firstIndex+b.size-1)%len(b.points)]
}
