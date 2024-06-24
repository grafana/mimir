// SPDX-License-Identifier: AGPL-3.0-only
package types

import (
	"github.com/prometheus/prometheus/promql"
)

type RingBuffer[T any] struct {
	pool       RingBufferPool[T]
	points     []T
	firstIndex int // Index into 'points' of first point in this buffer.
	size       int // Number of points in this buffer.
}

type RingBufferPool[T any] interface {
	GetSlice(size int) ([]T, error)
	PutSlice(s []T)
	GetTimestamp(p T) int64
}

type FPointPool interface {
	GetFPointSlice(size int) ([]promql.FPoint, error)
	PutFPointSlice(s []promql.FPoint)
}

type FPointRingBufferPool struct {
	pool FPointPool
}

func (p *FPointRingBufferPool) GetSlice(size int) ([]promql.FPoint, error) {
	return p.pool.GetFPointSlice(size)
}

func (p *FPointRingBufferPool) PutSlice(s []promql.FPoint) {
	p.pool.PutFPointSlice(s)
}

func (p *FPointRingBufferPool) GetTimestamp(point promql.FPoint) int64 {
	return point.T
}

func NewFPointRingBuffer(pool FPointPool) *RingBuffer[promql.FPoint] {
	return &RingBuffer[promql.FPoint]{
		pool: &FPointRingBufferPool{pool: pool},
	}
}

// DiscardPointsBefore discards all points in this buffer with timestamp less than t.
func (b *RingBuffer[T]) DiscardPointsBefore(t int64) {
	for b.size > 0 && b.pool.GetTimestamp(b.points[b.firstIndex]) < t {
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
func (b *RingBuffer[T]) UnsafePoints(maxT int64) (head []T, tail []T) {
	size := b.size

	for size > 0 && b.pool.GetTimestamp(b.points[(b.firstIndex+size-1)%len(b.points)]) > maxT {
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
// PutSlice when it is no longer needed.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of this buffer.
func (b *RingBuffer[T]) CopyPoints(maxT int64) ([]T, error) {
	if b.size == 0 {
		return nil, nil
	}

	head, tail := b.UnsafePoints(maxT)
	combined, err := b.pool.GetSlice(len(head) + len(tail))
	if err != nil {
		return nil, err
	}

	combined = append(combined, head...)
	combined = append(combined, tail...)

	return combined, nil
}

// ForEach calls f for each point in this buffer.
func (b *RingBuffer[T]) ForEach(f func(p T)) {
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
func (b *RingBuffer[T]) Append(p T) error {
	if b.size == len(b.points) {
		// Create a new slice, copy the elements from the current slice.
		newSize := b.size * 2
		if newSize == 0 {
			newSize = 2
		}

		newSlice, err := b.pool.GetSlice(newSize)
		if err != nil {
			return err
		}

		newSlice = newSlice[:cap(newSlice)]
		pointsAtEnd := b.size - b.firstIndex
		copy(newSlice, b.points[b.firstIndex:])
		copy(newSlice[pointsAtEnd:], b.points[:b.firstIndex])

		b.pool.PutSlice(b.points)
		b.points = newSlice
		b.firstIndex = 0
	}

	nextIndex := (b.firstIndex + b.size) % len(b.points)
	b.points[nextIndex] = p
	b.size++
	return nil
}

// Reset clears the contents of this buffer.
func (b *RingBuffer[T]) Reset() {
	b.firstIndex = 0
	b.size = 0
}

// Close releases any resources associated with this buffer.
func (b *RingBuffer[T]) Close() {
	b.Reset()
	b.pool.PutSlice(b.points)
	b.points = nil
}

// First returns the first point in this buffer.
// It panics if the buffer is empty.
func (b *RingBuffer[T]) First() T {
	if b.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return b.points[b.firstIndex]
}

// LastAtOrBefore returns the last point in this buffer with timestamp less than or equal to maxT.
// It returns false if there is no point satisfying this requirement.
func (b *RingBuffer[T]) LastAtOrBefore(maxT int64) (T, bool) {
	size := b.size

	for size > 0 {
		p := b.points[(b.firstIndex+size-1)%len(b.points)]

		if b.pool.GetTimestamp(p) <= maxT {
			return p, true
		}

		size--
	}

	var zero T
	return zero, false
}
