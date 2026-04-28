// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"fmt"
	"slices"
)

// SeriesDataRingBuffer implements a ring buffer structure for series data.
//
// It supports appending elements to the end of the buffer as well as removing elements at any
// position in the buffer.
//
// Removed elements in the middle of the active segment of the buffer are tombstoned (marked as present = false),
// to avoid the need for shuffling when removing series not at the start or end of the buffer.
//
// Why use a ring buffer for this rather than a map?
//   - In the case where a single expression is duplicated multiple times, we expect the series to be contiguous
//     and most reads and writes to be at the start or end of the buffer. (In the case where there are only two consumers,
//     which is the most common case, then reads and writes are always only at the start and end of the buffer.)
//     Using a ring buffer rather than a map makes this far more efficient.
//   - In the case where one consumer is a subset of the other, which is the next most common case, then the same applies:
//     reads and writes are always only at the start and end of the buffer.
//   - In the case where there are many consumers, then the buffer is still not expected to be large and only some reads will
//     pay the cost of searching for the desired element, so this is an acceptable trade-off.
//
// Note that there are two terms used here:
// - "element position" refers to the index into the elements slice
// - "element index" refers to the index of the element as makes sense to the caller (eg. series index in an overall stream of series)
type SeriesDataRingBuffer[T any] struct {
	elements []seriesDataRingBufferElement[T]

	startIndex   int // Position in items of the first item.
	elementCount int // Number of elements (live or tombstones) in the buffer.
}

type seriesDataRingBufferElement[T any] struct {
	data         T
	elementIndex int
	present      bool // If false, this element is a tombstone.
}

func (b *SeriesDataRingBuffer[T]) Append(d T, elementIndex int) {
	if b.elementCount > 0 && elementIndex <= b.LastElementIndex() {
		panic(fmt.Sprintf("attempted to append element with index %v, but last element index in buffer is %v", elementIndex, b.LastElementIndex()))
	}

	if len(b.elements) == b.elementCount {
		// Buffer is full, need to grow it.
		newSize := max(len(b.elements)*2, 2) // Grow by powers of two, and ensure we have at least 2 slots.
		newData := make([]seriesDataRingBufferElement[T], newSize)
		copy(newData, b.elements[b.startIndex:])
		copy(newData[len(b.elements)-b.startIndex:], b.elements[:b.startIndex])
		b.startIndex = 0
		b.elements = newData
	}

	b.elements[(b.startIndex+b.elementCount)%len(b.elements)] = seriesDataRingBufferElement[T]{
		elementIndex: elementIndex,
		data:         d,
		present:      true,
	}

	b.elementCount++
}

func (b *SeriesDataRingBuffer[T]) FirstElementIndex() int {
	return b.getElementAtPosition(0).elementIndex
}

func (b *SeriesDataRingBuffer[T]) LastElementIndex() int {
	return b.getElementAtPosition(b.elementCount - 1).elementIndex
}

func (b *SeriesDataRingBuffer[T]) getElementAtPosition(position int) seriesDataRingBufferElement[T] {
	return b.elements[(b.startIndex+position)%len(b.elements)]
}

// Remove removes and returns the first series in the buffer, and panics if that is not the series with index seriesIndex.
func (b *SeriesDataRingBuffer[T]) Remove(elementIndex int) T {
	if b.elementCount == 0 {
		panic(fmt.Sprintf("attempted to remove element at index %v, but buffer is empty", elementIndex))
	}

	firstElementIndex := b.FirstElementIndex()
	lastElementIndex := b.LastElementIndex()

	if elementIndex < firstElementIndex || elementIndex > lastElementIndex {
		panic(fmt.Sprintf("attempted to remove element at index %v, but have elements from index %v to %v", elementIndex, firstElementIndex, lastElementIndex))
	}

	position := b.findElementPositionForElementIndex(elementIndex)
	return b.removeAtPosition(position)
}

func (b *SeriesDataRingBuffer[T]) removeAtPosition(position int) T {
	idx := (b.startIndex + position) % len(b.elements)

	d := b.elements[idx].data
	b.elements[idx].present = false
	var empty T
	b.elements[idx].data = empty // Clear the element.

	switch position {
	case 0:
		// We've just removed the first element. Clear it and any tombstones that follow.
		for b.elementCount > 0 && !b.getElementAtPosition(0).present {
			b.startIndex = (b.startIndex + 1) % len(b.elements)
			b.elementCount--
		}
	case b.elementCount - 1:
		// We've just removed the last element. Clear it and any tombstones that precede it.
		for b.elementCount > 0 && !b.getElementAtPosition(b.elementCount-1).present {
			b.elementCount--
		}
	}

	if b.elementCount == 0 {
		b.startIndex = 0
	}

	return d
}

// RemoveFirst removes and returns the first element in the buffer.
// Calling RemoveFirst on an empty buffer panics.
func (b *SeriesDataRingBuffer[T]) RemoveFirst() T {
	if b.elementCount == 0 {
		panic("attempted to remove first element of empty buffer")
	}

	return b.Remove(b.FirstElementIndex())
}

// RemoveIfPresent removes and returns the element with the given element index, if it is present in the buffer.
func (b *SeriesDataRingBuffer[T]) RemoveIfPresent(elementIndex int) (T, bool) {
	pos, found := b.tryToFindElementPositionForElementIndex(elementIndex)
	if !found || !b.getElementAtPosition(pos).present {
		var empty T

		return empty, false
	}

	return b.removeAtPosition(pos), true
}

func (b *SeriesDataRingBuffer[T]) Get(elementIndex int) T {
	if b.elementCount == 0 {
		panic(fmt.Sprintf("attempted to get element at index %v, but buffer is empty", elementIndex))
	}

	firstElementIndex := b.FirstElementIndex()
	lastElementIndex := b.LastElementIndex()

	if elementIndex < firstElementIndex || elementIndex > lastElementIndex {
		panic(fmt.Sprintf("attempted to get element at index %v, but have element from index %v to %v", elementIndex, firstElementIndex, lastElementIndex))
	}

	position := b.findElementPositionForElementIndex(elementIndex)
	return b.elements[(b.startIndex+position)%len(b.elements)].data
}

func (b *SeriesDataRingBuffer[T]) GetIfPresent(elementIndex int) (T, bool) {
	pos, found := b.tryToFindElementPositionForElementIndex(elementIndex)
	if !found || !b.getElementAtPosition(pos).present {
		var empty T
		return empty, false
	}

	return b.elements[(b.startIndex+pos)%len(b.elements)].data, true
}

func (b *SeriesDataRingBuffer[T]) findElementPositionForElementIndex(elementIndex int) int {
	pos, found := b.tryToFindElementPositionForElementIndex(elementIndex)
	if !found {
		panic(fmt.Sprintf("attempted to find element position for element index %d, but it is not present in this buffer (first element index is %d, last element index is %d)", elementIndex, b.FirstElementIndex(), b.LastElementIndex()))
	}

	if !b.getElementAtPosition(pos).present {
		panic(fmt.Sprintf("attempted to find element position for element index %d, but this element has been removed", elementIndex))
	}

	return pos
}

func (b *SeriesDataRingBuffer[T]) tryToFindElementPositionForElementIndex(elementIndex int) (int, bool) {
	// FIXME: we could possibly make this slightly more efficient by guessing where the element should be assuming the
	// elements are evenly distributed and doing linear interpolation to find the expected position of the element.

	if b.elementCount == 0 {
		return -1, false
	}

	// Fast path: first or last element.
	if b.getElementAtPosition(0).elementIndex == elementIndex {
		return 0, true
	}

	if b.getElementAtPosition(b.elementCount-1).elementIndex == elementIndex {
		return b.elementCount - 1, true
	}

	if b.getElementAtPosition(0).elementIndex > elementIndex || b.getElementAtPosition(b.elementCount-1).elementIndex < elementIndex {
		return -1, false
	}

	cmp := func(e seriesDataRingBufferElement[T], target int) int {
		return e.elementIndex - target
	}
	haveWrappedAround := b.startIndex+b.elementCount > len(b.elements)
	elementIndexAppearsInTail := elementIndex > b.elements[len(b.elements)-1].elementIndex

	if haveWrappedAround && elementIndexAppearsInTail {
		tailSize := b.startIndex + b.elementCount - len(b.elements)
		headSize := b.elementCount - tailSize
		posInTail, found := slices.BinarySearchFunc(b.elements[0:tailSize], elementIndex, cmp)

		return posInTail + headSize, found
	}

	// Using a binary search here only works because the elements in the buffer are ordered by element index.
	// This is an invariant enforced by Append().
	return slices.BinarySearchFunc(b.elements[b.startIndex:min(b.startIndex+b.elementCount, len(b.elements))], elementIndex, cmp)
}

// Size returns the number of elements in the buffer, including any empty elements due to
// the removal of elements out of order.
func (b *SeriesDataRingBuffer[T]) Size() int {
	return b.elementCount
}
