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
type SeriesDataRingBuffer[T any] struct {
	elements []seriesDataRingBufferElement[T]

	startIndex   int // Position in items of the first item.
	elementCount int // Number of elements (live or tombstones) in the buffer.
}

type seriesDataRingBufferElement[T any] struct {
	data        T
	seriesIndex int
	present     bool // If false, this element is a tombstone.
}

func (b *SeriesDataRingBuffer[T]) Append(d T, seriesIndex int) {
	if b.elementCount > 0 && seriesIndex <= b.LastElementSeriesIndex() {
		panic(fmt.Sprintf("attempted to append series with index %v, but last series index in buffer is %v", seriesIndex, b.LastElementSeriesIndex()))
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
		seriesIndex: seriesIndex,
		data:        d,
		present:     true,
	}

	b.elementCount++
}

func (b *SeriesDataRingBuffer[T]) FirstElementSeriesIndex() int {
	return b.getElementAtPosition(0).seriesIndex
}

func (b *SeriesDataRingBuffer[T]) LastElementSeriesIndex() int {
	return b.getElementAtPosition(b.elementCount - 1).seriesIndex
}

func (b *SeriesDataRingBuffer[T]) getElementAtPosition(position int) seriesDataRingBufferElement[T] {
	return b.elements[(b.startIndex+position)%len(b.elements)]
}

// Remove removes and returns the first series in the buffer, and panics if that is not the series with index seriesIndex.
func (b *SeriesDataRingBuffer[T]) Remove(seriesIndex int) T {
	if b.elementCount == 0 {
		panic(fmt.Sprintf("attempted to remove series at index %v, but buffer is empty", seriesIndex))
	}

	firstElementSeriesIndex := b.FirstElementSeriesIndex()
	lastElementSeriesIndex := b.LastElementSeriesIndex()

	if seriesIndex < firstElementSeriesIndex || seriesIndex > lastElementSeriesIndex {
		panic(fmt.Sprintf("attempted to remove series at index %v, but have series from index %v to %v", seriesIndex, firstElementSeriesIndex, lastElementSeriesIndex))
	}

	position := b.findElementPositionForSeriesIndex(seriesIndex)
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
			b.startIndex++
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

// RemoveFirst removes and returns the first series in the buffer.
// Calling RemoveFirst on an empty buffer panics.
func (b *SeriesDataRingBuffer[T]) RemoveFirst() T {
	if b.elementCount == 0 {
		panic("attempted to remove first series of empty buffer")
	}

	return b.Remove(b.FirstElementSeriesIndex())
}

// RemoveIfPresent removes and returns the series with the given series index, if it is present in the buffer.
func (b *SeriesDataRingBuffer[T]) RemoveIfPresent(seriesIndex int) (T, bool) {
	pos, found := b.tryToFindElementPositionForSeriesIndex(seriesIndex)
	if !found || !b.getElementAtPosition(pos).present {
		var empty T

		return empty, false
	}

	return b.removeAtPosition(pos), true
}

func (b *SeriesDataRingBuffer[T]) Get(seriesIndex int) T {
	if b.elementCount == 0 {
		panic(fmt.Sprintf("attempted to get series at index %v, but buffer is empty", seriesIndex))
	}

	firstElementSeriesIndex := b.FirstElementSeriesIndex()
	lastElementSeriesIndex := b.LastElementSeriesIndex()

	if seriesIndex < firstElementSeriesIndex || seriesIndex > lastElementSeriesIndex {
		panic(fmt.Sprintf("attempted to remove series at index %v, but have series from index %v to %v", seriesIndex, firstElementSeriesIndex, lastElementSeriesIndex))
	}

	position := b.findElementPositionForSeriesIndex(seriesIndex)
	return b.elements[(b.startIndex+position)%len(b.elements)].data
}

func (b *SeriesDataRingBuffer[T]) GetIfPresent(seriesIndex int) (T, bool) {
	pos, found := b.tryToFindElementPositionForSeriesIndex(seriesIndex)
	if !found {
		var empty T
		return empty, false
	}

	return b.elements[(b.startIndex+pos)%len(b.elements)].data, true
}

func (b *SeriesDataRingBuffer[T]) findElementPositionForSeriesIndex(seriesIndex int) int {
	pos, found := b.tryToFindElementPositionForSeriesIndex(seriesIndex)

	if !found {
		panic(fmt.Sprintf("attempted to find element position for series index %d, but it is not present in this buffer (first series index is %d, last series index is %d)", seriesIndex, b.FirstElementSeriesIndex(), b.LastElementSeriesIndex()))
	}

	if !b.getElementAtPosition(pos).present {
		panic(fmt.Sprintf("attempted to find element position for series index %d, but this element has been removed", seriesIndex))
	}

	return pos
}

func (b *SeriesDataRingBuffer[T]) tryToFindElementPositionForSeriesIndex(seriesIndex int) (int, bool) {
	// FIXME: we could possibly make this slightly more efficient by guessing where the series should be assuming the
	// series are evenly distributed and doing linear interpolation to find the expected position of the series.

	if b.elementCount == 0 {
		return -1, false
	}

	// Fast path: first or last element.
	if b.getElementAtPosition(0).seriesIndex == seriesIndex {
		return 0, true
	}

	if b.getElementAtPosition(b.elementCount-1).seriesIndex == seriesIndex {
		return b.elementCount - 1, true
	}

	if b.getElementAtPosition(0).seriesIndex > seriesIndex || b.getElementAtPosition(b.elementCount-1).seriesIndex < seriesIndex {
		return -1, false
	}

	cmp := func(e seriesDataRingBufferElement[T], target int) int {
		return e.seriesIndex - target
	}
	haveWrappedAround := b.startIndex+b.elementCount > len(b.elements)
	seriesIndexAppearsInTail := seriesIndex > b.elements[len(b.elements)-1].seriesIndex

	if haveWrappedAround && seriesIndexAppearsInTail {
		headSize := b.startIndex + b.elementCount - len(b.elements)
		posInTail, found := slices.BinarySearchFunc(b.elements[0:b.elementCount-headSize], seriesIndex, cmp)

		return posInTail + headSize, found
	}

	// Using a binary search here only works because the elements in the buffer are ordered by series index.
	// This is an invariant enforced by Append().
	return slices.BinarySearchFunc(b.elements[b.startIndex:min(b.startIndex+b.elementCount, len(b.elements))], seriesIndex, cmp)
}

// Size returns the number of elements in the buffer, including any empty elements due to
// the removal of elements out of order.
func (b *SeriesDataRingBuffer[T]) Size() int {
	return b.elementCount
}
