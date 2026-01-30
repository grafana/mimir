// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"errors"
	"fmt"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/math"
	"github.com/grafana/mimir/pkg/util/pool"
)

// FPointRingBuffer and HPointRingBuffer are nearly identical, but exist for each
// specific point type.
// The code is duplicated due to the cost of indirection in golang.
// The tests are combined to ensure consistency. We may wish to consider codegen
// or similar in the future if we end up with more data types etc.
// (see: https://github.com/grafana/mimir/pull/8508#discussion_r1654668995)

type FPointRingBuffer struct {
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	points                   []promql.FPoint
	pointsIndexMask          int // Bitmask used to calculate indices into points efficiently. Computing modulo is relatively expensive, but points is always sized as a power of two, so we can a bitmask to calculate remainders cheaply.
	firstIndex               int // Index into 'points' of first point in this buffer.
	size                     int // Number of points in this buffer.
}

func NewFPointRingBuffer(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *FPointRingBuffer {
	return &FPointRingBuffer{memoryConsumptionTracker: memoryConsumptionTracker}
}

func (b *FPointRingBuffer) resizeIfRequired(additionalPoints int, appendingAtStart bool) error {
	newRequestedSize := b.size + additionalPoints

	if newRequestedSize <= len(b.points) {
		return nil
	}

	newSize := math.NextPowerTwo(newRequestedSize)

	// We create a new slice, and we will copy the elements from the current slice to the new slice
	newSlice, err := getFPointSliceForRingBuffer(newSize, b.memoryConsumptionTracker)
	if err != nil {
		return err
	}

	if !pool.IsPowerOfTwo(cap(newSlice)) {
		// We rely on the capacity being a power of two for the pointsIndexMask optimisation below.
		// If we can guarantee that newSlice has a capacity that is a power of two in the future, then we can drop this check.
		// Note that the capacity of newSlice is guaranteed to be at least 2 due to the implementation in math.NextPowerTwo()
		return fmt.Errorf("pool returned slice of capacity %v (requested %v), but wanted a power of two", cap(newSlice), newSize)
	}

	newSlice = newSlice[:cap(newSlice)]
	pointsAtEnd := len(b.points) - b.firstIndex

	headOffset := 0
	newFirstIndex := 0

	// If we are appending to the start of the buffer, we offset our copy by the number of points being prepended.
	// This ensures that once the insert has completed the firstIndex will be 0
	if appendingAtStart {
		headOffset += additionalPoints
		pointsAtEnd += additionalPoints
		newFirstIndex += additionalPoints
	}

	copy(newSlice[headOffset:], b.points[b.firstIndex:])
	copy(newSlice[pointsAtEnd:], b.points[:(b.firstIndex+b.size)&b.pointsIndexMask])

	b.firstIndex = newFirstIndex

	putFPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
	b.points = newSlice
	b.pointsIndexMask = cap(newSlice) - 1
	return nil
}

// DiscardPointsAtOrBefore discards all points in this buffer with timestamp less than or equal to the given timestamp.
// Note that the actual underlying points buffer is not reduced in size.
func (b *FPointRingBuffer) DiscardPointsAtOrBefore(t int64) {
	for b.size > 0 && b.points[b.firstIndex].T <= t {
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

// RemoveLast will remove the last point from the buffer.
// It is safe to call this function on an empty buffer.
func (b *FPointRingBuffer) RemoveLast() {
	if b.size > 0 {
		b.size--
	}
}

func (b *FPointRingBuffer) RemoveFirst() {
	if b.size > 0 {
		b.firstIndex++
		b.size--

		if b.firstIndex >= len(b.points) {
			b.firstIndex = 0
		}
	}
}

// ReplaceLast will replace the last point in the buffer with the given point.
// An error will be returned if the buffer is empty.
// It is the responsibility of the caller to ensure that replacing the point maintains chronological order of the buffer.
func (b *FPointRingBuffer) ReplaceLast(point promql.FPoint) error {
	if b.size == 0 {
		return errors.New("unable to replace point to the tail of the buffer - current buffer is empty")
	}

	position := b.size - 1
	b.points[(b.firstIndex+position)&b.pointsIndexMask] = point
	return nil
}

// ReplaceFirst will replace the first point in the buffer with the given point.
// An error will be returned if the buffer is empty.
// It is the responsibility of the caller to ensure that replacing the point maintains chronological order of the buffer.
func (b *FPointRingBuffer) ReplaceFirst(point promql.FPoint) error {
	if b.size == 0 {
		return errors.New("unable to replace point to the head of the buffer - current buffer is empty")
	}

	b.points[b.firstIndex] = point
	return nil
}

// AppendAtStart will insert the given point into the head of this buffer, expanding if required.
// Subsequently calling PointAt(0) will return this point.
// It is the responsibility of the caller to ensure that inserting this point maintains chronological order of the buffer.
func (b *FPointRingBuffer) AppendAtStart(point promql.FPoint) error {
	if err := b.resizeIfRequired(1, true); err != nil {
		return err
	}

	b.firstIndex = (b.firstIndex - 1) & b.pointsIndexMask
	b.points[b.firstIndex] = point
	b.size++

	return nil
}

// Append adds p to this buffer, expanding it if required.
// It is the responsibility of the caller to ensure that inserting this point maintains chronological order of the buffer.
func (b *FPointRingBuffer) Append(p promql.FPoint) error {
	if err := b.resizeIfRequired(1, false); err != nil {
		return err
	}

	nextIndex := (b.firstIndex + b.size) & b.pointsIndexMask
	b.points[nextIndex] = p
	b.size++
	return nil
}

// AppendSlice will append all the given points to the buffer.
//
// It is more efficient to call this for a collection of points then to call Append()
// for each individual point. In this function the underlying buffer will only be grown once based
// off the given slice length.
//
// It is the caller's responsibility to ensure that the given points are in chronological order
// and that the points chronologically follow any existing points in the buffer.
func (b *FPointRingBuffer) AppendSlice(points []promql.FPoint) error {
	if len(points) == 0 {
		return nil
	}

	if err := b.resizeIfRequired(len(points), false); err != nil {
		return err
	}

	for _, pt := range points {
		nextIndex := (b.firstIndex + b.size) & b.pointsIndexMask
		b.points[nextIndex] = pt
		b.size++
	}

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

	for size < b.size && b.PointAt(size).T <= maxT {
		size++
	}

	existing.offset = 0
	existing.size = size
	return existing
}

// ViewAll returns a view which includes all points in the ring buffer.
// The returned view is no longer valid if this buffer is modified (eg. a point is added, or the buffer is reset or closed).
func (b *FPointRingBuffer) ViewAll(existing *FPointRingBufferView) *FPointRingBufferView {
	if existing == nil {
		existing = &FPointRingBufferView{buffer: b}
	}
	existing.offset = 0
	existing.size = b.size
	return existing
}

// ViewUntilSearchingBackwards is like ViewUntilSearchingForwards, except it examines the points from the end of the buffer, so
// is preferred over ViewUntilSearchingForwards if it is expected that only a few of the points will have timestamp greater than maxT.
func (b *FPointRingBuffer) ViewUntilSearchingBackwards(maxT int64, existing *FPointRingBufferView) *FPointRingBufferView {
	if existing == nil {
		existing = &FPointRingBufferView{buffer: b}
	}

	nextPositionToCheck := b.size - 1

	for nextPositionToCheck >= 0 && b.PointAt(nextPositionToCheck).T > maxT {
		nextPositionToCheck--
	}

	existing.offset = 0
	existing.size = nextPositionToCheck + 1
	return existing
}

// PointAt returns the point at index 'position'.
// Note that it is the caller's responsibility to have checked that the buffer size is gt the given position.
func (b *FPointRingBuffer) PointAt(position int) promql.FPoint {
	return b.points[(b.firstIndex+position)&b.pointsIndexMask]
}

// Last returns the last point in the buffer.
// Note that it is the caller's responsibility to have checked that the buffer size is not empty.
func (b *FPointRingBuffer) Last() promql.FPoint {
	return b.PointAt(b.size - 1)
}

// Count returns the current number of points in the buffer.
func (b *FPointRingBuffer) Count() int {
	return b.size
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
	putFPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
	b.points = nil
}

// Use replaces the contents of this buffer with s.
// The points in s must be in time order, not contain duplicate timestamps and start at index 0.
// s will be modified in place when the buffer is modified, and callers should not modify s after passing it off to the ring buffer via Use.
// s will be returned to the pool when Close is called, Use is called again, or the buffer needs to expand, so callers
// should not return s to the pool themselves.
// s must have a capacity that is a power of two.
func (b *FPointRingBuffer) Use(s []promql.FPoint) error {
	if !pool.IsPowerOfTwo(cap(s)) {
		// We rely on the capacity being a power of two for the pointsIndexMask optimisation below.
		return fmt.Errorf("slice capacity must be a power of two, but is %v", cap(s))
	}

	putFPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)

	b.points = s[:cap(s)]
	b.firstIndex = 0
	b.size = len(s)
	b.pointsIndexMask = cap(s) - 1
	return nil
}

// Close releases any resources associated with this buffer.
func (b *FPointRingBuffer) Close() {
	putFPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
	b.points = nil
}

type FPointRingBufferView struct {
	buffer *FPointRingBuffer
	offset int // Offset from buffer's firstIndex where this view starts
	size   int
}

// UnsafePoints returns slices of the points in this buffer view.
// Either or both slice could be empty.
// Callers must not modify the values in the returned slices nor return them to a pool.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of the buffer.
// The returned slices are no longer valid if this buffer is modified (eg. a point is added, or the buffer is reset or closed).
//
// FIXME: the fact we have to expose this is a bit gross, but the overhead of calling a function with ForEach is terrible.
// Perhaps we can use range-over function iterators (https://go.dev/wiki/RangefuncExperiment) once this is not experimental?
func (v *FPointRingBufferView) UnsafePoints() (head []promql.FPoint, tail []promql.FPoint) {
	if v.size == 0 {
		return nil, nil
	}

	startIndex := (v.buffer.firstIndex + v.offset) & v.buffer.pointsIndexMask
	endOfHeadSegment := startIndex + v.size

	if endOfHeadSegment > len(v.buffer.points) {
		// Need to wrap around.
		endOfTailSegment := endOfHeadSegment - len(v.buffer.points)
		endOfHeadSegment = len(v.buffer.points)
		return v.buffer.points[startIndex:endOfHeadSegment], v.buffer.points[0:endOfTailSegment]
	}

	return v.buffer.points[startIndex:endOfHeadSegment], nil
}

// CopyPoints returns a single slice of the points in this buffer view.
// Callers may modify the values in the returned slice, and should return the slice to the pool by calling
// PutFPointSlice when it is no longer needed.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of this buffer.
func (v *FPointRingBufferView) CopyPoints() ([]promql.FPoint, error) {
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
func (v *FPointRingBufferView) ForEach(f func(p promql.FPoint)) {
	for i := 0; i < v.size; i++ {
		f(v.PointAt(i))
	}
}

// First returns the first point in this ring buffer view.
// It panics if the buffer is empty.
func (v *FPointRingBufferView) First() promql.FPoint {
	if v.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return v.PointAt(0)
}

// Last returns the last point in this ring buffer view.
// It returns false if the view is empty.
func (v *FPointRingBufferView) Last() (promql.FPoint, bool) {
	if v.size == 0 {
		return promql.FPoint{}, false
	}

	return v.PointAt(v.size - 1), true
}

// Count returns the number of points in this ring buffer view.
func (v *FPointRingBufferView) Count() int {
	return v.size
}

// Any returns true if this ring buffer view contains any points.
func (v *FPointRingBufferView) Any() bool {
	return v.size != 0
}

// PointAt returns the point at index i in this ring buffer view.
// It panics if i is outside the range of points in this view.
func (v *FPointRingBufferView) PointAt(i int) promql.FPoint {
	if i >= v.size {
		panic(fmt.Sprintf("PointAt(): out of range, requested index %v but have length %v", i, v.size))
	}

	return v.buffer.PointAt(v.offset + i)
}

// Clone returns a clone of this view and its underlying ring buffer.
// The caller is responsible for closing the returned ring buffer when it is no longer needed.
func (v *FPointRingBufferView) Clone() (*FPointRingBufferView, *FPointRingBuffer, error) {
	if v.size == 0 {
		return &FPointRingBufferView{}, nil, nil
	}

	points, err := v.CopyPoints()
	if err != nil {
		return nil, nil, err
	}

	buffer := NewFPointRingBuffer(v.buffer.memoryConsumptionTracker)
	if err := buffer.Use(points); err != nil {
		return nil, nil, err
	}

	view := &FPointRingBufferView{
		buffer: buffer,
		size:   v.size,
	}

	return view, buffer, nil
}

// SubView returns a view with only points in range (minT, maxT].
// If previousSubView is provided, it will be reused to create the new subview. previousSubView must be a previous
// subview for the same parent view and the next subview is assumed to cover a later range (we only start searching from
// after the samples of the previous subview).
func (v *FPointRingBufferView) SubView(minT int64, maxT int64, previousSubView *FPointRingBufferView) *FPointRingBufferView {
	if v.size == 0 {
		if previousSubView == nil {
			return &FPointRingBufferView{}
		}
		previousSubView.offset = v.offset
		previousSubView.size = 0
		return previousSubView
	}

	var startIdx int
	if previousSubView == nil {
		startIdx = v.offset
		previousSubView = &FPointRingBufferView{buffer: v.buffer}
	} else {
		startIdx = previousSubView.offset + previousSubView.size
	}

	endIdx := v.offset + v.size
	if startIdx >= endIdx {
		previousSubView.offset = endIdx
		previousSubView.size = 0
		return previousSubView
	}

	// Find start idx for subview
	currentIdx := startIdx
	// PointAt expects relative index for parent view so we adjust by subtracting the parent offset
	for currentIdx < endIdx && v.PointAt(currentIdx-v.offset).T <= minT {
		currentIdx++
	}
	previousSubView.offset = currentIdx

	// Find size for subview
	size := 0
	for currentIdx < endIdx && v.PointAt(currentIdx-v.offset).T <= maxT {
		size++
		currentIdx++
	}

	previousSubView.size = size
	return previousSubView
}

// These hooks exist so we can override them during unit tests.
var getFPointSliceForRingBuffer = FPointSlicePool.Get
var putFPointSliceForRingBuffer = FPointSlicePool.Put
