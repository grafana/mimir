// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"fmt"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/pool"
)

// FPointRingBuffer and HPointRingBuffer are nearly identical, but exist for each
// specific point type.
// The code is duplicated due to the cost of indirection in golang.
// The tests are combined to ensure consistency. We may wish to consider codegen
// or similar in the future if we end up with more data types etc.
// (see: https://github.com/grafana/mimir/pull/8508#discussion_r1654668995)

type HPointRingBuffer struct {
	memoryConsumptionTracker *limiter.MemoryConsumptionTracker
	points                   []promql.HPoint
	pointsIndexMask          int // Bitmask used to calculate indices into points efficiently. Computing modulo is relatively expensive, but points is always sized as a power of two, so we can a bitmask to calculate remainders cheaply.
	firstIndex               int // Index into 'points' of first point in this buffer.
	size                     int // Number of points in this buffer.
}

func NewHPointRingBuffer(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *HPointRingBuffer {
	return &HPointRingBuffer{memoryConsumptionTracker: memoryConsumptionTracker}
}

// DiscardPointsAtOrBefore discards all points in this buffer with timestamp less than or equal to t.
func (b *HPointRingBuffer) DiscardPointsAtOrBefore(t int64) {
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

// ViewUntilSearchingForwards returns a view into this buffer, including only points with timestamps less than or equal to maxT.
// ViewUntilSearchingForwards examines the points in the buffer starting from the front of the buffer, so is preferred over
// ViewUntilSearchingBackwards if it is expected that there are many points with timestamp greater than maxT, and few points with
// earlier timestamps.
// existing is an existing view instance for this buffer that is reused if provided. It can be nil.
// The returned view is no longer valid if this buffer is modified (eg. a point is added, or the buffer is reset or closed).
func (b *HPointRingBuffer) ViewUntilSearchingForwards(maxT int64, existing *HPointRingBufferView) *HPointRingBufferView {
	if existing == nil {
		existing = &HPointRingBufferView{buffer: b}
	}

	size := 0

	for size < b.size && b.pointAt(size).T <= maxT {
		size++
	}

	existing.offset = 0
	existing.size = size
	return existing
}

// ViewUntilSearchingBackwards is like ViewUntilSearchingForwards, except it examines the points from the end of the buffer, so
// is preferred over ViewUntilSearchingForwards if it is expected that only a few of the points will have timestamp greater than maxT.
func (b *HPointRingBuffer) ViewUntilSearchingBackwards(maxT int64, existing *HPointRingBufferView) *HPointRingBufferView {
	if existing == nil {
		existing = &HPointRingBufferView{buffer: b}
	}

	nextPositionToCheck := b.size - 1

	for nextPositionToCheck >= 0 && b.pointAt(nextPositionToCheck).T > maxT {
		nextPositionToCheck--
	}

	existing.offset = 0
	existing.size = nextPositionToCheck + 1
	return existing
}

// pointAt returns the point at index 'position'.
func (b *HPointRingBuffer) pointAt(position int) promql.HPoint {
	return b.points[(b.firstIndex+position)&b.pointsIndexMask]
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

		if !pool.IsPowerOfTwo(cap(newSlice)) {
			// We rely on the capacity being a power of two for the pointsIndexMask optimisation below.
			// If we can guarantee that newSlice has a capacity that is a power of two in the future, then we can drop this check.
			return nil, fmt.Errorf("pool returned slice of capacity %v (requested %v), but wanted a power of two", cap(newSlice), newSize)
		}

		newSlice = newSlice[:cap(newSlice)]
		pointsAtEnd := b.size - b.firstIndex
		copy(newSlice, b.points[b.firstIndex:])
		copy(newSlice[pointsAtEnd:], b.points[:b.firstIndex])

		// We must clear b.points before returning it to the pool, as the current query could continue using the
		// FloatHistogram instances it contains a reference to, but a later user of b.points may otherwise reuse
		// those instances instead of creating new FloatHistograms.
		clear(b.points)

		putHPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
		b.points = newSlice
		b.firstIndex = 0
		b.pointsIndexMask = cap(newSlice) - 1
	}

	nextIndex := (b.firstIndex + b.size) & b.pointsIndexMask
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

// Reset clears the contents of this buffer, but retains the underlying point slice for future reuse.
func (b *HPointRingBuffer) Reset() {
	b.firstIndex = 0
	b.size = 0
}

// Release clears the contents of this buffer and releases the underlying point slice.
// The buffer can be used again and will acquire a new slice when required.
func (b *HPointRingBuffer) Release() {
	b.Reset()
	putHPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
	b.points = nil
}

// Use replaces the contents of this buffer with s.
// The points in s must be in time order, not contain duplicate timestamps and start at index 0.
// s will be modified in place when the buffer is modified, and callers should not modify s after passing it off to the ring buffer via Use.
// s will be returned to the pool when Close is called, Use is called again, or the buffer needs to expand, so callers
// should not return s to the pool themselves.
// s must have a capacity that is a power of two.
func (b *HPointRingBuffer) Use(s []promql.HPoint) error {
	if !pool.IsPowerOfTwo(cap(s)) {
		// We rely on the capacity being a power of two for the pointsIndexMask optimisation below.
		return fmt.Errorf("slice capacity must be a power of two, but is %v", cap(s))
	}

	putHPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)

	b.points = s[:cap(s)]
	b.firstIndex = 0
	b.size = len(s)
	b.pointsIndexMask = cap(s) - 1
	return nil
}

// Close releases any resources associated with this buffer.
func (b *HPointRingBuffer) Close() {
	putHPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
	b.points = nil
}

type HPointRingBufferView struct {
	buffer *HPointRingBuffer
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
func (v HPointRingBufferView) UnsafePoints() (head []promql.HPoint, tail []promql.HPoint) {
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
// PutHPointSlice when it is no longer needed.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of this buffer.
func (v HPointRingBufferView) CopyPoints() ([]promql.HPoint, error) {
	if v.size == 0 {
		return nil, nil
	}

	head, tail := v.UnsafePoints()
	combined, err := getHPointSliceForRingBuffer(len(head)+len(tail), v.buffer.memoryConsumptionTracker)
	if err != nil {
		return nil, err
	}

	combined = combined[:len(head)+len(tail)]
	i := 0

	combine := func(points []promql.HPoint) {
		for pointIdx := range points {
			combined[i].T = points[pointIdx].T

			if combined[i].H == nil {
				combined[i].H = &histogram.FloatHistogram{}
			}

			points[pointIdx].H.CopyTo(combined[i].H)
			i++
		}
	}

	combine(head)
	combine(tail)

	return combined, nil
}

// ForEach calls f for each point in this buffer view.
func (v HPointRingBufferView) ForEach(f func(p promql.HPoint)) {
	for i := 0; i < v.size; i++ {
		f(v.PointAt(i))
	}
}

// First returns the first point in this ring buffer view.
// It panics if the buffer is empty.
func (v HPointRingBufferView) First() promql.HPoint {
	if v.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return v.PointAt(0)
}

// Last returns the last point in this ring buffer view.
// It returns false if the view is empty.
func (v HPointRingBufferView) Last() (promql.HPoint, bool) {
	if v.size == 0 {
		return promql.HPoint{}, false
	}

	return v.PointAt(v.size - 1), true
}

// Count returns the number of points in this ring buffer view.
func (v HPointRingBufferView) Count() int {
	return v.size
}

// EquivalentFloatSampleCount returns the equivalent number of float samples in this ring buffer view.
func (v HPointRingBufferView) EquivalentFloatSampleCount() int64 {
	count := int64(0)
	head, tail := v.UnsafePoints()

	for _, p := range head {
		count += EquivalentFloatSampleCount(p.H)
	}

	for _, p := range tail {
		count += EquivalentFloatSampleCount(p.H)
	}

	return count
}

// Any returns true if this ring buffer view contains any points.
func (v HPointRingBufferView) Any() bool {
	return v.size != 0
}

// PointAt returns the point at index i in this ring buffer view.
// It panics if i is outside the range of points in this view.
func (v HPointRingBufferView) PointAt(i int) promql.HPoint {
	if i >= v.size {
		panic(fmt.Sprintf("PointAt(): out of range, requested index %v but have length %v", i, v.size))
	}

	return v.buffer.pointAt(v.offset + i)
}

// Clone returns a clone of this view and its underlying ring buffer.
// All histogram.FloatHistogram instances in the underlying buffer are cloned.
// The caller is responsible for closing the returned ring buffer when it is no longer needed.
func (v HPointRingBufferView) Clone() (*HPointRingBufferView, *HPointRingBuffer, error) {
	if v.size == 0 {
		return &HPointRingBufferView{}, nil, nil
	}

	points, err := v.CopyPoints()
	if err != nil {
		return nil, nil, err
	}

	buffer := NewHPointRingBuffer(v.buffer.memoryConsumptionTracker)
	if err := buffer.Use(points); err != nil {
		return nil, nil, err
	}

	view := &HPointRingBufferView{
		buffer: buffer,
		size:   v.size,
	}

	return view, buffer, nil
}

// SubView returns a view with only points in range (minT, maxT].
// If previousSubView is provided, it will be reused to create the new subview. previousSubView must be a previous
// subview for the same parent view and the next subview is assumed to cover a later range (we only start searching from
// after the samples of the previous subview).
func (v *HPointRingBufferView) SubView(minT int64, maxT int64, previousSubView *HPointRingBufferView) *HPointRingBufferView {
	if v.size == 0 {
		if previousSubView == nil {
			return &HPointRingBufferView{}
		}
		previousSubView.offset = v.offset
		previousSubView.size = 0
		return previousSubView
	}

	var startIdx int
	if previousSubView == nil {
		startIdx = v.offset
		previousSubView = &HPointRingBufferView{buffer: v.buffer}
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
	parentIdx := startIdx
	// PointAt expects relative index for parent view so we subtract the parent offset
	for parentIdx < endIdx && v.PointAt(parentIdx-v.offset).T <= minT {
		parentIdx++
	}
	previousSubView.offset = parentIdx

	// Find size for subview
	size := 0
	for parentIdx < endIdx && v.PointAt(parentIdx-v.offset).T <= maxT {
		size++
		parentIdx++
	}

	previousSubView.size = size
	return previousSubView
}

// These hooks exist so we can override them during unit tests.
var getHPointSliceForRingBuffer = HPointSlicePool.Get
var putHPointSliceForRingBuffer = HPointSlicePool.Put
