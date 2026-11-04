// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
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
	generation               int // Incremented when views of this buffer are invalidated.

	// startTimestamps stores each point's start timestamp at the corresponding index in points.
	// A value of 0 means unknown. The slice stays nil until the first non-zero start timestamp is appended.
	// Once allocated, it has the same length as points.
	startTimestamps []int64
}

func NewHPointRingBuffer(memoryConsumptionTracker *limiter.MemoryConsumptionTracker) *HPointRingBuffer {
	return &HPointRingBuffer{memoryConsumptionTracker: memoryConsumptionTracker}
}

// allocateStartTimestamps creates the start timestamp slice for this buffer, with a start timestamp of 0 (unknown)
// for every point already in the buffer.
func (b *HPointRingBuffer) allocateStartTimestamps() error {
	startTimestamps, err := getStartTimestampSliceForRingBuffer(len(b.points), b.memoryConsumptionTracker)
	if err != nil {
		return err
	}

	b.startTimestamps = startTimestamps[:len(b.points)]
	clear(b.startTimestamps)

	return nil
}

// releaseStartTimestamps returns this buffer's start timestamp slice to the pool, if it has one.
func (b *HPointRingBuffer) releaseStartTimestamps() {
	if b.startTimestamps == nil {
		return
	}

	putStartTimestampSliceForRingBuffer(&b.startTimestamps, b.memoryConsumptionTracker)
	b.startTimestamps = nil
}

// DiscardPointsAtOrBefore discards all points in this buffer with timestamp less than or equal to t.
func (b *HPointRingBuffer) DiscardPointsAtOrBefore(t int64) {
	for b.size > 0 && b.points[b.firstIndex].T <= t {
		b.generation++
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

func (b *HPointRingBuffer) RemoveFirst() {
	if b.size > 0 {
		b.generation++
		b.firstIndex++
		b.size--

		if b.firstIndex >= len(b.points) {
			b.firstIndex = 0
		}
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

// AppendWithStartTimestamp adds p to this buffer with start timestamp st, expanding it if required.
// A start timestamp of 0 means unknown.
//
// The buffer starts storing start timestamps on the first non-zero value, so series without them use no extra memory.
//
// The caller must ensure that appending p preserves the buffer's chronological order.
func (b *HPointRingBuffer) AppendWithStartTimestamp(p promql.HPoint, st int64) error {
	hPoint, err := b.NextPointWithStartTimestamp(st)
	if err != nil {
		return err
	}
	hPoint.T = p.T
	hPoint.H = p.H
	return nil
}

// EmptyView returns an empty view associated with this buffer that is marked as dirty. This
// view cannot be used directly and instead must be recreated (using "existing") before being
// used by a caller. This differs from creating an empty HPointRingBufferView directly since
// this instance will be "dirty" while the struct created directly is not.
func (b *HPointRingBuffer) EmptyView() *HPointRingBufferView {
	return &HPointRingBufferView{
		buffer:     b,
		generation: -1,
	}
}

// ViewUntilSearchingForwards returns a view into this buffer, including only points with timestamps less than or equal to maxT.
// ViewUntilSearchingForwards examines the points in the buffer starting from the front of the buffer, so is preferred over
// ViewUntilSearchingBackwards if it is expected that there are many points with timestamp greater than maxT, and few points with
// earlier timestamps.
// existing is an existing view instance for this buffer that is reused if provided. It can be nil.
// The returned view is no longer valid if the buffer is modified in a way that causes HPointRingBufferView.IsDirty() to return true.
func (b *HPointRingBuffer) ViewUntilSearchingForwards(maxT int64, existing *HPointRingBufferView) *HPointRingBufferView {
	if existing == nil {
		existing = &HPointRingBufferView{buffer: b}
	}

	size := 0

	for size < b.size && b.PointAt(size).T <= maxT {
		size++
	}

	existing.generation = b.generation
	existing.offset = 0
	existing.size = size
	return existing
}

// ViewAll returns a view which includes all points in the ring buffer.
// The returned view is no longer valid if the buffer is modified in a way that causes HPointRingBufferView.IsDirty() to return true.
func (b *HPointRingBuffer) ViewAll(existing *HPointRingBufferView) *HPointRingBufferView {
	if existing == nil {
		existing = &HPointRingBufferView{buffer: b}
	}

	existing.generation = b.generation
	existing.offset = 0
	existing.size = b.size
	return existing
}

// ViewUntilSearchingBackwards is like ViewUntilSearchingForwards, except it examines the points from the end of the buffer, so
// is preferred over ViewUntilSearchingForwards if it is expected that only a few of the points will have timestamp greater than maxT.
// The returned view is no longer valid if the buffer is modified in a way that causes HPointRingBufferView.IsDirty() to return true.
func (b *HPointRingBuffer) ViewUntilSearchingBackwards(maxT int64, existing *HPointRingBufferView) *HPointRingBufferView {
	if existing == nil {
		existing = &HPointRingBufferView{buffer: b}
	}

	nextPositionToCheck := b.size - 1

	for nextPositionToCheck >= 0 && b.PointAt(nextPositionToCheck).T > maxT {
		nextPositionToCheck--
	}

	existing.generation = b.generation
	existing.offset = 0
	existing.size = nextPositionToCheck + 1
	return existing
}

// ViewBetweenSearchingBackwards returns a view into this buffer including only points with timestamps
// greater than minT and less than or equal to maxT. The method panics if minT is greater than maxT.
// The returned view is no longer valid if the buffer is modified in a way that causes HPointRingBufferView.IsDirty() to return true.
func (b *HPointRingBuffer) ViewBetweenSearchingBackwards(minT, maxT int64, existing *HPointRingBufferView) *HPointRingBufferView {
	if existing == nil {
		existing = &HPointRingBufferView{buffer: b}
	}

	if minT > maxT {
		assert.Unreachable("attempted to create an HPointRingBufferView with minT > maxT", map[string]any{
			"min_t": minT,
			"max_t": maxT,
		})
		panic(fmt.Sprintf("attempted to create an HPointRingBufferView with minT(%d) > maxT(%d) (this is a bug)", minT, maxT))
	}

	existing.generation = b.generation

	// If the buffer is empty or min time is beyond the last point in this buffer,
	// return a zero-sized view since there are no points or no matching points.
	if b.size == 0 || b.Last().T < minT {
		existing.offset = 0
		existing.size = 0
		return existing
	}

	var start int
	for start = b.size - 1; start >= 0; start-- {
		if b.PointAt(start).T <= minT {
			break
		}
	}

	var end int
	for end = b.size - 1; end >= 0; end-- {
		if b.PointAt(end).T <= maxT {
			break
		}
	}

	existing.offset = start + 1
	existing.size = end - start

	return existing
}

// PointAt returns the point at index 'position'.
func (b *HPointRingBuffer) PointAt(position int) promql.HPoint {
	return b.points[(b.firstIndex+position)&b.pointsIndexMask]
}

// StartTimestampAt returns the start timestamp of the point at position, or 0 if it is unknown.
// The caller must check that position is within the buffer's bounds.
func (b *HPointRingBuffer) StartTimestampAt(position int) int64 {
	if b.startTimestamps == nil {
		return 0
	}

	return b.startTimestamps[(b.firstIndex+position)&b.pointsIndexMask]
}

// Last returns the last point in the buffer.
// Note that it is the caller's responsibility to have checked that the buffer size is not empty.
func (b *HPointRingBuffer) Last() promql.HPoint {
	return b.PointAt(b.size - 1)
}

// Count returns the current number of points in the buffer.
func (b *HPointRingBuffer) Count() int {
	return b.size
}

// NextPoint gets the next point in this buffer, expanding it if required.
//
// The returned point's timestamp (HPoint.T) must be set to greater than or equal
// to the timestamp of the last point in the buffer before further methods
// are called on this buffer (with the exception of RemoveLastPoint, Reset or Close).
//
// This method allows reusing an existing HPoint in this buffer where possible,
// reducing the number of FloatHistograms allocated.
//
// The returned point's start timestamp is unknown (0). Use NextPointWithStartTimestamp to store a start timestamp.
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

		var newStartTimestamps []int64
		if b.startTimestamps != nil {
			newStartTimestamps, err = getStartTimestampSliceForRingBuffer(len(newSlice), b.memoryConsumptionTracker)
			if err != nil {
				// Only let a copy of the slice header escape on error, so successful resizes don't allocate one.
				sliceToRelease := newSlice
				putHPointSliceForRingBuffer(&sliceToRelease, b.memoryConsumptionTracker)
				return nil, err
			}

			newStartTimestamps = newStartTimestamps[:len(newSlice)]
		}

		pointsAtEnd := b.size - b.firstIndex
		copy(newSlice, b.points[b.firstIndex:])
		copy(newSlice[pointsAtEnd:], b.points[:b.firstIndex])

		if newStartTimestamps != nil {
			copy(newStartTimestamps, b.startTimestamps[b.firstIndex:])
			copy(newStartTimestamps[pointsAtEnd:], b.startTimestamps[:b.firstIndex])

			putStartTimestampSliceForRingBuffer(&b.startTimestamps, b.memoryConsumptionTracker)
			b.startTimestamps = newStartTimestamps
		}

		// We must clear b.points before returning it to the pool, as the current query could continue using the
		// FloatHistogram instances it contains a reference to, but a later user of b.points may otherwise reuse
		// those instances instead of creating new FloatHistograms.
		clear(b.points)

		putHPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
		b.points = newSlice
		b.firstIndex = 0
		b.pointsIndexMask = cap(newSlice) - 1
		b.generation++
	}

	nextIndex := (b.firstIndex + b.size) & b.pointsIndexMask

	if b.startTimestamps != nil {
		// This point's start timestamp is unknown. Clear the start timestamp of the point that was previously at this index.
		b.startTimestamps[nextIndex] = 0
	}

	b.size++

	return &b.points[nextIndex], nil
}

// NextPointWithStartTimestamp is like NextPoint, but also stores st as the returned point's start timestamp.
// A start timestamp of 0 means unknown.
//
// The buffer starts storing start timestamps on the first non-zero value, so series without them use no extra memory.
//
// If the returned point is then removed with RemoveLastPoint, its start timestamp is removed with it.
func (b *HPointRingBuffer) NextPointWithStartTimestamp(st int64) (*promql.HPoint, error) {
	// NextPoint expands the start timestamp slice along with the points, and sets the new point's start timestamp to 0.
	hPoint, err := b.NextPoint()
	if err != nil {
		return nil, err
	}

	if st == 0 {
		return hPoint, nil
	}

	if b.startTimestamps == nil {
		if err := b.allocateStartTimestamps(); err != nil {
			// The caller won't populate the point NextPoint returned, so remove it.
			b.RemoveLastPoint()
			return nil, err
		}
	}

	b.startTimestamps[(b.firstIndex+b.size-1)&b.pointsIndexMask] = st

	return hPoint, nil
}

// RemoveLastPoint removes the last point that was allocated.
// This is used for when NextPoint or NextPointWithStartTimestamp allocates a point that is then unused and
// needs to be returned to the ring buffer.
// This occurs when a histogram point has a stale marker.
// It panics if the buffer is empty.
func (b *HPointRingBuffer) RemoveLastPoint() {
	if b.size == 0 {
		panic("There are no points to remove")
	}

	b.generation++
	b.size--
	if b.size == 0 {
		b.firstIndex = 0
	}
}

// CountUntil returns the number of samples in this buffer with timestamp up to and including maxT.
func (b *HPointRingBuffer) CountUntil(maxT int64) int {
	count := b.size

	for count > 0 && b.PointAt(count-1).T > maxT {
		count--
	}

	return count
}

// CountBetween returns the number of samples in this buffer with timestamp after minT and up to and including maxT.
func (b *HPointRingBuffer) CountBetween(minT, maxT int64) int {
	// Why do this rather than just loop over all the elements?
	// It's expected that most of the elements in the buffer are in the time range, so it's faster
	// to check for points outside the range at the start and end to count how many are in the range.
	countToMaxT := b.CountUntil(maxT)

	if countToMaxT == 0 {
		return 0
	}

	countAtOrBeforeMinT := 0

	for countAtOrBeforeMinT < countToMaxT && b.PointAt(countAtOrBeforeMinT).T <= minT {
		countAtOrBeforeMinT++
	}

	return countToMaxT - countAtOrBeforeMinT
}

// Reset clears the contents of this buffer, but retains the underlying point and start timestamp slices for future reuse.
func (b *HPointRingBuffer) Reset() {
	b.generation++
	b.firstIndex = 0
	b.size = 0
}

// Release clears the contents of this buffer and releases the underlying point and start timestamp slices.
// The buffer can be used again and will acquire new slices when required.
func (b *HPointRingBuffer) Release() {
	b.Reset()
	putHPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
	b.points = nil
	b.releaseStartTimestamps()
}

// Use replaces the contents of this buffer with s.
// The points in s must be in time order, not contain duplicate timestamps and start at index 0.
// s will be modified in place when the buffer is modified, and callers should not modify s after passing it off to the ring buffer via Use.
// s will be returned to the pool when Close is called, Use is called again, or the buffer needs to expand, so callers
// should not return s to the pool themselves.
// s must have a capacity that is a power of two.
// The points in s have no start timestamps, so Use releases any start timestamps this buffer has.
func (b *HPointRingBuffer) Use(s []promql.HPoint) error {
	if !pool.IsPowerOfTwo(cap(s)) {
		// We rely on the capacity being a power of two for the pointsIndexMask optimisation below.
		return fmt.Errorf("slice capacity must be a power of two, but is %v", cap(s))
	}

	putHPointSliceForRingBuffer(&b.points, b.memoryConsumptionTracker)
	b.releaseStartTimestamps()

	b.generation++
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
	b.releaseStartTimestamps()
}

type HPointRingBufferView struct {
	buffer     *HPointRingBuffer
	offset     int // Offset from buffer's firstIndex where this view starts
	size       int
	generation int // Generation of the buffer when this view was created
}

// IsDirty returns true if this view is associated with a buffer and the buffer
// has been modified in a way that invalidates the view. Not all changes to the
// buffer invalidate the view. When it is the case that the view has been
// invalidated, the view must be recreated before being used by callers.
func (v *HPointRingBufferView) IsDirty() bool {
	if v.buffer == nil {
		return false
	}

	return v.generation != v.buffer.generation
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
func (v *HPointRingBufferView) UnsafePoints() (head []promql.HPoint, tail []promql.HPoint) {
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

// UnsafePointsInIndexRange returns the points with logical indices in [first, last] (inclusive) as
// up to two contiguous slices. The second slice is non-empty only when the requested range wraps
// the underlying ring buffer. It is the index-range equivalent of UnsafePoints, and lets callers
// iterate a sub-range with a plain slice range loop instead of repeated PointAt calls.
// Like UnsafePoints, the returned slices alias the buffer and must not be modified nor returned to
// a pool.
func (v *HPointRingBufferView) UnsafePointsInIndexRange(first, last int) (head []promql.HPoint, tail []promql.HPoint) {
	if first < 0 || last >= v.size || first > last {
		panic("HPointRingBufferView.UnsafePointsInIndexRange(): index out of range")
	}

	// A view starting at v.offset+first holding (last-first+1) points has the same wraparound
	// structure as any other view, so delegate to UnsafePoints to keep that logic in one place.
	sub := HPointRingBufferView{buffer: v.buffer, offset: v.offset + first, size: last - first + 1}
	return sub.UnsafePoints()
}

// HasStartTimestamps returns true if this view's buffer stores start timestamps.
//
// Storage is allocated when the first non-zero start timestamp is appended.
// If this returns false, every point in the view has an unknown start timestamp (0).
// If this returns true, some or all start timestamps may still be 0.
func (v *HPointRingBufferView) HasStartTimestamps() bool {
	return v.buffer != nil && v.buffer.startTimestamps != nil
}

// UnsafeStartTimestamps returns start timestamp slices for the points in this view.
// The head and tail slices match the corresponding slices from UnsafePoints, with one start timestamp per point.
// A value of 0 means unknown. Both slices are nil if HasStartTimestamps returns false.
//
// As with UnsafePoints, callers must not modify the returned slices or return them to a pool.
// The slices become invalid when the buffer is modified.
func (v *HPointRingBufferView) UnsafeStartTimestamps() (head []int64, tail []int64) {
	// This repeats UnsafePoints' indexing to keep both sets of slices aligned. A shared helper made
	// UnsafePoints too expensive to inline.
	// Check the size before accessing v.buffer, since an empty view may have no buffer.
	if v.size == 0 || v.buffer.startTimestamps == nil {
		return nil, nil
	}

	startTimestamps := v.buffer.startTimestamps
	startIndex := (v.buffer.firstIndex + v.offset) & v.buffer.pointsIndexMask
	endOfHeadSegment := startIndex + v.size

	if endOfHeadSegment > len(startTimestamps) {
		// Need to wrap around.
		endOfTailSegment := endOfHeadSegment - len(startTimestamps)
		endOfHeadSegment = len(startTimestamps)
		return startTimestamps[startIndex:endOfHeadSegment], startTimestamps[0:endOfTailSegment]
	}

	return startTimestamps[startIndex:endOfHeadSegment], nil
}

// CopyPoints returns a single slice of the points in this buffer view.
// Callers may modify the values in the returned slice, and should return the slice to the pool by calling
// PutHPointSlice when it is no longer needed.
// Calling UnsafePoints is more efficient than calling CopyPoints, as CopyPoints will create a new slice and copy all
// points into the slice, whereas UnsafePoints returns a view into the internal state of this buffer.
func (v *HPointRingBufferView) CopyPoints() ([]promql.HPoint, error) {
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
func (v *HPointRingBufferView) ForEach(f func(p promql.HPoint)) {
	for i := 0; i < v.size; i++ {
		f(v.PointAt(i))
	}
}

// First returns the first point in this ring buffer view.
// It panics if the buffer is empty.
func (v *HPointRingBufferView) First() promql.HPoint {
	if v.size == 0 {
		panic("Can't get first element of empty buffer")
	}

	return v.PointAt(0)
}

// Last returns the last point in this ring buffer view.
// It returns false if the view is empty.
func (v *HPointRingBufferView) Last() (promql.HPoint, bool) {
	if v.size == 0 {
		return promql.HPoint{}, false
	}

	return v.PointAt(v.size - 1), true
}

// Count returns the number of points in this ring buffer view.
func (v *HPointRingBufferView) Count() int {
	return v.size
}

// EquivalentFloatSampleCount returns the equivalent number of float samples in this ring buffer view.
func (v *HPointRingBufferView) EquivalentFloatSampleCount() int64 {
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

// EquivalentFloatSampleCountUntil returns the number of samples in this buffer with timestamp up to and including maxT.
func (b *HPointRingBuffer) EquivalentFloatSampleCountUntil(maxT int64) int64 {
	count := int64(0)

	for i := range b.size {
		p := b.PointAt(i)

		if p.T > maxT {
			// We've reached the end of the range we need to search.
			break
		}

		count += EquivalentFloatSampleCount(p.H)
	}

	return count
}

// EquivalentFloatSampleCountBetween returns the number of samples in this buffer with timestamp after minT and up to and including maxT.
func (b *HPointRingBuffer) EquivalentFloatSampleCountBetween(minT, maxT int64) int64 {
	count := int64(0)

	for i := range b.size {
		p := b.PointAt(i)

		if p.T > maxT {
			// We've reached the end of the range we need to search.
			break
		} else if p.T <= minT {
			continue
		}

		count += EquivalentFloatSampleCount(p.H)
	}

	return count
}

// Any returns true if this ring buffer view contains any points.
func (v *HPointRingBufferView) Any() bool {
	return v.size != 0
}

// PointAt returns the point at index i in this ring buffer view.
// It panics if i is outside the range of points in this view.
func (v *HPointRingBufferView) PointAt(i int) promql.HPoint {
	if i >= v.size {
		// A constant-string panic (rather than fmt.Sprintf) keeps PointAt cheap enough to be
		// inlined into hot loops, e.g. the per-sample passes in extendedHistogramRate.
		panic("HPointRingBufferView.PointAt(): index out of range")
	}

	return v.buffer.PointAt(v.offset + i)
}

// StartTimestampAt returns the start timestamp of the point at index i in this ring buffer view, or 0 if it is unknown.
// It panics if the index is outside the range of points in this view.
func (v *HPointRingBufferView) StartTimestampAt(i int) int64 {
	if i >= v.size {
		panic("HPointRingBufferView.StartTimestampAt(): index out of range")
	}

	return v.buffer.StartTimestampAt(v.offset + i)
}

// Clone returns a clone of this view and its underlying ring buffer.
// All histogram.FloatHistogram instances in the underlying buffer are cloned.
// The caller is responsible for closing the returned ring buffer when it is no longer needed.
// The clone preserves this view's start timestamps. Its buffer only allocates start timestamp storage
// if the original buffer has it.
func (v *HPointRingBufferView) Clone() (*HPointRingBufferView, *HPointRingBuffer, error) {
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

	if v.HasStartTimestamps() {
		if err := buffer.allocateStartTimestamps(); err != nil {
			buffer.Close()
			return nil, nil, err
		}

		// Use put the points at index 0 onwards, so copy the start timestamps to the same indices.
		head, tail := v.UnsafeStartTimestamps()
		n := copy(buffer.startTimestamps, head)
		copy(buffer.startTimestamps[n:], tail)
	}

	// Ensure that the newly created buffer (and hence view) has the same generation
	// as this view so they will compare as equal to this view (some tests rely on this).
	buffer.generation = v.generation
	view := buffer.ViewAll(nil)

	return view, buffer, nil
}

// These hooks exist so we can override them during unit tests.
var getHPointSliceForRingBuffer = HPointSlicePool.Get
var putHPointSliceForRingBuffer = HPointSlicePool.Put
