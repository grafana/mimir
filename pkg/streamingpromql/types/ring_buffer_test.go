// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/limiter"
)

// We want to ensure FPoint+HPoint ring buffers are tested consistently,
// and we don't care about performance here so we can use an interface+generics.
type ringBuffer[T any] interface {
	DiscardPointsAtOrBefore(t int64)
	Append(p T) error
	Reset()
	Use(s []T) error
	Release()
	ViewUntilSearchingForwardsForTesting(maxT int64) ringBufferView[T]
	ViewUntilSearchingBackwardsForTesting(maxT int64) ringBufferView[T]
	GetPoints() []T
	GetFirstIndex() int
	GetTimestamp(point T) int64
}

type ringBufferView[T any] interface {
	ForEach(f func(p T))
	UnsafePoints() (head []T, tail []T)
	CopyPoints() ([]T, error)
	Last() (T, bool)
	Count() int
	Any() bool
	First() T
	PointAt(i int) T
}

func TestRingBuffer(t *testing.T) {
	setupRingBufferTestingPools(t)

	t.Run("test FPoint ring buffer", func(t *testing.T) {
		points := []promql.FPoint{
			{T: 1, F: 100},
			{T: 2, F: 200},
			{T: 3, F: 300},
			{T: 4, F: 400},
			{T: 5, F: 500},
			{T: 6, F: 600},
			{T: 7, F: 700},
			{T: 8, F: 800},
			{T: 9, F: 900},
		}
		buf := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))}
		testRingBuffer(t, buf, points)
	})

	t.Run("test HPoint ring buffer", func(t *testing.T) {
		points := []promql.HPoint{
			{T: 1, H: &histogram.FloatHistogram{Count: 100}},
			{T: 2, H: &histogram.FloatHistogram{Count: 200}},
			{T: 3, H: &histogram.FloatHistogram{Count: 300}},
			{T: 4, H: &histogram.FloatHistogram{Count: 400}},
			{T: 5, H: &histogram.FloatHistogram{Count: 500}},
			{T: 6, H: &histogram.FloatHistogram{Count: 600}},
			{T: 7, H: &histogram.FloatHistogram{Count: 700}},
			{T: 8, H: &histogram.FloatHistogram{Count: 800}},
			{T: 9, H: &histogram.FloatHistogram{Count: 900}},
		}
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))}
		testRingBuffer(t, buf, points)
	})
}

func testRingBuffer[T any](t *testing.T, buf ringBuffer[T], points []T) {
	shouldHaveNoPoints(t, buf)

	buf.DiscardPointsAtOrBefore(0) // Should handle empty buffer.
	shouldHaveNoPoints(t, buf)

	require.NoError(t, buf.Append(points[0]))
	shouldHavePoints(t, buf, points[:1]...)

	require.NoError(t, buf.Append(points[1]))
	shouldHavePoints(t, buf, points[:2]...)

	buf.DiscardPointsAtOrBefore(0)
	shouldHavePoints(t, buf, points[:2]...) // No change.

	buf.DiscardPointsAtOrBefore(1)
	shouldHavePoints(t, buf, points[1:2]...)

	require.NoError(t, buf.Append(points[2]))
	shouldHavePoints(t, buf, points[1:3]...)

	buf.DiscardPointsAtOrBefore(3)
	shouldHaveNoPoints(t, buf)

	require.NoError(t, buf.Append(points[3]))
	require.NoError(t, buf.Append(points[4]))
	shouldHavePoints(t, buf, points[3:5]...)

	// Trigger expansion of buffer (we resize in powers of two, and the underlying slice comes from a pool that uses a factor of 2 as well).
	// Ideally we wouldn't reach into the internals here, but this helps ensure the test is testing the correct scenario.
	require.Len(t, buf.GetPoints(), 2, "expected underlying slice to have length 2, if this assertion fails, the test setup is not as expected")
	require.Equal(t, 2, cap(buf.GetPoints()), "expected underlying slice to have capacity 2, if this assertion fails, the test setup is not as expected")
	require.NoError(t, buf.Append(points[5]))
	require.NoError(t, buf.Append(points[6]))
	require.Greater(t, cap(buf.GetPoints()), 2, "expected underlying slice to be expanded, if this assertion fails, the test setup is not as expected")

	shouldHavePoints(t, buf, points[3:7]...)

	buf.Reset()
	shouldHaveNoPoints(t, buf)

	require.NoError(t, buf.Append(points[8]))
	shouldHavePoints(t, buf, points[8])

	pointsWithPowerOfTwoCapacity := make([]T, 0, 16) // Use must be passed a slice with a capacity that is equal to a power of 2.
	pointsWithPowerOfTwoCapacity = append(pointsWithPowerOfTwoCapacity, points...)
	err := buf.Use(pointsWithPowerOfTwoCapacity)
	require.NoError(t, err)
	shouldHavePoints(t, buf, points...)

	buf.DiscardPointsAtOrBefore(4)
	shouldHavePoints(t, buf, points[4:]...)

	buf.Release()
	shouldHaveNoPoints(t, buf)

	subsliceWithPowerOfTwoCapacity := make([]T, 0, 8) // Use must be passed a slice with a capacity that is equal to a power of 2.
	subsliceWithPowerOfTwoCapacity = append(subsliceWithPowerOfTwoCapacity, points[4:]...)
	err = buf.Use(subsliceWithPowerOfTwoCapacity)
	require.NoError(t, err)
	shouldHavePoints(t, buf, points[4:]...)

	nonPowerOfTwoSlice := make([]T, 0, 15)
	err = buf.Use(nonPowerOfTwoSlice)
	require.EqualError(t, err, "slice capacity must be a power of two, but is 15",
		"Error message should indicate the invalid capacity")
}

func TestRingBuffer_DiscardPointsBefore_ThroughWrapAround(t *testing.T) {
	setupRingBufferTestingPools(t)

	t.Run("test FPointRingBuffer", func(t *testing.T) {
		points := []promql.FPoint{
			{T: 1, F: 100},
			{T: 2, F: 200},
			{T: 3, F: 300},
			{T: 4, F: 400},
			{T: 5, F: 500},
			{T: 6, F: 600},
		}
		buf := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))}
		testDiscardPointsBeforeThroughWrapAround(t, buf, points)
	})

	t.Run("test HPointRingBuffer", func(t *testing.T) {
		points := []promql.HPoint{
			{T: 1, H: &histogram.FloatHistogram{Count: 100}},
			{T: 2, H: &histogram.FloatHistogram{Count: 200}},
			{T: 3, H: &histogram.FloatHistogram{Count: 300}},
			{T: 4, H: &histogram.FloatHistogram{Count: 400}},
			{T: 5, H: &histogram.FloatHistogram{Count: 500}},
			{T: 6, H: &histogram.FloatHistogram{Count: 600}},
		}
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))}
		testDiscardPointsBeforeThroughWrapAround(t, buf, points)
	})
}

func testDiscardPointsBeforeThroughWrapAround[T any](t *testing.T, buf ringBuffer[T], points []T) {
	// Set up the buffer so that the first point is part-way through the underlying slice.
	// We resize in powers of two, and the underlying slice comes from a pool that uses a factor of 2 as well.

	for _, p := range points[:4] {
		require.NoError(t, buf.Append(p))
	}

	// Ideally we wouldn't reach into the internals here, but this helps ensure the test is testing the correct scenario.
	require.Len(t, buf.GetPoints(), 4, "expected underlying slice to have length 4, if this assertion fails, the test setup is not as expected")
	require.Equal(t, 4, cap(buf.GetPoints()), "expected underlying slice to have capacity 4, if this assertion fails, the test setup is not as expected")
	buf.DiscardPointsAtOrBefore(2)
	require.NoError(t, buf.Append(points[4]))
	require.NoError(t, buf.Append(points[5]))

	// Should not have expanded slice.
	require.Len(t, buf.GetPoints(), 4, "expected underlying slice to have length 4")
	require.Equal(t, 4, cap(buf.GetPoints()), "expected underlying slice to have capacity 4")

	// Discard before end of underlying slice.
	buf.DiscardPointsAtOrBefore(3)
	shouldHavePoints(t, buf, points[3:6]...)

	require.Equal(t, 3, buf.GetFirstIndex(), "expected first point to be in middle of underlying slice, if this assertion fails, the test setup is not as expected")

	// Discard after wraparound.
	buf.DiscardPointsAtOrBefore(5)
	shouldHavePoints(t, buf, points[5])
}

func TestRingBuffer_RemoveLastPoint(t *testing.T) {
	setupRingBufferTestingPools(t)

	points := []promql.HPoint{
		{T: 1, H: &histogram.FloatHistogram{Count: 100}},
		{T: 2, H: &histogram.FloatHistogram{Count: 200}},
		{T: 3, H: &histogram.FloatHistogram{Count: 300}},
		{T: 4, H: &histogram.FloatHistogram{Count: 400}},
		{T: 5, H: &histogram.FloatHistogram{Count: 500}},
		{T: 6, H: &histogram.FloatHistogram{Count: 600}},
	}

	buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))}

	t.Run("test removing points until none exist", func(t *testing.T) {
		buf.Reset()
		for _, p := range points[:2] {
			require.NoError(t, buf.Append(p))
		}

		shouldHavePoints(t, buf, points[:2]...)
		require.Equal(t, 2, len(buf.GetPoints()))
		require.Equal(t, 2, buf.size)

		nextPoint, err := buf.NextPoint()
		require.NoError(t, err)
		require.Equal(t, 4, len(buf.GetPoints()), "underlying slice has expanded by a power of 2")
		require.Equal(t, 3, buf.size, "The size has increase to accommodate the next point")

		*nextPoint = points[2]
		// We assign "NextPoint" points[2], and then check it is in the ring
		shouldHavePoints(t, buf, points[:3]...)

		// However, now we decide that we don't actually want that point
		buf.RemoveLastPoint()
		shouldHavePoints(t, buf, points[:2]...)
		require.Equal(t, 4, len(buf.GetPoints()), "underlying slice is still 4 since it was allocated")
		require.Equal(t, 2, buf.size, "the size of the ring is reduced back down since we didn't use the 'NextPoint'")

		buf.RemoveLastPoint()
		shouldHavePoints(t, buf, points[:1]...)
		require.Equal(t, 4, len(buf.GetPoints()), "underlying slice remains the same")
		require.Equal(t, 1, buf.size, "size is reduced")

		buf.RemoveLastPoint()
		shouldHaveNoPoints(t, buf)
		require.Equal(t, 4, len(buf.GetPoints()), "underlying slice remains the same")
		require.Equal(t, 0, buf.size, "size is reduced")
		require.Equal(t, 0, buf.GetFirstIndex(), "the firstIndex is reset to 0 when the size reaches 0")

		require.Panics(t, func() { buf.RemoveLastPoint() }, "expected panic when removing point from empty buffer")
	})

	t.Run("test removing points at wrap around", func(t *testing.T) {
		buf.Reset()

		// Set up the buffer so that the first point is part-way through the underlying slice.
		// We resize in powers of two, and the underlying slice comes from a pool that uses a factor of 2 as well.

		for _, p := range points[:4] {
			require.NoError(t, buf.Append(p))
		}

		require.Len(t, buf.GetPoints(), 4, "expected underlying slice to have length 4, if this assertion fails, the test setup is not as expected")
		require.Equal(t, 4, cap(buf.GetPoints()), "expected underlying slice to have capacity 4, if this assertion fails, the test setup is not as expected")
		require.Equal(t, 4, buf.size, "The size includes all points")
		buf.DiscardPointsAtOrBefore(2)
		require.Equal(t, 2, buf.size, "The size is reduced by the removed points")
		require.Equal(t, 2, buf.GetFirstIndex(), "the firstIndex is half way through the ring")

		// Check we only have the expected points
		shouldHavePoints(t, buf, points[2:4]...)

		nextPoint, err := buf.NextPoint()
		require.NoError(t, err)
		require.Equal(t, 4, len(buf.GetPoints()), "underlying slice remains the same")
		require.Equal(t, 3, buf.size, "The size has increased")

		*nextPoint = points[4]
		// We assign "NextPoint" points[4], and then check it is in the ring
		// This should be at the wrapped around point
		shouldHavePoints(t, buf, points[2:5]...)

		// Remove the point at the wrap around
		buf.RemoveLastPoint()
		shouldHavePoints(t, buf, points[2:4]...)
		require.Equal(t, 4, len(buf.GetPoints()), "underlying slice is still 4")
		require.Equal(t, 2, buf.size, "the size of the ring is reduced back down since we didn't use the 'NextPoint'")
	})
}

func TestRingBuffer_ViewUntilWithExistingView(t *testing.T) {
	t.Run("FPoint ring buffer", func(t *testing.T) {
		buf := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
		require.NoError(t, buf.Append(promql.FPoint{T: 1, F: 100}))
		require.NoError(t, buf.Append(promql.FPoint{T: 2, F: 200}))
		require.NoError(t, buf.Append(promql.FPoint{T: 3, F: 300}))
		require.NoError(t, buf.Append(promql.FPoint{T: 4, F: 400}))

		view := buf.ViewUntilSearchingForwards(2, nil)
		viewShouldHavePoints(t, view, promql.FPoint{T: 1, F: 100}, promql.FPoint{T: 2, F: 200})

		// Test that reusing the view with ViewUntilSearchingForwards works correctly.
		newView := buf.ViewUntilSearchingForwards(1, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.FPoint{T: 1, F: 100})

		// Test that reusing the view with ViewUntilSearchingBackwards works correctly.
		newView = buf.ViewUntilSearchingBackwards(3, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.FPoint{T: 1, F: 100}, promql.FPoint{T: 2, F: 200}, promql.FPoint{T: 3, F: 300})
	})

	t.Run("HPoint ring buffer", func(t *testing.T) {
		h1 := &histogram.FloatHistogram{Count: 100}
		h2 := &histogram.FloatHistogram{Count: 200}
		h3 := &histogram.FloatHistogram{Count: 300}
		h4 := &histogram.FloatHistogram{Count: 400}

		buf := NewHPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
		require.NoError(t, buf.Append(promql.HPoint{T: 1, H: h1}))
		require.NoError(t, buf.Append(promql.HPoint{T: 2, H: h2}))
		require.NoError(t, buf.Append(promql.HPoint{T: 3, H: h3}))
		require.NoError(t, buf.Append(promql.HPoint{T: 4, H: h4}))

		view := buf.ViewUntilSearchingForwards(2, nil)
		viewShouldHavePoints(t, view, promql.HPoint{T: 1, H: h1}, promql.HPoint{T: 2, H: h2})

		// Test that reusing the view with ViewUntilSearchingForwards works correctly.
		newView := buf.ViewUntilSearchingForwards(1, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.HPoint{T: 1, H: h1})

		// Test that reusing the view with ViewUntilSearchingBackwards works correctly.
		newView = buf.ViewUntilSearchingBackwards(3, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.HPoint{T: 1, H: h1}, promql.HPoint{T: 2, H: h2}, promql.HPoint{T: 3, H: h3})
	})
}

func shouldHaveNoPoints[T any](t *testing.T, buf ringBuffer[T]) {
	shouldHavePoints(
		t,
		buf,
		/* nothing */
	)
}

func shouldHavePoints[T any](t *testing.T, buf ringBuffer[T], expected ...T) {
	var pointsFromForEachAfterSearchingForwards []T

	buf.ViewUntilSearchingForwardsForTesting(math.MaxInt64).ForEach(func(p T) {
		pointsFromForEachAfterSearchingForwards = append(pointsFromForEachAfterSearchingForwards, p)
	})

	require.Equal(t, expected, pointsFromForEachAfterSearchingForwards)

	var pointsFromForEachAfterSearchingBackwards []T

	buf.ViewUntilSearchingBackwardsForTesting(math.MaxInt64).ForEach(func(p T) {
		pointsFromForEachAfterSearchingBackwards = append(pointsFromForEachAfterSearchingBackwards, p)
	})

	require.Equal(t, expected, pointsFromForEachAfterSearchingBackwards)

	if len(expected) == 0 {
		shouldHavePointsAtOrBeforeTime(t, buf, math.MaxInt64, expected...)

		_, present := buf.ViewUntilSearchingForwardsForTesting(math.MaxInt64).Last()
		require.False(t, present)

		_, present = buf.ViewUntilSearchingBackwardsForTesting(math.MaxInt64).Last()
		require.False(t, present)
	} else {
		require.Equal(t, expected[0], buf.ViewUntilSearchingForwardsForTesting(math.MaxInt64).First())
		require.Equal(t, expected[0], buf.ViewUntilSearchingBackwardsForTesting(math.MaxInt64).First())
		// We test LastAtOrBefore() below.

		lastPointT := buf.GetTimestamp(expected[len(expected)-1])

		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT+1, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT-1, expected[:len(expected)-1]...)
	}
}

func shouldHavePointsAtOrBeforeTime[T any](t *testing.T, buf ringBuffer[T], ts int64, expected ...T) {
	viewShouldHavePoints(t, buf.ViewUntilSearchingForwardsForTesting(ts), expected...)
	viewShouldHavePoints(t, buf.ViewUntilSearchingBackwardsForTesting(ts), expected...)
}

func viewShouldHavePoints[T any](t *testing.T, view ringBufferView[T], expected ...T) {
	head, tail := view.UnsafePoints()
	combinedPoints := append(head, tail...)

	if len(expected) == 0 {
		require.Len(t, combinedPoints, 0)
		require.False(t, view.Any())
	} else {
		require.Equal(t, expected, combinedPoints)
		require.True(t, view.Any())
		require.NotEmpty(t, head, "head slice should not be empty for non-empty view")
	}

	require.Equal(t, len(expected), view.Count())

	copiedPoints, err := view.CopyPoints()
	require.NoError(t, err)
	if len(expected) == 0 {
		require.Nil(t, copiedPoints)
	} else {
		require.Equal(t, expected, copiedPoints)
	}

	end, present := view.Last()

	if len(expected) == 0 {
		require.False(t, present)
	} else {
		require.True(t, present)
		require.Equal(t, expected[len(expected)-1], end)
	}

	for idx, expectedPoint := range expected {
		actualPoint := view.PointAt(idx)
		require.Equal(t, expectedPoint, actualPoint)
	}
}

// Wrapper for FPointRingBuffer to work around indirection to get points
type fPointRingBufferWrapper struct {
	*FPointRingBuffer
}

func (w *fPointRingBufferWrapper) ViewUntilSearchingForwardsForTesting(maxT int64) ringBufferView[promql.FPoint] {
	return w.ViewUntilSearchingForwards(maxT, nil)
}

func (w *fPointRingBufferWrapper) ViewUntilSearchingBackwardsForTesting(maxT int64) ringBufferView[promql.FPoint] {
	return w.ViewUntilSearchingBackwards(maxT, nil)
}

func (w *fPointRingBufferWrapper) GetPoints() []promql.FPoint {
	return w.points
}

func (w *fPointRingBufferWrapper) GetFirstIndex() int {
	return w.firstIndex
}

func (w *fPointRingBufferWrapper) GetTimestamp(point promql.FPoint) int64 {
	return point.T
}

// Wrapper for HPointRingBuffer to work around indirection to get points
type hPointRingBufferWrapper struct {
	*HPointRingBuffer
}

func (w *hPointRingBufferWrapper) ViewUntilSearchingForwardsForTesting(maxT int64) ringBufferView[promql.HPoint] {
	return w.ViewUntilSearchingForwards(maxT, nil)
}

func (w *hPointRingBufferWrapper) ViewUntilSearchingBackwardsForTesting(maxT int64) ringBufferView[promql.HPoint] {
	return w.ViewUntilSearchingBackwards(maxT, nil)
}

func (w *hPointRingBufferWrapper) GetPoints() []promql.HPoint {
	return w.points
}

func (w *hPointRingBufferWrapper) GetFirstIndex() int {
	return w.firstIndex
}

func (w *hPointRingBufferWrapper) GetTimestamp(point promql.HPoint) int64 {
	return point.T
}

func TestRingBuffer_FPointView_Cloning(t *testing.T) {
	originalBuffer := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
	require.NoError(t, originalBuffer.Append(promql.FPoint{T: 0, F: 10}))
	require.NoError(t, originalBuffer.Append(promql.FPoint{T: 1, F: 11}))

	originalView := originalBuffer.ViewUntilSearchingBackwards(2, nil)
	clonedView, clonedBuffer, err := originalView.Clone()
	require.NoError(t, err)
	require.NotSame(t, originalView, clonedView)
	require.NotSame(t, originalBuffer, clonedBuffer)
	require.NotSame(t, &originalBuffer.points[0], &clonedBuffer.points[0], "cloned buffer should not share the same underlying slice")
	require.Equal(t, originalView.Count(), clonedView.Count())

	originalPoints, err := originalView.CopyPoints()
	require.NoError(t, err)
	clonedPoints, err := clonedView.CopyPoints()
	require.NoError(t, err)

	require.Equal(t, originalPoints, clonedPoints, "cloned views should contain same samples")
}

func TestRingBuffer_HPointView_Cloning(t *testing.T) {
	originalBuffer := NewHPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
	h1 := &histogram.FloatHistogram{Count: 100}
	h2 := &histogram.FloatHistogram{Count: 200}
	require.NoError(t, originalBuffer.Append(promql.HPoint{T: 0, H: h1}))
	require.NoError(t, originalBuffer.Append(promql.HPoint{T: 1, H: h2}))

	originalView := originalBuffer.ViewUntilSearchingBackwards(2, nil)
	clonedView, clonedBuffer, err := originalView.Clone()
	require.NoError(t, err)
	require.NotSame(t, originalView, clonedView)
	require.NotSame(t, originalBuffer, clonedBuffer)
	require.NotSame(t, &originalBuffer.points[0], &clonedBuffer.points[0], "cloned buffer should not share the same underlying slice")
	require.Equal(t, originalView.Count(), clonedView.Count())

	// We can't use CopyPoints below as that will create copies of the histograms as well, and we want to check that the two buffers don't share histogram instances.
	originalHead, originalTail := originalView.UnsafePoints()
	clonedHead, clonedTail := clonedView.UnsafePoints()

	originalPoints := append(originalHead, originalTail...)
	clonedPoints := append(clonedHead, clonedTail...)

	require.Equal(t, originalPoints, clonedPoints, "cloned views should contain same samples")
	require.Len(t, clonedPoints, 2)
	require.NotSame(t, originalPoints[0].H, clonedPoints[0].H, "cloned points should not share the same histogram instances")
	require.NotSame(t, originalPoints[1].H, clonedPoints[1].H, "cloned points should not share the same histogram instances")
}

func TestRingBufferView_SubView(t *testing.T) {
	setupRingBufferTestingPools(t)

	t.Run("FPoint ring buffer", func(t *testing.T) {
		testCases := []struct {
			name        string
			wraparound  bool
			setupBuffer func(*testing.T) *FPointRingBufferView
		}{
			{
				name:       "without wraparound",
				wraparound: false,
				setupBuffer: func(t *testing.T) *FPointRingBufferView {
					memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
					buf := NewFPointRingBuffer(memoryTracker)
					points := []promql.FPoint{
						{T: 10, F: 100},
						{T: 20, F: 200},
						{T: 30, F: 300},
						{T: 40, F: 400},
					}
					for _, p := range points {
						require.NoError(t, buf.Append(p))
					}
					return buf.ViewUntilSearchingBackwards(40, nil)
				},
			},
			{
				name:       "with wraparound",
				wraparound: true,
				setupBuffer: func(t *testing.T) *FPointRingBufferView {
					memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
					buf := NewFPointRingBuffer(memoryTracker)
					// Strategy: Create a buffer with wraparound where newer samples wrap to the beginning.
					// Final buffer: [T=40, T=10, T=20, T=30] with firstIndex=1, size=4

					// Step 1: Fill buffer to capacity 4 with T=1 (to discard), T=10, T=20, T=30
					require.NoError(t, buf.Append(promql.FPoint{T: 1, F: 10}))
					require.NoError(t, buf.Append(promql.FPoint{T: 10, F: 100}))
					require.NoError(t, buf.Append(promql.FPoint{T: 20, F: 200}))
					require.NoError(t, buf.Append(promql.FPoint{T: 30, F: 300}))

					// Step 2: Discard first point (T=1)
					buf.DiscardPointsAtOrBefore(1)

					// Step 3: Add T=40, which wraps to position 0 (overwrites discarded T=1)
					require.NoError(t, buf.Append(promql.FPoint{T: 40, F: 400}))

					// Verify wraparound occurred
					view := buf.ViewUntilSearchingBackwards(40, nil)
					head, tail := view.UnsafePoints()
					if len(tail) == 0 {
						panic("expected wraparound: tail should be non-nil and non-empty")
					}
					if len(head) == 0 {
						panic("expected wraparound: head should be non-empty")
					}

					return view
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fullView := tc.setupBuffer(t)

				require.Equal(t, 4, fullView.Count())

				// Test SubView with range (15, 35] - should get points at T=20, T=30
				subView := fullView.SubView(15, 35, nil)
				require.Equal(t, 2, subView.Count())
				require.Equal(t, int64(20), subView.First().T)
				last, hasLast := subView.Last()
				require.True(t, hasLast)
				require.Equal(t, int64(30), last.T)

				// Test other methods on this subview
				require.True(t, subView.Any())

				require.Equal(t, int64(20), subView.PointAt(0).T)
				require.Equal(t, int64(30), subView.PointAt(1).T)

				head, tail := subView.UnsafePoints()
				require.Equal(t, []promql.FPoint{{T: 20, F: 200}, {T: 30, F: 300}}, head)
				require.Len(t, tail, 0)

				var seenCount int
				subView.ForEach(func(p promql.FPoint) {
					seenCount++
				})
				require.Equal(t, 2, seenCount)

				copied, err := subView.CopyPoints()
				require.NoError(t, err)
				require.Equal(t, 2, len(copied))
				require.Equal(t, int64(20), copied[0].T)
				require.Equal(t, int64(30), copied[1].T)
				putFPointSliceForRingBuffer(&copied, subView.buffer.memoryConsumptionTracker)

				clonedView, clonedBuffer, err := subView.Clone()
				require.NoError(t, err)
				if clonedBuffer != nil {
					defer clonedBuffer.Close()
				}
				require.Equal(t, 2, clonedView.Count())
				require.Equal(t, int64(20), clonedView.First().T)
				clonedLast, _ := clonedView.Last()
				require.Equal(t, int64(30), clonedLast.T)

				// Test SubView with range (5, 45] - should get all points
				subView = fullView.SubView(5, 45, nil)
				require.Equal(t, 4, subView.Count())
				require.Equal(t, int64(10), subView.First().T)

				// Test SubView with range (25, 45] - should get points at T=30, T=40
				subView = fullView.SubView(25, 45, nil)
				require.Equal(t, 2, subView.Count())
				require.Equal(t, int64(30), subView.First().T)
				last, hasLast = subView.Last()
				require.True(t, hasLast)
				require.Equal(t, int64(40), last.T)

				head, tail = subView.UnsafePoints()
				if tc.wraparound {
					require.Equal(t, []promql.FPoint{{T: 30, F: 300}}, head, "subview(25,45) should have T=30 in head")
					require.Equal(t, []promql.FPoint{{T: 40, F: 400}}, tail, "subview(25,45) should have T=40 in tail")
				} else {
					require.Equal(t, []promql.FPoint{{T: 30, F: 300}, {T: 40, F: 400}}, head)
					require.Len(t, tail, 0)
				}

				// Test SubView with range (60, 70] - should get no points
				subView = fullView.SubView(60, 70, nil)
				require.Equal(t, 0, subView.Count())
				require.False(t, subView.Any())
				emptyHead, emptyTail := subView.UnsafePoints()
				require.Nil(t, emptyHead)
				require.Nil(t, emptyTail)

				// Test SubView with range (5, 15] - should get point at T=10
				subView = fullView.SubView(5, 15, nil)
				require.Equal(t, 1, subView.Count())
				require.Equal(t, int64(10), subView.First().T)
			})
		}
	})

	t.Run("HPoint ring buffer", func(t *testing.T) {
		testCases := []struct {
			name        string
			wraparound  bool
			setupBuffer func(*testing.T) *HPointRingBufferView
		}{
			{
				name:       "without wraparound",
				wraparound: false,
				setupBuffer: func(t *testing.T) *HPointRingBufferView {
					memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
					buf := NewHPointRingBuffer(memoryTracker)
					points := []promql.HPoint{
						{T: 10, H: &histogram.FloatHistogram{Count: 100}},
						{T: 20, H: &histogram.FloatHistogram{Count: 200}},
						{T: 30, H: &histogram.FloatHistogram{Count: 300}},
						{T: 40, H: &histogram.FloatHistogram{Count: 400}},
					}
					for _, p := range points {
						require.NoError(t, buf.Append(p))
					}
					return buf.ViewUntilSearchingBackwards(40, nil)
				},
			},
			{
				name:       "with wraparound",
				wraparound: true,
				setupBuffer: func(t *testing.T) *HPointRingBufferView {
					memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
					buf := NewHPointRingBuffer(memoryTracker)

					require.NoError(t, buf.Append(promql.HPoint{T: 1, H: &histogram.FloatHistogram{Count: 10}}))
					require.NoError(t, buf.Append(promql.HPoint{T: 10, H: &histogram.FloatHistogram{Count: 100}}))
					require.NoError(t, buf.Append(promql.HPoint{T: 20, H: &histogram.FloatHistogram{Count: 200}}))
					require.NoError(t, buf.Append(promql.HPoint{T: 30, H: &histogram.FloatHistogram{Count: 300}}))

					buf.DiscardPointsAtOrBefore(1)

					require.NoError(t, buf.Append(promql.HPoint{T: 40, H: &histogram.FloatHistogram{Count: 400}}))

					view := buf.ViewUntilSearchingBackwards(40, nil)
					head, tail := view.UnsafePoints()
					if len(tail) == 0 {
						panic("expected wraparound: tail should be non-nil and non-empty")
					}
					if len(head) == 0 {
						panic("expected wraparound: head should be non-empty")
					}

					return view
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fullView := tc.setupBuffer(t)

				require.Equal(t, 4, fullView.Count())

				// Test SubView with range (15, 35] - should get points at T=20, T=30
				subView := fullView.SubView(15, 35, nil)
				require.Equal(t, 2, subView.Count())
				require.Equal(t, int64(20), subView.First().T)
				last, hasLast := subView.Last()
				require.True(t, hasLast)
				require.Equal(t, int64(30), last.T)

				require.True(t, subView.Any())

				require.Equal(t, int64(20), subView.PointAt(0).T)
				require.Equal(t, int64(30), subView.PointAt(1).T)

				head, tail := subView.UnsafePoints()
				require.Equal(t, 2, len(head))
				require.Equal(t, int64(20), head[0].T)
				require.Equal(t, float64(200), head[0].H.Count)
				require.Equal(t, int64(30), head[1].T)
				require.Equal(t, float64(300), head[1].H.Count)
				require.Len(t, tail, 0)

				var seenCount int
				subView.ForEach(func(p promql.HPoint) {
					seenCount++
				})
				require.Equal(t, 2, seenCount)

				copied, err := subView.CopyPoints()
				require.NoError(t, err)
				require.Equal(t, 2, len(copied))
				require.Equal(t, int64(20), copied[0].T)
				require.Equal(t, int64(30), copied[1].T)
				putHPointSliceForRingBuffer(&copied, subView.buffer.memoryConsumptionTracker)

				clonedView, clonedBuffer, err := subView.Clone()
				require.NoError(t, err)
				if clonedBuffer != nil {
					defer clonedBuffer.Close()
				}
				require.Equal(t, 2, clonedView.Count())
				require.Equal(t, int64(20), clonedView.First().T)
				clonedLast, _ := clonedView.Last()
				require.Equal(t, int64(30), clonedLast.T)

				// Test SubView with range (5, 45] - should get all points
				subView = fullView.SubView(5, 45, nil)
				require.Equal(t, 4, subView.Count())
				require.Equal(t, int64(10), subView.First().T)

				// Test SubView with range (25, 45] - should get points at T=30, T=40
				subView = fullView.SubView(25, 45, nil)
				require.Equal(t, 2, subView.Count())
				require.Equal(t, int64(30), subView.First().T)
				last, hasLast = subView.Last()
				require.True(t, hasLast)
				require.Equal(t, int64(40), last.T)

				head, tail = subView.UnsafePoints()
				if tc.wraparound {
					require.Equal(t, 1, len(head))
					require.Equal(t, int64(30), head[0].T)
					require.Equal(t, float64(300), head[0].H.Count)
					require.Equal(t, 1, len(tail))
					require.Equal(t, int64(40), tail[0].T)
					require.Equal(t, float64(400), tail[0].H.Count)
				} else {
					require.Equal(t, 2, len(head))
					require.Equal(t, int64(30), head[0].T)
					require.Equal(t, float64(300), head[0].H.Count)
					require.Equal(t, int64(40), head[1].T)
					require.Equal(t, float64(400), head[1].H.Count)
					require.Len(t, tail, 0)
				}

				// Test SubView with range (60, 70] - should get no points
				subView = fullView.SubView(60, 70, nil)
				require.Equal(t, 0, subView.Count())
				require.False(t, subView.Any())
				emptyHead, emptyTail := subView.UnsafePoints()
				require.Nil(t, emptyHead)
				require.Nil(t, emptyTail)

				// Test SubView with range (5, 15] - should get point at T=10
				subView = fullView.SubView(5, 15, nil)
				require.Equal(t, 1, subView.Count())
				require.Equal(t, int64(10), subView.First().T)
			})
		}
	})
}

func TestRingBufferView_SubView_ConsecutiveRanges(t *testing.T) {
	t.Run("FPoint ring buffer", func(t *testing.T) {
		buffer := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))

		// Add points at T=10, 20, 30, 40, 50, 60, 70, 80, 90, 100
		for i := int64(1); i <= 10; i++ {
			err := buffer.Append(promql.FPoint{T: i * 10, F: float64(i)})
			require.NoError(t, err)
		}

		fullView := buffer.ViewUntilSearchingForwards(101, nil)
		require.Equal(t, 10, fullView.Count())

		// Create consecutive non-overlapping subviews: (5,30], (30,60], (60,85]
		// First range: (5, 30] - should get points at T=10, 20, 30
		subView1 := fullView.SubView(5, 30, nil)
		require.Equal(t, 3, subView1.Count())
		require.Equal(t, int64(10), subView1.First().T)
		last1, _ := subView1.Last()
		require.Equal(t, int64(30), last1.T)

		// Second range: (30, 60] - should get points at T=40, 50, 60
		subView2 := fullView.SubView(30, 60, subView1)
		require.Equal(t, 3, subView2.Count())
		require.Equal(t, int64(40), subView2.First().T)
		last2, _ := subView2.Last()
		require.Equal(t, int64(60), last2.T)

		// Third range: (60, 85] - should get points at T=70, 80
		subView3 := fullView.SubView(60, 85, subView2)
		require.Equal(t, 2, subView3.Count())
		require.Equal(t, int64(70), subView3.First().T)
		last3, _ := subView3.Last()
		require.Equal(t, int64(80), last3.T)
	})

	t.Run("HPoint ring buffer", func(t *testing.T) {
		buffer := NewHPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))

		// Add points at T=10, 20, 30, 40, 50, 60, 70, 80, 90, 100
		for i := int64(1); i <= 10; i++ {
			err := buffer.Append(promql.HPoint{T: i * 10, H: &histogram.FloatHistogram{Count: float64(i)}})
			require.NoError(t, err)
		}

		fullView := buffer.ViewUntilSearchingForwards(101, nil)
		require.Equal(t, 10, fullView.Count())

		// Create consecutive non-overlapping subviews: (5,30], (30,60], (60,85]
		// First range: (5, 30] - should get points at T=10, 20, 30
		subView1 := fullView.SubView(5, 30, nil)
		require.Equal(t, 3, subView1.Count())
		require.Equal(t, int64(10), subView1.First().T)
		last1, _ := subView1.Last()
		require.Equal(t, int64(30), last1.T)

		// Second range: (30, 60] - should get points at T=40, 50, 60
		subView2 := fullView.SubView(30, 60, subView1)
		require.Equal(t, 3, subView2.Count())
		require.Equal(t, int64(40), subView2.First().T)
		last2, _ := subView2.Last()
		require.Equal(t, int64(60), last2.T)

		// Third range: (60, 85] - should get points at T=70, 80
		subView3 := fullView.SubView(60, 85, subView2)
		require.Equal(t, 2, subView3.Count())
		require.Equal(t, int64(70), subView3.First().T)
		last3, _ := subView3.Last()
		require.Equal(t, int64(80), last3.T)
	})
}

func TestRingBufferView_SubView_OnSubView(t *testing.T) {
	t.Run("FPoint ring buffer", func(t *testing.T) {
		testCases := []struct {
			name        string
			setupBuffer func(*testing.T) *FPointRingBufferView
		}{
			{
				name: "without wraparound",
				setupBuffer: func(t *testing.T) *FPointRingBufferView {
					memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
					buf := NewFPointRingBuffer(memoryTracker)
					// Add points T=10, 20, 30, 40, 50, 60, 70, 80, 90, 100
					for i := int64(1); i <= 10; i++ {
						require.NoError(t, buf.Append(promql.FPoint{T: i * 10, F: float64(i * 100)}))
					}
					return buf.ViewUntilSearchingForwards(100, nil)
				},
			},
			{
				name: "with wraparound",
				setupBuffer: func(t *testing.T) *FPointRingBufferView {
					memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
					buf := NewFPointRingBuffer(memoryTracker)
					// Add initial points to fill capacity
					require.NoError(t, buf.Append(promql.FPoint{T: 1, F: 10}))
					require.NoError(t, buf.Append(promql.FPoint{T: 2, F: 10}))
					require.NoError(t, buf.Append(promql.FPoint{T: 10, F: 100}))
					require.NoError(t, buf.Append(promql.FPoint{T: 20, F: 200}))
					require.NoError(t, buf.Append(promql.FPoint{T: 30, F: 300}))
					// Discard T=1 to make room
					buf.DiscardPointsAtOrBefore(2)
					// Add remaining points to cause wraparound
					for i := int64(4); i <= 10; i++ {
						require.NoError(t, buf.Append(promql.FPoint{T: i * 10, F: float64(i * 100)}))
					}
					// Now buffer has [T=10,20,30,40,50,60,70,80,90,100] with wraparound
					return buf.ViewUntilSearchingForwards(100, nil)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fullView := tc.setupBuffer(t)
				require.Equal(t, 10, fullView.Count())

				// Create a subview (30, 70] from fullView: [40, 50, 60, 70]
				subView1 := fullView.SubView(30, 70, nil)
				require.Equal(t, 4, subView1.Count())
				require.Equal(t, int64(40), subView1.First().T)
				last1, _ := subView1.Last()
				require.Equal(t, int64(70), last1.T)

				// Create a subview of the subview: (50, 70] should give us [60, 70]
				subView2 := subView1.SubView(50, 70, nil)
				require.Equal(t, 2, subView2.Count())
				require.Equal(t, int64(60), subView2.First().T)
				last2, _ := subView2.Last()
				require.Equal(t, int64(70), last2.T)

				// Subview that extends beyond subView1's range should only contain points from subView1
				subView3 := subView1.SubView(20, 100, nil)
				require.Equal(t, 4, subView3.Count())
				require.Equal(t, int64(40), subView3.First().T)
				last3, _ := subView3.Last()
				require.Equal(t, int64(70), last3.T)
			})
		}
	})

	t.Run("HPoint ring buffer", func(t *testing.T) {
		testCases := []struct {
			name        string
			setupBuffer func(*testing.T) *HPointRingBufferView
		}{
			{
				name: "without wraparound",
				setupBuffer: func(t *testing.T) *HPointRingBufferView {
					memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
					buf := NewHPointRingBuffer(memoryTracker)
					// Add points T=10, 20, 30, 40, 50, 60, 70, 80, 90, 100
					for i := int64(1); i <= 10; i++ {
						require.NoError(t, buf.Append(promql.HPoint{T: i * 10, H: &histogram.FloatHistogram{Count: float64(i * 100)}}))
					}
					return buf.ViewUntilSearchingForwards(100, nil)
				},
			},
			{
				name: "with wraparound",
				setupBuffer: func(t *testing.T) *HPointRingBufferView {
					memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
					buf := NewHPointRingBuffer(memoryTracker)
					// Add initial points to fill capacity
					require.NoError(t, buf.Append(promql.HPoint{T: 1, H: &histogram.FloatHistogram{Count: 10}}))
					require.NoError(t, buf.Append(promql.HPoint{T: 2, H: &histogram.FloatHistogram{Count: 10}}))
					require.NoError(t, buf.Append(promql.HPoint{T: 10, H: &histogram.FloatHistogram{Count: 100}}))
					require.NoError(t, buf.Append(promql.HPoint{T: 20, H: &histogram.FloatHistogram{Count: 200}}))
					require.NoError(t, buf.Append(promql.HPoint{T: 30, H: &histogram.FloatHistogram{Count: 300}}))
					// Discard T=1 to make room
					buf.DiscardPointsAtOrBefore(2)
					// Add remaining points to cause wraparound
					for i := int64(4); i <= 10; i++ {
						require.NoError(t, buf.Append(promql.HPoint{T: i * 10, H: &histogram.FloatHistogram{Count: float64(i * 100)}}))
					}
					// Now buffer has [T=10,20,30,40,50,60,70,80,90,100] with wraparound
					return buf.ViewUntilSearchingForwards(100, nil)
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				fullView := tc.setupBuffer(t)
				require.Equal(t, 10, fullView.Count())

				// Create a subview (30, 70] from fullView: [40, 50, 60, 70]
				subView1 := fullView.SubView(30, 70, nil)
				require.Equal(t, 4, subView1.Count())
				require.Equal(t, int64(40), subView1.First().T)
				last1, _ := subView1.Last()
				require.Equal(t, int64(70), last1.T)

				// Create a subview of the subview: (50, 70] should give us [60, 70]
				subView2 := subView1.SubView(50, 70, nil)
				require.Equal(t, 2, subView2.Count())
				require.Equal(t, int64(60), subView2.First().T)
				last2, _ := subView2.Last()
				require.Equal(t, int64(70), last2.T)

				// Subview that extends beyond subView1's range should only contain points from subView1
				subView3 := subView1.SubView(20, 100, nil)
				require.Equal(t, 4, subView3.Count())
				require.Equal(t, int64(40), subView3.First().T)
				last3, _ := subView3.Last()
				require.Equal(t, int64(70), last3.T)
			})
		}
	})
}

func TestRingBufferView_ReuseWithNonZeroOffset(t *testing.T) {
	t.Run("FPoint ViewUntilSearchingForwards resets offset", func(t *testing.T) {
		memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
		buf := NewFPointRingBuffer(memoryTracker)

		// Add points T=10, 20, 30, 40, 50
		for i := int64(1); i <= 5; i++ {
			require.NoError(t, buf.Append(promql.FPoint{T: i * 10, F: float64(i * 100)}))
		}

		fullView := buf.ViewUntilSearchingForwards(50, nil)
		subView := fullView.SubView(25, 45, nil)
		require.Equal(t, 2, subView.Count())
		require.Equal(t, int64(30), subView.First().T)

		reusedView := buf.ViewUntilSearchingForwards(40, subView)
		require.Equal(t, 4, reusedView.Count())
		require.Equal(t, int64(10), reusedView.First().T)
	})

	t.Run("FPoint ViewUntilSearchingBackwards resets offset", func(t *testing.T) {
		memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
		buf := NewFPointRingBuffer(memoryTracker)

		for i := int64(1); i <= 5; i++ {
			require.NoError(t, buf.Append(promql.FPoint{T: i * 10, F: float64(i * 100)}))
		}

		fullView := buf.ViewUntilSearchingBackwards(50, nil)
		subView := fullView.SubView(25, 45, nil)
		require.Equal(t, 2, subView.Count())

		reusedView := buf.ViewUntilSearchingBackwards(40, subView)
		require.Equal(t, 4, reusedView.Count())
		require.Equal(t, int64(10), reusedView.First().T, "First point should be T=10, not affected by previous offset")
	})

	t.Run("FPoint ViewAll resets offset", func(t *testing.T) {
		memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
		buf := NewFPointRingBuffer(memoryTracker)

		for i := int64(1); i <= 5; i++ {
			require.NoError(t, buf.Append(promql.FPoint{T: i * 10, F: float64(i * 100)}))
		}

		fullView := buf.ViewAll(nil)
		subView := fullView.SubView(25, 45, nil)
		require.Equal(t, 2, subView.Count())

		reusedView := buf.ViewAll(subView)
		require.Equal(t, 5, reusedView.Count())
		require.Equal(t, int64(10), reusedView.First().T)
	})

	t.Run("HPoint ViewUntilSearchingForwards resets offset", func(t *testing.T) {
		memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
		buf := NewHPointRingBuffer(memoryTracker)

		for i := int64(1); i <= 5; i++ {
			require.NoError(t, buf.Append(promql.HPoint{T: i * 10, H: &histogram.FloatHistogram{Count: float64(i * 100)}}))
		}

		fullView := buf.ViewUntilSearchingForwards(50, nil)
		subView := fullView.SubView(25, 45, nil)
		require.Equal(t, 2, subView.Count())
		require.Equal(t, int64(30), subView.First().T)

		reusedView := buf.ViewUntilSearchingForwards(40, subView)
		require.Equal(t, 4, reusedView.Count())
		require.Equal(t, int64(10), reusedView.First().T)
	})

	t.Run("HPoint ViewUntilSearchingBackwards resets offset", func(t *testing.T) {
		memoryTracker := limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, "")
		buf := NewHPointRingBuffer(memoryTracker)

		for i := int64(1); i <= 5; i++ {
			require.NoError(t, buf.Append(promql.HPoint{T: i * 10, H: &histogram.FloatHistogram{Count: float64(i * 100)}}))
		}

		fullView := buf.ViewUntilSearchingBackwards(50, nil)
		subView := fullView.SubView(25, 45, nil)
		require.Equal(t, 2, subView.Count())

		reusedView := buf.ViewUntilSearchingBackwards(40, subView)
		require.Equal(t, 4, reusedView.Count())
		require.Equal(t, int64(10), reusedView.First().T)
	})
}

// setupRingBufferTestingPools sets up dummy pool implementations for testing ring buffers.
//
// This helps ensure that the tests behave as expected: the default global pool does not guarantee that
// slices returned have exactly the capacity requested. Instead, it only guarantees that slices have
// capacity at least as large as requested. This makes it difficult to consistently test scenarios like
// wraparound.
func setupRingBufferTestingPools(t *testing.T) {
	originalGetFPointSlice := getFPointSliceForRingBuffer
	originalPutFPointSlice := putFPointSliceForRingBuffer
	originalGetHPointSlice := getHPointSliceForRingBuffer
	originalPutHPointSlice := putHPointSliceForRingBuffer

	getFPointSliceForRingBuffer = func(size int, _ *limiter.MemoryConsumptionTracker) ([]promql.FPoint, error) {
		return make([]promql.FPoint, 0, size), nil
	}

	putFPointSliceForRingBuffer = func(_ *[]promql.FPoint, _ *limiter.MemoryConsumptionTracker) {}

	getHPointSliceForRingBuffer = func(size int, _ *limiter.MemoryConsumptionTracker) ([]promql.HPoint, error) {
		return make([]promql.HPoint, 0, size), nil
	}

	putHPointSliceForRingBuffer = func(_ *[]promql.HPoint, _ *limiter.MemoryConsumptionTracker) {}

	t.Cleanup(func() {
		getFPointSliceForRingBuffer = originalGetFPointSlice
		putFPointSliceForRingBuffer = originalPutFPointSlice
		getHPointSliceForRingBuffer = originalGetHPointSlice
		putHPointSliceForRingBuffer = originalPutHPointSlice
	})
}

func TestFPointRingBuffer_RemoveLast(t *testing.T) {
	testCases := map[string]struct {
		buff     []promql.FPoint
		expected []promql.FPoint
	}{
		"empty buffer": {
			buff:     []promql.FPoint{},
			expected: nil,
		},
		"single point": {
			buff:     []promql.FPoint{{T: 10, F: 20}},
			expected: nil,
		},
		"multiple points": {
			buff:     []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
			expected: []promql.FPoint{{T: 10, F: 20}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(tc.buff))
			buff.RemoveLast()
			shouldHavePoints(t, &fPointRingBufferWrapper{FPointRingBuffer: buff}, tc.expected...)
		})
	}
}

func TestFPointRingBuffer_RemoveFirst(t *testing.T) {
	testCases := map[string]struct {
		buff     []promql.FPoint
		expected []promql.FPoint
	}{
		"empty buff": {
			buff:     []promql.FPoint{},
			expected: nil,
		},
		"single point": {
			buff:     []promql.FPoint{{T: 10, F: 20}},
			expected: nil,
		},
		"multiple points": {
			buff:     []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
			expected: []promql.FPoint{{T: 20, F: 20}},
		},
	}
	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(tc.buff))
			buff.RemoveFirst()
			shouldHavePoints(t, &fPointRingBufferWrapper{FPointRingBuffer: buff}, tc.expected...)
		})
	}
}

func TestFPointRingBuffer_ReplaceLast(t *testing.T) {
	testCases := map[string]struct {
		tail     promql.FPoint
		err      string
		buff     []promql.FPoint
		expected []promql.FPoint
	}{
		"empty buff": {
			tail: promql.FPoint{T: 10, F: 20},
			buff: []promql.FPoint{},
			err:  "unable to replace point to the tail of the buffer - current buffer is empty",
		},
		"single point - replace same timestamp": {
			tail:     promql.FPoint{T: 10, F: 30},
			buff:     []promql.FPoint{{T: 10, F: 20}},
			expected: []promql.FPoint{{T: 10, F: 30}},
		},
		"single point - replace different timestamp": {
			tail:     promql.FPoint{T: 20, F: 20},
			buff:     []promql.FPoint{{T: 10, F: 20}},
			expected: []promql.FPoint{{T: 20, F: 20}},
		},
		"multiple points": {
			tail:     promql.FPoint{T: 30, F: 20},
			buff:     []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 30, F: 20}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(tc.buff))
			err := buff.ReplaceLast(tc.tail)
			if len(tc.err) > 0 {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			shouldHavePoints(t, &fPointRingBufferWrapper{FPointRingBuffer: buff}, tc.expected...)
		})
	}
}

func TestFPointRingBuffer_ReplaceFirst(t *testing.T) {
	testCases := map[string]struct {
		head     promql.FPoint
		err      string
		buff     []promql.FPoint
		expected []promql.FPoint
	}{
		"empty buff": {
			buff: []promql.FPoint{},
			err:  "unable to replace point to the head of the buffer - current buffer is empty",
		},
		"single point - replace same timestamp": {
			head:     promql.FPoint{T: 10, F: 30},
			buff:     []promql.FPoint{{T: 10, F: 20}},
			expected: []promql.FPoint{{T: 10, F: 30}},
		},
		"single point - replace different timestamp": {
			head:     promql.FPoint{T: 20, F: 20},
			buff:     []promql.FPoint{{T: 10, F: 20}},
			expected: []promql.FPoint{{T: 20, F: 20}},
		},
		"multiple points": {
			head:     promql.FPoint{T: 5, F: 20},
			buff:     []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
			expected: []promql.FPoint{{T: 5, F: 20}, {T: 20, F: 20}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(tc.buff))
			err := buff.ReplaceFirst(tc.head)
			if len(tc.err) > 0 {
				require.ErrorContains(t, err, tc.err)
				return
			}
			require.NoError(t, err)
			shouldHavePoints(t, &fPointRingBufferWrapper{FPointRingBuffer: buff}, tc.expected...)
		})
	}
}

func TestFPointRingBuffer_AppendSlice(t *testing.T) {

	testCases := map[string]struct {
		append   []promql.FPoint
		buff     []promql.FPoint
		expected []promql.FPoint
	}{
		"empty buffer - nil slice": {
			append:   nil,
			buff:     []promql.FPoint{},
			expected: nil,
		},
		"empty buffer - empty slice": {
			append:   []promql.FPoint{},
			buff:     []promql.FPoint{},
			expected: nil,
		},
		"empty buffer - slice len 1": {
			append:   []promql.FPoint{{T: 10, F: 20}},
			buff:     []promql.FPoint{},
			expected: []promql.FPoint{{T: 10, F: 20}},
		},
		"empty buffer - slice len not a power of 2": {
			append:   []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}},
			buff:     []promql.FPoint{},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}},
		},
		"empty buffer - slice len power of 2": {
			append:   []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}, {T: 40, F: 20}},
			buff:     []promql.FPoint{},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}, {T: 40, F: 20}},
		},

		"not empty buffer - nil slice": {
			append:   nil,
			buff:     []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
		},
		"not empty buffer - empty slice": {
			append:   []promql.FPoint{},
			buff:     []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
		},
		"not empty buffer - slice len 1": {
			append:   []promql.FPoint{{T: 30, F: 20}},
			buff:     []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}},
		},
		"not empty buffer - larger slice": {
			append:   []promql.FPoint{{T: 50, F: 20}, {T: 60, F: 20}, {T: 70, F: 20}},
			buff:     []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}, {T: 40, F: 20}},
			expected: []promql.FPoint{{T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}, {T: 40, F: 20}, {T: 50, F: 20}, {T: 60, F: 20}, {T: 70, F: 20}},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
			require.NoError(t, buff.Use(tc.buff))
			err := buff.AppendSlice(tc.append)
			require.NoError(t, err)
			shouldHavePoints(t, &fPointRingBufferWrapper{FPointRingBuffer: buff}, tc.expected...)
		})
	}
}

func TestFPointRingBuffer_AppendAtStart_FirstPointAlignment(t *testing.T) {
	buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
	require.NoError(t, buff.Append(promql.FPoint{T: 10, F: 20}))
	require.Equal(t, 1, buff.size)
	require.Equal(t, 0, buff.firstIndex)
	require.Len(t, buff.points, 2)

	// inserting another point will not grow the underlying buffer, so the new point is stored in the upper end of the existing 2 slot buffer
	require.NoError(t, buff.AppendAtStart(promql.FPoint{T: 9, F: 20}))
	require.Equal(t, 2, buff.size)
	require.Equal(t, 1, buff.firstIndex)
	require.Len(t, buff.points, 2)
	require.Equal(t, promql.FPoint{T: 9, F: 20}, buff.PointAt(0))

	// inserting another point will grow the underlying buffer - since we are growing the buffer we can re-align so that the firstIndex is 0 for the new head
	require.NoError(t, buff.AppendAtStart(promql.FPoint{T: 8, F: 20}))
	require.Equal(t, 3, buff.size)
	require.Equal(t, 0, buff.firstIndex)
	require.Len(t, buff.points, 4)
	require.Equal(t, promql.FPoint{T: 8, F: 20}, buff.PointAt(0))

	// inserting another point will not grow the underlying buffer, so the new point is stored at the end of the existing buffer
	require.NoError(t, buff.AppendAtStart(promql.FPoint{T: 7, F: 20}))
	require.Equal(t, 4, buff.size)
	require.Equal(t, 3, buff.firstIndex)
	require.Len(t, buff.points, 4)
	require.Equal(t, promql.FPoint{T: 7, F: 20}, buff.PointAt(0))

	// inserting another point will grow the underlying buffer - since we are growing the buffer we can re-align so that the firstIndex is 0 for the new head
	require.NoError(t, buff.AppendAtStart(promql.FPoint{T: 6, F: 20}))
	require.Equal(t, 5, buff.size)
	require.Equal(t, 0, buff.firstIndex)
	require.Len(t, buff.points, 8)
	require.Equal(t, promql.FPoint{T: 6, F: 20}, buff.PointAt(0))
}

func TestFPointRingBuffer_AppendSlice_Alignment(t *testing.T) {
	buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
	// insert 2 samples - the first point will be at the tail of the ring
	require.NoError(t, buff.AppendAtStart(promql.FPoint{T: 10, F: 20}))
	require.NoError(t, buff.AppendAtStart(promql.FPoint{T: 5, F: 20}))

	require.Equal(t, 2, buff.size)
	require.Equal(t, 1, buff.firstIndex)
	require.Len(t, buff.points, 2)

	// append a slice and validate that the buffer has been re-aligned to start at firstIndex=0
	require.NoError(t, buff.AppendSlice([]promql.FPoint{{T: 20, F: 20}, {T: 30, F: 20}, {T: 40, F: 20}}))
	shouldHavePoints(t, &fPointRingBufferWrapper{FPointRingBuffer: buff}, []promql.FPoint{{T: 5, F: 20}, {T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}, {T: 40, F: 20}}...)

	require.Equal(t, 0, buff.firstIndex)
	require.Equal(t, promql.FPoint{T: 5, F: 20}, buff.PointAt(0))
}

// TestSizeLessThanFirstIndex creates a scenario where the buffer size is 1, the underlying point slice is 4 and the firstIndex is 3
func TestFPointRingBuffer_AppendSlice_SizeLessThanFirstIndex(t *testing.T) {
	buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
	require.NoError(t, buff.Append(promql.FPoint{T: 10}))
	require.NoError(t, buff.Append(promql.FPoint{T: 20}))
	require.NoError(t, buff.Append(promql.FPoint{T: 30}))
	require.NoError(t, buff.Append(promql.FPoint{T: 40}))
	require.Equal(t, 4, buff.size)
	require.Len(t, buff.points, 4)
	require.Equal(t, 0, buff.firstIndex)

	buff.RemoveLast()
	require.NoError(t, buff.AppendAtStart(promql.FPoint{T: 5}))
	require.Equal(t, 4, buff.size)
	require.Len(t, buff.points, 4)
	require.Equal(t, 3, buff.firstIndex)

	buff.RemoveLast()
	buff.RemoveLast()
	buff.RemoveLast()
	require.Equal(t, 1, buff.size)
	require.Len(t, buff.points, 4)
	require.Equal(t, 3, buff.firstIndex)

	require.NoError(t, buff.AppendSlice([]promql.FPoint{{T: 50}, {T: 60}, {T: 70}, {T: 80}}))
}
