// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"context"
	"fmt"
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
	Append(p T) (bool, error)
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
		buf := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
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
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		testRingBuffer(t, buf, points)
	})
}

func testRingBuffer[T any](t *testing.T, buf ringBuffer[T], points []T) {
	shouldHaveNoPoints(t, buf)

	buf.DiscardPointsAtOrBefore(0) // Should handle empty buffer.
	shouldHaveNoPoints(t, buf)

	mustAppend(t, buf, points[0])
	shouldHavePoints(t, buf, points[:1]...)

	mustAppend(t, buf, points[1])
	shouldHavePoints(t, buf, points[:2]...)

	buf.DiscardPointsAtOrBefore(0)
	shouldHavePoints(t, buf, points[:2]...) // No change.

	buf.DiscardPointsAtOrBefore(1)
	shouldHavePoints(t, buf, points[1:2]...)

	mustAppend(t, buf, points[2])
	shouldHavePoints(t, buf, points[1:3]...)

	buf.DiscardPointsAtOrBefore(3)
	shouldHaveNoPoints(t, buf)

	mustAppend(t, buf, points[3])
	mustAppend(t, buf, points[4])
	shouldHavePoints(t, buf, points[3:5]...)

	// Trigger expansion of buffer (we resize in powers of two, and the underlying slice comes from a pool that uses a factor of 2 as well).
	resized, err := buf.Append(points[5])
	require.NoError(t, err)
	require.True(t, resized, "expected Append() to trigger a resize")
	resized, err = buf.Append(points[6])
	require.NoError(t, err)
	require.False(t, resized, "expected Append() not to trigger a resize")

	shouldHavePoints(t, buf, points[3:7]...)

	buf.Reset()
	shouldHaveNoPoints(t, buf)

	mustAppend(t, buf, points[8])
	shouldHavePoints(t, buf, points[8])

	pointsWithPowerOfTwoCapacity := make([]T, 0, 16) // Use must be passed a slice with a capacity that is equal to a power of 2.
	pointsWithPowerOfTwoCapacity = append(pointsWithPowerOfTwoCapacity, points...)
	err = buf.Use(pointsWithPowerOfTwoCapacity)
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
		buf := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
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
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		testDiscardPointsBeforeThroughWrapAround(t, buf, points)
	})
}

func testDiscardPointsBeforeThroughWrapAround[T any](t *testing.T, buf ringBuffer[T], points []T) {
	// Set up the buffer so that the first point is part-way through the underlying slice.
	// We resize in powers of two, and the underlying slice comes from a pool that uses a factor of 2 as well.

	for _, p := range points[:4] {
		mustAppend(t, buf, p)
	}

	// Ideally we wouldn't reach into the internals here, but this helps ensure the test is testing the correct scenario.
	require.Len(t, buf.GetPoints(), 4, "expected underlying slice to have length 4, if this assertion fails, the test setup is not as expected")
	require.Equal(t, 4, cap(buf.GetPoints()), "expected underlying slice to have capacity 4, if this assertion fails, the test setup is not as expected")
	buf.DiscardPointsAtOrBefore(2)
	mustAppend(t, buf, points[4])
	mustAppend(t, buf, points[5])

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

	buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}

	t.Run("test removing points until none exist", func(t *testing.T) {
		buf.Reset()
		for _, p := range points[:2] {
			mustAppend(t, buf, p)
		}

		shouldHavePoints(t, buf, points[:2]...)
		require.Equal(t, 2, len(buf.GetPoints()))
		require.Equal(t, 2, buf.size)

		nextPoint, resized, err := buf.NextPoint()
		require.NoError(t, err)
		require.True(t, resized, "expected NextPoint() to trigger a resize")

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
			mustAppend(t, buf, p)
		}

		require.Len(t, buf.GetPoints(), 4, "expected underlying slice to have length 4, if this assertion fails, the test setup is not as expected")
		require.Equal(t, 4, cap(buf.GetPoints()), "expected underlying slice to have capacity 4, if this assertion fails, the test setup is not as expected")
		require.Equal(t, 4, buf.size, "The size includes all points")
		buf.DiscardPointsAtOrBefore(2)
		require.Equal(t, 2, buf.size, "The size is reduced by the removed points")
		require.Equal(t, 2, buf.GetFirstIndex(), "the firstIndex is half way through the ring")

		// Check we only have the expected points
		shouldHavePoints(t, buf, points[2:4]...)

		nextPoint, resized, err := buf.NextPoint()
		require.NoError(t, err)
		require.False(t, resized, "expected NextPoint() not to trigger a resize")

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
		buf := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		mustAppend(t, buf, promql.FPoint{T: 1, F: 100})
		mustAppend(t, buf, promql.FPoint{T: 2, F: 200})
		mustAppend(t, buf, promql.FPoint{T: 3, F: 300})
		mustAppend(t, buf, promql.FPoint{T: 4, F: 400})

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

		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		mustAppend(t, buf, promql.HPoint{T: 1, H: h1})
		mustAppend(t, buf, promql.HPoint{T: 2, H: h2})
		mustAppend(t, buf, promql.HPoint{T: 3, H: h3})
		mustAppend(t, buf, promql.HPoint{T: 4, H: h4})

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

func TestRingBuffer_ViewBetweenSearchingBackwards(t *testing.T) {
	t.Run("FPoint ring buffer", func(t *testing.T) {
		buf := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		mustAppend(t, buf, promql.FPoint{T: 1, F: 100})
		mustAppend(t, buf, promql.FPoint{T: 2, F: 200})
		mustAppend(t, buf, promql.FPoint{T: 3, F: 300})
		mustAppend(t, buf, promql.FPoint{T: 4, F: 400})
		mustAppend(t, buf, promql.FPoint{T: 5, F: 500})
		mustAppend(t, buf, promql.FPoint{T: 6, F: 600})
		mustAppend(t, buf, promql.FPoint{T: 7, F: 700})
		buf.RemoveFirst()                               // remove T=1
		buf.RemoveFirst()                               // remove T=2
		mustAppend(t, buf, promql.FPoint{T: 8, F: 800}) // Takes index 7 in the initial cap(8) slice.
		mustAppend(t, buf, promql.FPoint{T: 9, F: 900}) // Takes index 0 in the initial cap(8) slice.

		view := buf.ViewBetweenSearchingBackwards(2, 3, nil)
		viewShouldHavePoints(t, view, promql.FPoint{T: 3, F: 300})

		newView := buf.ViewBetweenSearchingBackwards(1, 5, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.FPoint{T: 3, F: 300}, promql.FPoint{T: 4, F: 400}, promql.FPoint{T: 5, F: 500})

		newView = buf.ViewBetweenSearchingBackwards(2, 4, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.FPoint{T: 3, F: 300}, promql.FPoint{T: 4, F: 400})

		newView = buf.ViewBetweenSearchingBackwards(3, 3, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, []promql.FPoint{}...)

		newView = buf.ViewBetweenSearchingBackwards(10, 15, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, []promql.FPoint{}...)

		newView = buf.ViewBetweenSearchingBackwards(7, 9, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.FPoint{T: 8, F: 800}, promql.FPoint{T: 9, F: 900})

		require.Panics(t, func() {
			buf.ViewBetweenSearchingBackwards(4, 3, view)
		})
	})

	t.Run("HPoint ring buffer", func(t *testing.T) {
		h1 := &histogram.FloatHistogram{Count: 100}
		h2 := &histogram.FloatHistogram{Count: 200}
		h3 := &histogram.FloatHistogram{Count: 300}
		h4 := &histogram.FloatHistogram{Count: 400}
		h5 := &histogram.FloatHistogram{Count: 500}
		h6 := &histogram.FloatHistogram{Count: 600}
		h7 := &histogram.FloatHistogram{Count: 700}
		h8 := &histogram.FloatHistogram{Count: 800}
		h9 := &histogram.FloatHistogram{Count: 900}

		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		mustAppend(t, buf, promql.HPoint{T: 1, H: h1})
		mustAppend(t, buf, promql.HPoint{T: 2, H: h2})
		mustAppend(t, buf, promql.HPoint{T: 3, H: h3})
		mustAppend(t, buf, promql.HPoint{T: 4, H: h4})
		mustAppend(t, buf, promql.HPoint{T: 5, H: h5})
		mustAppend(t, buf, promql.HPoint{T: 6, H: h6})
		mustAppend(t, buf, promql.HPoint{T: 7, H: h7})
		buf.RemoveFirst()                              // remove T=1
		buf.RemoveFirst()                              // remove T=2
		mustAppend(t, buf, promql.HPoint{T: 8, H: h8}) // Takes index 7 in the initial cap(8) slice.
		mustAppend(t, buf, promql.HPoint{T: 9, H: h9}) // Takes index 0 in the initial cap(8) slice.

		view := buf.ViewBetweenSearchingBackwards(2, 3, nil)
		viewShouldHavePoints(t, view, promql.HPoint{T: 3, H: h3})

		newView := buf.ViewBetweenSearchingBackwards(1, 5, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.HPoint{T: 3, H: h3}, promql.HPoint{T: 4, H: h4}, promql.HPoint{T: 5, H: h5})

		newView = buf.ViewBetweenSearchingBackwards(2, 4, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.HPoint{T: 3, H: h3}, promql.HPoint{T: 4, H: h4})

		newView = buf.ViewBetweenSearchingBackwards(3, 3, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, []promql.HPoint{}...)

		newView = buf.ViewBetweenSearchingBackwards(10, 15, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, []promql.HPoint{}...)

		newView = buf.ViewBetweenSearchingBackwards(7, 9, view)
		require.Same(t, newView, view)
		viewShouldHavePoints(t, view, promql.HPoint{T: 8, H: h8}, promql.HPoint{T: 9, H: h9})

		require.Panics(t, func() {
			buf.ViewBetweenSearchingBackwards(4, 3, view)
		})
	})
}

func mustAppend[T any](t *testing.T, buf ringBuffer[T], point T) {
	_, err := buf.Append(point)
	require.NoError(t, err)
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
	originalBuffer := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
	mustAppend(t, originalBuffer, promql.FPoint{T: 0, F: 10})
	mustAppend(t, originalBuffer, promql.FPoint{T: 1, F: 11})

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
	originalBuffer := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
	h1 := &histogram.FloatHistogram{Count: 100}
	h2 := &histogram.FloatHistogram{Count: 200}
	mustAppend(t, originalBuffer, promql.HPoint{T: 0, H: h1})
	mustAppend(t, originalBuffer, promql.HPoint{T: 1, H: h2})

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
			_, err := buff.AppendSlice(tc.append)
			require.NoError(t, err)
			shouldHavePoints(t, &fPointRingBufferWrapper{FPointRingBuffer: buff}, tc.expected...)
		})
	}
}

func TestFPointRingBuffer_AppendAtStart(t *testing.T) {
	buff := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))}
	mustAppend(t, buff, promql.FPoint{T: 10, F: 20})
	require.Equal(t, 1, buff.size)
	require.Equal(t, 0, buff.firstIndex)
	require.Len(t, buff.points, 2)

	// inserting another point will not grow the underlying buffer, so the new point is stored in the upper end of the existing 2 slot buffer
	resized, err := buff.AppendAtStart(promql.FPoint{T: 9, F: 20})
	require.NoError(t, err)
	require.False(t, resized, "expected AppendAtStart() not to trigger a buffer resize")
	require.Equal(t, 2, buff.size)
	require.Equal(t, 1, buff.firstIndex)
	require.Len(t, buff.points, 2)
	require.Equal(t, promql.FPoint{T: 9, F: 20}, buff.PointAt(0))

	// inserting another point will grow the underlying buffer - since we are growing the buffer we can re-align so that the firstIndex is 0 for the new head
	resized, err = buff.AppendAtStart(promql.FPoint{T: 8, F: 20})
	require.NoError(t, err)
	require.True(t, resized, "expected AppendAtStart() to trigger a buffer resize")
	require.Equal(t, 3, buff.size)
	require.Equal(t, 0, buff.firstIndex)
	require.Len(t, buff.points, 4)
	require.Equal(t, promql.FPoint{T: 8, F: 20}, buff.PointAt(0))

	// inserting another point will not grow the underlying buffer, so the new point is stored at the end of the existing buffer
	resized, err = buff.AppendAtStart(promql.FPoint{T: 7, F: 20})
	require.NoError(t, err)
	require.False(t, resized, "expected AppendAtStart() not to trigger a buffer resize")
	require.Equal(t, 4, buff.size)
	require.Equal(t, 3, buff.firstIndex)
	require.Len(t, buff.points, 4)
	require.Equal(t, promql.FPoint{T: 7, F: 20}, buff.PointAt(0))

	// inserting another point will grow the underlying buffer - since we are growing the buffer we can re-align so that the firstIndex is 0 for the new head
	resized, err = buff.AppendAtStart(promql.FPoint{T: 6, F: 20})
	require.NoError(t, err)
	require.True(t, resized, "expected AppendAtStart() to trigger a buffer resize")
	require.Equal(t, 5, buff.size)
	require.Equal(t, 0, buff.firstIndex)
	require.Len(t, buff.points, 8)
	require.Equal(t, promql.FPoint{T: 6, F: 20}, buff.PointAt(0))
}

func TestFPointRingBuffer_AppendSlice_Alignment(t *testing.T) {
	buff := NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))
	// insert 2 samples - the first point will be at the tail of the ring
	_, err := buff.AppendAtStart(promql.FPoint{T: 10, F: 20})
	require.NoError(t, err)
	_, err = buff.AppendAtStart(promql.FPoint{T: 5, F: 20})
	require.NoError(t, err)

	require.Equal(t, 2, buff.size)
	require.Equal(t, 1, buff.firstIndex)
	require.Len(t, buff.points, 2)

	// append a slice and validate that the buffer has been re-aligned to start at firstIndex=0
	resized, err := buff.AppendSlice([]promql.FPoint{{T: 20, F: 20}, {T: 30, F: 20}, {T: 40, F: 20}})
	require.NoError(t, err)
	require.True(t, resized, "expected AppendSlice() to trigger a buffer resize")
	shouldHavePoints(t, &fPointRingBufferWrapper{FPointRingBuffer: buff}, []promql.FPoint{{T: 5, F: 20}, {T: 10, F: 20}, {T: 20, F: 20}, {T: 30, F: 20}, {T: 40, F: 20}}...)

	require.Equal(t, 0, buff.firstIndex)
	require.Equal(t, promql.FPoint{T: 5, F: 20}, buff.PointAt(0))
}

// TestSizeLessThanFirstIndex creates a scenario where the buffer size is 1, the underlying point slice is 4 and the firstIndex is 3
func TestFPointRingBuffer_AppendSlice_SizeLessThanFirstIndex(t *testing.T) {
	buff := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewMemoryConsumptionTracker(context.Background(), 0, nil, ""))}
	mustAppend(t, buff, promql.FPoint{T: 10})
	mustAppend(t, buff, promql.FPoint{T: 20})
	mustAppend(t, buff, promql.FPoint{T: 30})
	mustAppend(t, buff, promql.FPoint{T: 40})
	require.Equal(t, 4, buff.size)
	require.Len(t, buff.points, 4)
	require.Equal(t, 0, buff.firstIndex)

	buff.RemoveLast()
	_, err := buff.AppendAtStart(promql.FPoint{T: 5})
	require.NoError(t, err)
	require.Equal(t, 4, buff.size)
	require.Len(t, buff.points, 4)
	require.Equal(t, 3, buff.firstIndex)

	buff.RemoveLast()
	buff.RemoveLast()
	buff.RemoveLast()
	require.Equal(t, 1, buff.size)
	require.Len(t, buff.points, 4)
	require.Equal(t, 3, buff.firstIndex)

	_, err = buff.AppendSlice([]promql.FPoint{{T: 50}, {T: 60}, {T: 70}, {T: 80}})
	require.NoError(t, err)
}

func TestRingBuffer_CountUntil(t *testing.T) {

	fbuff := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
	hbuff := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}

	// Should work with empty buffer.
	for _, until := range []int64{0, 100} {
		require.Zero(t, fbuff.CountUntil(until))
		require.Zero(t, hbuff.CountUntil(until))
	}

	// Test input/output on populated ring buffers
	tcs := []struct {
		until    int64
		expected int
	}{
		{0, 0},
		{5, 0},
		{10, 1},
		{15, 1},

		{20, 2},
		{25, 2},
		{30, 3},

		{35, 3},
		{40, 4},
		{45, 4},
	}

	// append dummy points at these timestamps
	h := &histogram.FloatHistogram{}
	for _, ts := range []int64{10, 20, 30, 40} {
		mustAppend(t, fbuff, promql.FPoint{T: ts})
		mustAppend(t, hbuff, promql.HPoint{T: ts, H: h})
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("fpoint %v", tc.until), func(t *testing.T) {
			require.Equal(t, tc.expected, fbuff.CountUntil(tc.until))
		})

		t.Run(fmt.Sprintf("hpoint %v", tc.until), func(t *testing.T) {
			require.Equal(t, tc.expected, hbuff.CountUntil(tc.until))
		})
	}
}

func TestRingBuffer_CountBetween(t *testing.T) {

	fbuff := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
	hbuff := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}

	// Should work with empty buffer.
	for _, tc := range []struct{ minT, maxT int64 }{{0, 0}, {0, 100}} {
		require.Zero(t, fbuff.CountBetween(tc.minT, tc.maxT))
		require.Zero(t, hbuff.CountBetween(tc.minT, tc.maxT))
	}

	// append dummy points at these timestamps
	h := &histogram.FloatHistogram{}
	for _, ts := range []int64{10, 20, 30, 40} {
		mustAppend(t, fbuff, promql.FPoint{T: ts})
		mustAppend(t, hbuff, promql.HPoint{T: ts, H: h})
	}

	// Test input/output on populated ring buffers
	tcs := []struct {
		minT, maxT int64
		expected   int
	}{
		{0, 0, 0},
		{0, 100, 4},
		{0, 40, 4},
		{9, 40, 4},
		{10, 40, 3},
		{5, 10, 1},
		{5, 20, 2},
		{15, 20, 1},
		{31, 40, 1},
		{40, 40, 0},
		{41, 100, 0},
	}

	for _, tc := range tcs {
		t.Run(fmt.Sprintf("fpoint %v-%v", tc.minT, tc.maxT), func(t *testing.T) {
			require.Equal(t, tc.expected, fbuff.CountBetween(tc.minT, tc.maxT))
		})

		t.Run(fmt.Sprintf("hpoint %v-%v", tc.minT, tc.maxT), func(t *testing.T) {
			require.Equal(t, tc.expected, hbuff.CountBetween(tc.minT, tc.maxT))
		})
	}
}

func TestHPointRingBuffer_EquivalentFloatSampleCountUntil(t *testing.T) {
	buff := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}

	// Should work with empty buffer.
	require.Zero(t, buff.EquivalentFloatSampleCountUntil(0))
	require.Zero(t, buff.EquivalentFloatSampleCountUntil(100))

	h := &histogram.FloatHistogram{}
	mustAppend(t, buff, promql.HPoint{T: 10, H: h})
	mustAppend(t, buff, promql.HPoint{T: 20, H: h})
	mustAppend(t, buff, promql.HPoint{T: 30, H: h})
	mustAppend(t, buff, promql.HPoint{T: 40, H: h})

	equivalentSampleCount := EquivalentFloatSampleCount(h)

	require.Equal(t, 0*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(0))
	require.Equal(t, 0*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(5))
	require.Equal(t, 1*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(10))
	require.Equal(t, 1*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(15))
	require.Equal(t, 2*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(20))
	require.Equal(t, 2*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(25))
	require.Equal(t, 3*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(30))
	require.Equal(t, 3*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(35))
	require.Equal(t, 4*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(40))
	require.Equal(t, 4*equivalentSampleCount, buff.EquivalentFloatSampleCountUntil(45))
}

func TestHPointRingBuffer_EquivalentFloatSampleCountBetween(t *testing.T) {
	buff := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}

	// Should work with empty buffer.
	require.Zero(t, buff.EquivalentFloatSampleCountBetween(0, 0))
	require.Zero(t, buff.EquivalentFloatSampleCountBetween(0, 100))

	h := &histogram.FloatHistogram{}
	mustAppend(t, buff, promql.HPoint{T: 10, H: h})
	mustAppend(t, buff, promql.HPoint{T: 20, H: h})
	mustAppend(t, buff, promql.HPoint{T: 30, H: h})
	mustAppend(t, buff, promql.HPoint{T: 40, H: h})

	equivalentSampleCount := EquivalentFloatSampleCount(h)

	require.Equal(t, 0*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(0, 0))
	require.Equal(t, 4*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(0, 100))
	require.Equal(t, 4*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(0, 40))
	require.Equal(t, 4*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(9, 40))
	require.Equal(t, 3*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(10, 40))
	require.Equal(t, 1*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(5, 10))
	require.Equal(t, 2*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(5, 20))
	require.Equal(t, 1*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(15, 20))
	require.Equal(t, 1*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(31, 40))
	require.Equal(t, 0*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(40, 40))
	require.Equal(t, 0*equivalentSampleCount, buff.EquivalentFloatSampleCountBetween(41, 100))
}

func TestRingBuffer_CountAndLast(t *testing.T) {
	t.Run("fpoint", func(t *testing.T) {
		buff := &fPointRingBufferWrapper{NewFPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}

		require.Zero(t, buff.Count())

		points := []promql.FPoint{
			{T: 10},
			{T: 20},
			{T: 30},
			{T: 40},
		}

		for i, p := range points {
			mustAppend(t, buff, p)
			require.Equal(t, i+1, buff.Count())
			require.Equal(t, p, buff.Last())
		}

		require.Equal(t, points[len(points)-1], buff.Last())
	})

	t.Run("hpoint", func(t *testing.T) {
		buff := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}

		require.Zero(t, buff.Count())

		h := &histogram.FloatHistogram{}
		points := []promql.HPoint{
			{T: 10, H: h},
			{T: 20, H: h},
			{T: 30, H: h},
			{T: 40, H: h},
		}

		for i, p := range points {
			mustAppend(t, buff, p)
			require.Equal(t, i+1, buff.Count())
			require.Equal(t, p, buff.Last())
		}

		require.Equal(t, points[len(points)-1], buff.Last())
	})
}

func TestHPointRingBufferView_UnsafePointsInIndexRange(t *testing.T) {
	setupRingBufferTestingPools(t)

	points := []promql.HPoint{
		{T: 1, H: &histogram.FloatHistogram{Count: 100}},
		{T: 2, H: &histogram.FloatHistogram{Count: 200}},
		{T: 3, H: &histogram.FloatHistogram{Count: 300}},
		{T: 4, H: &histogram.FloatHistogram{Count: 400}},
		{T: 5, H: &histogram.FloatHistogram{Count: 500}},
		{T: 6, H: &histogram.FloatHistogram{Count: 600}},
	}

	// assertMatchesPointAt checks that, for every valid sub-range, the two slices returned by
	// UnsafePointsInIndexRange concatenate to exactly the points PointAt reports for that range.
	assertMatchesPointAt := func(t *testing.T, view *HPointRingBufferView) {
		t.Helper()
		n := view.Count()
		for first := 0; first < n; first++ {
			for last := first; last < n; last++ {
				head, tail := view.UnsafePointsInIndexRange(first, last)
				combined := append(append([]promql.HPoint{}, head...), tail...)

				expected := make([]promql.HPoint, 0, last-first+1)
				for i := first; i <= last; i++ {
					expected = append(expected, view.PointAt(i))
				}
				require.Equal(t, expected, combined, "range [%d,%d]", first, last)
			}
		}
	}

	t.Run("contiguous (non-wrapping) view", func(t *testing.T) {
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		for _, p := range points {
			mustAppend(t, buf, p)
		}
		view := buf.ViewUntilSearchingForwards(math.MaxInt64, nil)
		require.Equal(t, len(points), view.Count())
		_, tail := view.UnsafePoints()
		require.Empty(t, tail, "test setup expects a non-wrapping view")
		assertMatchesPointAt(t, view)
	})

	t.Run("wrapping view", func(t *testing.T) {
		// Force the buffer to wrap so the view spans both the head and tail segments, mirroring the
		// setup in testDiscardPointsBeforeThroughWrapAround.
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		for _, p := range points[:4] {
			mustAppend(t, buf, p)
		}
		require.Equal(t, 4, cap(buf.GetPoints()), "test setup expects underlying capacity 4")
		buf.DiscardPointsAtOrBefore(2)
		mustAppend(t, buf, points[4])
		mustAppend(t, buf, points[5])
		require.Equal(t, 2, buf.GetFirstIndex(), "test setup expects a wrapped buffer")

		view := buf.ViewUntilSearchingForwards(math.MaxInt64, nil)
		require.Equal(t, 4, view.Count())
		head, tail := view.UnsafePoints()
		require.NotEmpty(t, head)
		require.NotEmpty(t, tail, "test setup expects the view to wrap (non-empty tail)")
		assertMatchesPointAt(t, view)
	})

	t.Run("panics on invalid ranges", func(t *testing.T) {
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiter.NewUnlimitedMemoryConsumptionTracker(context.Background()))}
		for _, p := range points[:3] {
			mustAppend(t, buf, p)
		}
		view := buf.ViewUntilSearchingForwards(math.MaxInt64, nil)
		require.Panics(t, func() { view.UnsafePointsInIndexRange(-1, 0) })
		require.Panics(t, func() { view.UnsafePointsInIndexRange(0, view.Count()) })
		require.Panics(t, func() { view.UnsafePointsInIndexRange(2, 1) })
	})
}
