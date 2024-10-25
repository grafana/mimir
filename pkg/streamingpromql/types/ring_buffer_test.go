// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
)

// We want to ensure FPoint+HPoint ring buffers are tested consistently,
// and we don't care about performance here so we can use an interface+generics.
type ringBuffer[T any] interface {
	DiscardPointsBefore(t int64)
	Append(p T) error
	Reset()
	Use(s []T)
	Release()
	ViewUntilForTesting(maxT int64, searchForwards bool) ringBufferView[T]
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
		buf := &fPointRingBufferWrapper{NewFPointRingBuffer(limiting.NewMemoryConsumptionTracker(0, nil))}
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
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiting.NewMemoryConsumptionTracker(0, nil))}
		testRingBuffer(t, buf, points)
	})
}

func testRingBuffer[T any](t *testing.T, buf ringBuffer[T], points []T) {
	shouldHaveNoPoints(t, buf)

	buf.DiscardPointsBefore(1) // Should handle empty buffer.
	shouldHaveNoPoints(t, buf)

	require.NoError(t, buf.Append(points[0]))
	shouldHavePoints(t, buf, points[:1]...)

	require.NoError(t, buf.Append(points[1]))
	shouldHavePoints(t, buf, points[:2]...)

	buf.DiscardPointsBefore(1)
	shouldHavePoints(t, buf, points[:2]...) // No change.

	buf.DiscardPointsBefore(2)
	shouldHavePoints(t, buf, points[1:2]...)

	require.NoError(t, buf.Append(points[2]))
	shouldHavePoints(t, buf, points[1:3]...)

	buf.DiscardPointsBefore(4)
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

	buf.Use(points)
	shouldHavePoints(t, buf, points...)

	buf.DiscardPointsBefore(5)
	shouldHavePoints(t, buf, points[4:]...)

	buf.Release()
	shouldHaveNoPoints(t, buf)

	buf.Use(points[4:])
	shouldHavePoints(t, buf, points[4:]...)
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
		buf := &fPointRingBufferWrapper{NewFPointRingBuffer(limiting.NewMemoryConsumptionTracker(0, nil))}
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
		buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiting.NewMemoryConsumptionTracker(0, nil))}
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
	buf.DiscardPointsBefore(3)
	require.NoError(t, buf.Append(points[4]))
	require.NoError(t, buf.Append(points[5]))

	// Should not have expanded slice.
	require.Len(t, buf.GetPoints(), 4, "expected underlying slice to have length 4")
	require.Equal(t, 4, cap(buf.GetPoints()), "expected underlying slice to have capacity 4")

	// Discard before end of underlying slice.
	buf.DiscardPointsBefore(4)
	shouldHavePoints(t, buf, points[3:6]...)

	require.Equal(t, 3, buf.GetFirstIndex(), "expected first point to be in middle of underlying slice, if this assertion fails, the test setup is not as expected")

	// Discard after wraparound.
	buf.DiscardPointsBefore(6)
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

	buf := &hPointRingBufferWrapper{NewHPointRingBuffer(limiting.NewMemoryConsumptionTracker(0, nil))}

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
		buf.DiscardPointsBefore(3)
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

func shouldHaveNoPoints[T any](t *testing.T, buf ringBuffer[T]) {
	shouldHavePoints(
		t,
		buf,
		/* nothing */
	)
}

func shouldHavePoints[T any](t *testing.T, buf ringBuffer[T], expected ...T) {
	var pointsFromForEachAfterSearchingForwards []T

	buf.ViewUntilForTesting(math.MaxInt64, true).ForEach(func(p T) {
		pointsFromForEachAfterSearchingForwards = append(pointsFromForEachAfterSearchingForwards, p)
	})

	require.Equal(t, expected, pointsFromForEachAfterSearchingForwards)

	var pointsFromForEachAfterSearchingBackwards []T

	buf.ViewUntilForTesting(math.MaxInt64, false).ForEach(func(p T) {
		pointsFromForEachAfterSearchingBackwards = append(pointsFromForEachAfterSearchingBackwards, p)
	})

	require.Equal(t, expected, pointsFromForEachAfterSearchingBackwards)

	if len(expected) == 0 {
		shouldHavePointsAtOrBeforeTime(t, buf, math.MaxInt64, expected...)

		_, present := buf.ViewUntilForTesting(math.MaxInt64, true).Last()
		require.False(t, present)

		_, present = buf.ViewUntilForTesting(math.MaxInt64, false).Last()
		require.False(t, present)
	} else {
		require.Equal(t, expected[0], buf.ViewUntilForTesting(math.MaxInt64, true).First())
		require.Equal(t, expected[0], buf.ViewUntilForTesting(math.MaxInt64, false).First())
		// We test LastAtOrBefore() below.

		lastPointT := buf.GetTimestamp(expected[len(expected)-1])

		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT+1, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT-1, expected[:len(expected)-1]...)
	}
}

func shouldHavePointsAtOrBeforeTime[T any](t *testing.T, buf ringBuffer[T], ts int64, expected ...T) {
	viewShouldHavePoints(t, buf.ViewUntilForTesting(ts, true), expected...)
	viewShouldHavePoints(t, buf.ViewUntilForTesting(ts, false), expected...)
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
}

// Wrapper for FPointRingBuffer to work around indirection to get points
type fPointRingBufferWrapper struct {
	*FPointRingBuffer
}

func (w *fPointRingBufferWrapper) ViewUntilForTesting(maxT int64, searchForwards bool) ringBufferView[promql.FPoint] {
	return w.ViewUntil(maxT, searchForwards, nil)
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

func (w *hPointRingBufferWrapper) ViewUntilForTesting(maxT int64, searchForwards bool) ringBufferView[promql.HPoint] {
	return w.ViewUntil(maxT, searchForwards, nil)
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

	getFPointSliceForRingBuffer = func(size int, _ *limiting.MemoryConsumptionTracker) ([]promql.FPoint, error) {
		return make([]promql.FPoint, 0, size), nil
	}

	putFPointSliceForRingBuffer = func(_ []promql.FPoint, _ *limiting.MemoryConsumptionTracker) {}

	getHPointSliceForRingBuffer = func(size int, _ *limiting.MemoryConsumptionTracker) ([]promql.HPoint, error) {
		return make([]promql.HPoint, 0, size), nil
	}

	putHPointSliceForRingBuffer = func(_ []promql.HPoint, _ *limiting.MemoryConsumptionTracker) {}

	t.Cleanup(func() {
		getFPointSliceForRingBuffer = originalGetFPointSlice
		putFPointSliceForRingBuffer = originalPutFPointSlice
		getHPointSliceForRingBuffer = originalGetHPointSlice
		putHPointSliceForRingBuffer = originalPutHPointSlice
	})
}
