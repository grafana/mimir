// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

// We want to ensure fpoint+hpoint ring buffers are tested consistently,
// and we don't care about performance here so we can use an interface+generics.
type ringBuffer[T any] interface {
	DiscardPointsBefore(t int64)
	Append(p T) error
	ForEach(f func(p T))
	UnsafePoints(maxT int64) (head []T, tail []T)
	CopyPoints(maxT int64) ([]T, error)
	LastAtOrBefore(maxT int64) (T, bool)
	First() T
	Reset()
	GetPoints() []T
	GetFirstIndex() int
	GetTimestamp(point T) int64
}

func TestRingBuffer(t *testing.T) {
	t.Run("test fpoint ring buffer", func(t *testing.T) {
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
		buf := &fPointRingBufferWrapper{&FPointRingBuffer{pool: &fPointPoolForRingBufferTesting{}}}
		testRingBuffer(t, buf, points)
	})
	t.Run("test hpoint ring buffer", func(t *testing.T) {
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
		buf := &hPointRingBufferWrapper{&HPointRingBuffer{pool: &hPointPoolForRingBufferTesting{}}}
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
}

func TestRingBuffer_DiscardPointsBefore_ThroughWrapAround(t *testing.T) {
	t.Run("test fpoint ring buffer", func(t *testing.T) {
		points := []promql.FPoint{
			{T: 1, F: 100},
			{T: 2, F: 200},
			{T: 3, F: 300},
			{T: 4, F: 400},
			{T: 5, F: 500},
			{T: 6, F: 600},
		}
		buf := &fPointRingBufferWrapper{&FPointRingBuffer{pool: &fPointPoolForRingBufferTesting{}}}
		testDiscardPointsBeforeThroughWrapAround(t, buf, points)
	})
	t.Run("test hpoint ring buffer", func(t *testing.T) {
		points := []promql.HPoint{
			{T: 1, H: &histogram.FloatHistogram{Count: 100}},
			{T: 2, H: &histogram.FloatHistogram{Count: 200}},
			{T: 3, H: &histogram.FloatHistogram{Count: 300}},
			{T: 4, H: &histogram.FloatHistogram{Count: 400}},
			{T: 5, H: &histogram.FloatHistogram{Count: 500}},
			{T: 6, H: &histogram.FloatHistogram{Count: 600}},
		}
		buf := &hPointRingBufferWrapper{&HPointRingBuffer{pool: &hPointPoolForRingBufferTesting{}}}
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

func shouldHaveNoPoints[T any](t *testing.T, buf ringBuffer[T]) {
	shouldHavePoints(
		t,
		buf,
		/* nothing */
	)
}

func shouldHavePoints[T any](t *testing.T, buf ringBuffer[T], expected ...T) {
	var pointsFromForEach []T

	buf.ForEach(func(p T) {
		pointsFromForEach = append(pointsFromForEach, p)
	})

	require.Equal(t, expected, pointsFromForEach)

	if len(expected) == 0 {
		shouldHavePointsAtOrBeforeTime(t, buf, math.MaxInt64, expected...)
		_, present := buf.LastAtOrBefore(math.MaxInt64)
		require.False(t, present)
	} else {
		require.Equal(t, expected[0], buf.First())
		// We test LastAtOrBefore() below.

		lastPointT := buf.GetTimestamp(expected[len(expected)-1])

		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT+1, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT-1, expected[:len(expected)-1]...)
	}
}

func shouldHavePointsAtOrBeforeTime[T any](t *testing.T, buf ringBuffer[T], ts int64, expected ...T) {
	head, tail := buf.UnsafePoints(ts)
	combinedPoints := append(head, tail...)

	if len(expected) == 0 {
		require.Len(t, combinedPoints, 0)
	} else {
		require.Equal(t, expected, combinedPoints)
	}

	copiedPoints, err := buf.CopyPoints(ts)
	require.NoError(t, err)
	require.Equal(t, expected, copiedPoints)

	end, present := buf.LastAtOrBefore(ts)

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

func (w *fPointRingBufferWrapper) GetPoints() []promql.FPoint {
	return w.points
}

func (w *fPointRingBufferWrapper) GetFirstIndex() int {
	return w.firstIndex
}

func (w *fPointRingBufferWrapper) GetTimestamp(point promql.FPoint) int64 {
	return point.T
}

// fPointPoolForRingBufferTesting is a dummy pool implementation for testing RingBuffer.
//
// This helps ensure that the tests behave as expected: the default global pool does not guarantee that
// slices returned have exactly the capacity requested. Instead, it only guarantees that slices have
// capacity at least as large as requested. This makes it difficult to consistently test scenarios like
// wraparound.
type fPointPoolForRingBufferTesting struct{}

func (p fPointPoolForRingBufferTesting) GetFPointSlice(size int) ([]promql.FPoint, error) {
	return make([]promql.FPoint, 0, size), nil
}

func (p fPointPoolForRingBufferTesting) PutFPointSlice(_ []promql.FPoint) {
	// Drop slice on the floor - we don't need it.
}

// Wrapper for HPointRingBuffer to work around indirection to get points
type hPointRingBufferWrapper struct {
	*HPointRingBuffer
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

// hPointPoolForRingBufferTesting is a dummy pool implementation for testing RingBuffer.
//
// This helps ensure that the tests behave as expected: the default global pool does not guarantee that
// slices returned have exactly the capacity requested. Instead, it only guarantees that slices have
// capacity at least as large as requested. This makes it difficult to consistently test scenarios like
// wraparound.
type hPointPoolForRingBufferTesting struct{}

func (p hPointPoolForRingBufferTesting) GetHPointSlice(size int) ([]promql.HPoint, error) {
	return make([]promql.HPoint, 0, size), nil
}

func (p hPointPoolForRingBufferTesting) PutHPointSlice(_ []promql.HPoint) {
	// Drop slice on the floor - we don't need it.
}
