// SPDX-License-Identifier: AGPL-3.0-only

package types

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestRingBuffer(t *testing.T) {
	tests := []struct {
		name   string
		points interface{}
		buf    interface{}
	}{
		{
			name: "FPoint",
			points: []promql.FPoint{
				{T: 1, F: 100},
				{T: 2, F: 200},
				{T: 3, F: 300},
				{T: 4, F: 400},
				{T: 5, F: 500},
				{T: 6, F: 600},
				{T: 7, F: 700},
				{T: 8, F: 800},
				{T: 9, F: 900},
			},
			buf: &RingBuffer[promql.FPoint]{pool: &fPointRingBufferPoolForTesting{}},
		},
		{
			name: "HPoint",
			points: []promql.HPoint{
				{T: 1, H: &histogram.FloatHistogram{Count: 100}},
				{T: 2, H: &histogram.FloatHistogram{Count: 200}},
				{T: 3, H: &histogram.FloatHistogram{Count: 300}},
				{T: 4, H: &histogram.FloatHistogram{Count: 400}},
				{T: 5, H: &histogram.FloatHistogram{Count: 500}},
				{T: 6, H: &histogram.FloatHistogram{Count: 600}},
				{T: 7, H: &histogram.FloatHistogram{Count: 700}},
				{T: 8, H: &histogram.FloatHistogram{Count: 800}},
				{T: 9, H: &histogram.FloatHistogram{Count: 900}},
			},
			buf: &RingBuffer[promql.HPoint]{pool: &hPointRingBufferPoolForTesting{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			switch buf := tc.buf.(type) {
			case *RingBuffer[promql.FPoint]:
				points := tc.points.([]promql.FPoint)
				testRingBuffer(t, buf, points)
			case *RingBuffer[promql.HPoint]:
				points := tc.points.([]promql.HPoint)
				testRingBuffer(t, buf, points)
			}
		})
	}
}

func testRingBuffer[T any](t *testing.T, buf *RingBuffer[T], points []T) {
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
	require.Len(t, buf.points, 2, "expected underlying slice to have length 2, if this assertion fails, the test setup is not as expected")
	require.Equal(t, 2, cap(buf.points), "expected underlying slice to have capacity 2, if this assertion fails, the test setup is not as expected")
	require.NoError(t, buf.Append(points[5]))
	require.NoError(t, buf.Append(points[6]))
	require.Greater(t, cap(buf.points), 2, "expected underlying slice to be expanded, if this assertion fails, the test setup is not as expected")

	shouldHavePoints(t,
		buf,
		points[3:7]...,
	)

	buf.Reset()
	shouldHaveNoPoints(t, buf)

	require.NoError(t, buf.Append(points[8]))
	shouldHavePoints(t, buf, points[8])
}

func TestRingBuffer_DiscardPointsBefore_ThroughWrapAround(t *testing.T) {
	tests := []struct {
		name   string
		points interface{}
		buf    interface{}
	}{
		{
			name: "FPoint",
			points: []promql.FPoint{
				{T: 1, F: 100},
				{T: 2, F: 200},
				{T: 3, F: 300},
				{T: 4, F: 400},
				{T: 5, F: 500},
				{T: 6, F: 600},
			},
			buf: &RingBuffer[promql.FPoint]{pool: &fPointRingBufferPoolForTesting{}},
		},
		{
			name: "HPoint",
			points: []promql.HPoint{
				{T: 1, H: &histogram.FloatHistogram{Count: 100}},
				{T: 2, H: &histogram.FloatHistogram{Count: 200}},
				{T: 3, H: &histogram.FloatHistogram{Count: 300}},
				{T: 4, H: &histogram.FloatHistogram{Count: 400}},
				{T: 5, H: &histogram.FloatHistogram{Count: 500}},
				{T: 6, H: &histogram.FloatHistogram{Count: 600}},
			},
			buf: &RingBuffer[promql.HPoint]{pool: &hPointRingBufferPoolForTesting{}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			switch buf := tc.buf.(type) {
			case *RingBuffer[promql.FPoint]:
				points := tc.points.([]promql.FPoint)
				testDiscardPointsBeforeThroughWrapAround(t, buf, points)
			case *RingBuffer[promql.HPoint]:
				points := tc.points.([]promql.HPoint)
				testDiscardPointsBeforeThroughWrapAround(t, buf, points)
			}
		})
	}
}

func testDiscardPointsBeforeThroughWrapAround[T any](t *testing.T, buf *RingBuffer[T], points []T) {
	for _, p := range points[:4] {
		require.NoError(t, buf.Append(p))
	}

	// Ideally we wouldn't reach into the internals here, but this helps ensure the test is testing the correct scenario.
	require.Len(t, buf.points, 4, "expected underlying slice to have length 4, if this assertion fails, the test setup is not as expected")
	require.Equal(t, 4, cap(buf.points), "expected underlying slice to have capacity 4, if this assertion fails, the test setup is not as expected")
	buf.DiscardPointsBefore(3)
	require.NoError(t, buf.Append(points[4]))
	require.NoError(t, buf.Append(points[5]))

	// Should not have expanded slice.
	require.Len(t, buf.points, 4, "expected underlying slice to have length 4")
	require.Equal(t, 4, cap(buf.points), "expected underlying slice to have capacity 4")

	// Discard before end of underlying slice.
	buf.DiscardPointsBefore(4)
	shouldHavePoints(t,
		buf,
		points[3:6]...,
	)

	require.Equal(t, 3, buf.firstIndex, "expected first point to be in middle of underlying slice, if this assertion fails, the test setup is not as expected")

	// Discard after wraparound.
	buf.DiscardPointsBefore(6)
	shouldHavePoints(t,
		buf,
		points[5],
	)
}

func shouldHaveNoPoints[T any](t *testing.T, buf *RingBuffer[T]) {
	shouldHavePoints(
		t,
		buf,
		/* nothing */
	)
}

func shouldHavePoints[T any](t *testing.T, buf *RingBuffer[T], expected ...T) {
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

		lastPointT := buf.pool.GetTimestamp(expected[len(expected)-1])

		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT+1, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT-1, expected[:len(expected)-1]...)
	}
}

func shouldHavePointsAtOrBeforeTime[T any](t *testing.T, buf *RingBuffer[T], ts int64, expected ...T) {
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

// fPointRingBufferPoolForTesting is a dummy pool implementation for testing RingBuffer with FPoint.
//
// This helps ensure that the tests behave as expected: the default global pool does not guarantee that
// slices returned have exactly the capacity requested. Instead, it only guarantees that slices have
// capacity at least as large as requested. This makes it difficult to consistently test scenarios like
// wraparound.
type fPointRingBufferPoolForTesting struct{}

func (p *fPointRingBufferPoolForTesting) GetSlice(size int) ([]promql.FPoint, error) {
	return make([]promql.FPoint, 0, size), nil
}

func (p *fPointRingBufferPoolForTesting) PutSlice(_ []promql.FPoint) {
	// Drop slice on the floor - we don't need it.
}

func (p *fPointRingBufferPoolForTesting) GetTimestamp(point promql.FPoint) int64 {
	return point.T
}

// hPointRingBufferPoolForTesting is a dummy pool implementation for testing RingBuffer with HPoint.
//
// This helps ensure that the tests behave as expected: the default global pool does not guarantee that
// slices returned have exactly the capacity requested. Instead, it only guarantees that slices have
// capacity at least as large as requested. This makes it difficult to consistently test scenarios like
// wraparound.
type hPointRingBufferPoolForTesting struct{}

func (p *hPointRingBufferPoolForTesting) GetSlice(size int) ([]promql.HPoint, error) {
	return make([]promql.HPoint, 0, size), nil
}

func (p *hPointRingBufferPoolForTesting) PutSlice(_ []promql.HPoint) {
	// Drop slice on the floor - we don't need it.
}

func (p *hPointRingBufferPoolForTesting) GetTimestamp(point promql.HPoint) int64 {
	return point.T
}
