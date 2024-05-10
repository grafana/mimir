// SPDX-License-Identifier: AGPL-3.0-only

package operator

import (
	"math"
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"
)

func TestRingBuffer(t *testing.T) {
	buf := &RingBuffer{}
	shouldHaveNoPoints(t, buf)

	buf.DiscardPointsBefore(1) // Should handle empty buffer.
	shouldHaveNoPoints(t, buf)

	buf.Append(promql.FPoint{T: 1, F: 100})
	shouldHavePoints(t, buf, promql.FPoint{T: 1, F: 100})

	buf.Append(promql.FPoint{T: 2, F: 200})
	shouldHavePoints(t, buf, promql.FPoint{T: 1, F: 100}, promql.FPoint{T: 2, F: 200})

	buf.DiscardPointsBefore(1)
	shouldHavePoints(t, buf, promql.FPoint{T: 1, F: 100}, promql.FPoint{T: 2, F: 200}) // No change.

	buf.DiscardPointsBefore(2)
	shouldHavePoints(t, buf, promql.FPoint{T: 2, F: 200})

	buf.Append(promql.FPoint{T: 3, F: 300})
	shouldHavePoints(t, buf, promql.FPoint{T: 2, F: 200}, promql.FPoint{T: 3, F: 300})

	buf.DiscardPointsBefore(4)
	shouldHaveNoPoints(t, buf)

	buf.Append(promql.FPoint{T: 4, F: 400})
	buf.Append(promql.FPoint{T: 5, F: 500})
	shouldHavePoints(t, buf, promql.FPoint{T: 4, F: 400}, promql.FPoint{T: 5, F: 500})

	// Trigger expansion of buffer (we resize in powers of two, and the underlying slice comes from a pool that uses a factor of 2 as well).
	// Ideally we wouldn't reach into the internals here, but this helps ensure the test is testing the correct scenario.
	require.Len(t, buf.points, 2, "expected underlying slice to have length 2, if this assertion fails, the test setup is not as expected")
	require.Equal(t, 2, cap(buf.points), "expected underlying slice to have capacity 2, if this assertion fails, the test setup is not as expected")
	buf.Append(promql.FPoint{T: 6, F: 600})
	buf.Append(promql.FPoint{T: 7, F: 700})
	require.Greater(t, cap(buf.points), 2, "expected underlying slice to be expanded, if this assertion fails, the test setup is not as expected")

	shouldHavePoints(t,
		buf,
		promql.FPoint{T: 4, F: 400},
		promql.FPoint{T: 5, F: 500},
		promql.FPoint{T: 6, F: 600},
		promql.FPoint{T: 7, F: 700},
	)

	buf.Reset()
	shouldHaveNoPoints(t, buf)

	buf.Append(promql.FPoint{T: 9, F: 900})
	shouldHavePoints(t, buf, promql.FPoint{T: 9, F: 900})
}

func TestRingBuffer_DiscardPointsBefore_ThroughWrapAround(t *testing.T) {
	// Set up the buffer so that the first point is part-way through the underlying slice.
	// We resize in powers of two, and the underlying slice comes from a pool that uses a factor of 2 as well.
	buf := &RingBuffer{}
	buf.Append(promql.FPoint{T: 1, F: 100})
	buf.Append(promql.FPoint{T: 2, F: 200})
	buf.Append(promql.FPoint{T: 3, F: 300})
	buf.Append(promql.FPoint{T: 4, F: 400})

	// Ideally we wouldn't reach into the internals here, but this helps ensure the test is testing the correct scenario.
	require.Len(t, buf.points, 4, "expected underlying slice to have length 4, if this assertion fails, the test setup is not as expected")
	require.Equal(t, 4, cap(buf.points), "expected underlying slice to have capacity 4, if this assertion fails, the test setup is not as expected")
	buf.DiscardPointsBefore(3)
	buf.Append(promql.FPoint{T: 5, F: 500})
	buf.Append(promql.FPoint{T: 6, F: 600})

	// Should not have expanded slice.
	require.Len(t, buf.points, 4, "expected underlying slice to have length 4")
	require.Equal(t, 4, cap(buf.points), "expected underlying slice to have capacity 4")

	// Discard before end of underlying slice.
	buf.DiscardPointsBefore(4)
	shouldHavePoints(t,
		buf,
		promql.FPoint{T: 4, F: 400},
		promql.FPoint{T: 5, F: 500},
		promql.FPoint{T: 6, F: 600},
	)

	require.Equal(t, 3, buf.firstIndex, "expected first point to be in middle of underlying slice, if this assertion fails, the test setup is not as expected")

	// Discard after wraparound.
	buf.DiscardPointsBefore(6)
	shouldHavePoints(t,
		buf,
		promql.FPoint{T: 6, F: 600},
	)
}

func shouldHaveNoPoints(t *testing.T, buf *RingBuffer) {
	shouldHavePoints(
		t,
		buf,
		/* nothing */
	)
}

func shouldHavePoints(t *testing.T, buf *RingBuffer, expected ...promql.FPoint) {
	var pointsFromForEach []promql.FPoint

	buf.ForEach(func(p promql.FPoint) {
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

		lastPointT := expected[len(expected)-1].T

		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT+1, expected...)
		shouldHavePointsAtOrBeforeTime(t, buf, lastPointT-1, expected[:len(expected)-1]...)
	}
}

func shouldHavePointsAtOrBeforeTime(t *testing.T, buf *RingBuffer, ts int64, expected ...promql.FPoint) {
	head, tail := buf.UnsafePoints(ts)
	combinedPoints := append(head, tail...)

	if len(expected) == 0 {
		require.Len(t, combinedPoints, 0)
	} else {
		require.Equal(t, expected, combinedPoints)
	}

	copiedPoints := buf.CopyPoints(ts)
	require.Equal(t, expected, copiedPoints)

	end, present := buf.LastAtOrBefore(ts)

	if len(expected) == 0 {
		require.False(t, present)
	} else {
		require.True(t, present)
		require.Equal(t, expected[len(expected)-1], end)
	}
}
