// SPDX-License-Identifier: AGPL-3.0-only

package commonsubexpressionelimination

import (
	"testing"

	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestSeriesDataRingBuffer(t *testing.T) {
	buffer := &SeriesDataRingBuffer[types.InstantVectorSeriesData]{}

	// Test empty buffer.
	require.Equal(t, 0, buffer.Size())
	data, found := buffer.RemoveIfPresent(123)
	require.False(t, found)
	require.Equal(t, types.InstantVectorSeriesData{}, data)

	// Add some series.
	s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
	s2 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 2, F: 2}}}
	s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
	buffer.Append(s1, 120)
	buffer.Append(s2, 121)
	buffer.Append(s3, 122)

	require.Equal(t, 3, buffer.Size())
	require.Len(t, buffer.elements, 4, "expected slice to be resized in powers of 2")
	require.Equal(t, s1, buffer.Get(120))
	require.Equal(t, s2, buffer.Get(121))
	require.Equal(t, s3, buffer.Get(122))

	// Remove a series.
	removed := buffer.Remove(120)
	require.Equal(t, s1, removed)
	require.Equal(t, s2, buffer.Get(121))
	require.Equal(t, s3, buffer.Get(122))
	require.Equal(t, 2, buffer.Size())

	// Append two more series: the first will take the last slot in the slice, and the next will take the first slot.
	s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
	buffer.Append(s4, 123)
	require.Equal(t, s2, buffer.Get(121))
	require.Equal(t, s3, buffer.Get(122))
	require.Equal(t, s4, buffer.Get(123))
	require.Equal(t, 3, buffer.Size())
	require.Len(t, buffer.elements, 4, "expected slice to be unchanged")

	s5 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 6, F: 6}}}
	buffer.Append(s5, 124)
	require.Equal(t, s2, buffer.Get(121))
	require.Equal(t, s3, buffer.Get(122))
	require.Equal(t, s4, buffer.Get(123))
	require.Equal(t, s5, buffer.Get(124))
	require.Equal(t, 4, buffer.Size())
	require.Len(t, buffer.elements, 4, "expected slice to be unchanged")

	// Remove everything and then add another series to ensure the buffer is correctly reset when emptied.
	require.Equal(t, s2, buffer.RemoveFirst())
	require.Equal(t, s3, buffer.RemoveFirst())
	require.Equal(t, s4, buffer.RemoveFirst())
	require.Equal(t, s5, buffer.RemoveFirst())

	require.Equal(t, 0, buffer.Size())
	require.Len(t, buffer.elements, 4, "expected slice to be unchanged")

	s6 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 7, F: 7}}}
	buffer.Append(s6, 20)
	require.Equal(t, 1, buffer.Size())
	require.Equal(t, s6, buffer.Get(20))
	require.Len(t, buffer.elements, 4, "expected slice to be unchanged")

	d, found := buffer.RemoveIfPresent(19)
	require.False(t, found)
	require.Equal(t, types.InstantVectorSeriesData{}, d)

	d, found = buffer.RemoveIfPresent(20)
	require.True(t, found)
	require.Equal(t, s6, d)
}

func TestSeriesDataRingBuffer_Resizing(t *testing.T) {
	buffer := &SeriesDataRingBuffer[types.InstantVectorSeriesData]{}

	// Add two series to fill the slice.
	s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
	s2 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 2, F: 2}}}
	buffer.Append(s1, 120)
	buffer.Append(s2, 121)
	require.Len(t, buffer.elements, 2, "expected slice to be resized to length 2")
	require.Equal(t, 2, buffer.Size())

	// Remove the first series, then add two more to trigger a resize while the first series is not the first in the slice.
	require.Equal(t, s1, buffer.Remove(120))
	require.Len(t, buffer.elements, 2, "expected slice to be unchanged")
	require.Equal(t, 1, buffer.Size())

	s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
	s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
	buffer.Append(s3, 122)
	buffer.Append(s4, 123)
	require.Len(t, buffer.elements, 4, "expected slice to be resized to length 4")
	require.Equal(t, 3, buffer.Size())

	// Check we can still get series as expected after resizing.
	require.Equal(t, s2, buffer.Get(121))
	require.Equal(t, s3, buffer.Get(122))
	require.Equal(t, s4, buffer.Get(123))
}

func TestSeriesDataRingBuffer_RemovingThroughWrapAround(t *testing.T) {
	buffer := &SeriesDataRingBuffer[types.InstantVectorSeriesData]{}

	// Add four series to fill the slice.
	s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
	s2 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 2, F: 2}}}
	s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
	s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
	buffer.Append(s1, 120)
	buffer.Append(s2, 121)
	buffer.Append(s3, 122)
	buffer.Append(s4, 123)
	require.Len(t, buffer.elements, 4, "expected slice to be resized to length 4")
	require.Equal(t, 4, buffer.Size())

	// Remove the first three series, then add two more to wrap around to the front of the slice.
	require.Equal(t, s1, buffer.Remove(120))
	require.Equal(t, s2, buffer.Remove(121))
	require.Equal(t, s3, buffer.Remove(122))
	require.Len(t, buffer.elements, 4, "expected slice to still be length 4")
	require.Equal(t, 1, buffer.Size())

	s5 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 5, F: 5}}}
	s6 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 6, F: 6}}}
	buffer.Append(s5, 124)
	buffer.Append(s6, 125)
	require.Len(t, buffer.elements, 4, "expected slice to still be length 4")
	require.Equal(t, 3, buffer.Size())

	// Check we can still remove series as expected after wrap around.
	require.Equal(t, s4, buffer.Remove(123))
	require.Equal(t, s5, buffer.Remove(124))
	require.Equal(t, s6, buffer.Remove(125))
}

func TestSeriesDataRingBuffer_NonContiguousSeries(t *testing.T) {
	buffer := &SeriesDataRingBuffer[types.InstantVectorSeriesData]{}

	s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
	s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
	s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
	s5 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 5, F: 5}}}
	s6 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 6, F: 6}}}
	buffer.Append(s1, 121)
	buffer.Append(s3, 123)
	buffer.Append(s4, 124)
	buffer.Append(s5, 125)
	buffer.Append(s6, 126)
	require.Len(t, buffer.elements, 8, "expected slice to be resized to length 8")
	require.Equal(t, 5, buffer.Size())

	require.Equal(t, s1, buffer.Get(121))
	require.Equal(t, s3, buffer.Get(123))
	require.Equal(t, s4, buffer.Get(124))
	require.Equal(t, s5, buffer.Get(125))
	require.Equal(t, s6, buffer.Get(126))

	require.Equal(t, s3, buffer.Remove(123))
	require.Equal(t, 5, buffer.Size(), "expected size to be unchanged after removing series in middle of buffer")
	require.Equal(t, s1, buffer.Get(121))
	require.Equal(t, s4, buffer.Get(124))
	require.Equal(t, s5, buffer.Get(125))
	require.Equal(t, s6, buffer.Get(126))

	data, found := buffer.RemoveIfPresent(123)
	require.False(t, found)
	require.Equal(t, types.InstantVectorSeriesData{}, data)

	// Remove the element at the front, should also resize to clear the empty slot at the head.
	buffer.Remove(121)
	require.Equal(t, 3, buffer.Size())
	require.Equal(t, s4, buffer.Get(124))
	require.Equal(t, s5, buffer.Get(125))
	require.Equal(t, s6, buffer.Get(126))

	buffer.Remove(125)
	require.Equal(t, 3, buffer.Size(), "expected size to be unchanged after removing series in middle of buffer")
	require.Equal(t, s4, buffer.Get(124))
	require.Equal(t, s6, buffer.Get(126))

	// Remove the element at the end, should also resize to clear the empty slot at the tail.
	buffer.Remove(126)
	require.Equal(t, 1, buffer.Size())
	require.Equal(t, s4, buffer.Get(124))
}

func TestSeriesDataRingBuffer_FindElementPositionForSeriesIndex(t *testing.T) {
	t.Run("odd number of elements", func(t *testing.T) {
		buffer := &SeriesDataRingBuffer[types.InstantVectorSeriesData]{}

		s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
		s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
		s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
		s5 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 5, F: 5}}}
		s6 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 6, F: 6}}}
		buffer.Append(s1, 121)
		buffer.Append(s3, 123)
		buffer.Append(s4, 124)
		buffer.Append(s5, 125)
		buffer.Append(s6, 126)

		require.Equal(t, 0, buffer.findElementPositionForSeriesIndex(121))
		require.Equal(t, 1, buffer.findElementPositionForSeriesIndex(123))
		require.Equal(t, 2, buffer.findElementPositionForSeriesIndex(124))
		require.Equal(t, 3, buffer.findElementPositionForSeriesIndex(125))
		require.Equal(t, 4, buffer.findElementPositionForSeriesIndex(126))

		require.PanicsWithValue(t, "attempted to find element position for series index 120, but it is not present in this buffer (first series index is 121, last series index is 126)", func() {
			buffer.findElementPositionForSeriesIndex(120)
		})

		require.PanicsWithValue(t, "attempted to find element position for series index 122, but it is not present in this buffer (first series index is 121, last series index is 126)", func() {
			buffer.findElementPositionForSeriesIndex(122)
		})

		require.PanicsWithValue(t, "attempted to find element position for series index 127, but it is not present in this buffer (first series index is 121, last series index is 126)", func() {
			buffer.findElementPositionForSeriesIndex(127)
		})

		// Remove an element in the middle, and confirm we correctly handle tombstones.
		buffer.Remove(123)

		require.Equal(t, 0, buffer.findElementPositionForSeriesIndex(121))
		require.Equal(t, 2, buffer.findElementPositionForSeriesIndex(124))
		require.Equal(t, 3, buffer.findElementPositionForSeriesIndex(125))
		require.Equal(t, 4, buffer.findElementPositionForSeriesIndex(126))

		require.PanicsWithValue(t, "attempted to find element position for series index 123, but this element has been removed", func() {
			buffer.findElementPositionForSeriesIndex(123)
		})
	})

	t.Run("even number of elements", func(t *testing.T) {
		buffer := &SeriesDataRingBuffer[types.InstantVectorSeriesData]{}

		s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
		s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
		s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
		s5 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 5, F: 5}}}
		buffer.Append(s1, 121)
		buffer.Append(s3, 123)
		buffer.Append(s4, 124)
		buffer.Append(s5, 125)

		require.Equal(t, 0, buffer.findElementPositionForSeriesIndex(121))
		require.Equal(t, 1, buffer.findElementPositionForSeriesIndex(123))
		require.Equal(t, 2, buffer.findElementPositionForSeriesIndex(124))
		require.Equal(t, 3, buffer.findElementPositionForSeriesIndex(125))
	})

	t.Run("buffer wrapped around", func(t *testing.T) {
		buffer := &SeriesDataRingBuffer[types.InstantVectorSeriesData]{}

		s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
		s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
		s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
		s5 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 5, F: 5}}}
		s6 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 6, F: 6}}}
		s7 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 7, F: 7}}}
		buffer.Append(s1, 121)
		buffer.Append(s3, 123)
		buffer.Append(s4, 124)
		buffer.Append(s5, 125)
		buffer.Remove(121)
		buffer.Remove(123)
		buffer.Append(s6, 126)
		buffer.Append(s7, 130)
		require.Equal(t, 4, buffer.Size())

		require.Equal(t, 0, buffer.findElementPositionForSeriesIndex(124))
		require.Equal(t, 1, buffer.findElementPositionForSeriesIndex(125))
		require.Equal(t, 2, buffer.findElementPositionForSeriesIndex(126))
		require.Equal(t, 3, buffer.findElementPositionForSeriesIndex(130))

		require.PanicsWithValue(t, "attempted to find element position for series index 123, but it is not present in this buffer (first series index is 124, last series index is 130)", func() {
			buffer.findElementPositionForSeriesIndex(123)
		})

		require.PanicsWithValue(t, "attempted to find element position for series index 127, but it is not present in this buffer (first series index is 124, last series index is 130)", func() {
			buffer.findElementPositionForSeriesIndex(127)
		})

		require.PanicsWithValue(t, "attempted to find element position for series index 131, but it is not present in this buffer (first series index is 124, last series index is 130)", func() {
			buffer.findElementPositionForSeriesIndex(131)
		})
	})
}
