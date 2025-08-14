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
	require.False(t, buffer.IsPresent(0))
	require.False(t, buffer.IsPresent(1))

	// Add some series.
	s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
	s2 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 2, F: 2}}}
	s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
	buffer.Append(s1, 120)
	buffer.Append(s2, 121)
	buffer.Append(s3, 122)

	require.Equal(t, 3, buffer.Size())
	require.False(t, buffer.IsPresent(0))
	require.False(t, buffer.IsPresent(1))
	require.True(t, buffer.IsPresent(120))
	require.True(t, buffer.IsPresent(121))
	require.True(t, buffer.IsPresent(122))
	require.False(t, buffer.IsPresent(123))
	require.Len(t, buffer.data, 4, "expected slice to be resized in powers of 2")
	require.Equal(t, s1, buffer.Get(120))
	require.Equal(t, s2, buffer.Get(121))
	require.Equal(t, s3, buffer.Get(122))

	// Remove a series.
	removed := buffer.Remove(120)
	require.Equal(t, s1, removed)
	require.False(t, buffer.IsPresent(120))
	require.True(t, buffer.IsPresent(121))
	require.Equal(t, 2, buffer.Size())

	// Append two more series: the first will take the last slot in the slice, and the next will take the first slot.
	s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
	buffer.Append(s4, 123)
	require.True(t, buffer.IsPresent(123))
	require.Equal(t, 3, buffer.Size())
	require.Len(t, buffer.data, 4, "expected slice to be unchanged")

	s5 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 6, F: 6}}}
	buffer.Append(s5, 124)
	require.True(t, buffer.IsPresent(124))
	require.Equal(t, 4, buffer.Size())
	require.Len(t, buffer.data, 4, "expected slice to be unchanged")

	// Remove everything and then add another series to ensure the buffer is correctly reset when emptied.
	require.Equal(t, s2, buffer.RemoveFirst())
	require.Equal(t, s3, buffer.RemoveFirst())
	require.Equal(t, s4, buffer.RemoveFirst())
	require.Equal(t, s5, buffer.RemoveFirst())

	require.Equal(t, 0, buffer.Size())
	require.False(t, buffer.IsPresent(123))
	require.False(t, buffer.IsPresent(124))
	require.Len(t, buffer.data, 4, "expected slice to be unchanged")

	s6 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 7, F: 7}}}
	buffer.Append(s6, 20)
	require.Equal(t, 1, buffer.Size())
	require.True(t, buffer.IsPresent(20))
	require.Equal(t, s6, buffer.Get(20))
	require.Len(t, buffer.data, 4, "expected slice to be unchanged")
}

func TestSeriesDataRingBuffer_Resizing(t *testing.T) {
	buffer := &SeriesDataRingBuffer[types.InstantVectorSeriesData]{}

	// Add two series to fill the slice.
	s1 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 1, F: 1}}}
	s2 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 2, F: 2}}}
	buffer.Append(s1, 120)
	buffer.Append(s2, 121)
	require.Len(t, buffer.data, 2, "expected slice to be resized to length 2")
	require.Equal(t, 2, buffer.Size())

	// Remove the first series, then add two more to trigger a resize while the first series is not the first in the slice.
	require.Equal(t, s1, buffer.Remove(120))
	require.Len(t, buffer.data, 2, "expected slice to be unchanged")
	require.Equal(t, 1, buffer.Size())

	s3 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 3, F: 3}}}
	s4 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 4, F: 4}}}
	buffer.Append(s3, 122)
	buffer.Append(s4, 123)
	require.Len(t, buffer.data, 4, "expected slice to be resized to length 4")
	require.Equal(t, 3, buffer.Size())

	// Check we can still get series as expected after resizing.
	require.Equal(t, s2, buffer.Get(121))
	require.Equal(t, s3, buffer.Get(122))
	require.Equal(t, s4, buffer.Get(123))
	require.True(t, buffer.IsPresent(121))
	require.True(t, buffer.IsPresent(122))
	require.True(t, buffer.IsPresent(123))
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
	require.Len(t, buffer.data, 4, "expected slice to be resized to length 4")
	require.Equal(t, 4, buffer.Size())

	// Remove the first three series, then add two more to wrap around to the front of the slice.
	require.Equal(t, s1, buffer.Remove(120))
	require.Equal(t, s2, buffer.Remove(121))
	require.Equal(t, s3, buffer.Remove(122))
	require.Len(t, buffer.data, 4, "expected slice to still be length 4")
	require.Equal(t, 1, buffer.Size())

	s5 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 5, F: 5}}}
	s6 := types.InstantVectorSeriesData{Floats: []promql.FPoint{{T: 6, F: 6}}}
	buffer.Append(s5, 124)
	buffer.Append(s6, 125)
	require.Len(t, buffer.data, 4, "expected slice to still be length 4")
	require.Equal(t, 3, buffer.Size())

	// Check we can still remove series as expected after wrap around.
	require.Equal(t, s4, buffer.Remove(123))
	require.Equal(t, s5, buffer.Remove(124))
	require.Equal(t, s6, buffer.Remove(125))
}
