// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeriesKeepsItsTrackerMatches(t *testing.T) {
	var series Series
	require.Empty(t, series.matchedTrackersInto(nil))

	// The first 32 trackers are bits of the slot, which allocates nothing.
	series.setMatchedTrackers([]uint16{0, 5, 31})
	require.Equal(t, []uint16{0, 5, 31}, series.matchedTrackersInto(nil))
	require.Nil(t, series.extra)

	// One past them moves all of the matches to the extra state, and back when they go.
	series.setMatchedTrackers([]uint16{1, 40})
	require.Equal(t, []uint16{1, 40}, series.matchedTrackersInto(nil))
	series.setMatchedTrackers([]uint16{2})
	require.Equal(t, []uint16{2}, series.matchedTrackersInto(nil))
	require.Nil(t, series.extra)

	series.setMatchedTrackers(nil)
	require.Empty(t, series.matchedTrackersInto(nil))
}

// A series keeps its matches of the first trackers in its bits, and of more in a set it shares with the series that
// match the same: what it matches is the same either way.
func TestSeriesKeepsTheTrackersItMatches(t *testing.T) {
	for _, matches := range [][]uint16{nil, {0}, {3, 30}, {0, 31}, {5, 40, 63}, {200}} {
		var first, second Series
		first.setMatchedTrackers(matches)
		second.setMatchedTrackers(slices.Clone(matches))
		require.Equal(t, matches, first.matchedTrackersInto(nil), "%v", matches)
		require.Equal(t, first.trackerBits, second.trackerBits, "the same set for the same matches")
		require.Nil(t, first.extra)
	}
	var series Series
	series.setMatchedTrackers([]uint16{1, 50})
	series.setMatchedTrackers([]uint16{2})
	require.Equal(t, []uint16{2}, series.matchedTrackersInto(nil), "back to bits")
}
