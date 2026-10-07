// SPDX-License-Identifier: AGPL-3.0-only

package store

import (
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
