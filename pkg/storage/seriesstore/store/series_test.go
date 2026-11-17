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

// A series' newest sample ends its last native metadata version, so it's found wherever the series keeps it: in its
// open heads, and once they're cut, in its chunks and out-of-order samples.
func TestSeries_newestSample(t *testing.T) {
	for name, tc := range map[string]struct {
		series     func() *Series
		wantNewest int64
		wantOK     bool
	}{
		"without samples": {
			series: func() *Series { return &Series{} },
		},
		"in the float head, which holds the newest in-order sample": {
			series: func() *Series {
				s := &Series{}
				s.pushChunk(ChunkMeta{MinTime: 0, MaxTime: 1_000})
				for _, ts := range []int64{2_000, 3_000} {
					_, err := appendFloatInOrder(s, nil, ts, 1, true)
					require.NoError(t, err)
				}
				return s
			},
			wantNewest: 3_000, wantOK: true,
		},
		// Out-of-order compactions write chunks after newer ones.
		"in a chunk before the last one": {
			series: func() *Series {
				s := &Series{}
				s.pushChunk(ChunkMeta{MinTime: 4_000, MaxTime: 5_000})
				s.pushChunk(ChunkMeta{MinTime: 1_000, MaxTime: 2_000, OutOfOrder: true})
				return s
			},
			wantNewest: 5_000, wantOK: true,
		},
		"in the open out-of-order chunk": {
			series: func() *Series {
				s := &Series{}
				s.pushChunk(ChunkMeta{MinTime: 1_000, MaxTime: 2_000})
				s.setOutOfOrder([]oooSample{{T: 2_500}, {T: 2_600}})
				return s
			},
			wantNewest: 2_600, wantOK: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			newest, ok := tc.series().newestSample()
			require.Equal(t, tc.wantOK, ok)
			require.Equal(t, tc.wantNewest, newest)
		})
	}
}
