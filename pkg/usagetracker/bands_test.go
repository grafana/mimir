// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

func TestBandTableSpaceSaving(t *testing.T) {
	var table bandTable
	for i := 0; i < maxRetainedBands; i++ {
		table.admit(uint16(i))
	}
	_, counts := table.snapshot()
	require.Len(t, counts, maxRetainedBands)

	// Every retained band has one series, so a new band may replace one of them
	// and starts at 1. Expiring that series removes it.
	table.admit(1000)
	_, counts = table.snapshot()
	require.Len(t, counts, maxRetainedBands)
	var found bool
	for _, c := range counts {
		if c.band == 1000 {
			found = true
			require.Equal(t, uint64(1), c.count)
		}
	}
	require.True(t, found)
	table.expire(1000)
	_, counts = table.snapshot()
	require.Len(t, counts, maxRetainedBands-1)
	for _, c := range counts {
		require.NotEqual(t, uint16(1000), c.band)
	}

	// A band hotter than one series is not evicted by a newcomer.
	table = bandTable{}
	for i := 0; i < maxRetainedBands; i++ {
		table.admit(uint16(i))
		table.admit(uint16(i))
	}
	table.admit(1000)
	_, counts = table.snapshot()
	require.Len(t, counts, maxRetainedBands)
	for _, c := range counts {
		require.NotEqual(t, uint16(1000), c.band)
		require.Equal(t, uint64(2), c.count)
	}
}

func TestTrackSeriesKeepsLocalityAndReloadsSnapshot(t *testing.T) {
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	tracker := newTrackerStore(20*time.Minute, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())

	const band = uint32(0x00ab)
	locality := band << 16
	rejected, err := tracker.trackSeries(context.Background(), "tenant", []uint64{7, 8, 9}, []uint32{locality, locality, 0}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)

	views := tracker.tenantBands("tenant")
	require.Len(t, views, 1)
	require.Equal(t, uint64(2), views[0].localitySeries)
	require.Equal(t, uint64(3), views[0].total)
	require.Len(t, views[0].counts, 1)
	require.Equal(t, uint16(band), views[0].counts[0].band)
	require.Equal(t, uint64(2), views[0].counts[0].count)

	// A later update does not double-count a series that already has a hash.
	rejected, err = tracker.trackSeries(context.Background(), "tenant", []uint64{7}, []uint32{locality}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)
	views = tracker.tenantBands("tenant")
	require.Equal(t, uint64(2), views[0].localitySeries)

	snap := make([][]byte, shards)
	for i := uint8(0); i < shards; i++ {
		snap[i] = tracker.snapshot(i, now, nil)
	}
	restored := newTrackerStore(20*time.Minute, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
	require.NoError(t, restored.loadSnapshots(snap, now.Add(time.Minute)))
	views = restored.tenantBands("tenant")
	require.Len(t, views, 1)
	require.Equal(t, uint64(2), views[0].localitySeries)
	require.Equal(t, uint16(band), views[0].counts[0].band)
}

func TestCleanupDecrementsBand(t *testing.T) {
	now := time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)
	tracker := newTrackerStore(time.Minute, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
	locality := uint32(3) << 16
	_, err := tracker.trackSeries(context.Background(), "tenant", []uint64{1}, []uint32{locality}, now)
	require.NoError(t, err)

	tracker.cleanup(now.Add(2 * time.Minute))
	views := tracker.tenantBands("tenant")
	if len(views) == 0 {
		return
	}
	require.Zero(t, views[0].localitySeries)
	require.Empty(t, views[0].counts)
}
