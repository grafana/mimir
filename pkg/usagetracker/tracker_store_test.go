// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestTrackerStore_HappyCase(t *testing.T) {
	const idleTimeout = 20 * time.Minute
	const testUser1 = "user1"
	limits := limiterMock{testUser1: 3}

	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	tracker := newTrackerStore(idleTimeout, log.NewNopLogger(), limits, noopEvents{})
	{
		// Push 2 series, both are accepted.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{1, 2}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(2), tracker.tenants[testUser1].series.Load())
	}
	now = now.Add(idleTimeout / 2)
	{
		// Push 2 more series, one is accepted, one is rejected.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{3, 4}, now)
		require.NoError(t, err)
		require.Equal(t, []uint64{4}, rejected)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(3), tracker.tenants[testUser1].series.Load())
	}
	{
		// Cleanup now is a noop, nothing expired yet.
		tracker.cleanup(now)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(3), tracker.tenants[testUser1].series.Load())
	}
	{
		// Push only series 2, series 1 will expire.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{2}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(3), tracker.tenants[testUser1].series.Load())
	}
	now = now.Add(idleTimeout / 2)
	{
		// Cleanup again will remove series 1.
		tracker.cleanup(now)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(2), tracker.tenants[testUser1].series.Load())
	}
	{
		// Pushing 3, 4 works now.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{3, 4}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Len(t, tracker.tenants, 1)
		require.Equal(t, uint64(3), tracker.tenants[testUser1].series.Load())
	}
}

func TestTrackerStore_CreatedSeriesCommunication(t *testing.T) {
	const idleTimeout = 20 * time.Minute
	const testUser1 = "user1"
	limits := limiterMock{testUser1: 3}

	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	tracker1Events := eventsPipe{}
	tracker1 := newTrackerStore(idleTimeout, log.NewNopLogger(), limits, &tracker1Events)
	tracker2Events := eventsPipe{}
	tracker2 := newTrackerStore(idleTimeout, log.NewNopLogger(), limits, &tracker2Events)
	tracker1Events.listeners = []*trackerStore{tracker2}
	tracker2Events.listeners = []*trackerStore{tracker1}

	{
		// Push 2 series to tracker 1, both accepted.
		rejected, err := tracker1.trackSeries(context.Background(), testUser1, []uint64{1, 2}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Len(t, tracker1.tenants, 1)
		require.Equal(t, uint64(2), tracker1.tenants[testUser1].series.Load())
		tracker1Events.transmit()
		// Tracker 2 is updated too.
		require.Equal(t, uint64(2), tracker2.tenants[testUser1].series.Load())
		requireSameSeries(t, tracker1, tracker2)
	}
	now = now.Add(idleTimeout / 4)
	{
		// Push 2 more series to the tracker 1.
		// Don't transmit them yet.
		rejected, err := tracker1.trackSeries(context.Background(), testUser1, []uint64{3, 4}, now)
		require.NoError(t, err)
		require.Equal(t, []uint64{4}, rejected)
		require.Len(t, tracker1.tenants, 1)
		require.Equal(t, uint64(3), tracker1.tenants[testUser1].series.Load())
		require.Equal(t, uint64(2), tracker2.tenants[testUser1].series.Load())
	}
	now = now.Add(idleTimeout / 4)
	{
		// Push 2 different series to the tracker 2.
		// Don't transmit them yet.
		rejected, err := tracker2.trackSeries(context.Background(), testUser1, []uint64{5, 6}, now)
		require.NoError(t, err)
		require.Equal(t, []uint64{6}, rejected)
		require.Equal(t, uint64(3), tracker1.tenants[testUser1].series.Load())
		require.Equal(t, uint64(3), tracker2.tenants[testUser1].series.Load())
	}
	now = now.Add(idleTimeout / 4)
	{
		// transmit tracker1 events, this will make tracker2 go over the limit.
		tracker1Events.transmit()
		require.Equal(t, uint64(4), tracker2.tenants[testUser1].series.Load())
	}

	{
		// transmit tracker2 events, this will make tracker1 go over the limit.
		tracker2Events.transmit()
		require.Equal(t, uint64(4), tracker1.tenants[testUser1].series.Load())
		require.Equal(t, uint64(4), tracker2.tenants[testUser1].series.Load())
		// Series should be the same in both now.
		requireSameSeries(t, tracker1, tracker2)
	}
	now = now.Add(idleTimeout / 4)
	{
		tracker1.cleanup(now)
		tracker2.cleanup(now)
		// Only last 2 series remaining
		require.Equal(t, uint64(2), tracker1.tenants[testUser1].series.Load())
		require.Equal(t, uint64(2), tracker2.tenants[testUser1].series.Load())
	}
	now = now.Add(idleTimeout / 4)
	{
		tracker1.cleanup(now)
		tracker2.cleanup(now)
		// Only last 1 series remaining (the one that we pushed to tracker2)
		// This tests that creation event uses the correct creation timestamp.
		require.Equal(t, uint64(1), tracker1.tenants[testUser1].series.Load())
		require.Equal(t, uint64(1), tracker2.tenants[testUser1].series.Load())
	}
}

func TestTrackerStore_Snapshot(t *testing.T) {
	const idleTimeoutMinutes = 20
	const testUser1 = "user1"
	const testUser2 = "user2"
	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	tracker := newTrackerStore(idleTimeoutMinutes*time.Minute, log.NewNopLogger(), limiterMock{}, noopEvents{})
	for i := 0; i < 60; i++ {
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{uint64(i)}, now)
		require.Empty(t, rejected)
		require.NoError(t, err)

		rejected, err = tracker.trackSeries(context.Background(), testUser2, []uint64{uint64(i * 1000), uint64(i * 10000)}, now)
		require.Empty(t, rejected)
		require.NoError(t, err)

		tracker.cleanup(now)
		now = now.Add(time.Minute)
	}

	// testUser1 has 1 series per each one of the last idleTimeout minutes.
	require.Equal(t, idleTimeoutMinutes, int(tracker.tenants[testUser1].series.Load()))
	// testUser2  has 2 series per each one of the last idleTimeout minutes.
	require.Equal(t, 2*idleTimeoutMinutes, int(tracker.tenants[testUser2].series.Load()))

	tracker2 := newTrackerStore(idleTimeoutMinutes*time.Minute, log.NewNopLogger(), limiterMock{}, noopEvents{})
	var data []byte
	for shard := uint8(0); shard < shards; shard++ {
		data = tracker.snapshot(shard, now, data[:0])
		err := tracker2.loadSnapshot(data, now)
		require.NoError(t, err)
	}

	require.Equal(t, idleTimeoutMinutes, int(tracker.tenants[testUser1].series.Load()))
	require.Equal(t, 2*idleTimeoutMinutes, int(tracker.tenants[testUser2].series.Load()))

	// Check that they hold the same data.
	requireSameSeries(t, tracker, tracker2)

	// Loading same snapshot again should be a noop.
	for shard := uint8(0); shard < shards; shard++ {
		data = tracker.snapshot(shard, now, data[:0])
		err := tracker2.loadSnapshot(data, now)
		require.NoError(t, err)
	}

	// Check that the total series counts are the same.
	require.Equal(t, idleTimeoutMinutes, int(tracker.tenants[testUser1].series.Load()))
	require.Equal(t, 2*idleTimeoutMinutes, int(tracker.tenants[testUser2].series.Load()))

	// Check that they hold the same data.
	requireSameSeries(t, tracker, tracker2)
}

func requireSameSeries(t *testing.T, tracker *trackerStore, tracker2 *trackerStore) {
	t.Helper()

	for i := uint8(0); i < shards; i++ {
		for tenantID, originalShard := range tracker.data[i] {
			loadedShard, ok := tracker2.data[i][tenantID]
			require.True(t, ok || len(originalShard.series) == 0, "shard %d, tenant %s", i, tenantID)
			for series, ts := range originalShard.series {
				loadedTs, ok := loadedShard.series[series]
				require.True(t, ok, "shard %d, tenant %s, series %d", i, tenantID, series)
				require.Equal(t, ts.Load(), loadedTs.Load(), "shard %d, tenant %s, series %d", i, tenantID, series)
			}
		}
	}
}

func TestTrackerStore_Cleanup_OffByOneError(t *testing.T) {
	const testUser1 = "user1"

	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)
	tracker := newTrackerStore(time.Minute, log.NewNopLogger(), limiterMock{}, noopEvents{})

	rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{1}, now)
	require.Empty(t, rejected)
	require.NoError(t, err)

	now = now.Add(time.Minute)

	rejected, err = tracker.trackSeries(context.Background(), testUser1, []uint64{2}, now)
	require.Empty(t, rejected)
	require.NoError(t, err)

	tracker.cleanup(now)

	// There should be exactly 1 series, the other one expired.
	require.Equal(t, 1, int(tracker.tenants[testUser1].series.Load()))
}

func TestTrackerStore_Cleanup_Tenants(t *testing.T) {
	const defaultIdleTimeout = 20 * time.Minute
	const testUser1 = "user1"
	const testUser2 = "user2"
	limits := limiterMock{testUser1: 3}

	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	tracker := newTrackerStore(defaultIdleTimeout, log.NewNopLogger(), limits, noopEvents{})
	// Push 2 series to testUser1, both are accepted.
	rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{1, 2}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)
	// Push 2 series to testUser2, both are accepted.
	rejected, err = tracker.trackSeries(context.Background(), testUser2, []uint64{1, 2, 3}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)

	now = now.Add(defaultIdleTimeout / 2)

	// Update series 1, 2 for testUser2. Series 3 will expire.
	rejected, err = tracker.trackSeries(context.Background(), testUser2, []uint64{1, 2}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)

	now = now.Add(defaultIdleTimeout / 2)
	tracker.cleanup(now)
	// Tenant 1 is deleted.
	require.Nil(t, tracker.data[1][testUser1])
	require.Nil(t, tracker.data[2][testUser1])
	require.Nil(t, tracker.tenants[testUser1])
	// testUser2 has only shard 3 deleted.
	require.NotNil(t, tracker.data[1][testUser2])
	require.NotNil(t, tracker.data[2][testUser2])
	require.Nil(t, tracker.data[3][testUser2])
	require.NotNil(t, tracker.tenants[testUser2])

	now = now.Add(defaultIdleTimeout / 2)

	tracker.cleanup(now)
	// testUser2 is deleted.
	require.Nil(t, tracker.data[1][testUser2])
	require.Nil(t, tracker.data[2][testUser2])
	require.Nil(t, tracker.tenants[testUser2])
}

// TestTrackerStore_Cleanup_Concurrency runs cleanup() and trackSeries() concurrently.
// It attempts to catch any race condition that may happen between series deletion marking and series creation.
func TestTrackerStore_Cleanup_Concurrency(t *testing.T) {
	const tenant = "user"
	const idleTimeoutMinutes = 5
	const maxSeriesRange = 10000
	nowUnixMinutes := atomic.NewInt64(0)
	now := func() time.Time { return time.Unix(nowUnixMinutes.Load()*60, 0) }

	createdSeries := createdSeriesCounter{count: atomic.NewUint64(0)}
	tracker := newTrackerStore(idleTimeoutMinutes*time.Minute, log.NewNopLogger(), limiterMock{}, createdSeries)

	wg := sync.WaitGroup{}
	wg.Add(1)
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				seriesID := uint64(rand.Int63n(maxSeriesRange))
				_, _ = tracker.trackSeries(context.Background(), tenant, []uint64{seriesID}, now())
			}
		}
	}()

	// Wait until we perform 1e3 cleanups and create 1e3 series.
	cleanups := 0
	for createdSeries.count.Load() < 100*maxSeriesRange {
		info := tracker.getOrCreateTenantInfo(tenant)
		info.release()
		// Keep increasing the timestamp every time.
		nowUnixMinutes.Inc()
		tracker.cleanup(now())
		cleanups++
	}
	close(done)
	t.Logf("Total series created: %d, cleanups: %d", createdSeries.count.Load(), cleanups)
	wg.Wait()

	// Wait an idle period then cleanup.
	nowUnixMinutes.Add(idleTimeoutMinutes)
	tracker.cleanup(now())
	// Tracker should be empty nowUnixMinutes.
	// If it's not, then we did something wrong.
	for _, info := range tracker.tenants {
		t.Errorf("Tenant %q is still present with %d series %d refs", tenant, info.series.Load(), info.refs.Load())
	}
	for i := uint8(0); i < shards; i++ {
		for tenantID, shard := range tracker.data[i] {
			t.Errorf("Shard %d still has tenant %q with %d series=", i, tenantID, len(shard.series))
			for s, ts := range shard.series {
				t.Errorf("Tenant %q: series %d with timestamp=%d (nowUnixMinutes=%d)", tenantID, s, minutes(ts.Load()), toMinutes(time.Unix(nowUnixMinutes.Load(), 0)))
			}
		}
	}
}

type createdSeriesCounter struct {
	count *atomic.Uint64
}

func (c createdSeriesCounter) publishCreatedSeries(_ context.Context, _ string, series []uint64, _ time.Time) error {
	c.count.Add(uint64(len(series)))
	return nil
}

type limiterMock map[string]uint64

func (l limiterMock) localSeriesLimit(userID string) uint64 { return l[userID] }

type noopEvents struct{}

func (n noopEvents) publishCreatedSeries(_ context.Context, _ string, _ []uint64, _ time.Time) error {
	return nil
}

type eventsPipe struct {
	listeners []*trackerStore
	events    []createdSeriesEvent
}

type createdSeriesEvent struct {
	tenantID  string
	series    []uint64
	timestamp time.Time
}

func (ep *eventsPipe) publishCreatedSeries(_ context.Context, tenantID string, series []uint64, timestamp time.Time) error {
	ep.events = append(ep.events, createdSeriesEvent{tenantID, series, timestamp})
	return nil
}

func (ep *eventsPipe) transmit() {
	for _, t := range ep.listeners {
		for _, ev := range ep.events {
			t.processCreatedSeriesEvent(ev.tenantID, slices.Clone(ev.series), ev.timestamp, ev.timestamp)
		}
	}
	ep.events = nil
}

func TestMinutes(t *testing.T) {
	// t0 is at 4-hour boundary
	t0 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(30 * time.Minute)
	t2 := t0.Add(-30 * time.Minute)

	t.Run("sub", func(t *testing.T) {
		for _, tc := range []struct {
			a, b     minutes
			expected int
		}{
			{a: toMinutes(t0), b: toMinutes(t0.Add(-5 * time.Minute)), expected: 5},
			{a: toMinutes(t1), b: toMinutes(t1.Add(-5 * time.Minute)), expected: 5},
			{a: toMinutes(t2), b: toMinutes(t2.Add(-5 * time.Minute)), expected: 5},
			{a: toMinutes(t0), b: toMinutes(t0.Add(5 * time.Minute)), expected: -5},
			{a: toMinutes(t1), b: toMinutes(t1.Add(5 * time.Minute)), expected: -5},
			{a: toMinutes(t2), b: toMinutes(t2.Add(5 * time.Minute)), expected: -5},

			{a: toMinutes(t0), b: toMinutes(t0.Add(-59 * time.Minute)), expected: 59},
			{a: toMinutes(t1), b: toMinutes(t1.Add(-59 * time.Minute)), expected: 59},
			{a: toMinutes(t2), b: toMinutes(t2.Add(-59 * time.Minute)), expected: 59},
			{a: toMinutes(t0), b: toMinutes(t0.Add(59 * time.Minute)), expected: -59},
			{a: toMinutes(t1), b: toMinutes(t1.Add(59 * time.Minute)), expected: -59},
			{a: toMinutes(t2), b: toMinutes(t2.Add(59 * time.Minute)), expected: -59},
		} {
			t.Run(fmt.Sprintf("%s sub %s = %d", tc.a, tc.b, tc.expected), func(t *testing.T) {
				require.Equal(t, tc.expected, tc.a.sub(tc.b))
			})
		}
	})

	t.Run("greaterThan", func(t *testing.T) {
		for _, tc := range []struct {
			a, b     minutes
			expected bool
		}{
			{a: toMinutes(t0), b: toMinutes(t0.Add(-5 * time.Minute)), expected: true},
			{a: toMinutes(t1), b: toMinutes(t1.Add(-5 * time.Minute)), expected: true},
			{a: toMinutes(t2), b: toMinutes(t2.Add(-5 * time.Minute)), expected: true},
			{a: toMinutes(t0), b: toMinutes(t0.Add(5 * time.Minute)), expected: false},
			{a: toMinutes(t1), b: toMinutes(t1.Add(5 * time.Minute)), expected: false},
			{a: toMinutes(t2), b: toMinutes(t2.Add(5 * time.Minute)), expected: false},

			{a: toMinutes(t0), b: toMinutes(t0.Add(-59 * time.Minute)), expected: true},
			{a: toMinutes(t1), b: toMinutes(t1.Add(-59 * time.Minute)), expected: true},
			{a: toMinutes(t2), b: toMinutes(t2.Add(-59 * time.Minute)), expected: true},
			{a: toMinutes(t0), b: toMinutes(t0.Add(59 * time.Minute)), expected: false},
			{a: toMinutes(t1), b: toMinutes(t1.Add(59 * time.Minute)), expected: false},
			{a: toMinutes(t2), b: toMinutes(t2.Add(59 * time.Minute)), expected: false},
		} {
			t.Run(fmt.Sprintf("%s > %s = %t", tc.a, tc.b, tc.expected), func(t *testing.T) {
				require.Equal(t, tc.expected, tc.a.greaterThan(tc.b))
			})
		}
	})
}
