// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"context"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
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
		require.Equal(t, map[string]uint64{testUser1: 2}, tracker.seriesCounts())
	}
	now = now.Add(idleTimeout / 2)
	{
		// Push 2 more series, one is accepted, one is rejected.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{3, 4}, now)
		require.NoError(t, err)
		require.Len(t, rejected, 1)
		require.Equal(t, map[string]uint64{testUser1: 3}, tracker.seriesCounts())
	}
	{
		// Cleanup now is a noop, nothing expired yet.
		tracker.cleanup(now)
		require.Equal(t, map[string]uint64{testUser1: 3}, tracker.seriesCounts())
	}
	{
		// Push only series 2, series 1 will expire.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{2}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Equal(t, map[string]uint64{testUser1: 3}, tracker.seriesCounts())
	}
	now = now.Add(idleTimeout / 2)
	{
		// Cleanup again will remove series 1.
		tracker.cleanup(now)
		require.Equal(t, map[string]uint64{testUser1: 2}, tracker.seriesCounts())
	}
	{
		// Pushing 3, 4 works now.
		rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{3, 4}, now)
		require.NoError(t, err)
		require.Empty(t, rejected)
		require.Equal(t, map[string]uint64{testUser1: 3}, tracker.seriesCounts())
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
		require.Equal(t, map[string]uint64{testUser1: 2}, tracker1.seriesCounts())
		tracker1Events.transmit()
		// Tracker 2 is updated too.
		require.Equal(t, map[string]uint64{testUser1: 2}, tracker2.seriesCounts())
		requireTrackersSameData(t, tracker1, tracker2)
	}
	now = now.Add(idleTimeout / 4)
	{
		// Push 2 more series to the tracker 1.
		// Don't transmit them yet.
		rejected, err := tracker1.trackSeries(context.Background(), testUser1, []uint64{3, 4}, now)
		require.NoError(t, err)
		require.Len(t, rejected, 1) // We can't know which one is rejected, it's racy.
		require.Equal(t, map[string]uint64{testUser1: 3}, tracker1.seriesCounts())
		require.Equal(t, map[string]uint64{testUser1: 2}, tracker2.seriesCounts())
	}
	now = now.Add(idleTimeout / 4)
	{
		// Push 2 different series to the tracker 2.
		// Don't transmit them yet.
		rejected, err := tracker2.trackSeries(context.Background(), testUser1, []uint64{5, 6}, now)
		require.NoError(t, err)
		require.Len(t, rejected, 1) // We can't know which one is rejected, it's racy.
		require.Equal(t, map[string]uint64{testUser1: 3}, tracker1.seriesCounts())
		require.Equal(t, map[string]uint64{testUser1: 3}, tracker2.seriesCounts())
	}
	now = now.Add(idleTimeout / 4)
	{
		// transmit tracker1 events, this will make tracker2 go over the limit.
		tracker1Events.transmit()
		require.Equal(t, map[string]uint64{testUser1: 4}, tracker2.seriesCounts())
	}

	{
		// transmit tracker2 events, this will make tracker1 go over the limit.
		tracker2Events.transmit()
		require.Equal(t, map[string]uint64{testUser1: 4}, tracker1.seriesCounts())
		require.Equal(t, map[string]uint64{testUser1: 4}, tracker2.seriesCounts())
		// Series should be the same in both now.
		requireTrackersSameData(t, tracker1, tracker2)
	}
	now = now.Add(idleTimeout / 4)
	{
		tracker1.cleanup(now)
		tracker2.cleanup(now)
		// Only last 2 series remaining
		require.Equal(t, map[string]uint64{testUser1: 2}, tracker1.seriesCounts())
		require.Equal(t, map[string]uint64{testUser1: 2}, tracker2.seriesCounts())
	}
	now = now.Add(idleTimeout / 4)
	{
		tracker1.cleanup(now)
		tracker2.cleanup(now)
		// Only last 1 series remaining (the one that we pushed to tracker2)
		// This tests that creation event uses the correct creation timestamp.
		require.Equal(t, map[string]uint64{testUser1: 1}, tracker1.seriesCounts())
		require.Equal(t, map[string]uint64{testUser1: 1}, tracker2.seriesCounts())
	}
}

func TestTrackerStore_Snapshot(t *testing.T) {
	const idleTimeoutMinutes = 20
	const testUser1 = "user1"
	const testUser2 = "user2"
	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	tracker1 := newTrackerStore(idleTimeoutMinutes*time.Minute, log.NewNopLogger(), limiterMock{}, noopEvents{})

	for i := 0; i < 60; i++ {
		rejected, err := tracker1.trackSeries(context.Background(), testUser1, []uint64{uint64(i)}, now)
		require.Empty(t, rejected)
		require.NoError(t, err)

		rejected, err = tracker1.trackSeries(context.Background(), testUser2, []uint64{uint64(i * 1000), uint64(i * 10000)}, now)
		require.Empty(t, rejected)
		require.NoError(t, err)

		tracker1.cleanup(now)
		now = now.Add(time.Minute)
	}

	// testUser1 has 1 series per each one of the last idleTimeout clock.Minutes.
	// testUser2  has 2 series per each one of the last idleTimeout clock.Minutes.
	require.Equal(t, map[string]uint64{
		testUser1: idleTimeoutMinutes,
		testUser2: 2 * idleTimeoutMinutes,
	}, tracker1.seriesCounts())

	tracker2 := newTrackerStore(idleTimeoutMinutes*time.Minute, log.NewNopLogger(), limiterMock{}, noopEvents{})

	var data []byte
	for shard := uint8(0); shard < shards; shard++ {
		data = tracker1.snapshot(shard, now, data[:0])
		err := tracker2.loadSnapshot(data, now)
		require.NoError(t, err)
	}
	require.Equal(t, map[string]uint64{
		testUser1: idleTimeoutMinutes,
		testUser2: 2 * idleTimeoutMinutes,
	}, tracker2.seriesCounts())

	// Check that they hold the same data.
	requireTrackersSameData(t, tracker1, tracker2)

	// Loading same snapshot again should be a noop.
	for shard := uint8(0); shard < shards; shard++ {
		data = tracker1.snapshot(shard, now, data[:0])
		err := tracker2.loadSnapshot(data, now)
		require.NoError(t, err)
	}

	// Check that the total series counts are the same.
	require.Equal(t, map[string]uint64{
		testUser1: idleTimeoutMinutes,
		testUser2: 2 * idleTimeoutMinutes,
	}, tracker2.seriesCounts())

	// Check that they hold the same data.
	requireTrackersSameData(t, tracker1, tracker2)
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
	require.Equal(t, map[string]uint64{testUser1: 1}, tracker.seriesCounts())
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
	lastUpdate := now

	// Update series 1, 2 for testUser2. Series 3 will expire.
	rejected, err = tracker.trackSeries(context.Background(), testUser2, []uint64{1, 2}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)

	now = now.Add(defaultIdleTimeout / 2)
	tracker.cleanup(now)
	// Tenant 1 is deleted.
	// testUser2 still has series 1 and 2, with no data in shard3
	require.Equal(t, map[string]uint64{testUser2: 2}, tracker.seriesCounts())
	require.Equal(t, map[string]map[uint64]clock.Minutes{
		testUser2: {1: clock.ToMinutes(lastUpdate)},
	}, decodeSnapshot(t, tracker.snapshot(1, now, nil)))
	require.Equal(t, map[string]map[uint64]clock.Minutes{
		testUser2: {2: clock.ToMinutes(lastUpdate)},
	}, decodeSnapshot(t, tracker.snapshot(2, now, nil)))

	// Even though testUser2 doesn't have data in shard 3, it's still included in the snapshot.
	require.Equal(t, map[string]map[uint64]clock.Minutes{testUser2: {}}, decodeSnapshot(t, tracker.snapshot(3, now, nil)))

	now = now.Add(defaultIdleTimeout / 2)

	tracker.cleanup(now)
	// testUser2 is deleted.
	require.Empty(t, tracker.seriesCounts())
	for i := 0; i < shards; i++ {
		require.Emptyf(t, decodeSnapshot(t, tracker.snapshot(uint8(i), now, nil)), "shard %d should have an empty snapshot", i)
	}
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

	require.Empty(t, tracker.seriesCounts(), "there should be no tenants with series counts")
	for i := uint8(0); i < shards; i++ {
		require.Emptyf(t, decodeSnapshot(t, tracker.snapshot(i, now(), nil)), "shard %d should have an empty snapshot", i)
	}
}

type createdSeriesCounter struct {
	count *atomic.Uint64
}

func (c createdSeriesCounter) publishCreatedSeries(_ context.Context, _ string, series []uint64, _ time.Time) error {
	c.count.Add(uint64(len(series)))
	return nil
}

func TestTrackerStore_PrometheusCollector(t *testing.T) {
	const defaultIdleTimeout = 20 * time.Minute
	const testUser1 = "user1"
	const testUser2 = "user2"

	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	tracker := newTrackerStore(defaultIdleTimeout, log.NewNopLogger(), limiterMock{}, noopEvents{})

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(tracker))

	rejected, err := tracker.trackSeries(context.Background(), testUser1, []uint64{1, 2}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)
	rejected, err = tracker.trackSeries(context.Background(), testUser2, []uint64{1, 2, 3}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)

	require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(`
		# HELP cortex_usage_tracker_active_series Number of active series tracker for each user.
		# TYPE cortex_usage_tracker_active_series gauge
		cortex_usage_tracker_active_series{user="user1"} 2
		cortex_usage_tracker_active_series{user="user2"} 3
	`), "cortex_usage_tracker_active_series"))

	now = now.Add(defaultIdleTimeout / 2)

	// Update series 1, 2 for testUser2. Series 3 will expire.
	rejected, err = tracker.trackSeries(context.Background(), testUser2, []uint64{1, 2}, now)
	require.NoError(t, err)
	require.Empty(t, rejected)

	now = now.Add(defaultIdleTimeout / 2)
	tracker.cleanup(now)

	// Tenant 1 is deleted.
	require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(`
		# HELP cortex_usage_tracker_active_series Number of active series tracker for each user.
		# TYPE cortex_usage_tracker_active_series gauge
		cortex_usage_tracker_active_series{user="user2"} 2
	`), "cortex_usage_tracker_active_series"))

	now = now.Add(defaultIdleTimeout / 2)

	tracker.cleanup(now)
	// testUser2 is deleted.
	require.NoError(t, testutil.CollectAndCompare(reg, strings.NewReader(`
		# HELP cortex_usage_tracker_active_series Number of active series tracker for each user.
		# TYPE cortex_usage_tracker_active_series gauge
	`), "cortex_usage_tracker_active_series"))
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

func requireTrackersSameData(t *testing.T, tracker1, tracker2 *trackerStore) {
	t.Helper()
	snapshotTime := time.Unix(0, 0)
	for i := uint8(0); i < shards; i++ {
		tracker1Snapshot := decodeSnapshot(t, tracker1.snapshot(i, snapshotTime, nil))
		tracker2Snapshot := decodeSnapshot(t, tracker2.snapshot(i, snapshotTime, nil))
		require.Equalf(t, tracker1Snapshot, tracker2Snapshot, "shard %d", i)
	}
}

func decodeSnapshot(t *testing.T, data []byte) map[string]map[uint64]clock.Minutes {
	snapshot := encoding.Decbuf{B: data}
	version := snapshot.Byte()
	require.NoError(t, snapshot.Err())
	require.Equal(t, uint8(snapshotEncodingVersion), version)
	shard := snapshot.Byte()
	require.NoError(t, snapshot.Err())
	require.True(t, shard < shards)

	_ = time.Unix(int64(snapshot.Be64()), 0)
	require.NoError(t, snapshot.Err())

	tenantsLen := snapshot.Uvarint64()
	require.NoError(t, snapshot.Err())

	res := make(map[string]map[uint64]clock.Minutes, tenantsLen)
	for i := 0; i < int(tenantsLen); i++ {
		// We don't check for tenantID string length here, because we don't require it to be non-empty when we track series.
		tenantID := snapshot.UvarintStr()
		require.NoErrorf(t, snapshot.Err(), "can't read tenantID %d", i)

		seriesLen := int(snapshot.Uvarint64())
		require.NoError(t, snapshot.Err())
		shard := make(map[uint64]clock.Minutes, seriesLen)

		for i := 0; i < seriesLen; i++ {
			series := snapshot.Be64()
			require.NoError(t, snapshot.Err())
			ts := clock.Minutes(snapshot.Byte())
			require.NoError(t, snapshot.Err())
			shard[series] = ts
		}
		res[tenantID] = shard
	}
	return res
}
