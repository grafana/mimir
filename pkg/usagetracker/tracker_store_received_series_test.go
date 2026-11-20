// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/usagetracker/hll"
)

// testReceivedSeriesPrecision is higher than the default, so that the estimates are within a few percent:
// these tests are about which series are counted, not about the error of the sketches.
const testReceivedSeriesPrecision = 12

func newReceivedSeriesTestStore(lim limiter, precision uint8, salt uint64) *trackerStore {
	store := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), lim, noopEvents{}, false, 0, newTestShardFactory())
	store.receivedSeries = newReceivedSeriesEstimation(precision, salt, testIdleTimeout)
	return store
}

// receivedSeriesLimiter is limiterMock with the received series estimation disabled for some tenants.
type receivedSeriesLimiter struct {
	limiterMock
	disabled map[string]bool
}

func (l receivedSeriesLimiter) receivedSeriesEstimationEnabled(userID string) bool {
	return !l.disabled[userID]
}

// seriesRange returns the series [from, to). It's a new slice every time, because trackSeries reorders it.
func seriesRange(from, to int) []uint64 {
	series := make([]uint64, 0, to-from)
	for s := from; s < to; s++ {
		series = append(series, uint64(s))
	}
	return series
}

func getTenant(t *testing.T, store *trackerStore, tenantID string) *trackedTenant {
	t.Helper()
	store.mtx.RLock()
	defer store.mtx.RUnlock()
	tenant := store.tenants[tenantID]
	require.NotNil(t, tenant, "tenant %s doesn't exist", tenantID)
	return tenant
}

// timelineEstimate returns the estimate of the tenant's timeline at now.
func timelineEstimate(t *testing.T, store *trackerStore, tenantID string, now time.Time) float64 {
	t.Helper()
	timeline := getTenant(t, store, tenantID).receivedSeries.Load()
	require.NotNil(t, timeline)
	return timeline.Estimate(unixMinute(now), hll.NewSketch(store.receivedSeries.precision))
}

func TestTrackerStore_ReceivedSeries(t *testing.T) {
	const tenantID = "tenant"
	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)

	t.Run("accepted and rejected series are counted once over the window", func(t *testing.T) {
		store := newReceivedSeriesTestStore(limiterMock{tenantID: 200}, testReceivedSeriesPrecision, 1)
		for m := range 20 {
			// The same series every minute: most of them are rejected.
			rejected, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 1000), now.Add(time.Duration(m)*time.Minute))
			require.NoError(t, err)
			require.NotEmpty(t, rejected)
		}

		require.Less(t, getTenant(t, store, tenantID).series.Load(), uint64(200))
		require.InEpsilon(t, 1000, timelineEstimate(t, store, tenantID, now.Add(19*time.Minute)), 0.05)
	})

	t.Run("series are forgotten after the idle timeout", func(t *testing.T) {
		store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 1000), now)
		require.NoError(t, err)
		_, err = store.trackSeries(t.Context(), tenantID, seriesRange(1000, 1100), now.Add(10*time.Minute))
		require.NoError(t, err)

		require.InEpsilon(t, 1100, timelineEstimate(t, store, tenantID, now.Add(19*time.Minute)), 0.05)
		// The minute of now is out of the window.
		require.InEpsilon(t, 100, timelineEstimate(t, store, tenantID, now.Add(20*time.Minute)), 0.05)
		require.Zero(t, timelineEstimate(t, store, tenantID, now.Add(30*time.Minute)))
	})

	t.Run("a tenant without series is kept while it has received series in the window", func(t *testing.T) {
		// With 2 zones, the local limit is 100: the first call fills it, and the second one is rejected.
		store := newReceivedSeriesTestStore(limiterMock{tenantID: 200}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 1000), now)
		require.NoError(t, err)
		rejected, err := store.trackSeries(t.Context(), tenantID, seriesRange(1000, 2000), now.Add(10*time.Minute))
		require.NoError(t, err)
		require.Len(t, rejected, 1000)

		// The accepted series expire, but the minute of the rejected ones is still in the window.
		cleanupTime := now.Add(testIdleTimeout + time.Minute)
		store.cleanup(cleanupTime)
		require.Zero(t, getTenant(t, store, tenantID).series.Load())
		require.InEpsilon(t, 1000, timelineEstimate(t, store, tenantID, cleanupTime), 0.05)

		// Once the rejected series are out of the window too, the tenant is deleted.
		store.cleanup(now.Add(10*time.Minute + testIdleTimeout + time.Minute))
		store.mtx.RLock()
		require.NotContains(t, store.tenants, tenantID)
		store.mtx.RUnlock()
	})

	t.Run("a tenant without series is kept while it has received series after the time of the cleanup", func(t *testing.T) {
		// With 2 zones, the local limit is 100: the first call fills it.
		store := newReceivedSeriesTestStore(limiterMock{tenantID: 200}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 100), now)
		require.NoError(t, err)

		// The accepted series expire at the time of the cleanup, but a call in the next minute comes before the cleanup
		// removes them, like a call while the cleanup goes through the shards: it's rejected.
		cleanupTime := now.Add(testIdleTimeout + time.Minute)
		rejected, err := store.trackSeries(t.Context(), tenantID, seriesRange(100, 1100), cleanupTime.Add(time.Minute))
		require.NoError(t, err)
		require.Len(t, rejected, 1000)

		store.cleanup(cleanupTime)
		require.Zero(t, getTenant(t, store, tenantID).series.Load())
		require.InEpsilon(t, 1000, timelineEstimate(t, store, tenantID, cleanupTime.Add(time.Minute)), 0.05)
	})

	t.Run("an empty call doesn't keep a tenant without series", func(t *testing.T) {
		store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, nil, now)
		require.NoError(t, err)
		// The call creates the tenant, but it received no series to keep it for.
		getTenant(t, store, tenantID)

		store.cleanup(now)
		store.mtx.RLock()
		require.NotContains(t, store.tenants, tenantID)
		store.mtx.RUnlock()
	})

	t.Run("the metric is the estimate at the time of the scrape", func(t *testing.T) {
		store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
		scrapeTime := now
		store.receivedSeries.now = func() time.Time { return scrapeTime }
		reg := prometheus.NewRegistry()
		require.NoError(t, reg.Register(store))

		// No cleanup runs: each scrape counts the series of the current minute.
		for _, series := range [][]uint64{seriesRange(0, 1000), seriesRange(500, 1500)} {
			_, err := store.trackSeries(t.Context(), tenantID, series, now)
			require.NoError(t, err)
			requireReceivedSeriesMetric(t, reg, map[string]float64{tenantID: timelineEstimate(t, store, tenantID, now)})
		}
		require.InEpsilon(t, 1500, timelineEstimate(t, store, tenantID, now), 0.05)

		// The minute of the series is out of the window of the scrape.
		scrapeTime = now.Add(testIdleTimeout)
		requireReceivedSeriesMetric(t, reg, map[string]float64{tenantID: 0})
	})

	t.Run("estimation can be disabled per tenant", func(t *testing.T) {
		lim := receivedSeriesLimiter{limiterMock: limiterMock{}, disabled: map[string]bool{"disabled": true}}
		store := newReceivedSeriesTestStore(lim, testReceivedSeriesPrecision, 1)
		store.receivedSeries.now = func() time.Time { return now }
		reg := prometheus.NewRegistry()
		require.NoError(t, reg.Register(store))

		for _, tenantID := range []string{"enabled", "disabled"} {
			_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 10), now)
			require.NoError(t, err)
		}
		require.NotNil(t, getTenant(t, store, "enabled").receivedSeries.Load())
		require.Nil(t, getTenant(t, store, "disabled").receivedSeries.Load())

		requireReceivedSeriesMetric(t, reg, map[string]float64{"enabled": timelineEstimate(t, store, "enabled", now)})

		// The limits change: the next cleanup applies them.
		lim.disabled["enabled"], lim.disabled["disabled"] = true, false
		store.cleanup(now)
		require.Nil(t, getTenant(t, store, "enabled").receivedSeries.Load())
		require.NotNil(t, getTenant(t, store, "disabled").receivedSeries.Load())

		_, err := store.trackSeries(t.Context(), "disabled", seriesRange(0, 10), now)
		require.NoError(t, err)
		require.InEpsilon(t, 10, timelineEstimate(t, store, "disabled", now), 0.05)
		requireReceivedSeriesMetric(t, reg, map[string]float64{"disabled": timelineEstimate(t, store, "disabled", now)})
	})

	t.Run("nothing is tracked when the estimation is disabled", func(t *testing.T) {
		store := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
		reg := prometheus.NewRegistry()
		require.NoError(t, reg.Register(store))

		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 10), now)
		require.NoError(t, err)
		store.cleanup(now)
		require.Nil(t, getTenant(t, store, tenantID).receivedSeries.Load())
		requireReceivedSeriesMetric(t, reg, nil)
		require.Nil(t, store.snapshotReceivedSeries(now, math.MaxInt))
	})
}

func requireReceivedSeriesMetric(t *testing.T, reg *prometheus.Registry, expected map[string]float64) {
	t.Helper()
	const metric = "cortex_usage_tracker_all_received_active_series"
	text := ""
	if len(expected) > 0 {
		text = fmt.Sprintf("# HELP %s %s\n# TYPE %s gauge\n", metric, "Estimated number of distinct series received from each user in the last idle timeout, including the series rejected because of the limit. It only counts the series that this usage-tracker received, so take the maximum across zones.", metric)
		for user, value := range expected {
			text += fmt.Sprintf("%s{user=%q} %v\n", metric, user, value)
		}
	}
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(text), metric))
}

func TestTrackerStore_ReceivedSeriesSnapshot(t *testing.T) {
	const tenantID = "tenant"
	now := time.Date(2020, 1, 1, 1, 2, 3, 0, time.UTC)
	later := now.Add(time.Minute)

	// newSourceStore returns a store with series in two minutes for tenantID, and in one minute for another tenant.
	newSourceStore := func(t *testing.T) *trackerStore {
		store := newReceivedSeriesTestStore(limiterMock{tenantID: 400}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 1000), now)
		require.NoError(t, err)
		_, err = store.trackSeries(t.Context(), tenantID, seriesRange(500, 1500), later)
		require.NoError(t, err)
		_, err = store.trackSeries(t.Context(), "other", seriesRange(0, 10), later)
		require.NoError(t, err)
		return store
	}
	// sourceSnapshot returns the received series of a new source store, in a single chunk.
	sourceSnapshot := func(t *testing.T) []byte {
		chunks := newSourceStore(t).snapshotReceivedSeries(later, math.MaxInt)
		require.Len(t, chunks, 1)
		return chunks[0]
	}

	t.Run("loading merges the timelines of the tenants", func(t *testing.T) {
		snapshot := sourceSnapshot(t)

		// The loading store has its own series for tenantID, and doesn't have the other tenant.
		store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(2000, 2100), later)
		require.NoError(t, err)
		require.NoError(t, store.loadReceivedSeriesSnapshot(snapshot, later))

		// A store that got all the series itself has the same registers, so the same estimate.
		expected := newSourceStore(t)
		_, err = expected.trackSeries(t.Context(), tenantID, seriesRange(2000, 2100), later)
		require.NoError(t, err)
		require.Equal(t, timelineEstimate(t, expected, tenantID, later), timelineEstimate(t, store, tenantID, later))
		require.InEpsilon(t, 1600, timelineEstimate(t, store, tenantID, later), 0.05)

		store.mtx.RLock()
		require.NotContains(t, store.tenants, "other", "tenants that don't exist are not created")
		store.mtx.RUnlock()
	})

	t.Run("the snapshot is split between tenants", func(t *testing.T) {
		// With a size of 1 byte, each tenant ends a chunk.
		chunks := newSourceStore(t).snapshotReceivedSeries(later, 1)
		require.Len(t, chunks, 2)

		// load loads chunks, each on its own, into a store that has both tenants.
		load := func(t *testing.T, chunks ...[]byte) *trackerStore {
			store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
			for _, id := range []string{tenantID, "other"} {
				_, err := store.trackSeries(t.Context(), id, nil, later)
				require.NoError(t, err)
			}
			for _, chunk := range chunks {
				require.NoError(t, store.loadReceivedSeriesSnapshot(chunk, later))
			}
			return store
		}
		split, whole := load(t, chunks...), load(t, sourceSnapshot(t))
		for _, id := range []string{tenantID, "other"} {
			require.Equal(t, timelineEstimate(t, whole, id, later), timelineEstimate(t, split, id, later), id)
		}
	})

	t.Run("minutes out of the window are not loaded", func(t *testing.T) {
		snapshot := sourceSnapshot(t)
		loadTime := now.Add(testIdleTimeout) // The minute of now is out of the window, the minute of later is not.

		store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, nil, loadTime)
		require.NoError(t, err)
		require.NoError(t, store.loadReceivedSeriesSnapshot(snapshot, loadTime))
		require.InEpsilon(t, 1000, timelineEstimate(t, store, tenantID, loadTime), 0.05)
	})

	t.Run("snapshots with another precision or salt are skipped", func(t *testing.T) {
		snapshot := sourceSnapshot(t)
		for name, store := range map[string]*trackerStore{
			"precision": newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision-1, 1),
			"salt":      newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 2),
		} {
			t.Run(name, func(t *testing.T) {
				_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 10), later)
				require.NoError(t, err)
				require.NoError(t, store.loadReceivedSeriesSnapshot(snapshot, later))
				require.InEpsilon(t, 10, timelineEstimate(t, store, tenantID, later), 0.05)
			})
		}
	})

	t.Run("snapshots are ignored when the estimation is disabled", func(t *testing.T) {
		snapshot := sourceSnapshot(t)
		store := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 10), later)
		require.NoError(t, err)
		require.NoError(t, store.loadReceivedSeriesSnapshot(snapshot, later))
		require.Nil(t, getTenant(t, store, tenantID).receivedSeries.Load())
	})

	t.Run("invalid snapshots", func(t *testing.T) {
		snapshot := sourceSnapshot(t)
		newLoadingStore := func(t *testing.T) *trackerStore {
			store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
			for _, tenantID := range []string{tenantID, "other"} {
				_, err := store.trackSeries(t.Context(), tenantID, nil, later)
				require.NoError(t, err)
			}
			return store
		}

		require.NoError(t, newLoadingStore(t).loadReceivedSeriesSnapshot(snapshot, later))
		require.NoError(t, newLoadingStore(t).loadReceivedSeriesSnapshot(nil, later))

		for n := 1; n < len(snapshot); n++ {
			require.Error(t, newLoadingStore(t).loadReceivedSeriesSnapshot(snapshot[:n], later), "truncated to %d bytes", n)
		}

		wrongVersion := append([]byte{receivedSeriesSnapshotVersion + 1}, snapshot[1:]...)
		require.EqualError(t, newLoadingStore(t).loadReceivedSeriesSnapshot(wrongVersion, later), "unexpected received series snapshot version 2")

		// The last byte is the last register of the last sketch.
		wrongRegister := append([]byte(nil), snapshot...)
		wrongRegister[len(wrongRegister)-1] = 255 // Never a valid rank.
		require.ErrorContains(t, newLoadingStore(t).loadReceivedSeriesSnapshot(wrongRegister, later), "register out of range")
	})
}
