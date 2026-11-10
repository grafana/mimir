// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"fmt"
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

// timelineEstimate returns the estimate of the tenant's timeline now, without waiting for a cleanup.
func timelineEstimate(t *testing.T, store *trackerStore, tenantID string, now time.Time) float64 {
	t.Helper()
	timeline := getTenant(t, store, tenantID).receivedSeries.Load()
	require.NotNil(t, timeline)
	return timeline.Cleanup(unixMinute(now), hll.NewSketch(store.receivedSeries.precision))
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

		store.cleanup(now.Add(19 * time.Minute))
		tenant := getTenant(t, store, tenantID)
		require.Less(t, tenant.series.Load(), uint64(200))
		require.InEpsilon(t, 1000, tenant.receivedSeriesEstimate.Load(), 0.05)
	})

	t.Run("series are forgotten after the idle timeout", func(t *testing.T) {
		store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 1000), now)
		require.NoError(t, err)
		_, err = store.trackSeries(t.Context(), tenantID, seriesRange(1000, 1100), now.Add(10*time.Minute))
		require.NoError(t, err)
		tenant := getTenant(t, store, tenantID)

		store.cleanup(now.Add(19 * time.Minute))
		require.InEpsilon(t, 1100, tenant.receivedSeriesEstimate.Load(), 0.05)

		// The minute of now is out of the window.
		store.cleanup(now.Add(20 * time.Minute))
		require.InEpsilon(t, 100, tenant.receivedSeriesEstimate.Load(), 0.05)

		store.cleanup(now.Add(30 * time.Minute))
		require.Zero(t, tenant.receivedSeriesEstimate.Load())
	})

	t.Run("estimation can be disabled per tenant", func(t *testing.T) {
		lim := receivedSeriesLimiter{limiterMock: limiterMock{}, disabled: map[string]bool{"disabled": true}}
		store := newReceivedSeriesTestStore(lim, testReceivedSeriesPrecision, 1)
		reg := prometheus.NewRegistry()
		require.NoError(t, reg.Register(store))

		for _, tenantID := range []string{"enabled", "disabled"} {
			_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 10), now)
			require.NoError(t, err)
		}
		require.NotNil(t, getTenant(t, store, "enabled").receivedSeries.Load())
		require.Nil(t, getTenant(t, store, "disabled").receivedSeries.Load())

		store.cleanup(now)
		requireReceivedSeriesMetric(t, reg, map[string]float64{"enabled": getTenant(t, store, "enabled").receivedSeriesEstimate.Load()})

		// The limits change: the next cleanup applies them.
		lim.disabled["enabled"], lim.disabled["disabled"] = true, false
		store.cleanup(now)
		require.Nil(t, getTenant(t, store, "enabled").receivedSeries.Load())
		require.NotNil(t, getTenant(t, store, "disabled").receivedSeries.Load())

		_, err := store.trackSeries(t.Context(), "disabled", seriesRange(0, 10), now)
		require.NoError(t, err)
		store.cleanup(now)
		require.InEpsilon(t, 10, getTenant(t, store, "disabled").receivedSeriesEstimate.Load(), 0.05)
		requireReceivedSeriesMetric(t, reg, map[string]float64{"disabled": getTenant(t, store, "disabled").receivedSeriesEstimate.Load()})
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
		require.Nil(t, store.snapshotReceivedSeries(now))
	})
}

func requireReceivedSeriesMetric(t *testing.T, reg *prometheus.Registry, expected map[string]float64) {
	t.Helper()
	const metric = "cortex_usage_tracker_received_series_estimate"
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

	t.Run("loading merges the timelines of the tenants", func(t *testing.T) {
		snapshot := newSourceStore(t).snapshotReceivedSeries(later)

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

	t.Run("minutes out of the window are not loaded", func(t *testing.T) {
		snapshot := newSourceStore(t).snapshotReceivedSeries(later)
		loadTime := now.Add(testIdleTimeout) // The minute of now is out of the window, the minute of later is not.

		store := newReceivedSeriesTestStore(limiterMock{}, testReceivedSeriesPrecision, 1)
		_, err := store.trackSeries(t.Context(), tenantID, nil, loadTime)
		require.NoError(t, err)
		require.NoError(t, store.loadReceivedSeriesSnapshot(snapshot, loadTime))
		require.InEpsilon(t, 1000, timelineEstimate(t, store, tenantID, loadTime), 0.05)
	})

	t.Run("snapshots with another precision or salt are skipped", func(t *testing.T) {
		snapshot := newSourceStore(t).snapshotReceivedSeries(later)
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
		snapshot := newSourceStore(t).snapshotReceivedSeries(later)
		store := newTrackerStore(testIdleTimeout, 85, log.NewNopLogger(), limiterMock{}, noopEvents{}, false, 0, newTestShardFactory())
		_, err := store.trackSeries(t.Context(), tenantID, seriesRange(0, 10), later)
		require.NoError(t, err)
		require.NoError(t, store.loadReceivedSeriesSnapshot(snapshot, later))
		require.Nil(t, getTenant(t, store, tenantID).receivedSeries.Load())
	})

	t.Run("invalid snapshots", func(t *testing.T) {
		snapshot := newSourceStore(t).snapshotReceivedSeries(later)
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
		wrongRegister[len(wrongRegister)-1] = hll.MaxRank(testReceivedSeriesPrecision) + 1
		require.ErrorContains(t, newLoadingStore(t).loadReceivedSeriesSnapshot(wrongRegister, later), "register out of range")
	})
}
