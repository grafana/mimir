// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GetCALabels(t *testing.T) {
	tracker := newTestManager().Tracker("user1")
	assert.True(t, tracker.CompareCALabels([]string{"team"}), "Expected cost attribution labels mismatch")
}

func Test_GetMaxCardinality(t *testing.T) {
	tracker := newTestManager().Tracker("user1")
	assert.Equal(t, 5, tracker.MaxCardinality(), "Expected max cardinality mismatch")
}

func Test_CreateCleanupTracker(t *testing.T) {
	tManager := newTestManager()
	tracker := tManager.Tracker("user4")

	reg := prometheus.NewRegistry()
	err := reg.Register(tManager)
	require.NoError(t, err)

	tracker.IncrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), time.Unix(1, 0))
	tracker.IncrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "2"), time.Unix(2, 0))
	tracker.DecrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "3"))
	tracker.IncrementReceivedSamples(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), 5, time.Unix(4, 0))
	tracker.IncrementDiscardedSamples(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), 2, "sample-out-of-order", time.Unix(4, 0))
	tracker.IncrementActiveSeries(labels.FromStrings("platform", "bar", "tenant", "user4", "team", "2"), time.Unix(6, 0))
	tracker.IncrementActiveSeriesFailure(1)

	expectedMetrics := `
	# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
    # TYPE cortex_discarded_attributed_samples_total counter
    cortex_discarded_attributed_samples_total{platform="foo",reason="sample-out-of-order", tenant="user4",tracker="cost-attribution"} 2
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{platform="bar",tenant="user4",tracker="cost-attribution"} 1
    cortex_ingester_attributed_active_series{platform="foo",tenant="user4",tracker="cost-attribution"} 1
	# HELP cortex_ingester_attributed_active_series_failure The total number of failed active series decrement per user and tracker.
    # TYPE cortex_ingester_attributed_active_series_failure counter
    cortex_ingester_attributed_active_series_failure{tenant="user4",tracker="cost-attribution"} 1
    # HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
    # TYPE cortex_received_attributed_samples_total counter
    cortex_received_attributed_samples_total{platform="foo",tenant="user4",tracker="cost-attribution"} 5
	`

	metricNames := []string{
		"cortex_discarded_attributed_samples_total",
		"cortex_received_attributed_samples_total",
		"cortex_ingester_attributed_active_series",
		"cortex_ingester_attributed_active_series_failure",
	}
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	assert.Equal(t, []string{"foo"}, tracker.InactiveObservations(5))
	assert.NoError(t, tManager.purgeInactiveAttributionsUntil(5))

	expectedMetrics = `
	# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{platform="bar",tenant="user4",tracker="cost-attribution"} 1
	# HELP cortex_ingester_attributed_active_series_failure The total number of failed active series decrement per user and tracker.
    # TYPE cortex_ingester_attributed_active_series_failure counter
    cortex_ingester_attributed_active_series_failure{tenant="user4",tracker="cost-attribution"} 1
	`
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	tManager.deleteTracker("user4")
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), metricNames...))
}

func Test_UpdateCounters(t *testing.T) {
	tracker := newTestManager().Tracker("user3")
	lbls1 := labels.FromStrings("department", "foo", "service", "bar")
	lbls2 := labels.FromStrings("department", "bar", "service", "baz")
	lbls3 := labels.FromStrings("department", "baz", "service", "foo")

	tracker.updateCounters(lbls1, 1, 1, 0, 0, nil)
	assert.Equal(t, Normal, tracker.state, "First observation, should not overflow")

	tracker.updateCounters(lbls2, 2, 1, 0, 0, nil)
	assert.Equal(t, Normal, tracker.state, "Second observation, should not overflow")

	tracker.updateCounters(lbls3, 3, 1, 0, 0, nil)
	assert.Equal(t, Overflow, tracker.state, "Third observation, should overflow")

	tracker.updateCounters(lbls3, 4, 1, 0, 0, nil)
	assert.Equal(t, Overflow, tracker.state, "Fourth observation, should stay overflow")

	assert.Equal(t, int64(3+tracker.cooldownDuration), tracker.cooldownUntil.Load(), "CooldownUntil should be updated correctly")
}

func Test_GetInactiveObservations(t *testing.T) {
	// Setup the test environment: create a tracker for user1 with a "team" label and max cardinality of 5.
	tracker := newTestManager().Tracker("user1")

	// Create two observations with different last update timestamps.
	observations := []labels.Labels{
		labels.FromStrings("team", "foo"),
		labels.FromStrings("team", "bar"),
		labels.FromStrings("team", "baz"),
	}
	// Simulate samples discarded with different timestamps.
	tracker.IncrementDiscardedSamples(observations[0], 1, "invalid-metrics-name", time.Unix(1, 0))
	tracker.IncrementDiscardedSamples(observations[1], 2, "out-of-window-sample", time.Unix(12, 0))
	tracker.IncrementDiscardedSamples(observations[2], 3, "invalid-metrics-name", time.Unix(20, 0))

	// Ensure that two observations were successfully added to the tracker.
	require.Len(t, tracker.observed, 3)

	// Purge observations that haven't been updated in the last 10 seconds.
	purged := tracker.InactiveObservations(0)
	require.Len(t, purged, 0)

	purged = tracker.InactiveObservations(10)
	assert.ElementsMatch(t, []string{"foo"}, purged)

	purged = tracker.InactiveObservations(15)
	assert.ElementsMatch(t, []string{"foo", "bar"}, purged)

	// Check that the purged observation matches the expected details.
	purged = tracker.InactiveObservations(25)
	assert.ElementsMatch(t, []string{"foo", "bar", "baz"}, purged)
}

func Test_UpdateMaxCardinality(t *testing.T) {
	// user1 original max cardinality is 5
	tracker := newTestManager().Tracker("user1")
	tracker.UpdateMaxCardinality(2)
	assert.Equal(t, 2, tracker.MaxCardinality(), "Expected max cardinality update to 2")
}

func Test_Concurrency(t *testing.T) {
	m := newTestManager()
	tracker := m.Tracker("user1")

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lbls := labels.FromStrings("team", string(rune('A'+(i%26))))
			tracker.updateCounters(lbls, int64(i), 1, 0, 0, nil)
		}(i)
	}
	wg.Wait()

	// Verify no data races or inconsistencies
	assert.True(t, len(tracker.observed) > 0, "Observed set should not be empty after concurrent updates")
	assert.LessOrEqual(t, len(tracker.observed), 2*tracker.MaxCardinality(), "Observed count should not exceed 2 times of max cardinality")
	assert.Equal(t, Overflow, tracker.state, "Expected state to be Overflow")

	expectedMetrics := `
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{team="__overflow__",tenant="user1",tracker="cost-attribution"} 100
`
	assert.NoError(t, testutil.GatherAndCompare(m.reg, strings.NewReader(expectedMetrics), "cortex_ingester_attributed_active_series"))
}
