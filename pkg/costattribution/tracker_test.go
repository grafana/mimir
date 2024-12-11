// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_GetCALabels(t *testing.T) {
	cat := newTestManager().TrackerForUser("user1")
	assert.Equal(t, []string{"team"}, cat.CALabels(), "Expected cost attribution labels mismatch")
}

func Test_GetMaxCardinality(t *testing.T) {
	cat := newTestManager().TrackerForUser("user1")
	assert.Equal(t, 5, cat.MaxCardinality(), "Expected max cardinality mismatch")
}

func Test_CreateCleanupTracker(t *testing.T) {
	tManager := newTestManager()
	cat := tManager.TrackerForUser("user4")

	reg := prometheus.NewRegistry()
	err := reg.Register(cat)
	require.NoError(t, err)

	cat.IncrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), time.Unix(1, 0))
	cat.IncrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "2"), time.Unix(2, 0))
	cat.DecrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "3"), time.Unix(3, 0))
	cat.IncrementReceivedSamples(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), 5, time.Unix(4, 0))
	cat.IncrementDiscardedSamples(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), 2, "sample-out-of-order", time.Unix(4, 0))

	cat.IncrementActiveSeries(labels.FromStrings("platform", "bar", "tenant", "user4", "team", "2"), time.Unix(6, 0))

	cat.updateMetrics()

	expectedMetrics := `
	# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
    # TYPE cortex_discarded_attributed_samples_total counter
    cortex_discarded_attributed_samples_total{platform="foo",reason="sample-out-of-order", tenant="user4",tracker="cost-attribution"} 2
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{platform="bar",tenant="user4",tracker="cost-attribution"} 1
    cortex_ingester_attributed_active_series{platform="foo",tenant="user4",tracker="cost-attribution"} 1
    # HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
    # TYPE cortex_received_attributed_samples_total counter
    cortex_received_attributed_samples_total{platform="foo",tenant="user4",tracker="cost-attribution"} 5
	`

	metricNames := []string{
		"cortex_discarded_attributed_samples_total",
		"cortex_received_attributed_samples_total",
		"cortex_ingester_attributed_active_series",
	}
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	assert.Equal(t, 1, len(cat.GetInactiveObservations(5)))
	tManager.purgeInactiveAttributionsUntil(5)

	expectedMetrics = `
	# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{platform="bar",tenant="user4",tracker="cost-attribution"} 1
	`
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	cat.cleanupTracker()
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), metricNames...))
}

// func Test_GetKeyValues(t *testing.T) {
// 	cat := newTestManager().TrackerForUser("user3")

// 	// Test initial key values and overflow states
// 	keyVal1 := cat.updateOverflow(labels.FromStrings("department", "foo", "service", "bar"), 1)
// 	assert.Equal(t, []string{"foo", "bar", "user3"}, keyVal1, "First call, expecting values as-is")

// 	keyVal2 := cat.getKeyValues(labels.FromStrings("department", "foo"), 3)
// 	assert.Equal(t, []string{"foo", "__missing__", "user3"}, keyVal2, "Service missing, should return '__missing__'")

// 	keyVal3 := cat.getKeyValues(labels.FromStrings("department", "foo", "service", "baz", "team", "a"), 4)
// 	assert.Equal(t, []string{"__overflow__", "__overflow__", "user3"}, keyVal3, "Overflow state expected")

// 	keyVal4 := cat.getKeyValues(labels.FromStrings("department", "foo", "service", "bar"), 5)
// 	assert.Equal(t, []string{"__overflow__", "__overflow__", "user3"}, keyVal4, "Overflow state expected")
// }

func Test_UpdateCounters(t *testing.T) {
	cat := newTestManager().TrackerForUser("user3")
	lbls1 := labels.FromStrings("department", "foo", "service", "bar")
	lbls2 := labels.FromStrings("department", "bar", "service", "baz")
	lbls3 := labels.FromStrings("department", "baz", "service", "foo")

	cat.updateCounters(lbls1, 1, 1, 0, 0, nil)
	assert.False(t, cat.isOverflow, "First observation, should not overflow")

	cat.updateCounters(lbls2, 2, 1, 0, 0, nil)
	assert.False(t, cat.isOverflow, "Second observation, should not overflow")

	cat.updateCounters(lbls3, 3, 1, 0, 0, nil)
	assert.True(t, cat.isOverflow, "Third observation, should overflow")

	cat.updateCounters(lbls3, 4, 1, 0, 0, nil)
	assert.True(t, cat.isOverflow, "Fourth observation, should stay overflow")

	assert.Equal(t, int64(3+cat.cooldownDuration), cat.cooldownUntil.Load(), "CooldownUntil should be updated correctly")
}

func Test_GetInactiveObservations(t *testing.T) {
	// Setup the test environment: create a tracker for user1 with a "team" label and max cardinality of 5.
	cat := newTestManager().TrackerForUser("user1")

	// Create two observations with different last update timestamps.
	observations := []labels.Labels{
		labels.FromStrings("team", "foo"),
		labels.FromStrings("team", "bar"),
	}
	// Simulate samples discarded with different timestamps.
	cat.IncrementDiscardedSamples(observations[0], 1, "invalid-metrics-name", time.Unix(1, 0))
	cat.IncrementDiscardedSamples(observations[1], 2, "out-of-window-sample", time.Unix(12, 0))

	// Ensure that two observations were successfully added to the tracker.
	require.Len(t, cat.observed, 2)

	// Purge observations that haven't been updated in the last 10 seconds.
	purged := cat.GetInactiveObservations(5)
	require.Len(t, purged, 1)
}

func Test_UpdateMaxCardinality(t *testing.T) {
	// user1 original max cardinality is 5
	cat := newTestManager().TrackerForUser("user1")
	cat.UpdateMaxCardinality(2)
	assert.Equal(t, 2, cat.MaxCardinality(), "Expected max cardinality update to 2")
}
