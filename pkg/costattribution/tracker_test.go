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
	// Setup the test environment for the user4, user4 has cost attribution labels "platform", max cardinality limit is 5
	cat := newTestManager().TrackerForUser("user4")

	reg := prometheus.NewRegistry()
	err := reg.Register(cat)
	require.NoError(t, err)

	// Simulate some values in the metrics
	// platform="foo" tenant="user1" team="..."
	cat.IncrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), time.Unix(1, 0))
	cat.IncrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "2"), time.Unix(1, 0))
	cat.DecrementActiveSeries(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "3"), time.Unix(1, 0))
	cat.IncrementReceivedSamples(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), 5, time.Unix(1, 0))
	cat.IncrementDiscardedSamples(labels.FromStrings("platform", "foo", "tenant", "user4", "team", "1"), 2, "sample-out-of-order", time.Unix(1, 0))

	// platform="bar" tenant="user1" team="..."
	cat.IncrementActiveSeries(labels.FromStrings("platform", "bar", "tenant", "user4", "team", "2"), time.Unix(1, 0))

	// Verify the metrics
	expectedMetrics := `
	# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
    # TYPE cortex_discarded_attributed_samples_total counter
    cortex_discarded_attributed_samples_total{platform="foo",reason="sample-out-of-order", tenant="user4",tracker="custom_attribution"} 2
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{platform="bar",tenant="user4",tracker="custom_attribution"} 1
    cortex_ingester_attributed_active_series{platform="foo",tenant="user4",tracker="custom_attribution"} 1
    # HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
    # TYPE cortex_received_attributed_samples_total counter
    cortex_received_attributed_samples_total{platform="foo",tenant="user4",tracker="custom_attribution"} 5
	`

	metricNames := []string{
		"cortex_discarded_attributed_samples_total",
		"cortex_received_attributed_samples_total",
		"cortex_ingester_attributed_active_series",
	}
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))

	// Clean up the metrics with label values platform="foo" tenant="user1"
	cat.cleanupTrackerAttribution([]string{"foo", "user4"})

	expectedMetrics = `
	# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
	cortex_ingester_attributed_active_series{platform="bar",tenant="user4",tracker="custom_attribution"} 1
	`
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))

	// Clean up the metrics with label values tenant="user1"
	cat.cleanupTracker("user4")
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), metricNames...))
}

func Test_GetKeyValues(t *testing.T) {
	cat := newTestManager().TrackerForUser("user3")

	// Test initial key values and overflow states
	keyVal1 := cat.getKeyValues(labels.FromStrings("department", "foo", "service", "bar"), 1)
	assert.Equal(t, []string{"foo", "bar", "user3"}, keyVal1, "First call, expecting values as-is")

	keyVal2 := cat.getKeyValues(labels.FromStrings("department", "foo", "service", "baz"), 2)
	assert.Equal(t, []string{"foo", "baz", "user3"}, keyVal2, "Second call, expecting values as-is")

	keyVal3 := cat.getKeyValues(labels.FromStrings("department", "foo"), 3)
	assert.Equal(t, []string{"foo", "__missing__", "user3"}, keyVal3, "Service missing, should return '__missing__'")

	keyVal4 := cat.getKeyValues(labels.FromStrings("department", "foo", "service", "bar", "team", "a"), 4)
	assert.Equal(t, []string{"__overflow__", "__overflow__", "user3"}, keyVal4, "Overflow state expected")
}

func Test_Overflow(t *testing.T) {
	cat := newTestManager().TrackerForUser("user3")
	lbls1 := labels.FromStrings("department", "foo", "service", "bar")
	lbls2 := labels.FromStrings("department", "bar", "service", "baz")
	lbls3 := labels.FromStrings("department", "baz", "service", "foo")

	var buf []byte
	stream1, _ := lbls1.HashForLabels(buf, cat.caLabels...)
	stream2, _ := lbls2.HashForLabels(buf, cat.caLabels...)
	stream3, _ := lbls3.HashForLabels(buf, cat.caLabels...)

	assert.False(t, cat.overflow(stream1, []string{"foo", "bar", "user1"}, 1), "First observation, should not overflow")
	assert.False(t, cat.overflow(stream2, []string{"bar", "baz", "user1"}, 2), "Second observation, should not overflow")
	assert.False(t, cat.overflow(stream3, []string{"baz", "foo", "user1"}, 3), "Third observation, should not overflow")
	assert.True(t, cat.overflow(stream3, []string{"baz", "foo", "user1"}, 4), "Fourth observation, should overflow")
	assert.Equal(t, int64(4+cat.cooldownDuration), cat.cooldownUntil.Load(), "CooldownUntil should be updated correctly")
}

func Test_PurgeInactiveObservations(t *testing.T) {
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
	purged := cat.PurgeInactiveObservations(10)

	// Verify that only one observation was purged.
	require.Len(t, purged, 1)

	// Check that the purged observation matches the expected details.
	assert.Equal(t, int64(1), purged[0].lastUpdate.Load())
	assert.Equal(t, []string{"foo", "user1"}, purged[0].lvalues)

	// Verify that only one observation remains in the tracker. Confirm that the remaining observation has the correct last update timestamp.
	require.Len(t, cat.observed, 1)
	assert.NotNil(t, cat.observed[observations[1].Hash()].lastUpdate)
	assert.Equal(t, int64(12), cat.observed[observations[1].Hash()].lastUpdate.Load())
}

func Test_UpdateMaxCardinality(t *testing.T) {
	// user1 original max cardinality is 5
	cat := newTestManager().TrackerForUser("user1")
	cat.UpdateMaxCardinality(2)
	assert.Equal(t, 2, cat.MaxCardinality(), "Expected max cardinality update to 2")
}
