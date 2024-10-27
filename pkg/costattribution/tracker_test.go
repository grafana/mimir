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

func Test_NewTracker(t *testing.T) {

	// Setup the test environment
	reg := prometheus.NewRegistry()
	trackedLabel := []string{"platform"}
	cat, err := newTracker("user1", trackedLabel, 5)
	require.NoError(t, err)

	// Register the metrics
	err = reg.Register(cat)
	require.NoError(t, err)

	// Simulate some values in the metrics
	vals := []string{"foo", "user1"}
	cat.activeSeriesPerUserAttribution.WithLabelValues(vals...).Set(1.0)
	cat.receivedSamplesAttribution.WithLabelValues(vals...).Add(5)
	cat.discardedSampleAttribution.WithLabelValues(append(vals, "out-of-window")...).Add(2)

	// Verify the metrics
	expectedMetrics := `
	# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
    # TYPE cortex_discarded_attributed_samples_total counter
    cortex_discarded_attributed_samples_total{platform="foo",reason="out-of-window", user="user1"} 2
    # HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
    # TYPE cortex_ingester_attributed_active_series gauge
    cortex_ingester_attributed_active_series{platform="foo",user="user1"} 1
    # HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
    # TYPE cortex_received_attributed_samples_total counter
    cortex_received_attributed_samples_total{platform="foo",user="user1"} 5
	`

	metricNames := []string{
		"cortex_discarded_attributed_samples_total",
		"cortex_received_attributed_samples_total",
		"cortex_ingester_attributed_active_series",
	}
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))

	// Clean up the metrics
	cat.cleanupTrackerAttribution(vals)
}

func Test_PurgeInactiveObservations(t *testing.T) {
	// Setup the test environment, user1 cost attribution label is "team", max cardinality limit is 5
	cat := newTestManager().TrackerForUser("user1")

	// create 2 observations
	lbs := []labels.Labels{
		labels.FromStrings([]string{"team", "foo"}...),
		labels.FromStrings([]string{"team", "bar"}...),
	}
	cat.IncrementDiscardedSamples(lbs[0], 1, "invalid-metrics-name", time.Unix(1, 0))
	cat.IncrementDiscardedSamples(lbs[1], 2, "out-of-window-sample", time.Unix(12, 0))

	// Check the observations
	require.Len(t, cat.(*TrackerImp).observed, 2)

	// Purge the observations older than 10 seconds, we should have 1 observation left
	purged := cat.PurgeInactiveObservations(10)

	// Verify the purged observations
	require.Len(t, purged, 1)
	assert.Equal(t, int64(1), purged[0].lastUpdate.Load())
	assert.Equal(t, []string{"foo", "user1", "invalid-metrics-name"}, purged[0].lvalues)

	// Verify the remaining observations
	obs := cat.(*TrackerImp).observed
	require.Len(t, obs, 1)
	assert.Equal(t, int64(12), obs[lbs[1].Hash()].lastUpdate.Load())
}

func Test_GetMaxCardinality(t *testing.T) {
	// Setup the test environment
	cat := newTestManager().TrackerForUser("user1")

	// Verify the max cardinality
	assert.Equal(t, 5, cat.GetMaxCardinality())
}

func Test_GetCALabels(t *testing.T) {
	// Setup the test environment
	cat := newTestManager().TrackerForUser("user1")

	// Verify the CA labels
	assert.Equal(t, []string{"team"}, cat.GetCALabels())
}

func Test_UpdateMaxCardinality(t *testing.T) {
	// Setup the test environment
	cat := newTestManager().TrackerForUser("user1")

	// Update max cardinality
	cat.UpdateMaxCardinality(20)

	// Verify the max cardinality
	assert.Equal(t, 20, cat.GetMaxCardinality())
}
