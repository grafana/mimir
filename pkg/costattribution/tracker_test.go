package costattribution

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_NewTracker(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Initialize a new Tracker
	trackedLabel := []string{"platform"}
	cat, err := newTracker(trackedLabel, 5)
	require.NoError(t, err)
	err = reg.Register(cat)
	require.NoError(t, err)

	// Simulate some values in the metrics
	vals := []string{"foo", "user1"}
	cat.activeSeriesPerUserAttribution.WithLabelValues(vals...).Set(1.0)
	cat.receivedSamplesAttribution.WithLabelValues(vals...).Add(5)
	cat.discardedSampleAttribution.WithLabelValues(append(vals, "out-of-window")...).Add(2)

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

	// Clean the tracker for the user attribution
	cat.cleanupTrackerAttribution(vals)
}
