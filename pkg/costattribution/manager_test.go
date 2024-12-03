// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/util/validation"
)

func getMockLimits(idx int) (*validation.Overrides, error) {
	baseLimits := map[string]*validation.Limits{
		"user1": {MaxCostAttributionCardinalityPerUser: 5, CostAttributionLabels: []string{"team"}},
		"user2": {MaxCostAttributionCardinalityPerUser: 2, CostAttributionLabels: []string{}},
		"user3": {MaxCostAttributionCardinalityPerUser: 2, CostAttributionLabels: []string{"department", "service"}},
		"user4": {MaxCostAttributionCardinalityPerUser: 5, CostAttributionLabels: []string{"platform"}},
	}

	switch idx {
	case 1:
		baseLimits["user1"].CostAttributionLabels = []string{}
	case 2:
		baseLimits["user3"].CostAttributionLabels = []string{"team", "feature"}
	case 3:
		baseLimits["user3"].MaxCostAttributionCardinalityPerUser = 3
	case 4:
		baseLimits["user1"].MaxCostAttributionCardinalityPerUser = 2
	case 5:
		baseLimits["user1"].CostAttributionLabels = []string{"department"}
	}

	return validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(baseLimits))
}

func newTestManager() *Manager {
	logger := log.NewNopLogger()
	limits, _ := getMockLimits(0)
	reg := prometheus.NewRegistry()
	manager, err := NewManager(5*time.Second, 10*time.Second, logger, limits, reg)
	if err != nil {
		panic(err)
	}
	return manager
}

func Test_NewManager(t *testing.T) {
	manager := newTestManager()
	assert.NotNil(t, manager)
	assert.NotNil(t, manager.trackersByUserID)
	assert.Equal(t, 10*time.Second, manager.inactiveTimeout)
}

func Test_EnabledForUser(t *testing.T) {
	manager := newTestManager()
	assert.True(t, manager.EnabledForUser("user1"), "Expected cost attribution to be enabled for user1")
	assert.False(t, manager.EnabledForUser("user2"), "Expected cost attribution to be disabled for user2")
	assert.False(t, manager.EnabledForUser("user5"), "Expected cost attribution to be disabled for user5")
}

func Test_CreateDeleteTracker(t *testing.T) {
	manager := newTestManager()

	t.Run("Tracker existence and attributes", func(t *testing.T) {
		user1Tracker := manager.TrackerForUser("user1")
		assert.NotNil(t, user1Tracker)
		assert.Equal(t, []string{"team"}, user1Tracker.CALabels())
		assert.Equal(t, 5, user1Tracker.MaxCardinality())

		assert.Nil(t, manager.TrackerForUser("user2"))

		user3Tracker := manager.TrackerForUser("user3")
		assert.NotNil(t, user3Tracker)
		assert.Equal(t, []string{"department", "service"}, user3Tracker.CALabels())
		assert.Equal(t, 2, user3Tracker.MaxCardinality())
	})

	t.Run("Metrics tracking", func(t *testing.T) {
		manager.TrackerForUser("user1").IncrementDiscardedSamples(labels.FromStrings("team", "foo"), 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.TrackerForUser("user3").IncrementReceivedSamples(labels.FromStrings("department", "foo", "service", "dodo"), 1, time.Unix(20, 0))

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="custom_attribution"} 1
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{department="foo",service="dodo",tenant="user3",tracker="custom_attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_received_attributed_samples_total"))
	})

	t.Run("Purge inactive attributions", func(t *testing.T) {
		manager.purgeInactiveAttributionsUntil(time.Unix(10, 0).Unix())
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="custom_attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Disabling user cost attribution", func(t *testing.T) {
		manager.limits, _ = getMockLimits(1)
		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Unix())
		assert.Equal(t, 1, len(manager.trackersByUserID))

		expectedMetrics := `
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{department="foo",service="dodo",tenant="user3",tracker="custom_attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_received_attributed_samples_total"))
	})

	t.Run("Updating user cardinality and labels", func(t *testing.T) {
		manager.limits, _ = getMockLimits(2)
		manager.purgeInactiveAttributionsUntil(time.Unix(12, 0).Unix())
		// user3 tracker should be recreated with cost attribution labels changed to ["team", "feature"]
		assert.Equal(t, 1, len(manager.trackersByUserID))
		assert.Equal(t, []string{"feature", "team"}, manager.TrackerForUser("user3").CALabels())

		manager.TrackerForUser("user3").IncrementDiscardedSamples(labels.FromStrings("team", "foo"), 1, "invalid-metrics-name", time.Unix(13, 0))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{feature="__missing__",reason="invalid-metrics-name",team="foo",tenant="user3",tracker="custom_attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("When cost attribution get overflowed, all metrics are purged except overflow metrics", func(t *testing.T) {
		// user3 has maximum cardinality of 2, so adding 3rd attribution should trigger overflow
		manager.TrackerForUser("user3").IncrementReceivedSamples(labels.FromStrings("team", "bar", "feature", "bar"), 1, time.Unix(15, 0))
		manager.TrackerForUser("user3").IncrementReceivedSamples(labels.FromStrings("team", "baz", "feature", "baz"), 1, time.Unix(16, 0))
		manager.TrackerForUser("user3").IncrementReceivedSamples(labels.FromStrings("team", "foo", "feature", "foo"), 1, time.Unix(17, 0))
		expectedMetrics := `
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{feature="__overflow__",team="__overflow__",tenant="user3",tracker="custom_attribution"} 2
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_received_attributed_samples_total"))
	})
}

func Test_PurgeInactiveAttributionsUntil(t *testing.T) {
	manager := newTestManager()

	// Simulate metrics for multiple users to set up initial state
	manager.TrackerForUser("user1").IncrementReceivedSamples(labels.FromStrings("team", "foo"), 1, time.Unix(1, 0))
	manager.TrackerForUser("user1").IncrementDiscardedSamples(labels.FromStrings("team", "foo"), 1, "invalid-metrics-name", time.Unix(1, 0))
	manager.TrackerForUser("user3").IncrementDiscardedSamples(labels.FromStrings("department", "foo", "service", "bar"), 1, "out-of-window", time.Unix(10, 0))

	t.Run("Purge before inactive timeout", func(t *testing.T) {
		// Run purge at a timestamp that doesn't exceed inactive timeout
		manager.purgeInactiveAttributionsUntil(time.Unix(0, 0).Unix())

		// No purging should have occurred, track user metrics remain
		assert.Equal(t, 2, len(manager.trackersByUserID), "Expected trackers to remain active before timeout")

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="custom_attribution"} 1
        cortex_discarded_attributed_samples_total{department="foo",reason="out-of-window",service="bar",tenant="user3",tracker="custom_attribution"} 1
		`
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
		}
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Purge after inactive timeout", func(t *testing.T) {
		// disable cost attribution for user1 to test purging
		manager.limits, _ = getMockLimits(1)
		manager.purgeInactiveAttributionsUntil(time.Unix(5, 0).Unix())

		// User3's tracker should remain since it's active, user1's tracker should be removed
		assert.Equal(t, 1, len(manager.trackersByUserID), "Expected one active tracker after purging")
		assert.Nil(t, manager.TrackerForUser("user1"), "Expected user1 tracker to be purged")

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{department="foo",reason="out-of-window",service="bar",tenant="user3",tracker="custom_attribution"} 1
		`
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
		}
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Purge all trackers", func(t *testing.T) {
		// Trigger a purge that should remove all inactive trackers
		manager.purgeInactiveAttributionsUntil(time.Unix(20, 0).Unix())

		// Tracker would stay at 1 since user1's tracker is disabled
		assert.Equal(t, 1, len(manager.trackersByUserID), "Expected one active tracker after full purge")

		// No metrics should remain after all purged
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
			"cortex_received_attributed_samples_total",
		}
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(""), metricNames...))
	})
}
