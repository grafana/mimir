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
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func getMockLimits(idx int) (*validation.Overrides, error) {
	// Define base limits
	baseLimits := map[string]*validation.Limits{
		"user1": {
			MaxCostAttributionCardinalityPerUser: 5,
			CostAttributionLabels:                []string{"team"},
		},
		"user2": {
			MaxCostAttributionCardinalityPerUser: 2,
			CostAttributionLabels:                []string{},
		},
		"user3": {
			MaxCostAttributionCardinalityPerUser: 2,
			CostAttributionLabels:                []string{"department", "service"},
		},
	}

	// Adjust specific cases as needed
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
	inactiveTimeout := 10 * time.Second
	cleanupInterval := 5 * time.Second
	return NewManager(cleanupInterval, inactiveTimeout, logger, limits)
}

func Test_NewManager(t *testing.T) {
	manager := newTestManager()
	assert.NotNil(t, manager, "Expected manager to be initialized")
	assert.NotNil(t, manager.trackersByUserID, "Expected attribution tracker to be initialized")
	assert.Equal(t, 10*time.Second, manager.inactiveTimeout, "Expected inactiveTimeout to be initialized")
}

func Test_EnabledForUser(t *testing.T) {
	manager := newTestManager()
	assert.True(t, manager.EnabledForUser("user1"), "Expected cost attribution to be enabled for user1")
	assert.False(t, manager.EnabledForUser("user2"), "Expected cost attribution to be disabled for user2")
	assert.False(t, manager.EnabledForUser("user4"), "Expected cost attribution to be disabled for user4")
}

func Test_CreateDeleteTracker(t *testing.T) {
	// Create a new manager and register it with prometheus registry
	manager := newTestManager()
	reg := prometheus.NewRegistry()
	err := reg.Register(manager)
	require.NoError(t, err)

	t.Run("Get tracker for user", func(t *testing.T) {
		assert.NotNil(t, manager.TrackerForUser("user1").CALabels())
		assert.Equal(t, []string{"team"}, manager.TrackerForUser("user1").CALabels())
		assert.Equal(t, 5, manager.TrackerForUser("user1").MaxCardinality())

		// user2 is not enabled for cost attribution, so tracker would be nil
		tr2 := manager.TrackerForUser("user2")
		assert.Nil(t, tr2)
		assert.Equal(t, []string(nil), tr2.CALabels())

		assert.Equal(t, []string{"department", "service"}, manager.TrackerForUser("user3").CALabels())
		assert.Equal(t, 2, manager.TrackerForUser("user3").MaxCardinality())

		// user4 tenant config doesn't exist, so tracker would be nil
		tr4 := manager.TrackerForUser("user4")
		assert.Nil(t, tr4)
		assert.Equal(t, []string(nil), tr4.CALabels())

		assert.Equal(t, 2, len(manager.trackersByUserID))
	})

	t.Run("Track metrics for enabled user", func(t *testing.T) {
		// since user2 is not enabled for cost attribution, tracker would be nil, no metrics would be tracked
		manager.TrackerForUser("user2").IncrementReceivedSamples(labels.FromStrings([]string{"team", "foo"}...), 1, time.Unix(0, 0))

		// user1 and user3 is enabled for cost attribution, so metrics would be tracked
		manager.TrackerForUser("user1").IncrementDiscardedSamples(labels.FromStrings([]string{"team", "foo"}...), 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.TrackerForUser("user3").IncrementDiscardedSamples(labels.FromStrings([]string{"department", "foo"}...), 1, "out-of-window", time.Unix(0, 0))
		manager.TrackerForUser("user3").IncrementReceivedSamples(labels.FromStrings([]string{"department", "foo", "service", "dodo"}...), 1, time.Unix(20, 0))
		manager.TrackerForUser("user3").IncrementReceivedSamples(labels.FromStrings([]string{"department", "foo", "service", "bar"}...), 1, time.Unix(30, 0))
		manager.TrackerForUser("user3").IncrementReceivedSamples(labels.FromStrings([]string{"department", "foo", "service", "far"}...), 1, time.Unix(30, 0))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="custom_attribution"} 1
		cortex_discarded_attributed_samples_total{department="foo",reason="out-of-window",service="__missing__",tenant="user3",tracker="custom_attribution"} 1
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{department="__overflow__",service="__overflow__",tenant="user3",tracker="custom_attribution"} 1
        cortex_received_attributed_samples_total{department="foo",service="bar",tenant="user3",tracker="custom_attribution"} 1
		cortex_received_attributed_samples_total{department="foo",service="dodo",tenant="user3",tracker="custom_attribution"} 1
		`
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
			"cortex_received_attributed_samples_total",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Purge inactive attributions", func(t *testing.T) {
		// Purge inactive attributions until time 10, metrics cortex_discarded_attributed_samples_total of user3 should be deleted
		manager.purgeInactiveAttributionsUntil(time.Unix(10, 0).Unix())
		assert.Equal(t, 2, len(manager.trackersByUserID))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
        # TYPE cortex_discarded_attributed_samples_total counter
        cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="custom_attribution"} 1
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{department="__overflow__",service="__overflow__",tenant="user3",tracker="custom_attribution"} 1
        cortex_received_attributed_samples_total{department="foo",service="bar",tenant="user3",tracker="custom_attribution"} 1
		cortex_received_attributed_samples_total{department="foo",service="dodo",tenant="user3",tracker="custom_attribution"} 1
		`
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
			"cortex_received_attributed_samples_total",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Disable user cost attribution, tracker and metrics are removed", func(t *testing.T) {
		// We disable cost attribution for user1, so the tracker should be deleted
		manager.limits, err = getMockLimits(1)
		assert.NoError(t, err)

		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Unix())
		assert.Equal(t, 1, len(manager.trackersByUserID))
		expectedMetrics := `
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{department="__overflow__",service="__overflow__",tenant="user3",tracker="custom_attribution"} 1
        cortex_received_attributed_samples_total{department="foo",service="bar",tenant="user3",tracker="custom_attribution"} 1
		cortex_received_attributed_samples_total{department="foo",service="dodo",tenant="user3",tracker="custom_attribution"} 1
		`
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
			"cortex_received_attributed_samples_total",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Increase user cost attribution max cardinality, since current state is overflow, we clean up the counter", func(t *testing.T) {
		// user3 has cost attribution labels department and service, we change it to team and feature. user1 should not be affected
		manager.limits, err = getMockLimits(3)
		assert.NoError(t, err)
		manager.TrackerForUser("user1").IncrementDiscardedSamples(labels.FromStrings([]string{"team", "foo"}...), 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Unix())
		assert.Equal(t, 2, len(manager.trackersByUserID))

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
        # TYPE cortex_discarded_attributed_samples_total counter
        cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="custom_attribution"} 1
		`
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
			"cortex_received_attributed_samples_total",
		}
		assert.Equal(t, 3, manager.TrackerForUser("user3").MaxCardinality())
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Increase user cost attribution max cardinality, user is not in overflow, nothing changed", func(t *testing.T) {
		// user3 has cost attribution labels department and service, we change it to team and feature
		manager.limits, err = getMockLimits(4)
		assert.NoError(t, err)
		manager.TrackerForUser("user1").IncrementDiscardedSamples(labels.FromStrings([]string{"team", "foo"}...), 1, "invalid-metrics-name", time.Unix(13, 0))
		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Unix())
		assert.Equal(t, 2, len(manager.trackersByUserID))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
        # TYPE cortex_discarded_attributed_samples_total counter
        cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="custom_attribution"} 2
		`
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
			"cortex_received_attributed_samples_total",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
	})

	t.Run("Change user cost attribution lables, tracker and metrics are reinitialized", func(t *testing.T) {
		// user3 has cost attribution labels department and service, we change it to team and feature
		manager.limits, err = getMockLimits(5)
		assert.NoError(t, err)

		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Unix())
		assert.Equal(t, 2, len(manager.trackersByUserID))
		metricNames := []string{
			"cortex_discarded_attributed_samples_total",
			"cortex_received_attributed_samples_total",
		}
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(""), metricNames...))
	})
}
