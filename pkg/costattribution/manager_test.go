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

	"github.com/grafana/mimir/pkg/costattribution/testutils"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func newTestManager() *Manager {
	logger := log.NewNopLogger()
	limits, _ := testutils.NewMockCostAttributionLimits(0)
	reg := prometheus.NewRegistry()
	manager, err := NewManager(5*time.Second, 10*time.Second, logger, limits, reg)
	if err != nil {
		panic(err)
	}
	return manager
}

func TestManager_New(t *testing.T) {
	manager := newTestManager()
	assert.NotNil(t, manager)
	assert.NotNil(t, manager.sampleTrackersByUserID)
	assert.Equal(t, 10*time.Second, manager.inactiveTimeout)
}

func TestManager_CreateDeleteTracker(t *testing.T) {
	manager := newTestManager()

	t.Run("Tracker existence and attributes", func(t *testing.T) {
		user1SampleTracker := manager.SampleTracker("user1")
		assert.NotNil(t, user1SampleTracker)
		assert.True(t, user1SampleTracker.hasSameLabels([]string{"team"}))
		assert.Equal(t, 5, user1SampleTracker.maxCardinality)

		assert.Nil(t, manager.SampleTracker("user2"))

		user3ActiveTracker := manager.ActiveSeriesTracker("user3")
		assert.NotNil(t, user3ActiveTracker)
		assert.True(t, user3ActiveTracker.hasSameLabels([]string{"department", "service"}))
		assert.Equal(t, 2, user3ActiveTracker.maxCardinality)
	})

	t.Run("Metrics tracking", func(t *testing.T) {
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}}, 1, "invalid-metrics-name", time.Unix(6, 0))
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"department", "foo", "service", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{attributed_team="bar",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{attributed_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{attributed_department="foo",attributed_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{attributed_team="bar",tenant="user1",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_distributor_received_attributed_samples_total", "cortex_ingester_attributed_active_series"))
	})

	t.Run("Purge inactive attributions, only received/discarded samples are purged", func(t *testing.T) {
		err := manager.purgeInactiveAttributionsUntil(time.Unix(10, 0))
		assert.NoError(t, err)
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{attributed_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{attributed_team="bar",tenant="user1",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_ingester_attributed_active_series"))
	})

	t.Run("Disabling user cost attribution", func(t *testing.T) {
		var err error
		manager.limits, err = testutils.NewMockCostAttributionLimits(1)
		assert.NoError(t, err)
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(11, 0)))
		assert.Equal(t, 1, len(manager.sampleTrackersByUserID))

		expectedMetrics := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{attributed_department="foo",attributed_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_distributor_received_attributed_samples_total", "cortex_ingester_attributed_active_series"))
	})

	t.Run("Updating user cardinality and labels", func(t *testing.T) {
		var err error
		manager.limits, err = testutils.NewMockCostAttributionLimits(2)
		assert.NoError(t, err)
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(12, 0)))
		assert.Equal(t, 1, len(manager.sampleTrackersByUserID))
		assert.True(t, manager.SampleTracker("user3").hasSameLabels([]string{"feature", "team"}))
		assert.True(t, manager.ActiveSeriesTracker("user3").hasSameLabels([]string{"feature", "team"}))

		manager.SampleTracker("user3").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(13, 0))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{attributed_feature="__missing__",attributed_team="foo",reason="invalid-metrics-name",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Overflow metrics on cardinality limit", func(t *testing.T) {
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "bar", "feature", "bar"}, SamplesCount: 1}}), time.Unix(15, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "baz", "feature", "baz"}, SamplesCount: 1}}), time.Unix(16, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "foo", "feature", "foo"}, SamplesCount: 1}}), time.Unix(17, 0))
		expectedMetrics := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{attributed_feature="__overflow__",attributed_team="__overflow__",tenant="user3",tracker="cost-attribution"} 2
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_distributor_received_attributed_samples_total"))
	})
}

func TestManager_PurgeInactiveAttributionsUntil(t *testing.T) {
	manager := newTestManager()

	manager.SampleTracker("user1").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "foo"}, SamplesCount: 1}}), time.Unix(1, 0))
	manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(1, 0))
	manager.SampleTracker("user3").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "department", Value: "foo"}, {Name: "service", Value: "bar"}}, 1, "out-of-window", time.Unix(10, 0))

	t.Run("Purge before inactive timeout", func(t *testing.T) {
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(0, 0)))
		assert.Equal(t, 2, len(manager.sampleTrackersByUserID))

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{attributed_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{attributed_department="foo",attributed_service="bar",reason="out-of-window",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Purge after inactive timeout", func(t *testing.T) {
		// disable cost attribution for user1 to test purging
		manager.limits, _ = testutils.NewMockCostAttributionLimits(1)
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(5, 0)))

		// User3's tracker should remain since it's active, user1's tracker should be removed
		assert.Equal(t, 1, len(manager.sampleTrackersByUserID), "Expected one active tracker after purging")
		assert.Nil(t, manager.SampleTracker("user1"), "Expected user1 tracker to be purged")
		assert.Nil(t, manager.ActiveSeriesTracker("user1"), "Expected user1 tracker to be purged")

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{attributed_department="foo",attributed_service="bar",reason="out-of-window",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Purge all trackers", func(t *testing.T) {
		// Trigger a purge that should remove all inactive trackers
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(20, 0)))

		// Tracker would stay at 1 since user1's tracker is disabled
		assert.Equal(t, 1, len(manager.sampleTrackersByUserID), "Expected one active tracker after full purge")

		// No metrics should remain after all purged
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(""), "cortex_discarded_attributed_samples_total", "cortex_distributor_received_attributed_samples_total"))
	})
}
