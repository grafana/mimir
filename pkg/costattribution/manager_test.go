// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
	assert.NotNil(t, manager.trackersByUserID)
	assert.Equal(t, 10*time.Second, manager.inactiveTimeout)
}

func TestManager_CreateDeleteTracker(t *testing.T) {
	manager := newTestManager()

	t.Run("Tracker existence and attributes", func(t *testing.T) {
		user1Tracker := manager.Tracker("user1")
		assert.NotNil(t, user1Tracker)
		assert.True(t, user1Tracker.hasSameLabels([]string{"team"}))
		assert.Equal(t, 5, user1Tracker.maxCardinality)

		assert.Nil(t, manager.Tracker("user2"))

		user3Tracker := manager.Tracker("user3")
		assert.NotNil(t, user3Tracker)
		assert.True(t, user3Tracker.hasSameLabels([]string{"department", "service"}))
		assert.Equal(t, 2, user3Tracker.maxCardinality)
	})

	t.Run("Metrics tracking", func(t *testing.T) {
		manager.Tracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}}, 1, "invalid-metrics-name", time.Unix(6, 0))
		manager.Tracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.Tracker("user3").IncrementReceivedSamples([]mimirpb.LabelAdapter{{Name: "department", Value: "foo"}, {Name: "service", Value: "dodo"}}, 1, time.Unix(20, 0))

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="bar",tenant="user1",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="cost-attribution"} 1
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{department="foo",service="dodo",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_received_attributed_samples_total"))
	})

	t.Run("Purge inactive attributions", func(t *testing.T) {
		err := manager.purgeInactiveAttributionsUntil(time.Unix(10, 0).Unix())
		assert.NoError(t, err)
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Disabling user cost attribution", func(t *testing.T) {
		var err error
		manager.limits, err = testutils.NewMockCostAttributionLimits(1)
		assert.NoError(t, err)
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Unix()))
		assert.Equal(t, 1, len(manager.trackersByUserID))

		expectedMetrics := `
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{department="foo",service="dodo",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_received_attributed_samples_total"))
	})

	t.Run("Updating user cardinality and labels", func(t *testing.T) {
		var err error
		manager.limits, err = testutils.NewMockCostAttributionLimits(2)
		assert.NoError(t, err)
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(12, 0).Unix()))
		assert.Equal(t, 1, len(manager.trackersByUserID))
		assert.True(t, manager.Tracker("user3").hasSameLabels([]string{"feature", "team"}))

		manager.Tracker("user3").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(13, 0))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{feature="__missing__",reason="invalid-metrics-name",team="foo",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Overflow metrics on cardinality limit", func(t *testing.T) {

		manager.Tracker("user3").IncrementReceivedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}, {Name: "feature", Value: "bar"}}, 1, time.Unix(15, 0))
		manager.Tracker("user3").IncrementReceivedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "baz"}, {Name: "feature", Value: "baz"}}, 1, time.Unix(16, 0))
		manager.Tracker("user3").IncrementReceivedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}, {Name: "feature", Value: "foo"}}, 1, time.Unix(17, 0))
		expectedMetrics := `
		# HELP cortex_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_received_attributed_samples_total counter
		cortex_received_attributed_samples_total{feature="__overflow__",team="__overflow__",tenant="user3",tracker="cost-attribution"} 2
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_received_attributed_samples_total"))
	})
}

func TestManager_PurgeInactiveAttributionsUntil(t *testing.T) {
	manager := newTestManager()

	manager.Tracker("user1").IncrementReceivedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, time.Unix(1, 0))
	manager.Tracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(1, 0))
	manager.Tracker("user3").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "department", Value: "foo"}, {Name: "service", Value: "bar"}}, 1, "out-of-window", time.Unix(10, 0))

	t.Run("Purge before inactive timeout", func(t *testing.T) {
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(0, 0).Unix()))
		assert.Equal(t, 2, len(manager.trackersByUserID))

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{reason="invalid-metrics-name",team="foo",tenant="user1",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{department="foo",reason="out-of-window",service="bar",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Purge after inactive timeout", func(t *testing.T) {
		// disable cost attribution for user1 to test purging
		manager.limits, _ = testutils.NewMockCostAttributionLimits(1)
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(5, 0).Unix()))

		// User3's tracker should remain since it's active, user1's tracker should be removed
		assert.Equal(t, 1, len(manager.trackersByUserID), "Expected one active tracker after purging")
		assert.Nil(t, manager.Tracker("user1"), "Expected user1 tracker to be purged")

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{department="foo",reason="out-of-window",service="bar",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Purge all trackers", func(t *testing.T) {
		// Trigger a purge that should remove all inactive trackers
		assert.NoError(t, manager.purgeInactiveAttributionsUntil(time.Unix(20, 0).Unix()))

		// Tracker would stay at 1 since user1's tracker is disabled
		assert.Equal(t, 1, len(manager.trackersByUserID), "Expected one active tracker after full purge")

		// No metrics should remain after all purged
		assert.NoError(t, testutil.GatherAndCompare(manager.reg, strings.NewReader(""), "cortex_discarded_attributed_samples_total", "cortex_received_attributed_samples_total"))
	})
}
