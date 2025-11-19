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

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/costattribution/testutils"
	"github.com/grafana/mimir/pkg/mimirpb"
)

func newTestManager() (manager *Manager, reg, costAttributionReg *prometheus.Registry) {
	logger := log.NewNopLogger()
	limits := testutils.NewMockCostAttributionLimits(0)
	reg = prometheus.NewRegistry()
	costAttributionReg = prometheus.NewRegistry()
	manager, err := NewManager(5*time.Second, 10*time.Second, logger, limits, reg, costAttributionReg)
	if err != nil {
		panic(err)
	}
	return manager, reg, costAttributionReg
}

func TestManager_New(t *testing.T) {
	manager, _, _ := newTestManager()
	assert.NotNil(t, manager)
	assert.NotNil(t, manager.sampleTrackersByUserID)
	assert.Equal(t, 10*time.Second, manager.inactiveTimeout)
}

func TestManager_CreateDeleteTracker(t *testing.T) {
	manager, reg, costAttributionReg := newTestManager()

	t.Run("Tracker existence and attributes", func(t *testing.T) {
		user1SampleTracker := manager.SampleTracker("user1")
		assert.NotNil(t, user1SampleTracker)
		assert.True(t, user1SampleTracker.hasSameLabels(costattributionmodel.Labels{{Input: "team", Output: "my_team"}}))
		assert.Equal(t, 5, user1SampleTracker.maxCardinality)

		assert.Nil(t, manager.SampleTracker("user2"))

		user3ActiveTracker := manager.ActiveSeriesTracker("user3")
		assert.NotNil(t, user3ActiveTracker)
		assert.True(t, user3ActiveTracker.hasSameLabels(costattributionmodel.Labels{{Input: "department", Output: "my_department"}, {Input: "service", Output: "my_service"}}))
		assert.Equal(t, 2, user3ActiveTracker.maxCardinality)
	})

	t.Run("Metrics tracking", func(t *testing.T) {
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}}, 1, "invalid-metrics-name", time.Unix(6, 0))
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"department", "foo", "service", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 50)
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), -1)
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 2)

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_team="bar",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
        cortex_ingester_attributed_active_native_histogram_buckets{my_team="bar",tenant="user1",tracker="cost-attribution"} 52
        # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
        cortex_ingester_attributed_active_native_histogram_series{my_team="bar",tenant="user1",tracker="cost-attribution"} 2
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{my_team="bar",tenant="user1",tracker="cost-attribution"} 3
		# HELP cortex_attributed_series_overflow_labels The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.
		# TYPE cortex_attributed_series_overflow_labels gauge
		cortex_attributed_series_overflow_labels{my_team="__overflow__",tenant="user1",tracker="cost-attribution"} 1
		cortex_attributed_series_overflow_labels{my_department="__overflow__",my_service="__overflow__",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg,
			strings.NewReader(expectedMetrics),
			"cortex_discarded_attributed_samples_total",
			"cortex_distributor_received_attributed_samples_total",
			"cortex_ingester_attributed_active_series",
			"cortex_ingester_attributed_active_native_histogram_series",
			"cortex_ingester_attributed_active_native_histogram_buckets",
			"cortex_attributed_series_overflow_labels",
		))

		manager.ActiveSeriesTracker("user1").Decrement(labels.FromStrings("team", "bar"), 50)
		expectedMetrics = `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
        cortex_discarded_attributed_samples_total{my_team="bar",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
        cortex_discarded_attributed_samples_total{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
        # HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
        # TYPE cortex_distributor_received_attributed_samples_total counter
        cortex_distributor_received_attributed_samples_total{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
        cortex_ingester_attributed_active_native_histogram_buckets{my_team="bar",tenant="user1",tracker="cost-attribution"} 2
        # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
        cortex_ingester_attributed_active_native_histogram_series{my_team="bar",tenant="user1",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{my_team="bar",tenant="user1",tracker="cost-attribution"} 2
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg,
			strings.NewReader(expectedMetrics),
			"cortex_discarded_attributed_samples_total",
			"cortex_distributor_received_attributed_samples_total",
			"cortex_ingester_attributed_active_series",
			"cortex_ingester_attributed_active_native_histogram_series",
			"cortex_ingester_attributed_active_native_histogram_buckets",
		))

		manager.ActiveSeriesTracker("user1").Decrement(labels.FromStrings("team", "bar"), 2)
		expectedMetrics = `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
        cortex_discarded_attributed_samples_total{my_team="bar",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
        cortex_discarded_attributed_samples_total{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
        # HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
        # TYPE cortex_distributor_received_attributed_samples_total counter
        cortex_distributor_received_attributed_samples_total{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
		# TYPE cortex_ingester_attributed_active_series gauge
		cortex_ingester_attributed_active_series{my_team="bar",tenant="user1",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg,
			strings.NewReader(expectedMetrics),
			"cortex_discarded_attributed_samples_total",
			"cortex_distributor_received_attributed_samples_total",
			"cortex_ingester_attributed_active_series",
			"cortex_ingester_attributed_active_native_histogram_series",
			"cortex_ingester_attributed_active_native_histogram_buckets",
		))

		expectedMetrics = `
		# HELP cortex_cost_attribution_active_series_tracker_cardinality The cardinality of a cost attribution active series tracker for each user.
		# TYPE cortex_cost_attribution_active_series_tracker_cardinality gauge
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user1"} 1
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user3"} 0
		# HELP cortex_cost_attribution_sample_tracker_cardinality The cardinality of a cost attribution sample tracker for each user.
		# TYPE cortex_cost_attribution_sample_tracker_cardinality gauge
		cortex_cost_attribution_sample_tracker_cardinality{tracker="cost-attribution",user="user1"} 2
		cortex_cost_attribution_sample_tracker_cardinality{tracker="cost-attribution",user="user3"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics),
			"cortex_cost_attribution_sample_tracker_cardinality",
			"cortex_cost_attribution_sample_tracker_overflown",
			"cortex_cost_attribution_active_series_tracker_cardinality",
			"cortex_cost_attribution_active_series_tracker_overflown",
		))
	})

	t.Run("Purge inactive attributions, only received/discarded samples are purged", func(t *testing.T) {
		manager.purgeInactiveAttributionsUntil(time.Unix(10, 0).Add(manager.inactiveTimeout))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{my_team="bar",tenant="user1",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_ingester_attributed_active_series"))
	})

	t.Run("Disabling user cost attribution", func(t *testing.T) {
		manager.limits = testutils.NewMockCostAttributionLimits(1)
		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Add(manager.inactiveTimeout))
		assert.Equal(t, 1, len(manager.sampleTrackersByUserID))

		expectedMetrics := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_distributor_received_attributed_samples_total", "cortex_ingester_attributed_active_series"))
	})

	t.Run("Updating user cardinality and labels", func(t *testing.T) {
		manager.limits = testutils.NewMockCostAttributionLimits(2)
		manager.purgeInactiveAttributionsUntil(time.Unix(12, 0).Add(manager.inactiveTimeout))
		assert.Equal(t, 1, len(manager.sampleTrackersByUserID))
		assert.True(t, manager.SampleTracker("user3").hasSameLabels(costattributionmodel.Labels{{Input: "feature", Output: "my_feature"}, {Input: "team", Output: "my_team"}}))
		assert.True(t, manager.ActiveSeriesTracker("user3").hasSameLabels(costattributionmodel.Labels{{Input: "feature", Output: "my_feature"}, {Input: "team", Output: "my_team"}}))

		manager.SampleTracker("user3").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(13, 0))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_feature="__missing__",my_team="foo",reason="invalid-metrics-name",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Overflow metrics on cardinality limit", func(t *testing.T) {
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "bar", "feature", "bar"}, SamplesCount: 1}}), time.Unix(15, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "baz", "feature", "baz"}, SamplesCount: 1}}), time.Unix(16, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "foo", "feature", "foo"}, SamplesCount: 1}}), time.Unix(17, 0))
		expectedMetrics := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_feature="__overflow__",my_team="__overflow__",tenant="user3",tracker="cost-attribution"} 2
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_distributor_received_attributed_samples_total"))

		expectedMetrics = `
		# HELP cortex_cost_attribution_active_series_tracker_cardinality The cardinality of a cost attribution active series tracker for each user.
		# TYPE cortex_cost_attribution_active_series_tracker_cardinality gauge
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user3"} 0
		# HELP cortex_cost_attribution_sample_tracker_cardinality The cardinality of a cost attribution sample tracker for each user.
		# TYPE cortex_cost_attribution_sample_tracker_cardinality gauge
		cortex_cost_attribution_sample_tracker_cardinality{tracker="cost-attribution",user="user3"} 2
		# HELP cortex_cost_attribution_sample_tracker_overflown This metric is exported with value 1 when a sample tracker for a user is overflown. It's not exported otherwise.
		# TYPE cortex_cost_attribution_sample_tracker_overflown gauge
		cortex_cost_attribution_sample_tracker_overflown{tracker="cost-attribution",user="user3"} 1
		`

		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics),
			"cortex_cost_attribution_sample_tracker_cardinality",
			"cortex_cost_attribution_sample_tracker_overflown",
			"cortex_cost_attribution_active_series_tracker_cardinality",
			"cortex_cost_attribution_active_series_tracker_overflown",
		))
	})
}

func TestManager_PurgeInactiveAttributionsUntil(t *testing.T) {
	manager, _, costAttributionReg := newTestManager()

	manager.SampleTracker("user1").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "foo"}, SamplesCount: 1}}), time.Unix(1, 0))
	manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(1, 0))
	manager.SampleTracker("user3").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "department", Value: "foo"}, {Name: "service", Value: "bar"}}, 1, "out-of-window", time.Unix(10, 0))

	t.Run("Purge before inactive timeout", func(t *testing.T) {
		manager.purgeInactiveAttributionsUntil(time.Unix(0, 0).Add(manager.inactiveTimeout))
		assert.Equal(t, 2, len(manager.sampleTrackersByUserID))

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{my_department="foo",my_service="bar",reason="out-of-window",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Purge after inactive timeout", func(t *testing.T) {
		// disable cost attribution for user1 to test purging
		manager.limits = testutils.NewMockCostAttributionLimits(1)
		manager.purgeInactiveAttributionsUntil(time.Unix(5, 0).Add(manager.inactiveTimeout))

		// User3's tracker should remain since it's active, user1's tracker should be removed
		assert.Equal(t, 1, len(manager.sampleTrackersByUserID), "Expected one active tracker after purging")
		assert.Nil(t, manager.SampleTracker("user1"), "Expected user1 tracker to be purged")
		assert.Nil(t, manager.ActiveSeriesTracker("user1"), "Expected user1 tracker to be purged")

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_department="foo",my_service="bar",reason="out-of-window",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Purge all trackers", func(t *testing.T) {
		// Trigger a purge that should remove all inactive trackers
		manager.purgeInactiveAttributionsUntil(time.Unix(20, 0).Add(manager.inactiveTimeout))

		// Tracker would stay at 1 since user1's tracker is disabled
		assert.Equal(t, 1, len(manager.sampleTrackersByUserID), "Expected one active tracker after full purge")

		// No metrics should remain after all purged
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(""), "cortex_discarded_attributed_samples_total", "cortex_distributor_received_attributed_samples_total"))
	})

	t.Run("Overflown active series recover only after they are under the limit", func(t *testing.T) {
		// Track two series, this is under the limit of 2, so no overflow should happen yet.
		now := time.Unix(10, 0)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "1"), now, 0)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "2"), now, 0)

		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 2, len(ast.observed), "Should have 2 series tracked")
			require.True(t, ast.overflowSince.IsZero(), "Should not be in overflow state yet")
			require.Zero(t, ast.overflowCounter.activeSeries.Load(), "Overflow counter should be zero")
		})

		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "3"), now, 0)

		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 3, len(ast.observed), "Should have 3 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Zero(t, ast.overflowCounter.activeSeries.Load(), "Overflow counter should be zero")
		})

		// Track more series, one goes to observed, another two go to overflow.
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "4"), now, 0)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "5"), now, 0)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "6"), now, 0)

		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 4, len(ast.observed), "Should have 4 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(2), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		// Run purge after the cooldown, nothing should change since we are still over the limit.
		now = now.Add(testutils.TestAttributionCooldown)
		manager.purgeInactiveAttributionsUntil(now)

		// Nothing has changed.
		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 4, len(ast.observed), "Should have 4 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(2), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown / 10)
		// Decrement one of the observed series and one of the overflow series.
		manager.ActiveSeriesTracker("user7").Decrement(labels.FromStrings("team", "1"), 0)
		manager.ActiveSeriesTracker("user7").Decrement(labels.FromStrings("team", "6"), 0)

		// Check the updated state.
		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 3, len(ast.observed), "Should have 3 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		// Run purge after the cooldown, nothing should change since we are still over the limit.
		now = now.Add(testutils.TestAttributionCooldown)
		manager.purgeInactiveAttributionsUntil(now)

		// Nothing has changed.
		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 3, len(ast.observed), "Should have 3 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown / 10)
		// Increment a different series, it should go to observed because we have room there now.
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "7"), now, 0)
		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 4, len(ast.observed), "Should have 4 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown / 10)
		// Decrement two observed series, this should bring us under the limit and recover from overflow state.
		manager.ActiveSeriesTracker("user7").Decrement(labels.FromStrings("team", "3"), 0)
		manager.ActiveSeriesTracker("user7").Decrement(labels.FromStrings("team", "4"), 0)

		// NOTE: There are still series in the overflow counter, but can't tell how much cardinality those two add,
		// The best we can say is "it's at least 1", so we just ignore here (with a limit set to thousands, doing this plus one won't change much).
		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 2, len(ast.observed), "Should have 2 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		// Purge again, this should remove the tracker.
		now = now.Add(testutils.TestAttributionCooldown)
		manager.purgeInactiveAttributionsUntil(now)
		withLockedActiveSeriesTracker(manager.ActiveSeriesTracker("user7"), func(ast *ActiveSeriesTracker) {
			require.Equal(t, 0, len(ast.observed), "Should have no series tracked")
			require.True(t, ast.overflowSince.IsZero(), "Should not be in overflow state anymore")
			require.Zero(t, ast.overflowCounter.activeSeries.Load(), "Overflow counter should be zero")
		})
	})
}

func withLockedActiveSeriesTracker(ast *ActiveSeriesTracker, fn func(ast *ActiveSeriesTracker)) {
	ast.observedMtx.Lock()
	defer ast.observedMtx.Unlock()
	fn(ast)
}

func TestManager_OutputLabels(t *testing.T) {
	manager, _, costAttributionReg := newTestManager()

	manager.SampleTracker("user6").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}}, 1, "invalid-metrics-name", time.Unix(6, 0))
	manager.SampleTracker("user6").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(12, 0))
	manager.SampleTracker("user6").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"team", "foo", "feature", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))
	manager.ActiveSeriesTracker("user6").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 50)
	manager.ActiveSeriesTracker("user6").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), -1)
	manager.ActiveSeriesTracker("user6").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 2)

	expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{eng_team="bar",reason="invalid-metrics-name",tenant="user6",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{eng_team="foo",reason="invalid-metrics-name",tenant="user6",tracker="cost-attribution"} 1
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{eng_team="foo",tenant="user6",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
        cortex_ingester_attributed_active_native_histogram_buckets{eng_team="bar",tenant="user6",tracker="cost-attribution"} 52
        # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
        cortex_ingester_attributed_active_native_histogram_series{eng_team="bar",tenant="user6",tracker="cost-attribution"} 2
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{eng_team="bar",tenant="user6",tracker="cost-attribution"} 3
		`
	assert.NoError(t, testutil.GatherAndCompare(costAttributionReg,
		strings.NewReader(expectedMetrics),
		"cortex_discarded_attributed_samples_total",
		"cortex_distributor_received_attributed_samples_total",
		"cortex_ingester_attributed_active_series",
		"cortex_ingester_attributed_active_native_histogram_series",
		"cortex_ingester_attributed_active_native_histogram_buckets",
	))
}

func TestManager_InvalidTrackers(t *testing.T) {
	manager, reg, costAttributionReg := newTestManager()

	t.Run("Tracker existence and attributes", func(t *testing.T) {
		user1SampleTracker := manager.SampleTracker("user1")
		assert.NotNil(t, user1SampleTracker)
		assert.True(t, user1SampleTracker.hasSameLabels(costattributionmodel.Labels{{Input: "team", Output: "my_team"}}))
		assert.Equal(t, 5, user1SampleTracker.maxCardinality)

		assert.Nil(t, manager.SampleTracker("user2"))

		user3ActiveTracker := manager.ActiveSeriesTracker("user3")
		assert.NotNil(t, user3ActiveTracker)
		assert.True(t, user3ActiveTracker.hasSameLabels(costattributionmodel.Labels{{Input: "department", Output: "my_department"}, {Input: "service", Output: "my_service"}}))
		assert.Equal(t, 2, user3ActiveTracker.maxCardinality)
	})

	t.Run("Metrics tracking", func(t *testing.T) {
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}}, 1, "invalid-metrics-name", time.Unix(6, 0))
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"department", "foo", "service", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 50)
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), -1)
		manager.ActiveSeriesTracker("user3").Increment(labels.FromStrings("department", "foo", "service", "dodo"), time.Unix(10, 0), 2)

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_team="bar",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
        cortex_ingester_attributed_active_native_histogram_buckets{my_team="bar",tenant="user1",tracker="cost-attribution"} 50
        cortex_ingester_attributed_active_native_histogram_buckets{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 2
        # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
        cortex_ingester_attributed_active_native_histogram_series{my_team="bar",tenant="user1",tracker="cost-attribution"} 1
        cortex_ingester_attributed_active_native_histogram_series{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{my_team="bar",tenant="user1",tracker="cost-attribution"} 2
        cortex_ingester_attributed_active_series{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_attributed_series_overflow_labels The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.
		# TYPE cortex_attributed_series_overflow_labels gauge
		cortex_attributed_series_overflow_labels{my_team="__overflow__",tenant="user1",tracker="cost-attribution"} 1
		cortex_attributed_series_overflow_labels{my_department="__overflow__",my_service="__overflow__",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg,
			strings.NewReader(expectedMetrics),
			"cortex_discarded_attributed_samples_total",
			"cortex_distributor_received_attributed_samples_total",
			"cortex_ingester_attributed_active_series",
			"cortex_ingester_attributed_active_native_histogram_series",
			"cortex_ingester_attributed_active_native_histogram_buckets",
			"cortex_attributed_series_overflow_labels",
		))

		expectedMetrics = `
		# HELP cortex_cost_attribution_active_series_tracker_cardinality The cardinality of a cost attribution active series tracker for each user.
		# TYPE cortex_cost_attribution_active_series_tracker_cardinality gauge
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user1"} 1
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user3"} 1
		# HELP cortex_cost_attribution_sample_tracker_cardinality The cardinality of a cost attribution sample tracker for each user.
		# TYPE cortex_cost_attribution_sample_tracker_cardinality gauge
		cortex_cost_attribution_sample_tracker_cardinality{tracker="cost-attribution",user="user1"} 2
		cortex_cost_attribution_sample_tracker_cardinality{tracker="cost-attribution",user="user3"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics),
			"cortex_cost_attribution_sample_tracker_cardinality",
			"cortex_cost_attribution_sample_tracker_overflown",
			"cortex_cost_attribution_active_series_tracker_cardinality",
			"cortex_cost_attribution_active_series_tracker_overflown",
			"cortex_cost_attribution_tracker_creation_errors_total",
		))
	})

	t.Run("Update cost attribution labels with a bad label name", func(t *testing.T) {
		manager.limits = testutils.NewMockCostAttributionLimits(0, []string{"user1", "__team__"})
		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Add(manager.inactiveTimeout))

		expectedMetrics := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
        cortex_ingester_attributed_active_native_histogram_buckets{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 2
        # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
        cortex_ingester_attributed_active_native_histogram_series{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_attributed_series_overflow_labels The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.
		# TYPE cortex_attributed_series_overflow_labels gauge
		cortex_attributed_series_overflow_labels{my_department="__overflow__",my_service="__overflow__",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg,
			strings.NewReader(expectedMetrics),
			"cortex_discarded_attributed_samples_total",
			"cortex_distributor_received_attributed_samples_total",
			"cortex_ingester_attributed_active_series",
			"cortex_ingester_attributed_active_native_histogram_series",
			"cortex_ingester_attributed_active_native_histogram_buckets",
			"cortex_attributed_series_overflow_labels",
		))

		expectedMetrics = `
		# HELP cortex_cost_attribution_active_series_tracker_cardinality The cardinality of a cost attribution active series tracker for each user.
		# TYPE cortex_cost_attribution_active_series_tracker_cardinality gauge
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user3"} 1
		# HELP cortex_cost_attribution_sample_tracker_cardinality The cardinality of a cost attribution sample tracker for each user.
		# TYPE cortex_cost_attribution_sample_tracker_cardinality gauge
		cortex_cost_attribution_sample_tracker_cardinality{tracker="cost-attribution",user="user3"} 1
        # HELP cortex_cost_attribution_tracker_creation_errors_total The total number of errors creating cost attribution trackers for each user.
        # TYPE cortex_cost_attribution_tracker_creation_errors_total counter
        cortex_cost_attribution_tracker_creation_errors_total{tracker="active-series",user="user1"} 1
        cortex_cost_attribution_tracker_creation_errors_total{tracker="samples",user="user1"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics),
			"cortex_cost_attribution_sample_tracker_cardinality",
			"cortex_cost_attribution_sample_tracker_overflown",
			"cortex_cost_attribution_active_series_tracker_cardinality",
			"cortex_cost_attribution_active_series_tracker_overflown",
			"cortex_cost_attribution_tracker_creation_errors_total",
		))
	})

	t.Run("Update cost attribution labels with a good label name", func(t *testing.T) {
		manager.limits = testutils.NewMockCostAttributionLimits(0, []string{"user1", "team"})
		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Add(manager.inactiveTimeout))
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 50)

		expectedMetrics := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
        cortex_ingester_attributed_active_native_histogram_buckets{team="bar",tenant="user1",tracker="cost-attribution"} 50
        cortex_ingester_attributed_active_native_histogram_buckets{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 2
        # HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
        # TYPE cortex_ingester_attributed_active_native_histogram_series gauge
        cortex_ingester_attributed_active_native_histogram_series{team="bar",tenant="user1",tracker="cost-attribution"} 1
        cortex_ingester_attributed_active_native_histogram_series{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
        # TYPE cortex_ingester_attributed_active_series gauge
        cortex_ingester_attributed_active_series{team="bar",tenant="user1",tracker="cost-attribution"} 1
        cortex_ingester_attributed_active_series{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		# HELP cortex_attributed_series_overflow_labels The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.
		# TYPE cortex_attributed_series_overflow_labels gauge
        cortex_attributed_series_overflow_labels{team="__overflow__",tenant="user1",tracker="cost-attribution"} 1
		cortex_attributed_series_overflow_labels{my_department="__overflow__",my_service="__overflow__",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg,
			strings.NewReader(expectedMetrics),
			"cortex_discarded_attributed_samples_total",
			"cortex_distributor_received_attributed_samples_total",
			"cortex_ingester_attributed_active_series",
			"cortex_ingester_attributed_active_native_histogram_series",
			"cortex_ingester_attributed_active_native_histogram_buckets",
			"cortex_attributed_series_overflow_labels",
		))

		expectedMetrics = `
		# HELP cortex_cost_attribution_active_series_tracker_cardinality The cardinality of a cost attribution active series tracker for each user.
		# TYPE cortex_cost_attribution_active_series_tracker_cardinality gauge
        cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user1"} 1
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user3"} 1
		# HELP cortex_cost_attribution_sample_tracker_cardinality The cardinality of a cost attribution sample tracker for each user.
		# TYPE cortex_cost_attribution_sample_tracker_cardinality gauge
        cortex_cost_attribution_sample_tracker_cardinality{tracker="cost-attribution",user="user3"} 1
        # HELP cortex_cost_attribution_tracker_creation_errors_total The total number of errors creating cost attribution trackers for each user.
        # TYPE cortex_cost_attribution_tracker_creation_errors_total counter
        cortex_cost_attribution_tracker_creation_errors_total{tracker="active-series",user="user1"} 1
        cortex_cost_attribution_tracker_creation_errors_total{tracker="samples",user="user1"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics),
			"cortex_cost_attribution_sample_tracker_cardinality",
			"cortex_cost_attribution_sample_tracker_overflown",
			"cortex_cost_attribution_active_series_tracker_cardinality",
			"cortex_cost_attribution_active_series_tracker_overflown",
			"cortex_cost_attribution_tracker_creation_errors_total",
		))
	})
}
