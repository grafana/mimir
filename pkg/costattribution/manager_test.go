// SPDX-License-Identifier: AGPL-3.0-only

package costattribution

import (
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	"github.com/grafana/mimir/pkg/costattribution/testutils"
	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func newTestManager() (manager *Manager, reg, costAttributionReg *prometheus.Registry) {
	logger := log.NewNopLogger()
	limits := testutils.NewMockCostAttributionLimits()
	reg = prometheus.NewRegistry()
	costAttributionReg = prometheus.NewRegistry()
	manager, err := NewManager(5*time.Second, 10*time.Second, logger, limits, reg, costAttributionReg)
	if err != nil {
		panic(err)
	}
	return manager, reg, costAttributionReg
}

// caLabels builds cost-attribution labels from input→output pairs, e.g.
// caLabels("team", "my_team", "service", "my_service"). An empty output keeps the input name.
func caLabels(inputOutputPairs ...string) costattributionmodel.Labels {
	out := make(costattributionmodel.Labels, 0, len(inputOutputPairs)/2)
	for i := 0; i+1 < len(inputOutputPairs); i += 2 {
		out = append(out, costattributionmodel.Label{Input: inputOutputPairs[i], Output: inputOutputPairs[i+1]})
	}
	return out
}

// defaultTrackerConfig wraps the given labels in a single default ("cost-attribution") tracker.
func defaultTrackerConfig(lbls costattributionmodel.Labels) costattributionmodel.TrackerConfigs {
	return costattributionmodel.TrackerConfigs{costattributionmodel.DefaultTrackerName: {Labels: lbls}}
}

// costAttributionOverrides canonicalizes and hashes the per-tenant cost-attribution config and wraps it in Overrides.
func costAttributionOverrides(tenantLimits map[string]*validation.Limits) *validation.Overrides {
	for _, l := range tenantLimits {
		l.CostAttributionBaseTrackers.Canonicalize()
		l.AdditionalCostAttributionTrackers.Canonicalize()
		l.ComputeCostAttributionConfigHash()
	}
	return validation.NewOverrides(validation.Limits{}, validation.NewMockTenantLimits(tenantLimits))
}

// newManagerWithLimits builds a Manager whose tenants use the given per-tenant cost-attribution limits.
// Each (sub-)test defines exactly the tenants and trackers it needs, so tests are independent and can run in parallel.
func newManagerWithLimits(t *testing.T, tenantLimits map[string]*validation.Limits) (manager *Manager, reg, costAttributionReg *prometheus.Registry) {
	t.Helper()
	reg = prometheus.NewRegistry()
	costAttributionReg = prometheus.NewRegistry()
	manager, err := NewManager(5*time.Second, 10*time.Second, log.NewNopLogger(), costAttributionOverrides(tenantLimits), reg, costAttributionReg)
	require.NoError(t, err)
	return manager, reg, costAttributionReg
}

// trackerByName returns the individual tracker with the given name from the user's composite,
// or the zero value if there's no composite or no tracker with that name.
func (mt *managerTrackers[CT, IT]) trackerByName(userID, name string) IT {
	mt.RLock()
	defer mt.RUnlock()
	if c, ok := mt.composite[userID]; ok {
		for _, t := range c.getTrackers() {
			if t.trackerName() == name {
				return t
			}
		}
	}
	var zero IT
	return zero
}

func getSampleTracker(m *Manager, userID string) *sampleTracker {
	return m.sampleTrackers.trackerByName(userID, costattributionmodel.DefaultTrackerName)
}

func getActiveTracker(m *Manager, userID string) *activeSeriesTracker {
	return m.activeSeriesTrackers.trackerByName(userID, costattributionmodel.DefaultTrackerName)
}

func assertHasLabels(t *testing.T, tracker individualTracker, expected costattributionmodel.Labels) {
	t.Helper()
	l, _, _, _ := tracker.config()
	assert.Equal(t, expected, l)
}

func withLockedActiveSeriesTracker(m *Manager, userID string, fn func(ast *activeSeriesTracker)) {
	m.ActiveSeriesTracker(userID) // ensure tracker exists
	ast := getActiveTracker(m, userID)
	ast.observedMtx.Lock()
	defer ast.observedMtx.Unlock()
	fn(ast)
}

func TestManager_New(t *testing.T) {
	manager, _, _ := newTestManager()
	assert.NotNil(t, manager)
	assert.NotNil(t, manager.sampleTrackers.composite)
	assert.Equal(t, 10*time.Second, manager.inactiveTimeout)
}

func TestManager_CreateDeleteTracker(t *testing.T) {
	t.Run("Tracker existence and attributes", func(t *testing.T) {
		t.Parallel()
		manager, _, _ := newManagerWithLimits(t, map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team"))},
			"user2": {MaxCostAttributionCardinality: 2},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})

		assert.NotNil(t, manager.SampleTracker("user1"))
		st := getSampleTracker(manager, "user1")
		assertHasLabels(t, st, costattributionmodel.Labels{{Input: "team", Output: "my_team"}})
		assert.Equal(t, 5, st.maxCardinality)

		assert.Nil(t, manager.SampleTracker("user2"))

		assert.NotNil(t, manager.ActiveSeriesTracker("user3"))
		at := getActiveTracker(manager, "user3")
		assertHasLabels(t, at, costattributionmodel.Labels{{Input: "department", Output: "my_department"}, {Input: "service", Output: "my_service"}})
		assert.Equal(t, 2, at.maxCardinality)
	})

	t.Run("Metrics tracking", func(t *testing.T) {
		t.Parallel()
		manager, reg, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team"))},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})

		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}}, 1, "invalid-metrics-name", time.Unix(6, 0))
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"department", "foo", "service", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))
		manager.ActiveSeriesTracker("user3") // Create the (empty) tracker so its operational and overflow-label metrics are exported.
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
		t.Parallel()
		manager, _, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team"))},
		})
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}}, 1, "invalid-metrics-name", time.Unix(6, 0))
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), -1)

		// Deadline is t=10: the discarded sample at t=6 expires, the one at t=12 survives, and the
		// active series (which is never time-purged) is kept.
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
		t.Parallel()
		manager, _, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team"))},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(12, 0))
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"department", "foo", "service", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))
		require.Equal(t, 2, len(manager.sampleTrackers.composite))

		// Reload with cost attribution disabled for user1 (no trackers), keeping user3 unchanged.
		manager.limits = costAttributionOverrides(map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})
		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Add(manager.inactiveTimeout))
		assert.Equal(t, 1, len(manager.sampleTrackers.composite))

		expectedMetrics := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_department="foo",my_service="dodo",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total", "cortex_distributor_received_attributed_samples_total", "cortex_ingester_attributed_active_series"))
	})

	t.Run("Updating user cardinality and labels", func(t *testing.T) {
		t.Parallel()
		manager, _, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"department", "foo", "service", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))

		// Reload user3 with a different set of labels: the trackers are rebuilt with the new labels.
		manager.limits = costAttributionOverrides(map[string]*validation.Limits{
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("feature", "my_feature", "team", "my_team"))},
		})
		manager.purgeInactiveAttributionsUntil(time.Unix(12, 0).Add(manager.inactiveTimeout))
		manager.SampleTracker("user3")
		assert.Equal(t, 1, len(manager.sampleTrackers.composite))
		st := getSampleTracker(manager, "user3")
		assertHasLabels(t, st, costattributionmodel.Labels{{Input: "feature", Output: "my_feature"}, {Input: "team", Output: "my_team"}})
		manager.ActiveSeriesTracker("user3")
		at := getActiveTracker(manager, "user3")
		assertHasLabels(t, at, costattributionmodel.Labels{{Input: "feature", Output: "my_feature"}, {Input: "team", Output: "my_team"}})

		manager.SampleTracker("user3").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(13, 0))
		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_feature="__missing__",my_team="foo",reason="invalid-metrics-name",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Overflow metrics on cardinality limit", func(t *testing.T) {
		t.Parallel()
		manager, reg, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("feature", "my_feature", "team", "my_team"))},
		})
		// Create the (empty) active series tracker so its operational metrics are exported, and seed one
		// observed combination via a discarded sample.
		manager.ActiveSeriesTracker("user3")
		manager.SampleTracker("user3").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "foo"}}, 1, "invalid-metrics-name", time.Unix(13, 0))

		// Three more distinct combinations exceed maxCardinality (2), so the tracker overflows.
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
		assert.Equal(t, 2, len(manager.sampleTrackers.composite))

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_team="foo",reason="invalid-metrics-name",tenant="user1",tracker="cost-attribution"} 1
		cortex_discarded_attributed_samples_total{my_department="foo",my_service="bar",reason="out-of-window",tenant="user3",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics), "cortex_discarded_attributed_samples_total"))
	})

	t.Run("Purge after inactive timeout", func(t *testing.T) {
		// Reload with cost attribution disabled for user1 (no trackers); user3 and user7 are unchanged.
		manager.limits = costAttributionOverrides(map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
			"user7": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team")), CostAttributionCooldown: model.Duration(testutils.TestAttributionCooldown)},
		})
		manager.purgeInactiveAttributionsUntil(time.Unix(5, 0).Add(manager.inactiveTimeout))

		assert.Equal(t, 1, len(manager.sampleTrackers.composite), "Expected one active tracker after purging")
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
		manager.purgeInactiveAttributionsUntil(time.Unix(20, 0).Add(manager.inactiveTimeout))

		assert.Equal(t, 0, len(manager.sampleTrackers.composite), "Expected no active trackers after full purge")

		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(""), "cortex_discarded_attributed_samples_total", "cortex_distributor_received_attributed_samples_total"))
	})

	t.Run("Overflown active series recover only after they are under the limit", func(t *testing.T) {
		now := time.Unix(10, 0)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "1"), now, 0)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "2"), now, 0)

		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 2, len(ast.observed), "Should have 2 series tracked")
			require.True(t, ast.overflowSince.IsZero(), "Should not be in overflow state yet")
			require.Zero(t, ast.overflowCounter.activeSeries.Load(), "Overflow counter should be zero")
		})

		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "3"), now, 0)

		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 3, len(ast.observed), "Should have 3 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Zero(t, ast.overflowCounter.activeSeries.Load(), "Overflow counter should be zero")
		})

		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "4"), now, 0)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "5"), now, 0)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "6"), now, 0)

		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 4, len(ast.observed), "Should have 4 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(2), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown)
		manager.purgeInactiveAttributionsUntil(now)

		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 4, len(ast.observed), "Should have 4 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(2), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown / 10)
		manager.ActiveSeriesTracker("user7").Decrement(labels.FromStrings("team", "1"), 0)
		manager.ActiveSeriesTracker("user7").Decrement(labels.FromStrings("team", "6"), 0)

		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 3, len(ast.observed), "Should have 3 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown)
		manager.purgeInactiveAttributionsUntil(now)

		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 3, len(ast.observed), "Should have 3 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown / 10)
		manager.ActiveSeriesTracker("user7").Increment(labels.FromStrings("team", "7"), now, 0)
		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 4, len(ast.observed), "Should have 4 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown / 10)
		manager.ActiveSeriesTracker("user7").Decrement(labels.FromStrings("team", "3"), 0)
		manager.ActiveSeriesTracker("user7").Decrement(labels.FromStrings("team", "4"), 0)

		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 2, len(ast.observed), "Should have 2 series tracked")
			require.False(t, ast.overflowSince.IsZero(), "Should be in overflow state")
			require.Equal(t, int64(1), ast.overflowCounter.activeSeries.Load(), "Overflow counter should be 1")
		})

		now = now.Add(testutils.TestAttributionCooldown)
		manager.purgeInactiveAttributionsUntil(now)
		withLockedActiveSeriesTracker(manager, "user7", func(ast *activeSeriesTracker) {
			require.Equal(t, 0, len(ast.observed), "Should have no series tracked")
			require.True(t, ast.overflowSince.IsZero(), "Should not be in overflow state anymore")
			require.Zero(t, ast.overflowCounter.activeSeries.Load(), "Overflow counter should be zero")
		})
	})
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

// multiTrackerUser8Limits returns a tenant with a default tracker plus an additional "by-platform" tracker.
func multiTrackerUser8Limits() *validation.Limits {
	return &validation.Limits{
		MaxCostAttributionCardinality: 5,
		CostAttributionBaseTrackers:   defaultTrackerConfig(caLabels("team", "my_team")),
		AdditionalCostAttributionTrackers: costattributionmodel.TrackerConfigs{
			"by-platform": {Labels: caLabels("platform", "my_platform")},
		},
	}
}

// multiTrackerUser9Limits returns a tenant with no default tracker and only internal additional trackers.
func multiTrackerUser9Limits() *validation.Limits {
	return &validation.Limits{
		MaxCostAttributionCardinality: 3,
		AdditionalCostAttributionTrackers: costattributionmodel.TrackerConfigs{
			"by-team":    {Labels: caLabels("team", "my_team"), Internal: true},
			"by-service": {Labels: caLabels("service", "my_service"), Internal: true},
		},
	}
}

func TestManager_MultipleTrackers(t *testing.T) {
	t.Run("Default plus additional tracker", func(t *testing.T) {
		t.Parallel()
		manager, _, _ := newManagerWithLimits(t, map[string]*validation.Limits{"user8": multiTrackerUser8Limits()})

		// user8 has default tracker (team) + additional tracker (by-platform).
		st := manager.SampleTracker("user8")
		require.NotNil(t, st)
		require.Len(t, st.trackers, 2)

		at := manager.ActiveSeriesTracker("user8")
		require.NotNil(t, at)
		require.Len(t, at.trackers, 2)

		// Verify individual tracker labels.
		defaultST := manager.sampleTrackers.trackerByName("user8", costattributionmodel.DefaultTrackerName)
		require.NotNil(t, defaultST)
		assertHasLabels(t, defaultST, costattributionmodel.Labels{{Input: "team", Output: "my_team"}})

		platformST := manager.sampleTrackers.trackerByName("user8", "by-platform")
		require.NotNil(t, platformST)
		assertHasLabels(t, platformST, costattributionmodel.Labels{{Input: "platform", Output: "my_platform"}})
	})

	t.Run("Only additional trackers", func(t *testing.T) {
		t.Parallel()
		manager, _, _ := newManagerWithLimits(t, map[string]*validation.Limits{"user9": multiTrackerUser9Limits()})

		// user9 has no default tracker, only additional trackers (by-team, by-service).
		st := manager.SampleTracker("user9")
		require.NotNil(t, st)
		require.Len(t, st.trackers, 2)

		require.Nil(t, manager.sampleTrackers.trackerByName("user9", costattributionmodel.DefaultTrackerName))
		require.NotNil(t, manager.sampleTrackers.trackerByName("user9", "by-team"))
		require.NotNil(t, manager.sampleTrackers.trackerByName("user9", "by-service"))
	})

	t.Run("Metrics fan out to all trackers", func(t *testing.T) {
		t.Parallel()
		manager, _, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{"user8": multiTrackerUser8Limits()})

		// Increment samples on user8 — both trackers should see the data.
		manager.SampleTracker("user8").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{
			{LabelValues: []string{"team", "backend", "platform", "k8s"}, SamplesCount: 5},
		}), time.Unix(10, 0))

		manager.SampleTracker("user8").IncrementDiscardedSamples(
			[]mimirpb.LabelAdapter{{Name: "team", Value: "backend"}, {Name: "platform", Value: "k8s"}},
			2, "sample-out-of-order", time.Unix(11, 0),
		)

		expectedMetrics := `
		# HELP cortex_discarded_attributed_samples_total The total number of samples that were discarded per attribution.
		# TYPE cortex_discarded_attributed_samples_total counter
		cortex_discarded_attributed_samples_total{my_platform="k8s",reason="sample-out-of-order",tenant="user8",tracker="by-platform"} 2
		cortex_discarded_attributed_samples_total{my_team="backend",reason="sample-out-of-order",tenant="user8",tracker="cost-attribution"} 2
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_platform="k8s",tenant="user8",tracker="by-platform"} 5
		cortex_distributor_received_attributed_samples_total{my_team="backend",tenant="user8",tracker="cost-attribution"} 5
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics),
			"cortex_distributor_received_attributed_samples_total",
			"cortex_discarded_attributed_samples_total",
		))
	})

	t.Run("Active series fan out to all trackers", func(t *testing.T) {
		t.Parallel()
		manager, _, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{"user8": multiTrackerUser8Limits()})

		manager.ActiveSeriesTracker("user8").Increment(labels.FromStrings("team", "frontend", "platform", "bare-metal"), time.Unix(20, 0), 3)

		expectedMetrics := `
		# HELP cortex_ingester_attributed_active_native_histogram_buckets The total number of active native histogram buckets per user and attribution.
		# TYPE cortex_ingester_attributed_active_native_histogram_buckets gauge
		cortex_ingester_attributed_active_native_histogram_buckets{my_platform="bare-metal",tenant="user8",tracker="by-platform"} 3
		cortex_ingester_attributed_active_native_histogram_buckets{my_team="frontend",tenant="user8",tracker="cost-attribution"} 3
		# HELP cortex_ingester_attributed_active_native_histogram_series The total number of active native histogram series per user and attribution.
		# TYPE cortex_ingester_attributed_active_native_histogram_series gauge
		cortex_ingester_attributed_active_native_histogram_series{my_platform="bare-metal",tenant="user8",tracker="by-platform"} 1
		cortex_ingester_attributed_active_native_histogram_series{my_team="frontend",tenant="user8",tracker="cost-attribution"} 1
		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
		# TYPE cortex_ingester_attributed_active_series gauge
		cortex_ingester_attributed_active_series{my_platform="bare-metal",tenant="user8",tracker="by-platform"} 1
		cortex_ingester_attributed_active_series{my_team="frontend",tenant="user8",tracker="cost-attribution"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(costAttributionReg, strings.NewReader(expectedMetrics),
			"cortex_ingester_attributed_active_series",
			"cortex_ingester_attributed_active_native_histogram_series",
			"cortex_ingester_attributed_active_native_histogram_buckets",
		))
	})

	t.Run("Operational metrics include tracker name", func(t *testing.T) {
		t.Parallel()
		manager, reg, _ := newManagerWithLimits(t, map[string]*validation.Limits{
			"user8": multiTrackerUser8Limits(),
			"user9": multiTrackerUser9Limits(),
		})
		// user8 gets one observed combination in each of its sample and active series trackers.
		manager.SampleTracker("user8").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{
			{LabelValues: []string{"team", "backend", "platform", "k8s"}, SamplesCount: 5},
		}), time.Unix(10, 0))
		manager.ActiveSeriesTracker("user8").Increment(labels.FromStrings("team", "frontend", "platform", "bare-metal"), time.Unix(20, 0), 3)
		// user9's sample trackers exist but have no data, and it has no active series tracker.
		manager.SampleTracker("user9")

		expectedMetrics := `
		# HELP cortex_cost_attribution_sample_tracker_cardinality The cardinality of a cost attribution sample tracker for each user.
		# TYPE cortex_cost_attribution_sample_tracker_cardinality gauge
		cortex_cost_attribution_sample_tracker_cardinality{tracker="by-platform",user="user8"} 1
		cortex_cost_attribution_sample_tracker_cardinality{tracker="by-service",user="user9"} 0
		cortex_cost_attribution_sample_tracker_cardinality{tracker="by-team",user="user9"} 0
		cortex_cost_attribution_sample_tracker_cardinality{tracker="cost-attribution",user="user8"} 1
		# HELP cortex_cost_attribution_active_series_tracker_cardinality The cardinality of a cost attribution active series tracker for each user.
		# TYPE cortex_cost_attribution_active_series_tracker_cardinality gauge
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="by-platform",user="user8"} 1
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user8"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics),
			"cortex_cost_attribution_sample_tracker_cardinality",
			"cortex_cost_attribution_active_series_tracker_cardinality",
		))
	})

	t.Run("Operational metrics include the internal trackers", func(t *testing.T) {
		t.Parallel()
		manager, reg, _ := newManagerWithLimits(t, map[string]*validation.Limits{"user9": multiTrackerUser9Limits()})

		manager.SampleTracker("user9").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{
			{LabelValues: []string{"__name__", "testing", "team", "ops", "service", "gateway"}, SamplesCount: 1},
		}), time.Unix(100, 0))
		manager.ActiveSeriesTracker("user9").Increment(
			labels.FromStrings("__name__", "testing", "team", "ops", "service", "gateway"), time.Now(), 0,
		)
		expectedMetrics := `
		# HELP cortex_distributor_received_attributed_samples_total The total number of samples that were received per attribution.
		# TYPE cortex_distributor_received_attributed_samples_total counter
		cortex_distributor_received_attributed_samples_total{my_service="gateway",tenant="user9",tracker="by-service"} 1
		cortex_distributor_received_attributed_samples_total{my_team="ops",tenant="user9",tracker="by-team"} 1

		# HELP cortex_ingester_attributed_active_series The total number of active series per user and attribution.
		# TYPE cortex_ingester_attributed_active_series gauge
		cortex_ingester_attributed_active_series{my_service="gateway",tenant="user9",tracker="by-service"} 1
		cortex_ingester_attributed_active_series{my_team="ops",tenant="user9",tracker="by-team"} 1
		`
		assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics),
			"cortex_distributor_received_attributed_samples_total",
			"cortex_ingester_attributed_active_series",
		))
	})

	t.Run("Purge removes all trackers for a user when inactive", func(t *testing.T) {
		t.Parallel()
		manager, _, _ := newManagerWithLimits(t, map[string]*validation.Limits{"user9": multiTrackerUser9Limits()})

		// user9 with two additional trackers — add some data then purge.
		manager.SampleTracker("user9").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{
			{LabelValues: []string{"team", "ops", "service", "gateway"}, SamplesCount: 1},
		}), time.Unix(100, 0))

		require.Len(t, manager.sampleTrackers.composite["user9"].getTrackers(), 2)

		// Purge at t=105 → deadline=95. Data at 100 > 95, survives.
		manager.purgeInactiveAttributionsUntil(time.Unix(105, 0))
		require.Len(t, manager.sampleTrackers.composite["user9"].getTrackers(), 2, "user9 trackers should survive after early purge")

		// Purge at t=120 → deadline=110. Data at 100 <= 110, expired. Cardinality → 0, user removed.
		manager.purgeInactiveAttributionsUntil(time.Unix(120, 0))
		assert.Empty(t, manager.sampleTrackers.composite["user9"], "user9 composite should be purged")
	})
}

func TestManager_InvalidTrackers(t *testing.T) {
	t.Run("Tracker existence and attributes", func(t *testing.T) {
		t.Parallel()
		manager, _, _ := newManagerWithLimits(t, map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team"))},
			"user2": {MaxCostAttributionCardinality: 2},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})

		assert.NotNil(t, manager.SampleTracker("user1"))
		st := getSampleTracker(manager, "user1")
		assertHasLabels(t, st, costattributionmodel.Labels{{Input: "team", Output: "my_team"}})
		assert.Equal(t, 5, st.maxCardinality)

		assert.Nil(t, manager.SampleTracker("user2"))

		assert.NotNil(t, manager.ActiveSeriesTracker("user3"))
		at := getActiveTracker(manager, "user3")
		assertHasLabels(t, at, costattributionmodel.Labels{{Input: "department", Output: "my_department"}, {Input: "service", Output: "my_service"}})
		assert.Equal(t, 2, at.maxCardinality)
	})

	t.Run("Metrics tracking", func(t *testing.T) {
		t.Parallel()
		manager, reg, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team"))},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})

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
		t.Parallel()
		manager, reg, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team"))},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})
		// Seed user1 (so the reload rebuilds its trackers) and user3 (so it survives the reload+purge).
		manager.SampleTracker("user1").IncrementDiscardedSamples([]mimirpb.LabelAdapter{{Name: "team", Value: "bar"}}, 1, "invalid-metrics-name", time.Unix(6, 0))
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 50)
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"department", "foo", "service", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))
		manager.ActiveSeriesTracker("user3").Increment(labels.FromStrings("department", "foo", "service", "dodo"), time.Unix(10, 0), 2)

		// Reload user1 with an invalid (reserved) output label name: its trackers can't be created and user1 is dropped.
		manager.limits = costAttributionOverrides(map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 10, CostAttributionBaseTrackers: defaultTrackerConfig(costattributionmodel.Labels{{Input: "__team__"}})},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})
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
        cortex_cost_attribution_tracker_creation_errors_total{tracker_name="cost-attribution",tracker_type="active-series",user="user1"} 1
        cortex_cost_attribution_tracker_creation_errors_total{tracker_name="cost-attribution",tracker_type="samples",user="user1"} 1
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
		t.Parallel()
		manager, reg, costAttributionReg := newManagerWithLimits(t, map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 5, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("team", "my_team"))},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})
		// Seed user1's active series tracker (with the renamed label) so the reload rebuilds it,
		// and user3 so it survives the reload+purge.
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 50)
		manager.SampleTracker("user3").IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"department", "foo", "service", "dodo"}, SamplesCount: 1}}), time.Unix(20, 0))
		manager.ActiveSeriesTracker("user3").Increment(labels.FromStrings("department", "foo", "service", "dodo"), time.Unix(10, 0), 2)

		// Reload user1 to attribute by the raw "team" label (output defaults to the input name), which is valid.
		manager.limits = costAttributionOverrides(map[string]*validation.Limits{
			"user1": {MaxCostAttributionCardinality: 10, CostAttributionBaseTrackers: defaultTrackerConfig(costattributionmodel.Labels{{Input: "team"}})},
			"user3": {MaxCostAttributionCardinality: 2, CostAttributionBaseTrackers: defaultTrackerConfig(caLabels("department", "my_department", "service", "my_service"))},
		})
		manager.purgeInactiveAttributionsUntil(time.Unix(11, 0).Add(manager.inactiveTimeout))
		manager.ActiveSeriesTracker("user1").Increment(labels.FromStrings("team", "bar"), time.Unix(10, 0), 50)

		expectedMetrics := `
		# HELP cortex_attributed_series_overflow_labels The overflow labels for this tenant. This metric is always 1 for tenants with active series, it is only used to have the overflow labels available in the recording rules without knowing their names.
		# TYPE cortex_attributed_series_overflow_labels gauge
		cortex_attributed_series_overflow_labels{team="__overflow__",tenant="user1",tracker="cost-attribution"} 1
		cortex_attributed_series_overflow_labels{my_department="__overflow__",my_service="__overflow__",tenant="user3",tracker="cost-attribution"} 1
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

		// The reloaded label is valid, so no tracker creation errors are reported.
		expectedMetrics = `
		# HELP cortex_cost_attribution_active_series_tracker_cardinality The cardinality of a cost attribution active series tracker for each user.
		# TYPE cortex_cost_attribution_active_series_tracker_cardinality gauge
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user1"} 1
		cortex_cost_attribution_active_series_tracker_cardinality{tracker="cost-attribution",user="user3"} 1
		# HELP cortex_cost_attribution_sample_tracker_cardinality The cardinality of a cost attribution sample tracker for each user.
		# TYPE cortex_cost_attribution_sample_tracker_cardinality gauge
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
}

// newManagerForLimits builds a Manager whose single tenant userID uses the given limits.
func newManagerForLimits(t *testing.T, userID string, l *validation.Limits) *Manager {
	t.Helper()
	overrides := costAttributionOverrides(map[string]*validation.Limits{userID: l})
	// inactiveTimeout is small; tests drive "now" explicitly through purgeInactiveAttributionsUntil.
	m, err := NewManager(time.Minute, time.Second, log.NewNopLogger(), overrides, prometheus.NewRegistry(), prometheus.NewRegistry())
	require.NoError(t, err)
	return m
}

// TestManager_ActiveSeriesTrackerReplacedAfterOverflowRecovery asserts the contract the ingester
// relies on: when an active-series tracker recovers from overflow (its observed state is reset),
// the Manager must hand back a different ActiveSeriesTracker composite (!Equals). The ingester
// detects that change via CostAttributionDiffers (pointer identity) and re-tracks all current
// series into the fresh tracker. If the Manager keeps returning the same composite while having
// reset the underlying state, the ingester never re-tracks and later decrements operate on a
// tracker whose state was wiped underneath it.
func TestManager_ActiveSeriesTrackerReplacedAfterOverflowRecovery(t *testing.T) {
	const userID = "u"
	base := time.Unix(0, 0)
	// purgeAt is well past the cooldown so the tracker is eligible to recover.
	purgeAt := base.Add(time.Hour)

	t.Run("single tracker", func(t *testing.T) {
		m := newManagerForLimits(t, userID, &validation.Limits{
			MaxCostAttributionCardinality: 2,
			CostAttributionCooldown:       model.Duration(time.Minute),
			CostAttributionBaseTrackers: costattributionmodel.TrackerConfigs{
				"t1": {Labels: costattributionmodel.Labels{{Input: "a", Output: "a"}}},
			},
		})

		before := m.ActiveSeriesTracker(userID)
		require.NotNil(t, before)

		// Drive t1 into overflow (3 distinct combos > maxCardinality 2), then bring its observed
		// cardinality back to <= maxCardinality so the next purge recovers it.
		before.Increment(labels.FromStrings("a", "v1"), base, -1)
		before.Increment(labels.FromStrings("a", "v2"), base, -1)
		before.Increment(labels.FromStrings("a", "v3"), base, -1)
		before.Decrement(labels.FromStrings("a", "v3"), -1)

		m.purgeInactiveAttributionsUntil(purgeAt)

		after := m.ActiveSeriesTracker(userID)
		require.False(t, before.Equals(after), "recovered tracker must be replaced so the ingester re-tracks")
	})

	t.Run("one of several trackers recovers", func(t *testing.T) {
		// Only t1 overflows and recovers; t2 keeps cardinality, so the composite is not dropped as
		// a whole. The Manager must still return a different composite because t1's state was reset.
		m := newManagerForLimits(t, userID, &validation.Limits{
			MaxCostAttributionCardinality: 2,
			CostAttributionCooldown:       model.Duration(time.Minute),
			CostAttributionBaseTrackers: costattributionmodel.TrackerConfigs{
				"t1": {Labels: costattributionmodel.Labels{{Input: "a", Output: "a"}}},
			},
			AdditionalCostAttributionTrackers: costattributionmodel.TrackerConfigs{
				"t2": {Labels: costattributionmodel.Labels{{Input: "b", Output: "b"}}},
			},
		})

		before := m.ActiveSeriesTracker(userID)
		require.NotNil(t, before)

		// t1 sees a=v1,v2,v3 (overflows); t2 sees only b=w1 (stays below the limit).
		before.Increment(labels.FromStrings("a", "v1", "b", "w1"), base, -1)
		before.Increment(labels.FromStrings("a", "v2", "b", "w1"), base, -1)
		before.Increment(labels.FromStrings("a", "v3", "b", "w1"), base, -1)
		before.Decrement(labels.FromStrings("a", "v3", "b", "w1"), -1) // t1 back to {v1,v2}, still overflown

		m.purgeInactiveAttributionsUntil(purgeAt)

		after := m.ActiveSeriesTracker(userID)
		require.False(t, before.Equals(after), "recovered tracker must be replaced so the ingester re-tracks")
	})
}

// TestManager_ActiveSeriesTrackerRebuiltFreshOnConfigReload guards against double-counting active
// series on a config reload. The config hash covers the whole cost-attribution config, so an
// unrelated change (here: adding a second tracker) forces the active-series composite to be
// rebuilt even though the existing tracker's own config didn't change. The ingester detects the
// new composite via Equals and re-tracks all current series; if rebuild reused the populated
// active-series tracker, those series would be counted twice. So the active-series tracker must be
// rebuilt fresh (empty). Sample trackers have no such re-track contract, so an unchanged one must
// instead be reused to preserve its accumulated counts.
func TestManager_ActiveSeriesTrackerRebuiltFreshOnConfigReload(t *testing.T) {
	const userID = "u"

	buildLimits := func(l *validation.Limits) *validation.Overrides {
		return costAttributionOverrides(map[string]*validation.Limits{userID: l})
	}

	// configA has a single tracker t1.
	configA := buildLimits(&validation.Limits{
		MaxCostAttributionCardinality: 100,
		CostAttributionBaseTrackers: costattributionmodel.TrackerConfigs{
			"t1": {Labels: costattributionmodel.Labels{{Input: "a", Output: "a"}}},
		},
	})
	// configB keeps t1 identical but adds an unrelated t2, which bumps the overall config hash.
	configB := buildLimits(&validation.Limits{
		MaxCostAttributionCardinality: 100,
		CostAttributionBaseTrackers: costattributionmodel.TrackerConfigs{
			"t1": {Labels: costattributionmodel.Labels{{Input: "a", Output: "a"}}},
		},
		AdditionalCostAttributionTrackers: costattributionmodel.TrackerConfigs{
			"t2": {Labels: costattributionmodel.Labels{{Input: "b", Output: "b"}}},
		},
	})

	m, err := NewManager(time.Minute, time.Second, log.NewNopLogger(), configA, prometheus.NewRegistry(), prometheus.NewRegistry())
	require.NoError(t, err)

	// Track a series into both the active-series and the sample tracker for t1.
	astBefore := m.ActiveSeriesTracker(userID)
	require.NotNil(t, astBefore)
	astBefore.Increment(labels.FromStrings("a", "v1"), time.Unix(0, 0), -1)

	stBefore := m.SampleTracker(userID)
	require.NotNil(t, stBefore)
	stBefore.IncrementReceivedSamples(testutils.CreateRequest([]testutils.Series{{LabelValues: []string{"a", "v1"}, SamplesCount: 1}}), time.Unix(0, 0))

	activeT1Before := m.activeSeriesTrackers.trackerByName(userID, "t1")
	sampleT1Before := m.sampleTrackers.trackerByName(userID, "t1")
	require.NotNil(t, activeT1Before)
	require.NotNil(t, sampleT1Before)
	if c, _ := activeT1Before.cardinality(); c != 1 {
		t.Fatalf("expected active tracker cardinality 1 before reload, got %d", c)
	}

	// Reload: t1 is unchanged, but the hash changed because t2 was added.
	m.limits = configB

	astAfter := m.ActiveSeriesTracker(userID)
	require.False(t, astBefore.Equals(astAfter), "composite must change so the ingester re-tracks")

	// The active-series tracker for t1 must be a fresh, empty instance.
	activeT1After := m.activeSeriesTrackers.trackerByName(userID, "t1")
	require.NotNil(t, activeT1After)
	require.NotSame(t, activeT1Before, activeT1After, "active-series tracker must be rebuilt from scratch")
	if c, _ := activeT1After.cardinality(); c != 0 {
		t.Fatalf("expected rebuilt active tracker to be empty, got cardinality %d", c)
	}

	// The sample tracker for t1 must be reused and keep its counts.
	require.NotNil(t, m.SampleTracker(userID))
	sampleT1After := m.sampleTrackers.trackerByName(userID, "t1")
	require.Same(t, sampleT1Before, sampleT1After, "unchanged sample tracker must be reused to preserve counts")
	if c, _ := sampleT1After.cardinality(); c != 1 {
		t.Fatalf("expected reused sample tracker to keep cardinality 1, got %d", c)
	}
}
