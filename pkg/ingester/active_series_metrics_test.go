// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	util_test "github.com/grafana/mimir/pkg/util/test"
	"github.com/grafana/mimir/pkg/util/validation"
)

// TestActiveSeriesMetricsDuringConfigReload verifies that total active series metrics
// continue to be reported during tracker configuration reload, while tracker-specific
// metrics are removed when the config changes.
func TestActiveSeriesMetricsDuringConfigReload(t *testing.T) {
	labelsToPush := [][]mimirpb.LabelAdapter{
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "b"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "b"}},
	}
	labelsToPushHist := [][]mimirpb.LabelAdapter{
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "false"}, {Name: "team", Value: "b"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "a"}},
		{{Name: labels.MetricName, Value: "test_histogram_metric"}, {Name: "bool", Value: "true"}, {Name: "team", Value: "b"}},
	}

	req := func(lbls []mimirpb.LabelAdapter, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.ToWriteRequest(
			[][]mimirpb.LabelAdapter{lbls},
			[]mimirpb.Sample{{Value: 1, TimestampMs: t.UnixMilli()}},
			nil,
			nil,
			mimirpb.API,
		)
	}
	reqHist := func(lbls []mimirpb.LabelAdapter, t time.Time) *mimirpb.WriteRequest {
		return mimirpb.NewWriteRequest(nil, mimirpb.API).AddHistogramSeries([][]mimirpb.LabelAdapter{lbls},
			[]mimirpb.Histogram{mimirpb.FromHistogramToHistogramProto(t.UnixMilli(), util_test.GenerateTestGaugeHistogram(1))}, nil)
	}

	userID := "test_user"

	// Initial config with bool-based trackers
	initialConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"bool_is_true":  `{bool="true"}`,
		"bool_is_false": `{bool="false"}`,
	})

	// New config with team-based trackers
	newConfig := mustNewActiveSeriesCustomTrackersConfigFromMap(t, map[string]string{
		"team_a": `{team="a"}`,
		"team_b": `{team="b"}`,
	})

	registry := prometheus.NewRegistry()

	// Create an ingester with the initial config
	cfg := defaultIngesterTestConfig(t)
	cfg.ActiveSeriesMetrics.Enabled = true

	limits := defaultLimitsTestConfig()
	limits.ActiveSeriesBaseCustomTrackersConfig = initialConfig
	limits.NativeHistogramsIngestionEnabled = true
	overrides := validation.NewOverrides(limits, nil)

	ingester, r, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, nil, "", "", registry)
	require.NoError(t, err)
	startAndWaitHealthy(t, ingester, r)

	// Push data
	pushWithUser(t, ingester, labelsToPush, userID, req)
	pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)

	// Update active series metrics with initial config
	ingester.updateActiveSeries(time.Now())

	// Verify initial metrics state - both total and tracker-specific metrics should be present
	assert.Equal(t, 8.0, testutil.ToFloat64(ingester.metrics.activeSeriesPerUser.WithLabelValues(userID)),
		"Initial active series metric should be 8 (4 regular + 4 histogram)")
	assert.Equal(t, 4.0, testutil.ToFloat64(ingester.metrics.activeSeriesPerUserNativeHistograms.WithLabelValues(userID)),
		"Initial native histogram series metric should be 4")
	assert.Equal(t, 4.0, testutil.ToFloat64(ingester.metrics.activeSeriesCustomTrackersPerUser.WithLabelValues(userID, "bool_is_true")),
		"Initial bool_is_true metric should be 4")
	assert.Equal(t, 4.0, testutil.ToFloat64(ingester.metrics.activeSeriesCustomTrackersPerUser.WithLabelValues(userID, "bool_is_false")),
		"Initial bool_is_false metric should be 4")

	// Change the configuration
	newLimits := defaultLimitsTestConfig()
	newLimits.ActiveSeriesBaseCustomTrackersConfig = newConfig
	newLimits.NativeHistogramsIngestionEnabled = true
	ingester.limits = validation.NewOverrides(newLimits, nil)

	// Update metrics after config change - this detects the config change and wipes all active series tracking data
	ingester.updateActiveSeries(time.Now())

	// Push data again to repopulate the active series refs
	pushWithUser(t, ingester, labelsToPush, userID, req)
	pushWithUser(t, ingester, labelsToPushHist, userID, reqHist)

	// Update metrics again to read the repopulated active series data
	ingester.updateActiveSeries(time.Now())

	// Verify that total metrics are still reported after config change
	assert.Equal(t, 8.0, testutil.ToFloat64(ingester.metrics.activeSeriesPerUser.WithLabelValues(userID)),
		"Active series metric should still be 8 after config change")
	assert.Equal(t, 4.0, testutil.ToFloat64(ingester.metrics.activeSeriesPerUserNativeHistograms.WithLabelValues(userID)),
		"Native histogram series metric should still be 4 after config change")

	// Verify the loading metric is set
	assert.Equal(t, 1.0, testutil.ToFloat64(ingester.metrics.activeSeriesLoading.WithLabelValues(userID)),
		"Loading metric should be 1 during reload period")

	// Verify that the old tracker-specific metrics were deleted
	metricFamilies, err := registry.Gather()
	require.NoError(t, err)

	var customTrackerMetrics *dto.MetricFamily
	for _, mf := range metricFamilies {
		if mf.GetName() == "cortex_ingester_active_series_custom_tracker" {
			customTrackerMetrics = mf
			break
		}
	}

	if customTrackerMetrics != nil {
		for _, m := range customTrackerMetrics.Metric {
			// Check that neither bool_is_true nor bool_is_false exist in the metrics
			var trackerName string
			for _, label := range m.Label {
				if label.GetName() == "name" {
					trackerName = label.GetValue()
					break
				}
			}
			assert.NotEqual(t, "bool_is_true", trackerName, "bool_is_true tracker should not exist after config reload")
			assert.NotEqual(t, "bool_is_false", trackerName, "bool_is_false tracker should not exist after config reload")
		}
	}
}
