// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/concurrency"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/remote/otlptranslator/prometheusremotewrite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"

	"github.com/grafana/mimir/pkg/distributor/otlpappender"
	"github.com/grafana/mimir/pkg/mimirpb"
	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestOTelTranslationWarningDeduper(t *testing.T) {
	t.Run("suppresses repeats within TTL, allows them after", func(t *testing.T) {
		now := time.Unix(0, 0)
		d := newOTelTranslationWarningDeduper(15*time.Minute, func() time.Time { return now })

		require.True(t, d.allow("tenant1", "warning a"))
		require.False(t, d.allow("tenant1", "warning a"), "repeat within TTL should be suppressed")
		require.True(t, d.allow("tenant1", "warning b"), "distinct warning should be allowed")
		require.True(t, d.allow("tenant2", "warning a"), "same warning for another tenant should be allowed")

		now = now.Add(15 * time.Minute)
		require.True(t, d.allow("tenant1", "warning a"), "warning should be allowed again after TTL")
	})

	t.Run("enforces per-tenant budget for distinct warnings", func(t *testing.T) {
		now := time.Unix(0, 0)
		d := newOTelTranslationWarningDeduper(15*time.Minute, func() time.Time { return now })

		for i := 0; i < otelTranslationWarningMaxPerTTL; i++ {
			require.True(t, d.allow("tenant1", fmt.Sprintf("warning %d", i)))
		}
		require.False(t, d.allow("tenant1", "one warning too many"), "budget should be exhausted")
		require.True(t, d.allow("tenant2", "warning"), "budget should be per tenant")

		now = now.Add(15 * time.Minute)
		require.True(t, d.allow("tenant1", "one warning too many"), "budget should reset after TTL")
	})

	t.Run("is safe and exact under concurrency", func(t *testing.T) {
		// The clock is constant, so it can be read concurrently without
		// synchronization.
		now := time.Unix(0, 0)
		d := newOTelTranslationWarningDeduper(15*time.Minute, func() time.Time { return now })

		const goroutines = 16
		var allowed atomic.Int64
		var wg sync.WaitGroup

		// Phase A: all goroutines race on the same (tenant, message).
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if d.allow("tenant1", "same warning") {
					allowed.Add(1)
				}
			}()
		}
		wg.Wait()
		require.Equal(t, int64(1), allowed.Load(), "exactly one goroutine should be allowed to log the shared warning")

		// Phase B: distinct messages well past the budget; the total of allowed
		// lines across both phases is exactly the per-tenant budget.
		for g := 0; g < goroutines; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < 10; i++ {
					if d.allow("tenant1", fmt.Sprintf("warning %d-%d", g, i)) {
						allowed.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
		require.Equal(t, int64(otelTranslationWarningMaxPerTTL), allowed.Load())
	})

	t.Run("purges expired entries on schedule", func(t *testing.T) {
		now := time.Unix(0, 0)
		d := newOTelTranslationWarningDeduper(15*time.Minute, func() time.Time { return now })

		require.True(t, d.allow("tenant1", "warning a"))

		// Within the purge interval nothing gets purged.
		now = now.Add(30 * time.Second)
		require.True(t, d.allow("tenant2", "warning b"))
		require.Len(t, d.seen, 2)
		require.Len(t, d.tenantWindows, 2)

		// Once the purge interval has elapsed, the next allow purges entries
		// older than the TTL from both maps.
		now = now.Add(16 * time.Minute)
		require.True(t, d.allow("tenant3", "warning c"))
		require.Len(t, d.seen, 1)
		require.Len(t, d.tenantWindows, 1)
		_, ok := d.seen["tenant3\x00warning c"]
		require.True(t, ok, "the fresh entry should survive the purge")
	})
}

// buildOTLPMetricsForCollisionTest returns a payload with one resource holding
// the given resource attributes plus a gauge metric with the given data point
// attributes. service.name and service.instance.id are set so that target_info
// gets generated.
func buildOTLPMetricsForCollisionTest(resourceAttrs, dataPointAttrs map[string]string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	attrs := rm.Resource().Attributes()
	attrs.PutStr("service.name", "svc")
	attrs.PutStr("service.instance.id", "inst")
	for k, v := range resourceAttrs {
		attrs.PutStr(k, v)
	}
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test_metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetDoubleValue(1)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(100, 0)))
	for k, v := range dataPointAttrs {
		dp.Attributes().PutStr(k, v)
	}
	return md
}

func TestOTLPMimirConverter_TranslationWarnings(t *testing.T) {
	for name, tc := range map[string]struct {
		resourceAttrs    map[string]string
		dataPointAttrs   map[string]string
		allowUTF8        bool
		expectedWarnings []string
	}{
		"colliding resource attributes": {
			resourceAttrs: map[string]string{"k8s.pod.name": "foo", "k8s_pod_name": "bar"},
			expectedWarnings: []string{
				`OTLP attributes "k8s.pod.name", "k8s_pod_name" collide as label "k8s_pod_name" after name sanitization, values are concatenated with ';'`,
			},
		},
		"colliding resource attributes with identical values": {
			resourceAttrs: map[string]string{"k8s.pod.name": "foo", "k8s_pod_name": "foo"},
			expectedWarnings: []string{
				`OTLP attributes "k8s.pod.name", "k8s_pod_name" collide as label "k8s_pod_name" after name sanitization, values are concatenated with ';'`,
			},
		},
		"colliding data point attributes": {
			resourceAttrs:  map[string]string{"k8s.pod.name": "foo"},
			dataPointAttrs: map[string]string{"http.method": "GET", "http_method": "POST"},
			expectedWarnings: []string{
				`OTLP attributes "http.method", "http_method" collide as label "http_method" after name sanitization, values are concatenated with ';'`,
			},
		},
		"no collisions": {
			resourceAttrs:    map[string]string{"k8s.pod.name": "foo"},
			expectedWarnings: nil,
		},
		"UTF-8 translation produces no collisions": {
			resourceAttrs:    map[string]string{"k8s.pod.name": "foo", "k8s_pod_name": "bar"},
			allowUTF8:        true,
			expectedWarnings: nil,
		},
	} {
		t.Run(name, func(t *testing.T) {
			md := buildOTLPMetricsForCollisionTest(tc.resourceAttrs, tc.dataPointAttrs)
			converter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())
			opts := conversionOptions{
				allowUTF8:                   tc.allowUTF8,
				underscoreSanitization:      true,
				preserveMultipleUnderscores: true,
			}
			series, _, dropped, err := otelMetricsToSeriesAndMetadata(context.Background(), converter, md, opts, log.NewNopLogger())
			require.NoError(t, err)
			require.Zero(t, dropped)
			require.NotEmpty(t, series)

			assert.ElementsMatch(t, tc.expectedWarnings, converter.TranslationWarnings())
		})
	}
}

// TestOTLPMimirConverter_TranslationWarnings_concatenatedValue pins the translation
// behavior the warning describes: colliding values get concatenated in target_info,
// ordered by the original attribute keys.
func TestOTLPMimirConverter_TranslationWarnings_concatenatedValue(t *testing.T) {
	md := buildOTLPMetricsForCollisionTest(map[string]string{"k8s.pod.name": "foo", "k8s_pod_name": "bar"}, nil)
	converter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())
	series, _, dropped, err := otelMetricsToSeriesAndMetadata(context.Background(), converter, md, conversionOptions{
		underscoreSanitization:      true,
		preserveMultipleUnderscores: true,
	}, log.NewNopLogger())
	require.NoError(t, err)
	require.Zero(t, dropped)
	require.Len(t, converter.TranslationWarnings(), 1)

	foundTargetInfo := false
	for _, ts := range series {
		labels := mimirpb.FromLabelAdaptersToLabels(ts.Labels)
		if labels.Get(model.MetricNameLabel) != "target_info" {
			continue
		}
		foundTargetInfo = true
		assert.Equal(t, "foo;bar", labels.Get("k8s_pod_name"))
	}
	require.True(t, foundTargetInfo, "expected a target_info series")
}

// TestOTLPMimirConverter_TranslationWarnings_histogramWarningsNotIncluded ensures
// that the translator's other annotations, e.g. histogram conversion warnings
// which embed data point values and thus have unbounded distinct-message
// cardinality, are not included in TranslationWarnings.
func TestOTLPMimirConverter_TranslationWarnings_histogramWarningsNotIncluded(t *testing.T) {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	attrs := rm.Resource().Attributes()
	attrs.PutStr("service.name", "svc")
	attrs.PutStr("service.instance.id", "inst")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test_histogram")
	eh := m.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := eh.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(100, 0)))
	// Zero count with a non-zero sum makes the translator record a histogram
	// conversion annotation.
	dp.SetCount(0)
	dp.SetSum(42)

	converter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())
	annots, err := converter.converter.FromMetrics(context.Background(), md, prometheusremotewrite.Settings{
		LabelNameUnderscoreSanitization:      true,
		LabelNamePreserveMultipleUnderscores: true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, annots, "expected the payload to produce a histogram translation annotation")

	// The histogram warning is excluded from the logged collision warnings, but
	// it is still counted by the metric under its own category.
	assert.Empty(t, converter.TranslationWarnings())
	require.Equal(t, map[prometheusremotewrite.WarningCategory]int{
		prometheusremotewrite.WarningCategoryHistogramZeroCountNonZeroSum: 1,
	}, prometheusremotewrite.CountWarningsByCategory(annots))
}

func TestOTLPHandler_TranslationWarningLogging(t *testing.T) {
	testLimits := &validation.Limits{
		NameValidationScheme:                     model.LegacyValidation,
		OTelMetricSuffixesEnabled:                boolPtr(false),
		OTelLabelNameUnderscoreSanitization:      true,
		OTelLabelNamePreserveMultipleUnderscores: true,
		OTelLogTranslationWarnings:               true,
	}
	limits := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{"test": testLimits}),
	)

	pusher := func(_ context.Context, pushReq *Request) error {
		t.Cleanup(pushReq.CleanUp)
		// Calling WriteRequest triggers the supplier, which is where the OTLP
		// request gets translated.
		_, err := pushReq.WriteRequest()
		return err
	}
	logs := &concurrency.SyncBuffer{}
	reg := prometheus.NewPedanticRegistry()
	pushMetrics := newPushMetrics(reg)
	handler := OTLPHandler(
		100000, nil, nil, false, limits, nil, nil, RetryConfig{}, nil,
		pusher, pushMetrics, reg, util_log.MakeLeveledLogger(logs, "info"),
	)

	sendRequest := func() {
		md := buildOTLPMetricsForCollisionTest(map[string]string{"k8s.pod.name": "foo", "k8s_pod_name": "bar"}, nil)
		exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
		body, err := exportReq.MarshalProto()
		require.NoError(t, err)
		req := createOTLPRequest(t, body, "", pbContentType)

		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code, "response body: %s", resp.Body.String())
	}

	sendRequest()

	const warningMsg = `msg="OTLP translation warning"`
	assert.Equal(t, 1, strings.Count(logs.String(), warningMsg))
	assert.Contains(t, logs.String(), "collide as label")
	assert.Contains(t, logs.String(), "k8s.pod.name")
	assert.Contains(t, logs.String(), "insight=true")

	// A second identical request gets its warning deduplicated away, while the
	// warnings counter keeps counting.
	sendRequest()
	assert.Equal(t, 1, strings.Count(logs.String(), warningMsg))

	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_distributor_otlp_translation_warnings_total The total number of distinct warnings produced while translating OTLP requests to Prometheus format, counted once per request they occur in, broken down by category.
		# TYPE cortex_distributor_otlp_translation_warnings_total counter
		cortex_distributor_otlp_translation_warnings_total{category="label_name_collision",user="test"} 2
	`), "cortex_distributor_otlp_translation_warnings_total"))
}

// TestOTLPHandler_TranslationWarningLogging_limitDisabled ensures that with the
// default-off limit nothing gets logged, while the warnings counter still counts.
func TestOTLPHandler_TranslationWarningLogging_limitDisabled(t *testing.T) {
	testLimits := &validation.Limits{
		NameValidationScheme:                     model.LegacyValidation,
		OTelMetricSuffixesEnabled:                boolPtr(false),
		OTelLabelNameUnderscoreSanitization:      true,
		OTelLabelNamePreserveMultipleUnderscores: true,
	}
	limits := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{"test": testLimits}),
	)

	pusher := func(_ context.Context, pushReq *Request) error {
		t.Cleanup(pushReq.CleanUp)
		_, err := pushReq.WriteRequest()
		return err
	}
	logs := &concurrency.SyncBuffer{}
	reg := prometheus.NewPedanticRegistry()
	pushMetrics := newPushMetrics(reg)
	handler := OTLPHandler(
		100000, nil, nil, false, limits, nil, nil, RetryConfig{}, nil,
		pusher, pushMetrics, reg, util_log.MakeLeveledLogger(logs, "info"),
	)

	md := buildOTLPMetricsForCollisionTest(map[string]string{"k8s.pod.name": "foo", "k8s_pod_name": "bar"}, nil)
	exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
	body, err := exportReq.MarshalProto()
	require.NoError(t, err)
	req := createOTLPRequest(t, body, "", pbContentType)

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code, "response body: %s", resp.Body.String())

	assert.NotContains(t, logs.String(), `msg="OTLP translation warning"`)
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_distributor_otlp_translation_warnings_total The total number of distinct warnings produced while translating OTLP requests to Prometheus format, counted once per request they occur in, broken down by category.
		# TYPE cortex_distributor_otlp_translation_warnings_total counter
		cortex_distributor_otlp_translation_warnings_total{category="label_name_collision",user="test"} 1
	`), "cortex_distributor_otlp_translation_warnings_total"))
}

// TestOTLPHandler_TranslationWarningMetric_histogramCategoryCountedNotLogged
// ensures histogram warnings are counted by the metric under their own category
// even though, unlike collisions, they are never logged.
func TestOTLPHandler_TranslationWarningMetric_histogramCategoryCountedNotLogged(t *testing.T) {
	testLimits := &validation.Limits{
		NameValidationScheme:                     model.LegacyValidation,
		OTelMetricSuffixesEnabled:                boolPtr(false),
		OTelLabelNameUnderscoreSanitization:      true,
		OTelLabelNamePreserveMultipleUnderscores: true,
		OTelLogTranslationWarnings:               true,
	}
	limits := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{"test": testLimits}),
	)

	pusher := func(_ context.Context, pushReq *Request) error {
		t.Cleanup(pushReq.CleanUp)
		_, err := pushReq.WriteRequest()
		return err
	}
	logs := &concurrency.SyncBuffer{}
	reg := prometheus.NewPedanticRegistry()
	pushMetrics := newPushMetrics(reg)
	handler := OTLPHandler(
		100000, nil, nil, false, limits, nil, nil, RetryConfig{}, nil,
		pusher, pushMetrics, reg, util_log.MakeLeveledLogger(logs, "info"),
	)

	// An exponential histogram data point with zero count but a non-zero sum
	// produces a histogram translation warning (no attribute collision).
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "svc")
	rm.Resource().Attributes().PutStr("service.instance.id", "inst")
	m := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test_histogram")
	eh := m.SetEmptyExponentialHistogram()
	eh.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := eh.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetCount(0)
	dp.SetSum(155)

	exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
	body, err := exportReq.MarshalProto()
	require.NoError(t, err)
	req := createOTLPRequest(t, body, "", pbContentType)
	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code, "response body: %s", resp.Body.String())

	// Counted by the metric under the histogram category ...
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_distributor_otlp_translation_warnings_total The total number of distinct warnings produced while translating OTLP requests to Prometheus format, counted once per request they occur in, broken down by category.
		# TYPE cortex_distributor_otlp_translation_warnings_total counter
		cortex_distributor_otlp_translation_warnings_total{category="histogram_zero_count_non_zero_sum",user="test"} 1
	`), "cortex_distributor_otlp_translation_warnings_total"))
	// ... but not logged, even though logging is enabled.
	assert.NotContains(t, logs.String(), `msg="OTLP translation warning"`)
}

// TestOTLPHandler_TranslationWarningLogging_truncatesLongWarnings ensures that
// warnings embedding attacker-controlled long attribute names get truncated
// before deduplication and logging.
func TestOTLPHandler_TranslationWarningLogging_truncatesLongWarnings(t *testing.T) {
	testLimits := &validation.Limits{
		NameValidationScheme:                     model.LegacyValidation,
		OTelMetricSuffixesEnabled:                boolPtr(false),
		OTelLabelNameUnderscoreSanitization:      true,
		OTelLabelNamePreserveMultipleUnderscores: true,
		OTelLogTranslationWarnings:               true,
	}
	limits := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{"test": testLimits}),
	)

	pusher := func(_ context.Context, pushReq *Request) error {
		t.Cleanup(pushReq.CleanUp)
		_, err := pushReq.WriteRequest()
		return err
	}
	logs := &concurrency.SyncBuffer{}
	handler := OTLPHandler(
		100000, nil, nil, false, limits, nil, nil, RetryConfig{}, nil,
		pusher, nil, nil, util_log.MakeLeveledLogger(logs, "info"),
	)

	longName := strings.Repeat("a", 1100)
	md := buildOTLPMetricsForCollisionTest(map[string]string{longName + ".x": "foo", longName + "_x": "bar"}, nil)
	exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
	body, err := exportReq.MarshalProto()
	require.NoError(t, err)
	req := createOTLPRequest(t, body, "", pbContentType)

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)
	require.Equal(t, http.StatusOK, resp.Code, "response body: %s", resp.Body.String())

	require.Equal(t, 1, strings.Count(logs.String(), `msg="OTLP translation warning"`))
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		assert.LessOrEqual(t, len(line), 1500, "log lines must be bounded despite long attribute names: %s", line[:100])
	}
}

func TestOTLPHandler_TranslationWarningLogging_perRequestCap(t *testing.T) {
	testLimits := &validation.Limits{
		NameValidationScheme:                     model.LegacyValidation,
		OTelMetricSuffixesEnabled:                boolPtr(false),
		OTelLabelNameUnderscoreSanitization:      true,
		OTelLabelNamePreserveMultipleUnderscores: true,
		OTelLogTranslationWarnings:               true,
	}
	limits := validation.NewOverrides(
		validation.Limits{},
		validation.NewMockTenantLimits(map[string]*validation.Limits{"test": testLimits}),
	)

	pusher := func(_ context.Context, pushReq *Request) error {
		t.Cleanup(pushReq.CleanUp)
		_, err := pushReq.WriteRequest()
		return err
	}
	logs := &concurrency.SyncBuffer{}
	handler := OTLPHandler(
		100000, nil, nil, false, limits, nil, nil, RetryConfig{}, nil,
		pusher, nil, nil, util_log.MakeLeveledLogger(logs, "info"),
	)

	// Two more colliding attribute pairs than the per-request log cap.
	collisions := maxLoggedOTelTranslationWarningsPerRequest + 2
	resourceAttrs := map[string]string{}
	for i := 0; i < collisions; i++ {
		resourceAttrs[fmt.Sprintf("attr.%d", i)] = "x"
		resourceAttrs[fmt.Sprintf("attr_%d", i)] = "y"
	}

	sendRequest := func() {
		md := buildOTLPMetricsForCollisionTest(resourceAttrs, nil)
		exportReq := pmetricotlp.NewExportRequestFromMetrics(md)
		body, err := exportReq.MarshalProto()
		require.NoError(t, err)
		req := createOTLPRequest(t, body, "", pbContentType)

		resp := httptest.NewRecorder()
		handler.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code, "response body: %s", resp.Body.String())
	}

	const warningMsg = `msg="OTLP translation warning"`
	const summaryMsg = `msg="additional OTLP translation warnings were suppressed"`

	sendRequest()
	assert.Equal(t, maxLoggedOTelTranslationWarningsPerRequest, strings.Count(logs.String(), warningMsg))
	assert.Equal(t, 1, strings.Count(logs.String(), summaryMsg))
	assert.Contains(t, logs.String(), "suppressed=2")

	// On the second request the capped-out warnings get logged, since they were
	// not marked as seen; nothing is suppressed, and the summary is deduplicated
	// anyway.
	sendRequest()
	assert.Equal(t, collisions, strings.Count(logs.String(), warningMsg))
	assert.Equal(t, 1, strings.Count(logs.String(), summaryMsg))
}

// buildOTLPMetricsManySeries returns a payload with one resource and the given
// number of gauge series, each carrying the given data point attributes.
func buildOTLPMetricsManySeries(series int, dataPointAttrs map[string]string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	attrs := rm.Resource().Attributes()
	attrs.PutStr("service.name", "svc")
	attrs.PutStr("service.instance.id", "inst")
	attrs.PutStr("k8s.pod.name", "pod")
	sm := rm.ScopeMetrics().AppendEmpty()
	for i := 0; i < series; i++ {
		m := sm.Metrics().AppendEmpty()
		m.SetName(fmt.Sprintf("test_metric_%d", i))
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetDoubleValue(1)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(100, 0)))
		for k, v := range dataPointAttrs {
			dp.Attributes().PutStr(k, v)
		}
	}
	return md
}

func BenchmarkOTLPMimirConverter_TranslationWarnings(b *testing.B) {
	for name, md := range map[string]pmetric.Metrics{
		"no collisions": buildOTLPMetricsForCollisionTest(
			map[string]string{"k8s.pod.name": "foo", "k8s.namespace.name": "bar"}, nil),
		"resource attribute collision": buildOTLPMetricsForCollisionTest(
			map[string]string{"k8s.pod.name": "foo", "k8s_pod_name": "bar"}, nil),
		"500 series without collisions": buildOTLPMetricsManySeries(
			500, map[string]string{"http.method": "GET", "other.attr": "x"}),
		"500 series with colliding data point attributes": buildOTLPMetricsManySeries(
			500, map[string]string{"http.method": "GET", "http_method": "POST"}),
	} {
		b.Run(name, func(b *testing.B) {
			opts := conversionOptions{
				underscoreSanitization:      true,
				preserveMultipleUnderscores: true,
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				converter := newOTLPMimirConverter(otlpappender.NewCombinedAppender())
				_, _, _, err := otelMetricsToSeriesAndMetadata(context.Background(), converter, md, opts, log.NewNopLogger())
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
