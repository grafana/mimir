// SPDX-License-Identifier: AGPL-3.0-only

package metrics

import (
	"bytes"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/limits"
)

func encodeText(t *testing.T, registry *prometheus.Registry) string {
	t.Helper()
	families, err := registry.Gather()
	require.NoError(t, err)
	var buffer bytes.Buffer
	encoder := expfmt.NewEncoder(&buffer, expfmt.NewFormat(expfmt.TypeTextPlain))
	for _, family := range families {
		require.NoError(t, encoder.Encode(family))
	}
	return buffer.String()
}

func TestExportsActiveSeriesWithGoNamesAndLabels(t *testing.T) {
	report := ActiveSeriesReport{
		Tenant:                       "export-test",
		Series:                       5,
		Active:                       4,
		ActiveNativeHistograms:       1,
		ActiveNativeHistogramBuckets: 3,
		CustomTrackers:               []TrackerCounts{{Name: "api", Counts: [3]uint64{2, 0, 0}}, {Name: "idle"}},
		CostAttribution: []AttributedSeries{
			{
				Tracker:      "source-reservation",
				Internal:     true,
				OutputLabels: []string{"source", "reservation_id"},
				Values:       []AttributedValue{{Values: []string{"k6", "__missing__"}, Counts: [3]uint64{4, 1, 3}}},
				Cardinality:  1,
			},
			{
				Tracker:      "by-team",
				OutputLabels: []string{"team"},
				Values:       []AttributedValue{{Values: []string{"a"}, Counts: [3]uint64{4, 0, 0}}},
				Cardinality:  1,
			},
		},
		Metadata:         2,
		Exemplars:        7,
		ExemplarSeries:   3,
		OldestExemplarMs: 12_500,
		HasOldest:        true,
	}
	ExportActiveSeries([]ActiveSeriesReport{report})
	main := encodeText(t, Registry)
	for _, line := range []string{
		`cortex_ingester_active_series{user="export-test"} 4`,
		`cortex_ingester_active_series_custom_tracker{name="api",user="export-test"} 2`,
		`cortex_ingester_active_native_histogram_buckets{user="export-test"} 3`,
		"cortex_ingester_tsdb_exemplar_exemplars_in_storage 7",
		`cortex_ingester_tsdb_exemplar_series_with_exemplars_in_storage{user="export-test"} 3`,
		`cortex_ingester_tsdb_exemplar_last_exemplars_timestamp_seconds{user="export-test"} 12.5`,
		`cortex_cost_attribution_active_series_tracker_cardinality{tracker="by-team",user="export-test"} 1`,
		`cortex_ingester_attributed_active_series{reservation_id="__missing__",source="k6",tenant="export-test",tracker="source-reservation"} 4`,
		`cortex_attributed_series_overflow_labels{reservation_id="__overflow__",source="__overflow__",tenant="export-test",tracker="source-reservation"} 1`,
	} {
		require.Contains(t, main, line)
	}
	require.NotContains(t, main, `name="idle"`)
	// Non-internal trackers only go to the cost attribution registry.
	require.NotContains(t, main, `cortex_ingester_attributed_active_series{team="a"`)
	require.Contains(t, encodeText(t, UsageRegistry),
		`cortex_ingester_attributed_active_series{team="a",tenant="export-test",tracker="by-team"} 4`)
	// The next update replaces the previous one.
	ExportActiveSeries(nil)
	require.NotContains(t, encodeText(t, Registry), "export-test")
}

func TestKeepsSidecarFamiliesExceptProcessOnes(t *testing.T) {
	text := "# HELP process_cpu_seconds_total x\n# TYPE process_cpu_seconds_total counter\nprocess_cpu_seconds_total 1\n# HELP rust_ingester_ring_ready y\nrust_ingester_ring_ready 1\nmemberlist_client_cluster_members_count 3\ngo_goroutines 12\n"
	families, err := decodeFamilies(strings.NewReader(text), expfmt.NewFormat(expfmt.TypeTextPlain))
	require.NoError(t, err)
	var names []string
	for _, family := range sidecarFamilies(families, map[string]bool{"go_goroutines": true}) {
		names = append(names, family.GetName())
	}
	require.ElementsMatch(t, []string{"rust_ingester_ring_ready", "memberlist_client_cluster_members_count"}, names)
}

func TestIngestionRateIsAnEwmaOfSamplesPerSecond(t *testing.T) {
	var rate IngestionRate
	// The first tick counts everything since startup, like Go's first `Tick`.
	require.Equal(t, 1_000.0, rate.Tick(1_000))
	require.Equal(t, 900.0, rate.Tick(1_500))
	require.InDelta(t, 720.0, rate.Tick(1_500), 1e-9)
}

func TestExportHeadCountsRemovedChunksAndTheHeadRange(t *testing.T) {
	HeadChunksCreated.WithLabelValues("head-test").Add(10)
	ExportHead([]HeadReport{{Tenant: "head-test", MemorySeries: 2, HeadChunks: 4, HeadMinTime: 1_000, HeadMaxTime: 9_000}},
		[]LocalSeriesLimit{{Tenant: "head-test", Limit: 50}}, limitsFor(t), true)
	text := encodeText(t, Registry)
	require.Contains(t, text, `cortex_ingester_tsdb_head_chunks_removed_total{user="head-test"} 6`)
	require.Contains(t, text, `cortex_ingester_local_limits{limit="max_global_series_per_user",user="head-test"} 50`)
	require.Contains(t, text, "cortex_ingester_tsdb_head_min_timestamp_seconds 1")
	require.Contains(t, text, `cortex_ingester_instance_limits{limit="max_inflight_push_requests"} 30000`)
	// Removed chunks never decrease, even when the head holds more chunks again.
	ExportHead([]HeadReport{{Tenant: "head-test", HeadChunks: 9}}, nil, limitsFor(t), false)
	require.Contains(t, encodeText(t, Registry), `cortex_ingester_tsdb_head_chunks_removed_total{user="head-test"} 6`)
	require.Contains(t, encodeText(t, Registry), "cortex_ingester_tsdb_head_min_timestamp_seconds 0")
}

func TestServesProtobufWithNativeBucketsWhenAsked(t *testing.T) {
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("# TYPE rust_ingester_ring_ready gauge\nrust_ingester_ring_ready 1\n# TYPE process_open_fds gauge\nprocess_open_fds 3\n"))
	}))
	defer sidecar.Close()
	route := "/cortex.Ingester/ExpositionTest"
	for _, seconds := range []float64{0.0004, 0.0011, 0.003} {
		RequestDuration.WithLabelValues("gRPC", route, "OK", "false").Observe(seconds)
	}
	server := httptest.NewServer(Handler(sidecar.URL))
	defer server.Close()

	get := func(accept string) (*http.Response, []*dto.MetricFamily) {
		request, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		if accept != "" {
			request.Header.Set("Accept", accept)
		}
		response, err := http.DefaultClient.Do(request)
		require.NoError(t, err)
		defer response.Body.Close()
		families, err := decodeFamilies(response.Body, expfmt.ResponseFormat(response.Header))
		require.NoError(t, err)
		return response, families
	}
	find := func(families []*dto.MetricFamily, name string) *dto.MetricFamily {
		for _, family := range families {
			if family.GetName() == name {
				return family
			}
		}
		return nil
	}

	// Like Prometheus's negotiation: text without an Accept header, protobuf when asked.
	response, families := get("")
	require.True(t, strings.HasPrefix(response.Header.Get("Content-Type"), "text/plain"))
	require.NotNil(t, find(families, "rust_ingester_ring_ready"), "sidecar families are appended")
	require.Nil(t, find(families, "process_open_fds"), "the sidecar's process families are dropped")

	response, families = get("application/vnd.google.protobuf;proto=io.prometheus.client.MetricFamily;encoding=delimited;q=0.7,text/plain;version=0.0.4;q=0.3")
	require.True(t, strings.HasPrefix(response.Header.Get("Content-Type"), "application/vnd.google.protobuf"))
	require.NotNil(t, find(families, "rust_ingester_ring_ready"))
	family := find(families, "cortex_request_duration_seconds")
	require.NotNil(t, family)
	var histogram *dto.Histogram
	for _, metric := range family.GetMetric() {
		for _, label := range metric.GetLabel() {
			if label.GetName() == "route" && label.GetValue() == route {
				histogram = metric.GetHistogram()
			}
		}
	}
	require.NotNil(t, histogram)
	require.Equal(t, uint64(3), histogram.GetSampleCount())
	// A factor of 1.1 is schema 3, with buckets below the first classic one, 5 ms.
	require.Equal(t, int32(3), histogram.GetSchema())
	require.NotEmpty(t, histogram.GetPositiveSpan())
	require.Len(t, histogram.GetBucket(), 14, "classic buckets stay exact alongside")
	require.Equal(t, uint64(3), histogram.GetBucket()[0].GetCumulativeCount())
	require.InDelta(t, math.Pow(2, -128), histogram.GetZeroThreshold(), 1e-45)
}

func limitsFor(t *testing.T) limits.InstanceLimits {
	t.Helper()
	return limits.DefaultInstanceLimits()
}
