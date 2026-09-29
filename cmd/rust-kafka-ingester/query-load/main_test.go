// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestSampleMetricNamesKeepsMetricsWithinTheSeriesBounds(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/prometheus/api/v1/cardinality/label_values", r.URL.Path)
		require.Equal(t, "tenant", r.Header.Get("X-Scope-OrgID"))
		require.Equal(t, "500", r.URL.Query().Get("limit"), "Mimir rejects larger limits")
		_, _ = w.Write([]byte(`{"labels":[{"label_name":"__name__","cardinality":[
			{"label_value":"huge","series_count":5000000},
			{"label_value":"large","series_count":150000},
			{"label_value":"medium","series_count":2000},
			{"label_value":"tiny","series_count":10}]}]}`))
	}))
	defer server.Close()
	cfg := config{address: server.URL + "/prometheus", metricsSample: 10, minSeries: 1000, maxSeries: 200_000}
	names, err := sampleMetricNames(context.Background(), server.Client(), cfg, "tenant")
	require.NoError(t, err)
	slices.SortFunc(names, func(a, b sampled) int { return strings.Compare(a.name, b.name) })
	require.Equal(t, []sampled{{"large", 150000}, {"medium", 2000}}, names)
}

func TestHistogramsAreToldFromOtherMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/prometheus/api/v1/metadata", r.URL.Path)
		_, _ = w.Write([]byte(`{"status":"success","data":{
			"http_duration_seconds":[{"type":"histogram"}],
			"rpc_duration_seconds":[{"type":"histogram"}],
			"requests_total":[{"type":"counter"}]}}`))
	}))
	defer server.Close()
	cfg := config{address: server.URL + "/prometheus"}
	histograms, err := histogramFamilies(context.Background(), server.Client(), cfg, "tenant")
	require.NoError(t, err)
	require.Equal(t, []metric{
		{name: "http_duration_seconds", kind: classicHistogram},
		{name: "rpc_duration_seconds", kind: nativeHistogram},
		{name: "requests_total"},
		// Not a histogram family's buckets.
		{name: "queue_bucket"},
	}, classify([]sampled{
		{"http_duration_seconds_bucket", 5000},
		{"rpc_duration_seconds", 5000},
		{"requests_total", 5000},
		{"queue_bucket", 5000},
	}, histograms, 10_000))
	// Histograms too large to query are left out; other metrics aren't.
	require.Equal(t, []metric{{name: "requests_total"}}, classify([]sampled{
		{"http_duration_seconds_bucket", 20_000},
		{"rpc_duration_seconds", 20_000},
		{"requests_total", 20_000},
	}, histograms, 10_000))
}

func TestAggregatedMetricsAreNoLongerPicked(t *testing.T) {
	sampled := newMetrics()
	sampled.set("tenant", []metric{{name: "aggregated"}, {name: "raw"}})
	sampled.remove("tenant", "aggregated")
	for range 20 {
		m, ok := sampled.pick("tenant")
		require.True(t, ok)
		require.Equal(t, "raw", m.name)
	}
}

func TestQueriesParseAggregateAndStayWithinTheRange(t *testing.T) {
	cfg := config{maxRange: 6 * time.Hour, points: 60}
	now := time.Now()
	promql := parser.NewParser(parser.Options{})
	jobs := []string{"api", "dev/web.1", "a|b"}
	kinds := map[string]bool{}
	for _, m := range []metric{
		{name: "requests_total"},
		{name: "http_duration_seconds", kind: classicHistogram},
		{name: "rpc_duration_seconds", kind: nativeHistogram},
	} {
		for _, jobs := range [][]string{nil, jobs} {
			for range 300 {
				kind, _, params := nextQuery(cfg, m, jobs, []string{"up", "a.b"}, now)
				kinds[kind] = true
				if start := params.Get("start"); start != "" {
					seconds, err := strconv.ParseFloat(start, 64)
					require.NoError(t, err)
					require.LessOrEqual(t, now.Sub(time.UnixMilli(int64(seconds*1000))), cfg.maxRange+time.Second)
				}
				query := params.Get("query")
				if query == "" {
					continue
				}
				expression, err := promql.ParseExpr(query)
				require.NoError(t, err, query)
				// Queriers answer with a few series; ingesters still read every chunk.
				switch e := expression.(type) {
				case *parser.AggregateExpr:
				case *parser.Call:
					require.True(t, strings.HasPrefix(e.Func.Name, "histogram_"), query)
				default:
					require.Failf(t, "not aggregated", "%T %s", expression, query)
				}
			}
		}
	}
	require.Equal(t, map[string]bool{"range": true, "instant": true, "labels": true, "series": true, "label_values": true}, kinds)
}

func TestRegexesMatchTheNamesTheyAlternateLiterally(t *testing.T) {
	queries := regexQueries("requests_total", []string{"a|b"}, []string{"up", "a.b"})
	require.Contains(t, queries, `sum by (job) (rate({__name__="requests_total", job=~"a\\|b"}[5m]))`)
	require.Contains(t, queries, `sum(rate({__name__="requests_total", job!~"a\\|b"}[5m]))`)
	// Only the sampled metrics, never a whole family by prefix.
	require.Contains(t, queries, `count by (__name__) ({__name__=~"requests_total|up|a\\.b"})`)
}
