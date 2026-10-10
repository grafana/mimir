// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_grafana_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
	"github.com/grafana/mimir/pkg/mimirtool/util"
)

var dashboardMetrics = []string{
	"apiserver_request:availability30d",
	"apiserver_request_total",
	"bar_chart_metric",
	"candlestick_metric",
	"canvas_metric",
	"cluster_quantile:apiserver_request_duration_seconds:histogram_quantile",
	"code_resource:apiserver_request_total:rate5m",
	"flame_graph_metric",
	"geomap_metric",
	"go_goroutines",
	"histogram_metric",
	"kube_pod_info",
	"my_lovely_metric",
	"node_graph_metric",
	"pie_chart_metric",
	"polystat_panel_metric",
	"process_cpu_seconds_total",
	"process_resident_memory_bytes",
	"state_timeline_metric",
	"status_history_metric",
	"trend_metric",
	"workqueue_adds_total",
	"workqueue_depth",
	"workqueue_queue_duration_seconds_bucket",
	"xy_chart_metric",
}

var expectedParseErrors = []string{
	"unsupported panel type: \"text\"",
}

func TestParseMetricsInBoard(t *testing.T) {
	var board minisdk.Board
	output := &analyze.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	buf, err := loadFile("testdata/apiserver.json")
	require.NoError(t, err)

	err = json.Unmarshal(buf, &board)
	require.NoError(t, err)

	analyze.ParseMetricsInBoard(output, board, util.CreatePromQLParser(false), log.NewNopLogger())
	assert.Equal(t, dashboardMetrics, output.Dashboards[0].Metrics)
	assert.Equal(t, expectedParseErrors, output.Dashboards[0].ParseErrors)
}

func TestAnalyzeGrafana_ResolvesLibraryPanels(t *testing.T) {
	libraryElementRequests := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/search":
			require.Equal(t, http.MethodGet, r.Method)
			if r.URL.Query().Get("page") == "1" {
				_, err := w.Write([]byte(`[{"uid":"library-dashboard","title":"Library Dashboard"}]`))
				require.NoError(t, err)
				return
			}
			_, err := w.Write([]byte(`[]`))
			require.NoError(t, err)
		case "/api/dashboards/uid/library-dashboard":
			require.Equal(t, http.MethodGet, r.Method)
			_, err := w.Write([]byte(`{
				"dashboard": {
					"uid": "library-dashboard",
					"title": "Library Dashboard",
					"panels": [
						{
							"title": "CPU",
							"libraryPanel": {
								"name": "CPU",
								"uid": "lib-cpu"
							}
						},
						{
							"title": "CPU copy",
							"libraryPanel": {
								"name": "CPU",
								"uid": "lib-cpu"
							}
						}
					]
				}
			}`))
			require.NoError(t, err)
		case "/api/library-elements/lib-cpu":
			require.Equal(t, http.MethodGet, r.Method)
			libraryElementRequests++
			_, err := w.Write([]byte(`{
				"result": {
					"uid": "lib-cpu",
					"kind": 1,
					"model": {
						"type": "timeseries",
						"targets": [
							{"expr": "node_cpu_seconds_total"}
						]
					}
				}
			}`))
			require.NoError(t, err)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c, err := minisdk.NewClient(ts.URL, "", ts.Client(), "")
	require.NoError(t, err)

	output, err := AnalyzeGrafana(context.Background(), c, nil, time.Second, util.CreatePromQLParser(false), log.NewNopLogger())
	require.NoError(t, err)

	assert.Equal(t, 1, libraryElementRequests)
	assert.Equal(t, model.LabelValues{"node_cpu_seconds_total"}, output.MetricsUsed)
	require.Len(t, output.Dashboards, 1)
	assert.Equal(t, []string{"node_cpu_seconds_total"}, output.Dashboards[0].Metrics)
	assert.Empty(t, output.Dashboards[0].ParseErrors)
}

func TestAnalyzeGrafana_RecordsLibraryPanelFetchErrors(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/search":
			require.Equal(t, http.MethodGet, r.Method)
			if r.URL.Query().Get("page") == "1" {
				_, err := w.Write([]byte(`[{"uid":"library-dashboard","title":"Library Dashboard"}]`))
				require.NoError(t, err)
				return
			}
			_, err := w.Write([]byte(`[]`))
			require.NoError(t, err)
		case "/api/dashboards/uid/library-dashboard":
			require.Equal(t, http.MethodGet, r.Method)
			_, err := w.Write([]byte(`{
				"dashboard": {
					"uid": "library-dashboard",
					"title": "Library Dashboard",
					"panels": [
						{
							"title": "Missing",
							"libraryPanel": {
								"name": "Missing",
								"uid": "missing-lib"
							}
						}
					]
				}
			}`))
			require.NoError(t, err)
		case "/api/library-elements/missing-lib":
			require.Equal(t, http.MethodGet, r.Method)
			http.Error(w, "not found", http.StatusNotFound)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	c, err := minisdk.NewClient(ts.URL, "", ts.Client(), "")
	require.NoError(t, err)

	output, err := AnalyzeGrafana(context.Background(), c, nil, time.Second, util.CreatePromQLParser(false), log.NewNopLogger())
	require.NoError(t, err)

	require.Len(t, output.Dashboards, 1)
	assert.Empty(t, output.Dashboards[0].Metrics)
	require.Len(t, output.Dashboards[0].ParseErrors, 1)
	assert.Contains(t, output.Dashboards[0].ParseErrors[0], `library panel "Missing" (missing-lib): HTTP error 404`)
}

func BenchmarkParseMetricsInBoard(b *testing.B) {
	var board minisdk.Board
	output := &analyze.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	buf, err := loadFile("testdata/apiserver.json")
	if err != nil {
		b.FailNow()
	}

	err = json.Unmarshal(buf, &board)
	if err != nil {
		b.FailNow()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		analyze.ParseMetricsInBoard(output, board, util.CreatePromQLParser(false), log.NewNopLogger())
	}
}
