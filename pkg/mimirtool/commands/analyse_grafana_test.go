// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_grafana_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/minisdk"
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

	analyze.ParseMetricsInBoard(output, board)
	assert.Equal(t, dashboardMetrics, output.Dashboards[0].Metrics)
	assert.Equal(t, expectedParseErrors, output.Dashboards[0].ParseErrors)
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
		analyze.ParseMetricsInBoard(output, board)
	}
}
