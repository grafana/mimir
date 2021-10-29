package commands

import (
	"encoding/json"
	"testing"

	"github.com/grafana-tools/sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/cortex-tools/pkg/analyse"
)

var dashboardMetrics = []string{
	"apiserver_request:availability30d",
	"apiserver_request_total",
	"cluster_quantile:apiserver_request_duration_seconds:histogram_quantile",
	"code_resource:apiserver_request_total:rate5m",
	"go_goroutines",
	"process_cpu_seconds_total",
	"process_resident_memory_bytes",
	"workqueue_adds_total",
	"workqueue_depth",
	"workqueue_queue_duration_seconds_bucket",
}

func TestParseMetricsInBoard(t *testing.T) {
	var board sdk.Board
	output := &analyse.MetricsInGrafana{}
	output.OverallMetrics = make(map[string]struct{})

	buf, err := loadFile("testdata/apiserver.json")
	require.NoError(t, err)

	err = json.Unmarshal(buf, &board)
	require.NoError(t, err)

	analyse.ParseMetricsInBoard(output, board)
	assert.Equal(t, dashboardMetrics, output.Dashboards[0].Metrics)
}
