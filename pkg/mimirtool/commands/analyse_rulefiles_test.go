// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"testing"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestAnalyzeRuleFiles(t *testing.T) {
	mir, err := AnalyzeRuleFiles([]string{"testdata/prometheus_rules.yaml"})
	require.NoError(t, err)
	require.Equal(t, 28, len(mir.MetricsUsed))
	expectedMetrics := model.LabelValues{
		"apiserver_request_duration_seconds_bucket",
		"apiserver_request_duration_seconds_count",
		"apiserver_request_total",
		"code:apiserver_request_total:increase30d",
		"code_verb:apiserver_request_total:increase1h",
		"code_verb:apiserver_request_total:increase30d",
		"container_memory_cache",
		"container_memory_rss",
		"container_memory_swap",
		"container_memory_working_set_bytes",
		"kube_pod_container_resource_limits",
		"kube_pod_container_resource_requests",
		"kube_pod_info",
		"kube_pod_owner",
		"kube_pod_status_phase",
		"kube_replicaset_owner",
		"kubelet_node_name",
		"kubelet_pleg_relist_duration_seconds_bucket",
		"node_cpu_seconds_total",
		"node_memory_Buffers_bytes",
		"node_memory_Cached_bytes",
		"node_memory_MemAvailable_bytes",
		"node_memory_MemFree_bytes",
		"node_memory_Slab_bytes",
		"node_namespace_pod:kube_pod_info:",
		"scheduler_binding_duration_seconds_bucket",
		"scheduler_e2e_scheduling_duration_seconds_bucket",
		"scheduler_scheduling_algorithm_duration_seconds_bucket",
	}
	require.Equal(t, expectedMetrics, mir.MetricsUsed)
}
