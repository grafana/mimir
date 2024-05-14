// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/commands/analyse_rules_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package commands

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"

	"github.com/grafana/mimir/pkg/mimirtool/analyze"
	"github.com/grafana/mimir/pkg/mimirtool/rules"
)

var metricsInRuleGroup = []string{
	"apiserver_request_duration_seconds_bucket",
	"apiserver_request_duration_seconds_count",
	"apiserver_request_total",
}
var allMetricsInRuleTest = []string{
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

func TestParseMetricsInRuleFile(t *testing.T) {
	output := &analyze.MetricsInRuler{}
	output.OverallMetrics = make(map[string]struct{})

	nss, err := rules.ParseFiles("mimir", []string{"testdata/prometheus_rules.yaml"})
	require.NoError(t, err)

	for _, ns := range nss {
		for _, group := range ns.Groups {
			err := analyze.ParseMetricsInRuleGroup(output, group, ns.Namespace)
			require.NoError(t, err)
		}
	}
	// Check first RG
	assert.Equal(t, metricsInRuleGroup, output.RuleGroups[0].Metrics)

	// Check all metrics
	var metricsUsed []string
	for metric := range output.OverallMetrics {
		metricsUsed = append(metricsUsed, metric)
	}
	slices.Sort(metricsUsed)
	assert.Equal(t, allMetricsInRuleTest, metricsUsed)
}
