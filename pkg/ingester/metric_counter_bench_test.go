// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import "testing"

func metricCounterGoldenMetricNames() []string {
	return []string{
		"up",
		"scrape_duration_seconds",
		"http_requests_total",
		`grpc_server_handled_total{method="GET"}`,
		"process_cpu_seconds_total",
		"cortex_ingester_memory_series",
		"cortex_ingester_memory_series_created_total",
		"prometheus_remote_storage_samples_pending",
		"node_cpu_seconds_total",
		"apiserver_request_total",
		"container_cpu_usage_seconds_total",
		"go_goroutines",
	}
}

func setupBenchmarkMetricCounter() *metricCounter {
	m := newMetricCounter(nil, nil)
	for _, name := range metricCounterGoldenMetricNames() {
		shard := m.getShard(name)
		shard.mtx.Lock()
		shard.m[name] = 1_000_000_000
		shard.mtx.Unlock()
	}
	return m
}

func BenchmarkDecreaseSeriesForMetric(b *testing.B) {
	m := setupBenchmarkMetricCounter()
	metricNames := metricCounterGoldenMetricNames()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		for _, metricName := range metricNames {
			m.decreaseSeriesForMetric(metricName)
		}
	}
}
