// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"hash/fnv"
	"strings"
	"testing"
	"unsafe"
)

type metricCounterGoldenValue struct {
	name   string // subtest name; defaults to metric when empty
	metric string
	hash   uint64
}

func (tc metricCounterGoldenValue) testName() string {
	if tc.name != "" {
		return tc.name
	}
	return tc.metric
}

func metricCounterGoldenValues() []metricCounterGoldenValue {
	veryLongName := strings.Repeat("a", 256)

	return []metricCounterGoldenValue{
		// Typical Prometheus metric names.
		{metric: "up", hash: 0x8c43a07b566d980},
		{metric: "scrape_duration_seconds", hash: 0xb4687d91a4f00bc4},
		{metric: "http_requests_total", hash: 0xc7040274bc164605},
		{metric: `grpc_server_handled_total{method="GET"}`, hash: 0xcb58c44c07c09aa9},
		{metric: "process_cpu_seconds_total", hash: 0xb429e60330518a04},
		{metric: "cortex_ingester_memory_series", hash: 0x85b31538eaacf650},
		{metric: "cortex_ingester_memory_series_created_total", hash: 0xf7ff4f82a5aaa376},
		{metric: "prometheus_remote_storage_samples_pending", hash: 0xa2f62220fa4e000a},
		{metric: "node_cpu_seconds_total", hash: 0xd9b6ef551aee7467},
		{metric: "apiserver_request_total", hash: 0x9c141e50727df7f3},
		{metric: "container_cpu_usage_seconds_total", hash: 0x9d3538cd55ce3b26},
		{metric: "go_goroutines", hash: 0xceafedd1f6ae9b93},

		// Edge and error-ish cases that should still hash deterministically.
		{name: "empty string", metric: "", hash: 0xcbf29ce484222325},
		{name: "whitespace only", metric: "   ", hash: 0xc3b0d217ceb2bed7},
		{name: "single character", metric: "a", hash: 0xaf63dc4c8601ec8c},
		{name: "seven characters", metric: "abcdefg", hash: 0x406e475017aa7737},
		{name: "eight characters", metric: "abcdefgh", hash: 0x25da8c1836a8d66d},
		{name: "nine characters", metric: "abcdefghi", hash: 0xfb321124e0e3a8cc},
		{name: "legacy colon metric", metric: ":node_cpu:cpu0", hash: 0x2e3bd13ac6439760},
		{name: "unicode metric name", metric: "metric_名前_total", hash: 0x63b9d40005d0d0ea},
		{name: "name with newline", metric: "broken\nmetric", hash: 0x9f684fdfef61dba},
		{name: "name with high bytes", metric: "metric\xff\xfe", hash: 0x313b68e012fa048c},
		{name: "very long name", metric: veryLongName, hash: 0xfd2916200943d825},
	}
}

func TestMetricCounterHashGoldenValues(t *testing.T) {
	t.Parallel()

	for _, tc := range metricCounterGoldenValues() {
		t.Run(tc.testName(), func(t *testing.T) {
			t.Parallel()

			if got, want := hashMetricName(tc.metric), tc.hash; got != want {
				t.Fatalf("hashMetricName() = %#x, want %#x", got, want)
			}
		})
	}
}

func TestMetricCounterHashMatchesStdlibFNV1a64(t *testing.T) {
	t.Parallel()

	for _, tc := range metricCounterGoldenValues() {
		t.Run(tc.testName(), func(t *testing.T) {
			t.Parallel()

			h := fnv.New64a()
			_, err := h.Write(unsafe.Slice(unsafe.StringData(tc.metric), len(tc.metric)))
			if err != nil {
				t.Fatalf("Write() failed: %v", err)
			}

			if got, want := hashMetricName(tc.metric), h.Sum64(); got != want {
				t.Fatalf("hashMetricName() = %#x, want %#x", got, want)
			}
		})
	}
}
