// SPDX-License-Identifier: AGPL-3.0-only

package test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

var BucketPromToBenchmarkMetrics = map[string]string{
	// Metrics from Thanos objstore.WrapWithMetrics;
	// Thanos objstore no longer includes a "thanos_" prefix by default,
	// but we apply it everywhere with prometheus.WrapRegistererWithPrefix("thanos_", <registry>);
	// The same must be done in tests to record these as expected.
	"thanos_objstore_bucket_operations_total":              "%s-ops",
	"thanos_objstore_bucket_operation_fetched_bytes_total": "%s-bytes",

	// Metrics from Mimir's bucketcache.CachingBucket;
	// will only be recorded if caching is enabled for the corresponding operation
	// via bucketcache.CachingBucketConfig's CacheGet, CacheGetRange, etc.
	"thanos_store_bucket_cache_operation_requests_total":       "cache/%s-ops",
	"thanos_store_bucket_cache_operation_hits_total":           "cache/%s-hits",
	"thanos_store_bucket_cache_getrange_requested_bytes_total": "cache/%s-bytes-requested",
	"thanos_store_bucket_cache_getrange_fetched_bytes_total":   "cache/%s-bytes-fetched",
	"thanos_store_bucket_cache_getrange_refetched_bytes_total": "cache/%s-bytes-refetched",
}

func ReportBucketMetrics(b *testing.B, metrics map[string]float64) {
	keys := make([]string, 0, len(metrics))
	for k := range metrics {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		b.ReportMetric(metrics[k], k)
	}
}

func RecordBucketMetricsDiff(
	tb testing.TB, bucketReg *prometheus.Registry, ops []string, baseline map[string]float64,
) map[string]float64 {
	currBenchMetrics := RecordBucketMetrics(tb, bucketReg, ops)
	benchMetricsDiff := make(map[string]float64)

	for metric, value := range currBenchMetrics {
		oldValue := baseline[metric]
		benchMetricsDiff[metric] = value - oldValue
	}
	return benchMetricsDiff
}

func RecordBucketMetrics(
	tb testing.TB, bucketReg *prometheus.Registry, ops []string,
) map[string]float64 {
	promMetrics, err := bucketReg.Gather()
	require.NoError(tb, err)
	benchMetrics := make(map[string]float64)

MetricFamilyLoop:
	for _, mf := range promMetrics {
	MetricLoop:
		for _, m := range mf.GetMetric() {
			name := mf.GetName()

			benchMetricNameTmpl, ok := BucketPromToBenchmarkMetrics[name]
			if !ok {
				continue MetricFamilyLoop
			}

			for _, l := range m.GetLabel() {
				lName, lValue := l.GetName(), l.GetValue()
				if lName == "operation" {
					if !slices.Contains(ops, lValue) {
						continue MetricLoop
					} else {
						benchMetrics[fmt.Sprintf(benchMetricNameTmpl, lValue)+"/op"] = m.GetCounter().GetValue()
					}
				}
			}

		}
	}
	return benchMetrics
}
