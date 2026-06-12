// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import (
	"github.com/prometheus/client_golang/prometheus"
)

var _ prometheus.Collector = &trackerStore{}

const activeSeriesMetricName = "cortex_usage_tracker_active_series"

var activeSeriesMetricDesc = prometheus.NewDesc(
	activeSeriesMetricName,
	"Number of active series tracker for each user.",
	[]string{"user"}, nil,
)

var seriesCreatedTotalDesc = prometheus.NewDesc(
	"cortex_usage_tracker_series_created_total",
	"Total number of series created per user.",
	[]string{"user"}, nil,
)

var seriesRemovedTotalDesc = prometheus.NewDesc(
	"cortex_usage_tracker_series_removed_total",
	"Total number of series removed per user.",
	[]string{"user"}, nil,
)

var shardWorkerSubmitBlockedDesc = prometheus.NewDesc(
	"cortex_usage_tracker_shard_worker_submit_blocked_total",
	"Total number of shard worker submissions that had to block because the inbox was full when first attempted.",
	nil, nil,
)

var shardWorkerSubmitWaitSecondsDesc = prometheus.NewDesc(
	"cortex_usage_tracker_shard_worker_submit_wait_seconds_total",
	"Cumulative time, in seconds, spent waiting on a full shard worker inbox before the submission was accepted.",
	nil, nil,
)

var shardWorkerOpsTotalDesc = prometheus.NewDesc(
	"cortex_usage_tracker_shard_worker_ops_total",
	"Total number of shardOps processed by shard workers across all tenants.",
	nil, nil,
)

func (t *trackerStore) Describe(descs chan<- *prometheus.Desc) {
	descs <- activeSeriesMetricDesc
	if t.enableVerboseSeriesMetrics {
		descs <- seriesCreatedTotalDesc
		descs <- seriesRemovedTotalDesc
	}
	if t.shardWorkers != nil {
		descs <- shardWorkerSubmitBlockedDesc
		descs <- shardWorkerSubmitWaitSecondsDesc
		descs <- shardWorkerOpsTotalDesc
	}
}

func (t *trackerStore) Collect(metrics chan<- prometheus.Metric) {
	trackerStoreCollectTestHook()

	t.mtx.RLock()
	for _, tenantID := range t.sortedTenants {
		tenant := t.tenants[tenantID]
		metrics <- prometheus.MustNewConstMetric(activeSeriesMetricDesc, prometheus.GaugeValue, float64(tenant.series.Load()), tenantID)
		if t.enableVerboseSeriesMetrics {
			metrics <- prometheus.MustNewConstMetric(seriesCreatedTotalDesc, prometheus.CounterValue, float64(tenant.seriesCreated.Load()), tenantID)
			metrics <- prometheus.MustNewConstMetric(seriesRemovedTotalDesc, prometheus.CounterValue, float64(tenant.seriesRemoved.Load()), tenantID)
		}
	}
	t.mtx.RUnlock()

	if t.shardWorkers != nil {
		metrics <- prometheus.MustNewConstMetric(shardWorkerSubmitBlockedDesc, prometheus.CounterValue, float64(t.shardWorkerSubmitBlocked.Load()))
		metrics <- prometheus.MustNewConstMetric(shardWorkerSubmitWaitSecondsDesc, prometheus.CounterValue, float64(t.shardWorkerSubmitWaitNanos.Load())/float64(1e9))
		metrics <- prometheus.MustNewConstMetric(shardWorkerOpsTotalDesc, prometheus.CounterValue, float64(t.shardWorkerOpsTotal.Load()))
	}
}

var trackerStoreCollectTestHook = func() {}
