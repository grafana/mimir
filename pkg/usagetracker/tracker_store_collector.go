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

var localitySeriesDesc = prometheus.NewDesc(
	"cortex_usage_tracker_locality_series",
	"Active series on this tracker partition that carry a Nautilus locality hash.",
	[]string{"user"}, nil,
)

var retainedBandsDesc = prometheus.NewDesc(
	"cortex_usage_tracker_retained_bands",
	"Locality bands retained for the tenant on this tracker partition.",
	[]string{"user"}, nil,
)

var snapshotBytesDesc = prometheus.NewDesc(
	"cortex_usage_tracker_last_snapshot_bytes",
	"Size in bytes of the most recent snapshot written by this tracker partition.",
	nil, nil,
)

func (t *trackerStore) Describe(descs chan<- *prometheus.Desc) {
	descs <- activeSeriesMetricDesc
	descs <- localitySeriesDesc
	descs <- retainedBandsDesc
	descs <- snapshotBytesDesc
	if t.enableVerboseSeriesMetrics {
		descs <- seriesCreatedTotalDesc
		descs <- seriesRemovedTotalDesc
	}
}

func (t *trackerStore) Collect(metrics chan<- prometheus.Metric) {
	trackerStoreCollectTestHook()
	metrics <- prometheus.MustNewConstMetric(snapshotBytesDesc, prometheus.GaugeValue, float64(t.lastSnapshotBytes.Load()))

	t.mtx.RLock()
	defer t.mtx.RUnlock()
	for _, tenantID := range t.sortedTenants {
		tenant := t.tenants[tenantID]
		metrics <- prometheus.MustNewConstMetric(activeSeriesMetricDesc, prometheus.GaugeValue, float64(tenant.series.Load()), tenantID)
		localitySeries, counts := tenant.bands.snapshot()
		metrics <- prometheus.MustNewConstMetric(localitySeriesDesc, prometheus.GaugeValue, float64(localitySeries), tenantID)
		metrics <- prometheus.MustNewConstMetric(retainedBandsDesc, prometheus.GaugeValue, float64(len(counts)), tenantID)
		if t.enableVerboseSeriesMetrics {
			metrics <- prometheus.MustNewConstMetric(seriesCreatedTotalDesc, prometheus.CounterValue, float64(tenant.seriesCreated.Load()), tenantID)
			metrics <- prometheus.MustNewConstMetric(seriesRemovedTotalDesc, prometheus.CounterValue, float64(tenant.seriesRemoved.Load()), tenantID)
		}
	}
}

var trackerStoreCollectTestHook = func() {}
