// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import "github.com/prometheus/client_golang/prometheus"

var _ prometheus.Collector = &trackerStore{}

var trackerStoreMetricDesc = prometheus.NewDesc(
	"cortex_usage_tracker_active_series",
	"Number of active series tracker for each user.",
	[]string{"user"}, nil,
)

func (t *trackerStore) Describe(descs chan<- *prometheus.Desc) {
	descs <- trackerStoreMetricDesc
}

func (t *trackerStore) Collect(metrics chan<- prometheus.Metric) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	for tenantID, tenant := range t.tenants {
		metrics <- prometheus.MustNewConstMetric(trackerStoreMetricDesc, prometheus.GaugeValue, float64(tenant.series.Load()), tenantID)
	}
}
