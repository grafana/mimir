// SPDX-License-Identifier: AGPL-3.0-only

package usagetracker

import "github.com/prometheus/client_golang/prometheus"

var _ prometheus.Collector = &trackerStore{}

const activeSeriesMetricName = "cortex_usage_tracker_active_series"

const currentActiveSeriesLimit = "cortex_usage_tracker_active_series_current_limit"

var activeSeriesMetricDesc = prometheus.NewDesc(
	activeSeriesMetricName,
	"Number of active series tracker for each user.",
	[]string{"user"}, nil,
)

var currentLimitMetricDesc = prometheus.NewDesc(
	currentActiveSeriesLimit,
	"Current active series limit for each user.",
	[]string{"user"}, nil,
)

func (t *trackerStore) Describe(descs chan<- *prometheus.Desc) {
	descs <- activeSeriesMetricDesc
	descs <- currentLimitMetricDesc
}

func (t *trackerStore) Collect(metrics chan<- prometheus.Metric) {
	t.mtx.RLock()
	defer t.mtx.RUnlock()
	for _, tenantID := range t.sortedTenants {
		tenant := t.tenants[tenantID]
		metrics <- prometheus.MustNewConstMetric(activeSeriesMetricDesc, prometheus.GaugeValue, float64(tenant.series.Load()), tenantID)
		metrics <- prometheus.MustNewConstMetric(currentLimitMetricDesc, prometheus.GaugeValue, float64(tenant.currentLimit.Load()), tenantID)
	}
}
