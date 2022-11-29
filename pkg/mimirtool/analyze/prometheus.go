// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/cortex-tools/blob/main/pkg/analyse/prometheus.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package analyze

type MetricsInPrometheus struct {
	TotalActiveSeries      uint64 `json:"total_active_series"`
	InUseActiveSeries      uint64 `json:"in_use_active_series"`
	AdditionalActiveSeries uint64 `json:"additional_active_series"`

	InUseMetricCounts      []MetricCount `json:"in_use_metric_counts"`
	AdditionalMetricCounts []MetricCount `json:"additional_metric_counts"`

	Errors []string `json:"errors"`
}

type MetricCount struct {
	Metric    string     `json:"metric"`
	Count     int        `json:"count"`
	JobCounts []JobCount `json:"job_counts"`
}

type JobCount struct {
	Job   string `json:"job"`
	Count int    `json:"count"`
}
