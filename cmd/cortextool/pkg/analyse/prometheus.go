package analyse

type MetricsInPrometheus struct {
	TotalActiveSeries int           `json:"total_active_series"`
	MetricCounts      []MetricCount `json:"metric_counts"`
}

type MetricCount struct {
	Metric string `string:"metric"`
	Count  int    `int:"count"`
}
