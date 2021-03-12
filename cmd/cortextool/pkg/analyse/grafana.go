package analyse

type MetricsInGrafana struct {
	MetricsUsed []string           `json:"metricsUsed"`
	Dashboards  []DashboardMetrics `json:"dashboards"`
}

type DashboardMetrics struct {
	Slug        string   `json:"slug"`
	UID         string   `json:"uid,omitempty"`
	Title       string   `json:"title"`
	Metrics     []string `json:"metrics"`
	ParseErrors []string `json:"parse_errors"`
}
