package costattribution

import (
	"time"

	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
)

type Manager interface {
	services.Service

	EnabledForUser(userID string) bool
	GetUserAttributionLabel(userID string) string
	GetUserAttributionLimit(userID string) int
	UpdateAttributionTimestamp(user string, lbs labels.Labels, now time.Time) string
	SetActiveSeries(userID, attribution string, value float64)
	IncrementDiscardedSamples(userID, attribution string, value float64)
	IncrementReceivedSamples(userID, attribution string, value float64)

	Collect(out chan<- prometheus.Metric)
	Describe(chan<- *prometheus.Desc)
}
