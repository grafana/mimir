// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	planningDuration *prometheus.HistogramVec
}

func NewMetrics(reg prometheus.Registerer) Metrics {
	return Metrics{
		planningDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_ingester_lookup_planning_duration_seconds",
			Help:                            "Time spent planning query requests.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}, []string{"outcome"}),
	}
}
