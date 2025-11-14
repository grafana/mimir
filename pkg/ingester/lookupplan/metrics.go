// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metrics struct {
	planningDuration      prometheus.ObserverVec
	FilteredRatio         prometheus.ObserverVec
	IntersectionSizeRatio prometheus.ObserverVec
	FinalCardinalityRatio prometheus.ObserverVec
}

func NewMetrics(reg prometheus.Registerer) Metrics {
	// We want a scale of 2 so we represent better ratios between 0 and 1. 2^(2^-n) for n=2 gives us 1.189207115.
	// Prometheus picks the smallest scale such that its factor is still smaller than our constant, so we choose a value slightly higher than 1.189207115.
	const ratioHistogramBucketFactor = 1.19

	return Metrics{
		planningDuration: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                            "cortex_ingester_lookup_planning_duration_seconds",
			Help:                            "Time spent planning query requests.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
		}, []string{"outcome", "user"}),

		FilteredRatio: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                        "cortex_ingester_lookup_planning_filtered_ratio",
			Help:                        "Ratio of series retrieved from the index which were also matching the vector selectors from the query. This should always be 1.0 when index_lookup_planning_enabled: true.",
			NativeHistogramBucketFactor: ratioHistogramBucketFactor,
		}, []string{"user"}),
		IntersectionSizeRatio: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                        "cortex_ingester_lookup_planning_index_selection_accuracy_ratio",
			Help:                        "Ratio between estimated number of series selected from the index and the actual number of series selected from the index.",
			NativeHistogramBucketFactor: ratioHistogramBucketFactor,
		}, []string{"user"}),
		FinalCardinalityRatio: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                        "cortex_ingester_lookup_planning_block_cardinality_accuracy_ratio",
			Help:                        "Ratio between estimated final number of series after all filtering and the actual final number of series after all filtering.",
			NativeHistogramBucketFactor: ratioHistogramBucketFactor,
		}, []string{"user"}),
	}
}

func (m Metrics) ForUser(userID string) Metrics {
	userLabel := prometheus.Labels{"user": userID}
	return Metrics{
		planningDuration:      m.planningDuration.MustCurryWith(userLabel),
		FilteredRatio:         m.FilteredRatio.MustCurryWith(userLabel),
		IntersectionSizeRatio: m.IntersectionSizeRatio.MustCurryWith(userLabel),
		FinalCardinalityRatio: m.FinalCardinalityRatio.MustCurryWith(userLabel),
	}
}
