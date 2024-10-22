// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type schedulerMetrics struct {
	updateScheduleDuration prometheus.Histogram
	partitionStartOffset   *prometheus.GaugeVec
	partitionEndOffset     *prometheus.GaugeVec
}

func newSchedulerMetrics(reg prometheus.Registerer) schedulerMetrics {
	return schedulerMetrics{
		updateScheduleDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_blockbuilder_scheduler_schedule_update_seconds",
			Help: "Time spent updating the schedule.",

			NativeHistogramBucketFactor: 1.1,
		}),
		partitionStartOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_start_offset",
			Help: "The observed start offset of each partition.",
		}, []string{"partition"}),
		partitionEndOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_end_offset",
			Help: "The observed end offset of each partition.",
		}, []string{"partition"}),
	}
}
