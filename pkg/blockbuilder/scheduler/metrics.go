// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type schedulerMetrics struct {
	monitorPartitionsDuration prometheus.Histogram
	partitionStartOffsets     *prometheus.GaugeVec
	partitionEndOffsets       *prometheus.GaugeVec
}

func newSchedulerMetrics(reg prometheus.Registerer) schedulerMetrics {
	return schedulerMetrics{
		monitorPartitionsDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_blockbuilder_scheduler_monitor_partitions_duration_seconds",
			Help: "Time spent monitoring partitions.",

			NativeHistogramBucketFactor: 1.1,
		}),
		partitionStartOffsets: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_start_offsets",
			Help: "The observed start offset of each partition.",
		}, []string{"partition"}),
		partitionEndOffsets: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_end_offsets",
			Help: "The observed end offset of each partition.",
		}, []string{"partition"}),
	}
}
