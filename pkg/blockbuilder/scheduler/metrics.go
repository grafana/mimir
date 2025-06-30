// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type schedulerMetrics struct {
	updateScheduleDuration   prometheus.Histogram
	partitionStartOffset     *prometheus.GaugeVec
	partitionCommittedOffset *prometheus.GaugeVec
	partitionEndOffset       *prometheus.GaugeVec
	flushFailed              prometheus.Counter
	fetchOffsetsFailed       prometheus.Counter
	outstandingJobs          prometheus.Gauge
	assignedJobs             prometheus.Gauge
	pendingJobs              *prometheus.GaugeVec
	persistentJobFailures    prometheus.Counter
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
		partitionCommittedOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_committed_offset",
			Help: "The observed committed offset of each partition.",
		}, []string{"partition"}),
		flushFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_flush_failed_total",
			Help: "The total number of Kafka flushes that failed.",
		}),
		fetchOffsetsFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_fetch_offsets_failed_total",
			Help: "The number of times we've persistently failed to fetch committed offsets.",
		}),
		persistentJobFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_persistent_job_failures_total",
			Help: "The number of times a job failed persistently beyond the allowed max fail count.",
		}),
		outstandingJobs: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_outstanding_jobs",
			Help: "The number of outstanding jobs.",
		}),
		assignedJobs: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_assigned_jobs",
			Help: "The number of jobs assigned to workers.",
		}),
		pendingJobs: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_pending_jobs",
			Help: "The number of jobs in the pending queues.",
		}, []string{"partition"}),
	}
}
