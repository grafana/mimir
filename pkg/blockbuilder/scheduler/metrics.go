// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type schedulerMetrics struct {
	updateScheduleDuration prometheus.Histogram
	probeRecordTimeDelta   prometheus.Histogram
	fetchOffsetsFailed     prometheus.Counter
	outstandingJobs        prometheus.Gauge
	assignedJobs           prometheus.Gauge
	pendingJobs            *prometheus.GaugeVec
	persistentJobFailures  prometheus.Counter
	jobGapDetected         *prometheus.CounterVec
	startupJobsSkipped     prometheus.Counter

	offsetMarkerWrites        prometheus.Counter
	offsetMarkerWriteFailures prometheus.Counter
	offsetMarkerReadFailures  prometheus.Counter
	offsetMarkerLastWrite     prometheus.Gauge
	offsetMarkerMismatches    *prometheus.CounterVec
	lookbackFallbacks         prometheus.Counter

	// perClusterMetrics holds one set of metrics per cluster, indexed by cluster ID.
	perClusterMetrics []singleClusterMetrics
}

func newSchedulerMetrics(reg prometheus.Registerer, compartmentsEnabled bool, numClusters int) schedulerMetrics {
	scm := schedulerMetrics{
		updateScheduleDuration: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_blockbuilder_scheduler_schedule_update_seconds",
			Help: "Time spent updating the schedule.",

			NativeHistogramBucketFactor: 1.1,
		}),
		probeRecordTimeDelta: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name: "cortex_blockbuilder_scheduler_initial_probe_record_time_delta_seconds",
			Help: "Delta between a probe's requested time and the timestamp of the record at the returned offset, during initial offset probing.",

			NativeHistogramBucketFactor: 1.1,
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
		jobGapDetected: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_job_gap_detected",
			Help: "The number of times an unexpected gap was detected between jobs.",
		}, []string{"offset_type"}),
		startupJobsSkipped: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_startup_jobs_skipped_total",
			Help: "The total number of observed jobs skipped during startup recovery for unexpected reasons (excludes jobs skipped because they were already committed).",
		}),

		offsetMarkerWrites: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_offset_marker_writes_total",
			Help: "The total number of times the offset marker object was written to object storage.",
		}),
		offsetMarkerWriteFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_offset_marker_write_failures_total",
			Help: "The total number of times writing the offset marker object failed.",
		}),
		offsetMarkerReadFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_offset_marker_read_failures_total",
			Help: "The total number of times reading the offset marker object failed, or found one that could not be used.",
		}),
		offsetMarkerLastWrite: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_offset_marker_last_write_timestamp_seconds",
			Help: "The Unix timestamp of the last successful write of the offset marker object.",
		}),
		offsetMarkerMismatches: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_offset_marker_startup_mismatch_total",
			Help: "The number of partitions, at startup, whose offset in the marker object disagrees with the Kafka committed offset, by reason.",
		}, []string{"reason"}),
		lookbackFallbacks: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_lookback_fallback_partitions_total",
			Help: "The number of partitions, at startup, that had no committed or planned offset and resumed from the lookback-on-no-commit time, skipping records that were still in Kafka. Those records are never built into blocks.",
		}),
	}

	// Make sure the gap detection counters are pre-initialized. This avoids misleading blanks in the series on restart.
	scm.jobGapDetected.WithLabelValues(offsetNamePlanned)
	scm.jobGapDetected.WithLabelValues(offsetNameCommitted)

	for _, r := range mismatchReasons {
		scm.offsetMarkerMismatches.WithLabelValues(r)
	}

	scm.perClusterMetrics = newClusterMetrics(reg, compartmentsEnabled, numClusters)

	return scm
}

type singleClusterMetrics struct {
	startOffset          *prometheus.GaugeVec
	endOffset            *prometheus.GaugeVec
	committedOffset      *prometheus.GaugeVec
	plannedOffset        *prometheus.GaugeVec
	endOffsetProbeFailed prometheus.Counter
	flushFailed          prometheus.Counter
}

// newClusterMetrics builds one set of metrics per cluster, indexed by cluster ID.
// With compartments enabled each cluster's metrics carry a constant write_compartment
// label; with compartments disabled there is a single unlabeled set.
func newClusterMetrics(reg prometheus.Registerer, compartmentsEnabled bool, numClusters int) []singleClusterMetrics {
	metrics := make([]singleClusterMetrics, numClusters)
	for clusterID := range metrics {
		clusterReg := reg
		if compartmentsEnabled {
			clusterReg = prometheus.WrapRegistererWith(prometheus.Labels{"write_compartment": strconv.Itoa(clusterID)}, reg)
		}
		metrics[clusterID] = newSingleClusterMetrics(clusterReg)
	}
	return metrics
}

func newSingleClusterMetrics(reg prometheus.Registerer) singleClusterMetrics {
	return singleClusterMetrics{
		startOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_start_offset",
			Help: "The observed start offset of each partition.",
		}, []string{"partition"}),
		endOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_end_offset",
			Help: "The observed end offset of each partition.",
		}, []string{"partition"}),
		committedOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_committed_offset",
			Help: "The observed committed offset of each partition.",
		}, []string{"partition"}),
		plannedOffset: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_blockbuilder_scheduler_partition_planned_offset",
			Help: "The planned offset of each partition.",
		}, []string{"partition"}),
		endOffsetProbeFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_end_offset_probe_failed_total",
			Help: "The total number of times probing the cluster's end offsets failed.",
		}),
		flushFailed: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_blockbuilder_scheduler_flush_failed_total",
			Help: "The total number of times flushing the cluster's committed offsets to Kafka failed.",
		}),
	}
}
