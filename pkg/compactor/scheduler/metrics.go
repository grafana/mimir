// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/compactor"
)

const (
	jobTypePlan       = "plan"
	jobTypeCompaction = "compaction"
	jobTypeCleanup    = "cleanup"

	compactionTypeSplit = "split"
	compactionTypeMerge = "merge"
)

type schedulerMetrics struct {
	pendingJobs          *prometheus.GaugeVec
	pendingJobsByUser    *prometheus.GaugeVec
	pendingJobsLastEmpty prometheus.Gauge
	incompleteJobsBytes  *prometheus.GaugeVec
	activeJobs           *prometheus.GaugeVec
	activeJobsByUser     *prometheus.GaugeVec
	jobsCompleted        *prometheus.CounterVec
	repeatedJobFailures  prometheus.Counter

	// Per-tenant bucket state reported by cleanup jobs.
	tenantCleanupMetrics compactor.TenantCleanupMetrics
}

func newSchedulerMetrics(reg prometheus.Registerer) *schedulerMetrics {
	m := &schedulerMetrics{
		pendingJobs: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_pending_jobs",
			Help: "The number of queued pending jobs.",
		}, []string{"job_type"}),
		pendingJobsByUser: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_pending_jobs_by_user",
			Help: "The number of queued pending jobs, broken down by user.",
		}, []string{"user"}),
		pendingJobsLastEmpty: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_pending_jobs_last_empty_timestamp_seconds",
			Help: "Unix timestamp of the last time there were no pending jobs remaining.",
		}),
		incompleteJobsBytes: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes",
			Help: "The total bytes of blocks in compaction jobs that have not yet completed (pending or active).",
		}, []string{"compaction_type"}),
		activeJobs: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_active_jobs",
			Help: "The number of jobs active in workers.",
		}, []string{"job_type"}),
		activeJobsByUser: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_active_jobs_by_user",
			Help: "The number of jobs active in workers, broken down by user.",
		}, []string{"user"}),
		jobsCompleted: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_scheduler_jobs_completed_total",
			Help: "Total number of jobs successfully completed by workers.",
		}, []string{"job_type"}),
		repeatedJobFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_scheduler_repeated_job_failures_total",
			Help: "Total number of failures for jobs that exceeded the repeated failure threshold.",
		}),
		tenantCleanupMetrics: compactor.NewTenantCleanupMetrics(reg),
	}
	// Pre-initialize job type labels so we get zeros instead of no data.
	m.jobsCompleted.WithLabelValues(jobTypePlan)
	m.jobsCompleted.WithLabelValues(jobTypeCompaction)
	m.jobsCompleted.WithLabelValues(jobTypeCleanup)
	m.pendingJobs.WithLabelValues(jobTypePlan)
	m.pendingJobs.WithLabelValues(jobTypeCompaction)
	m.pendingJobs.WithLabelValues(jobTypeCleanup)
	m.activeJobs.WithLabelValues(jobTypePlan)
	m.activeJobs.WithLabelValues(jobTypeCompaction)
	m.activeJobs.WithLabelValues(jobTypeCleanup)
	m.incompleteJobsBytes.WithLabelValues(compactionTypeSplit)
	m.incompleteJobsBytes.WithLabelValues(compactionTypeMerge)
	return m
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		queue: &queueMetrics{
			pendingJobsByUser:     s.pendingJobsByUser.WithLabelValues(tenant),
			activeJobsByUser:      s.activeJobsByUser.WithLabelValues(tenant),
			pendingPlanJobs:       s.pendingJobs.WithLabelValues(jobTypePlan),
			pendingCompactionJobs: s.pendingJobs.WithLabelValues(jobTypeCompaction),
			pendingCleanupJobs:    s.pendingJobs.WithLabelValues(jobTypeCleanup),
			activePlanJobs:        s.activeJobs.WithLabelValues(jobTypePlan),
			activeCompactionJobs:  s.activeJobs.WithLabelValues(jobTypeCompaction),
			activeCleanupJobs:     s.activeJobs.WithLabelValues(jobTypeCleanup),
			incompleteSplitBytes:  s.incompleteJobsBytes.WithLabelValues(compactionTypeSplit),
			incompleteMergeBytes:  s.incompleteJobsBytes.WithLabelValues(compactionTypeMerge),
			clear: func() {
				s.pendingJobsByUser.DeleteLabelValues(tenant)
				s.activeJobsByUser.DeleteLabelValues(tenant)
				s.tenantCleanupMetrics.Delete(tenant)
			},
		},
		repeatedJobFailures: s.repeatedJobFailures,
	}
}

type trackerMetrics struct {
	queue               *queueMetrics
	repeatedJobFailures prometheus.Counter
}

// Clear deletes all per-tenant label values and subtracts this tenant's contribution from the
// shared gauges. Must be called when a tenant is removed.
func (m *trackerMetrics) Clear() {
	q := m.queue
	q.incompleteSplitBytes.Sub(float64(q.splitBytes))
	q.incompleteMergeBytes.Sub(float64(q.mergeBytes))
	q.pendingPlanJobs.Sub(float64(q.pendingPlanCount))
	q.pendingCompactionJobs.Sub(float64(q.pendingCompactionCount))
	q.pendingCleanupJobs.Sub(float64(q.pendingCleanupCount))
	q.activePlanJobs.Sub(float64(q.activePlanCount))
	q.activeCompactionJobs.Sub(float64(q.activeCompactionCount))
	q.activeCleanupJobs.Sub(float64(q.activeCleanupCount))
	q.splitBytes = 0
	q.mergeBytes = 0
	q.pendingPlanCount = 0
	q.pendingCompactionCount = 0
	q.pendingCleanupCount = 0
	q.activePlanCount = 0
	q.activeCompactionCount = 0
	q.activeCleanupCount = 0
	q.clear()
}

// queueMetrics encapsulates queue-level metrics for one tenant, allowing the caller to ignore
// the details of which metrics to update and how, focusing only on job state transitions.
// Callers are responsible for making valid transitions. Invalid calls (e.g. DropPending on an
// empty queue) will produce incorrect gauge values. Methods are not thread-safe.
type queueMetrics struct {
	pendingJobsByUser prometheus.Gauge
	activeJobsByUser  prometheus.Gauge

	// shared across tenants
	pendingPlanJobs       prometheus.Gauge
	pendingCompactionJobs prometheus.Gauge
	pendingCleanupJobs    prometheus.Gauge
	activePlanJobs        prometheus.Gauge
	activeCompactionJobs  prometheus.Gauge
	activeCleanupJobs     prometheus.Gauge
	incompleteSplitBytes  prometheus.Gauge
	incompleteMergeBytes  prometheus.Gauge

	// This tenant's contribution to the shared gauges, tracked so Clear() can subtract exactly
	// the right amount on tenant removal.
	splitBytes             uint64
	mergeBytes             uint64
	pendingPlanCount       int
	pendingCompactionCount int
	pendingCleanupCount    int
	activePlanCount        int
	activeCompactionCount  int
	activeCleanupCount     int
	clear                  func()
}

func (q *queueMetrics) Pending(j TrackedJob) {
	q.incPending(j.ID())
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.addBytes(cj)
	}
}

func (q *queueMetrics) Leased(j TrackedJob) {
	q.decPending(j.ID())
	q.incActive(j.ID())
}

// Recover records jobs restored from persisted state on startup.
func (q *queueMetrics) Recover(pending, leased []TrackedJob) {
	for _, j := range pending {
		q.Pending(j)
	}
	for _, j := range leased {
		q.incActive(j.ID())
		if cj, ok := j.(*TrackedCompactionJob); ok {
			q.addBytes(cj)
		}
	}
}

// Revive records a job moving from active back to pending (lease expired or cancelled).
func (q *queueMetrics) Revive(j TrackedJob) {
	q.decActive(j.ID())
	q.incPending(j.ID())
}

// Complete records a job leaving the system from the active queue (success or failure).
func (q *queueMetrics) Complete(j TrackedJob) {
	q.decActive(j.ID())
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

// DropPending records a job leaving the system from the pending queue.
func (q *queueMetrics) DropPending(j TrackedJob) {
	q.decPending(j.ID())
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

func (q *queueMetrics) incPending(id string) {
	q.pendingJobsByUser.Inc()
	switch id {
	case planJobId:
		q.pendingPlanJobs.Inc()
		q.pendingPlanCount++
	case cleanupJobId:
		q.pendingCleanupJobs.Inc()
		q.pendingCleanupCount++
	default:
		q.pendingCompactionJobs.Inc()
		q.pendingCompactionCount++
	}
}

func (q *queueMetrics) decPending(id string) {
	q.pendingJobsByUser.Dec()
	switch id {
	case planJobId:
		q.pendingPlanJobs.Dec()
		q.pendingPlanCount--
	case cleanupJobId:
		q.pendingCleanupJobs.Dec()
		q.pendingCleanupCount--
	default:
		q.pendingCompactionJobs.Dec()
		q.pendingCompactionCount--
	}
}

func (q *queueMetrics) incActive(id string) {
	q.activeJobsByUser.Inc()
	switch id {
	case planJobId:
		q.activePlanJobs.Inc()
		q.activePlanCount++
	case cleanupJobId:
		q.activeCleanupJobs.Inc()
		q.activeCleanupCount++
	default:
		q.activeCompactionJobs.Inc()
		q.activeCompactionCount++
	}
}

func (q *queueMetrics) decActive(id string) {
	q.activeJobsByUser.Dec()
	switch id {
	case planJobId:
		q.activePlanJobs.Dec()
		q.activePlanCount--
	case cleanupJobId:
		q.activeCleanupJobs.Dec()
		q.activeCleanupCount--
	default:
		q.activeCompactionJobs.Dec()
		q.activeCompactionCount--
	}
}

func (q *queueMetrics) addBytes(cj *TrackedCompactionJob) {
	if cj.value.isSplit {
		q.splitBytes += cj.totalBlockBytes
		q.incompleteSplitBytes.Add(float64(cj.totalBlockBytes))
	} else {
		q.mergeBytes += cj.totalBlockBytes
		q.incompleteMergeBytes.Add(float64(cj.totalBlockBytes))
	}
}

func (q *queueMetrics) subBytes(cj *TrackedCompactionJob) {
	if cj.value.isSplit {
		q.splitBytes -= cj.totalBlockBytes
		q.incompleteSplitBytes.Sub(float64(cj.totalBlockBytes))
	} else {
		q.mergeBytes -= cj.totalBlockBytes
		q.incompleteMergeBytes.Sub(float64(cj.totalBlockBytes))
	}
}
