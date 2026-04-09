// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	jobTypePlan       = "plan"
	jobTypeCompaction = "compaction"

	compactionTypeSplit = "split"
	compactionTypeMerge = "merge"
)

type schedulerMetrics struct {
	pendingJobs         *prometheus.GaugeVec
	incompleteJobsBytes *prometheus.GaugeVec
	activeJobs          *prometheus.GaugeVec
	jobsCompleted       *prometheus.CounterVec
	repeatedJobFailures prometheus.Counter
}

func newSchedulerMetrics(reg prometheus.Registerer) *schedulerMetrics {
	m := &schedulerMetrics{
		pendingJobs: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_pending_jobs",
			Help: "The number of queued pending jobs.",
		}, []string{"user", "job_type"}),
		incompleteJobsBytes: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes",
			Help: "The total bytes of blocks in compaction jobs that have not yet completed (pending or active).",
		}, []string{"compaction_type"}),
		activeJobs: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_active_jobs",
			Help: "The number of jobs active in workers.",
		}, []string{"user"}),
		jobsCompleted: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_scheduler_jobs_completed_total",
			Help: "Total number of jobs successfully completed by workers.",
		}, []string{"job_type"}),
		repeatedJobFailures: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_scheduler_repeated_job_failures_total",
			Help: "Total number of failures for jobs that exceeded the repeated failure threshold.",
		}),
	}
	// Pre-initialize job type labels so we get zeros instead of no data.
	m.jobsCompleted.WithLabelValues(jobTypePlan)
	m.jobsCompleted.WithLabelValues(jobTypeCompaction)
	m.incompleteJobsBytes.WithLabelValues(compactionTypeSplit)
	m.incompleteJobsBytes.WithLabelValues(compactionTypeMerge)
	return m
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		queue: &queueMetrics{
			pendingPlanJobs:       s.pendingJobs.WithLabelValues(tenant, jobTypePlan),
			pendingCompactionJobs: s.pendingJobs.WithLabelValues(tenant, jobTypeCompaction),
			activeJobs:            s.activeJobs.WithLabelValues(tenant),
			incompleteSplitBytes:  s.incompleteJobsBytes.WithLabelValues(compactionTypeSplit),
			incompleteMergeBytes:  s.incompleteJobsBytes.WithLabelValues(compactionTypeMerge),
			clear: func() {
				s.pendingJobs.DeleteLabelValues(tenant, jobTypePlan)
				s.pendingJobs.DeleteLabelValues(tenant, jobTypeCompaction)
				s.activeJobs.DeleteLabelValues(tenant)
			},
		},
		repeatedJobFailures: s.repeatedJobFailures,
	}
}

type trackerMetrics struct {
	queue               *queueMetrics
	repeatedJobFailures prometheus.Counter
}

// Clear deletes all per-tenant label values and subtracts this tenant's byte contribution from the
// shared gauge. Must be called when a tenant is removed.
func (m *trackerMetrics) Clear() {
	m.queue.incompleteSplitBytes.Sub(float64(m.queue.splitBytes))
	m.queue.incompleteMergeBytes.Sub(float64(m.queue.mergeBytes))
	m.queue.splitBytes = 0
	m.queue.mergeBytes = 0
	m.queue.clear()
}

// queueMetrics encapsulates queue-level metrics for one tenant, allowing the caller to ignore
// the details of which metrics to update and how, focusing only on job state transitions.
// Callers are responsible for making valid transitions. Invalid calls (e.g. DropPending on an
// empty queue) will produce incorrect gauge values. Methods are not thread-safe.
type queueMetrics struct {
	pendingPlanJobs       prometheus.Gauge
	pendingCompactionJobs prometheus.Gauge
	activeJobs            prometheus.Gauge

	// shared across tenants
	incompleteSplitBytes prometheus.Gauge
	incompleteMergeBytes prometheus.Gauge

	// splitBytes and mergeBytes track this tenant's contribution to the shared incomplete bytes
	// so we can subtract exactly the right amount on tenant removal.
	splitBytes uint64
	mergeBytes uint64
	clear      func()
}

func (q *queueMetrics) Pending(j TrackedJob) {
	if j.ID() == planJobId {
		q.pendingPlanJobs.Inc()
	} else {
		q.pendingCompactionJobs.Inc()
		q.addBytes(j.(*TrackedCompactionJob))
	}
}

func (q *queueMetrics) Leased(j TrackedJob) {
	if j.ID() == planJobId {
		q.pendingPlanJobs.Dec()
	} else {
		q.pendingCompactionJobs.Dec()
	}
	q.activeJobs.Inc()
}

// Recover records jobs restored from persisted state on startup.
func (q *queueMetrics) Recover(pending, leased []TrackedJob) {
	for _, j := range pending {
		if j.ID() == planJobId {
			q.pendingPlanJobs.Inc()
		} else {
			q.pendingCompactionJobs.Inc()
			q.addBytes(j.(*TrackedCompactionJob))
		}
	}
	for _, j := range leased {
		q.activeJobs.Inc()
		if cj, ok := j.(*TrackedCompactionJob); ok {
			q.addBytes(cj)
		}
	}
}

// Revive records a job moving from active back to pending (lease expired or cancelled).
func (q *queueMetrics) Revive(j TrackedJob) {
	q.activeJobs.Dec()
	if j.ID() == planJobId {
		q.pendingPlanJobs.Inc()
	} else {
		q.pendingCompactionJobs.Inc()
	}
}

// Complete records a job leaving the system from the active queue (success or failure).
func (q *queueMetrics) Complete(j TrackedJob) {
	q.activeJobs.Dec()
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

// DropPending records a job leaving the system from the pending queue.
func (q *queueMetrics) DropPending(j TrackedJob) {
	if j.ID() == planJobId {
		q.pendingPlanJobs.Dec()
	} else {
		q.pendingCompactionJobs.Dec()
		q.subBytes(j.(*TrackedCompactionJob))
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
