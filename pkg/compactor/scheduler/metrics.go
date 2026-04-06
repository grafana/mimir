// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	jobTypePlan       = "plan"
	jobTypeCompaction = "compaction"
)

type schedulerMetrics struct {
	pendingJobs         *prometheus.GaugeVec
	activeJobs          *prometheus.GaugeVec
	jobsCompleted       *prometheus.CounterVec
	repeatedJobFailures prometheus.Counter
}

func newSchedulerMetrics(reg prometheus.Registerer) *schedulerMetrics {
	m := &schedulerMetrics{
		pendingJobs: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_pending_jobs",
			Help: "The number of queued pending jobs.",
		}, []string{"user"}),
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
	// Pre-initialize job types labels so we get zeros instead of no data
	m.jobsCompleted.WithLabelValues(jobTypePlan)
	m.jobsCompleted.WithLabelValues(jobTypeCompaction)
	return m
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		queue: &queueMetrics{
			pendingJobs: s.pendingJobs,
			activeJobs:  s.activeJobs,
			tenant:      tenant,
		},
		repeatedJobFailures: s.repeatedJobFailures,
	}
}

type trackerMetrics struct {
	queue               *queueMetrics
	repeatedJobFailures prometheus.Counter
}

// queueMetrics is the single point of truth for all queue-level metric updates for one tenant.
// Every job state transition goes through one of its methods; no metric is updated outside this struct.
type queueMetrics struct {
	pendingJobs *prometheus.GaugeVec // {user}
	activeJobs  *prometheus.GaugeVec // {user}
	tenant      string
}

// enqueue records a job entering the pending queue.
func (q *queueMetrics) enqueue(_ TrackedJob) {
	q.pendingJobs.WithLabelValues(q.tenant).Inc()
}

// dequeue records a job moving from pending to active (leased by a worker).
func (q *queueMetrics) dequeue(_ TrackedJob) {
	q.pendingJobs.WithLabelValues(q.tenant).Dec()
	q.activeJobs.WithLabelValues(q.tenant).Inc()
}

// recover records a job being restored directly into the active queue from persisted state.
// Unlike enqueue+dequeue, it does not touch the pending counter.
func (q *queueMetrics) recover(_ TrackedJob) {
	q.activeJobs.WithLabelValues(q.tenant).Inc()
}

// revive records a job moving from active back to pending (lease expired or cancelled, will retry).
func (q *queueMetrics) revive(_ TrackedJob) {
	q.activeJobs.WithLabelValues(q.tenant).Dec()
	q.pendingJobs.WithLabelValues(q.tenant).Inc()
}

// complete records a job leaving the system from the active queue (finished or permanently failed).
func (q *queueMetrics) complete(_ TrackedJob) {
	q.activeJobs.WithLabelValues(q.tenant).Dec()
}

// drop records a job leaving the system from the pending queue.
func (q *queueMetrics) drop(_ TrackedJob) {
	q.pendingJobs.WithLabelValues(q.tenant).Dec()
}

// clear removes all per-tenant label values from the metric vecs.
// Must be called when a tenant is removed.
func (q *queueMetrics) clear() {
	q.pendingJobs.DeleteLabelValues(q.tenant)
	q.activeJobs.DeleteLabelValues(q.tenant)
}
