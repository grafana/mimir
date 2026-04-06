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

// Clear deletes all per-tenant label values. Must be called when a tenant is removed.
func (m *trackerMetrics) Clear() {
	m.queue.pendingJobs.DeleteLabelValues(m.queue.tenant)
	m.queue.activeJobs.DeleteLabelValues(m.queue.tenant)
}

// queueMetrics encapsulates queue-level metrics for one tenant, allowing the caller to ignore
// the details of which metrics to update and how, focusing only on job state transitions.
// Callers are responsible for making valid transitions. Invalid calls (e.g. DropPending on an
// empty queue) will produce incorrect gauge values.
type queueMetrics struct {
	pendingJobs *prometheus.GaugeVec // {user}
	activeJobs  *prometheus.GaugeVec // {user}
	tenant      string
}

func (q *queueMetrics) Pending(_ TrackedJob) {
	q.pendingJobs.WithLabelValues(q.tenant).Inc()
}

func (q *queueMetrics) Leased(_ TrackedJob) {
	q.pendingJobs.WithLabelValues(q.tenant).Dec()
	q.activeJobs.WithLabelValues(q.tenant).Inc()
}

// Recover records jobs restored from persisted state on startup.
func (q *queueMetrics) Recover(pending, leased []TrackedJob) {
	q.pendingJobs.WithLabelValues(q.tenant).Add(float64(len(pending)))
	q.activeJobs.WithLabelValues(q.tenant).Add(float64(len(leased)))
}

// Revive records a job moving from active back to pending (lease expired or cancelled).
func (q *queueMetrics) Revive(_ TrackedJob) {
	q.activeJobs.WithLabelValues(q.tenant).Dec()
	q.pendingJobs.WithLabelValues(q.tenant).Inc()
}

// Complete records a job leaving the system from the active queue (success or failure).
func (q *queueMetrics) Complete(_ TrackedJob) {
	q.activeJobs.WithLabelValues(q.tenant).Dec()
}

// DropPending records a job leaving the system from the pending queue.
func (q *queueMetrics) DropPending(_ TrackedJob) {
	q.pendingJobs.WithLabelValues(q.tenant).Dec()
}
