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
			pendingJobs: s.pendingJobs.WithLabelValues(tenant),
			activeJobs:  s.activeJobs.WithLabelValues(tenant),
			clear: func() {
				s.pendingJobs.DeleteLabelValues(tenant)
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

// Clear deletes all per-tenant label values. Must be called when a tenant is removed.
func (m *trackerMetrics) Clear() {
	m.queue.clear()
}

// queueMetrics encapsulates queue-level metrics for one tenant, allowing the caller to ignore
// the details of which metrics to update and how, focusing only on job state transitions.
// Callers are responsible for making valid transitions. Invalid calls (e.g. DropPending on an
// empty queue) will produce incorrect gauge values.
type queueMetrics struct {
	pendingJobs prometheus.Gauge
	activeJobs  prometheus.Gauge
	clear       func()
}

func (q *queueMetrics) Pending(_ TrackedJob) {
	q.pendingJobs.Inc()
}

func (q *queueMetrics) Leased(_ TrackedJob) {
	q.pendingJobs.Dec()
	q.activeJobs.Inc()
}

// Recover records jobs restored from persisted state on startup.
func (q *queueMetrics) Recover(pending, leased []TrackedJob) {
	q.pendingJobs.Add(float64(len(pending)))
	q.activeJobs.Add(float64(len(leased)))
}

// Revive records a job moving from active back to pending (lease expired or cancelled).
func (q *queueMetrics) Revive(_ TrackedJob) {
	q.activeJobs.Dec()
	q.pendingJobs.Inc()
}

// Complete records a job leaving the system from the active queue (success or failure).
func (q *queueMetrics) Complete(_ TrackedJob) {
	q.activeJobs.Dec()
}

// DropPending records a job leaving the system from the pending queue.
func (q *queueMetrics) DropPending(_ TrackedJob) {
	q.pendingJobs.Dec()
}
