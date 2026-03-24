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
	repeatedJobFailures *prometheus.CounterVec
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
		repeatedJobFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_scheduler_repeated_job_failures_total",
			Help: "Total number of failures for jobs that exceeded the repeated failure threshold.",
		}, []string{"user"}),
	}
	// Pre-initialize job types labels so we get zeros instead of no data
	m.jobsCompleted.WithLabelValues(jobTypePlan)
	m.jobsCompleted.WithLabelValues(jobTypeCompaction)
	return m
}

type trackerMetrics struct {
	pendingJobs         prometheus.Gauge
	activeJobs          prometheus.Gauge
	repeatedJobFailures prometheus.Counter
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		pendingJobs:         s.pendingJobs.WithLabelValues(tenant),
		activeJobs:          s.activeJobs.WithLabelValues(tenant),
		repeatedJobFailures: s.repeatedJobFailures.WithLabelValues(tenant),
	}
}

func (s *schedulerMetrics) deleteTenantMetrics(tenant string) {
	s.pendingJobs.DeleteLabelValues(tenant)
	s.activeJobs.DeleteLabelValues(tenant)
	s.repeatedJobFailures.DeleteLabelValues(tenant)
}
