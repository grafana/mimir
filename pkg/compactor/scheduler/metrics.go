// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type schedulerMetrics struct {
	pendingJobs            *prometheus.GaugeVec
	activeJobs             *prometheus.GaugeVec
	jobsLeased             prometheus.Counter
	persistentJobFailures  *prometheus.CounterVec
}

func newSchedulerMetrics(reg prometheus.Registerer) *schedulerMetrics {
	return &schedulerMetrics{
		pendingJobs: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_pending_jobs",
			Help: "The number of queued pending jobs.",
		}, []string{"user"}),
		activeJobs: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_active_jobs",
			Help: "The number of jobs active in workers.",
		}, []string{"user"}),
		jobsLeased: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_compactor_scheduler_jobs_leased_total",
			Help: "Total number of jobs leased to workers by the scheduler.",
		}),
		persistentJobFailures: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_compactor_scheduler_persistent_job_failures_total",
			Help: "Total number of jobs that have failed more than the allowed number of times.",
		}, []string{"user"}),
	}
}

type trackerMetrics struct {
	pendingJobs           prometheus.Gauge
	activeJobs            prometheus.Gauge
	persistentJobFailures prometheus.Counter
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		pendingJobs:           s.pendingJobs.WithLabelValues(tenant),
		activeJobs:            s.activeJobs.WithLabelValues(tenant),
		persistentJobFailures: s.persistentJobFailures.WithLabelValues(tenant),
	}
}

func (s *schedulerMetrics) deleteTenantMetrics(tenant string) {
	s.pendingJobs.DeleteLabelValues(tenant)
	s.activeJobs.DeleteLabelValues(tenant)
	s.persistentJobFailures.DeleteLabelValues(tenant)
}
