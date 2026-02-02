// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type schedulerMetrics struct {
	pendingJobs *prometheus.GaugeVec
	activeJobs  *prometheus.GaugeVec
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
	}
}

type trackerMetrics struct {
	pendingJobs prometheus.Gauge
	activeJobs  prometheus.Gauge
}

func (s *schedulerMetrics) trackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		pendingJobs: s.pendingJobs.WithLabelValues(tenant),
		activeJobs:  s.activeJobs.WithLabelValues(tenant),
	}
}
