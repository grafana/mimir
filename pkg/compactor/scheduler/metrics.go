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
	pendingJobsByUser   *prometheus.GaugeVec
	incompleteJobsBytes *prometheus.GaugeVec
	incompletePlanJobs  prometheus.Gauge
	activeJobs          *prometheus.GaugeVec
	activeJobsByUser    *prometheus.GaugeVec
	jobsCompleted       *prometheus.CounterVec
	repeatedJobFailures prometheus.Counter
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
		incompleteJobsBytes: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes",
			Help: "The total bytes of blocks in compaction jobs that have not yet completed (pending or active).",
		}, []string{"compaction_type"}),
		incompletePlanJobs: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_incomplete_plan_jobs",
			Help: "The total number of plan jobs that have not yet completed (pending or active).",
		}),
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
	}
	// Pre-initialize job type labels so we get zeros instead of no data.
	m.jobsCompleted.WithLabelValues(jobTypePlan)
	m.jobsCompleted.WithLabelValues(jobTypeCompaction)
	m.pendingJobs.WithLabelValues(jobTypePlan)
	m.pendingJobs.WithLabelValues(jobTypeCompaction)
	m.activeJobs.WithLabelValues(jobTypePlan)
	m.activeJobs.WithLabelValues(jobTypeCompaction)
	m.incompleteJobsBytes.WithLabelValues(compactionTypeSplit)
	m.incompleteJobsBytes.WithLabelValues(compactionTypeMerge)
	return m
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		queue: &queueMetrics{
			pendingJobsByUser: s.pendingJobsByUser.WithLabelValues(tenant),
			activeJobsByUser:  s.activeJobsByUser.WithLabelValues(tenant),
			pendingJobs: perJobTypeGauges{
				plan:       s.pendingJobs.WithLabelValues(jobTypePlan),
				compaction: s.pendingJobs.WithLabelValues(jobTypeCompaction),
			},
			activeJobs: perJobTypeGauges{
				plan:       s.activeJobs.WithLabelValues(jobTypePlan),
				compaction: s.activeJobs.WithLabelValues(jobTypeCompaction),
			},
			incompleteSplitBytes: s.incompleteJobsBytes.WithLabelValues(compactionTypeSplit),
			incompleteMergeBytes: s.incompleteJobsBytes.WithLabelValues(compactionTypeMerge),
			incompletePlanJobs:   s.incompletePlanJobs,
			clear: func() {
				s.pendingJobsByUser.DeleteLabelValues(tenant)
				s.activeJobsByUser.DeleteLabelValues(tenant)
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
	m.queue.incompleteSplitBytes.Sub(float64(m.queue.splitBytes))
	m.queue.incompleteMergeBytes.Sub(float64(m.queue.mergeBytes))
	m.queue.incompletePlanJobs.Sub(float64(m.queue.planJobCount))
	m.queue.pendingJobs.clear()
	m.queue.activeJobs.clear()
	m.queue.splitBytes = 0
	m.queue.mergeBytes = 0
	m.queue.planJobCount = 0
	m.queue.clear()
}

// perJobTypeGauges holds references to a pair of shared {plan,compaction} gauges together with
// this tenant's per-type contribution, so Clear() can subtract exactly the right amount.
type perJobTypeGauges struct {
	plan            prometheus.Gauge
	compaction      prometheus.Gauge
	planCount       int
	compactionCount int
}

func (g *perJobTypeGauges) inc(isPlan bool) {
	if isPlan {
		g.plan.Inc()
		g.planCount++
	} else {
		g.compaction.Inc()
		g.compactionCount++
	}
}

func (g *perJobTypeGauges) dec(isPlan bool) {
	if isPlan {
		g.plan.Dec()
		g.planCount--
	} else {
		g.compaction.Dec()
		g.compactionCount--
	}
}

func (g *perJobTypeGauges) clear() {
	g.plan.Sub(float64(g.planCount))
	g.compaction.Sub(float64(g.compactionCount))
	g.planCount = 0
	g.compactionCount = 0
}

// queueMetrics encapsulates queue-level metrics for one tenant, allowing the caller to ignore
// the details of which metrics to update and how, focusing only on job state transitions.
// Callers are responsible for making valid transitions. Invalid calls (e.g. DropPending on an
// empty queue) will produce incorrect gauge values. Methods are not thread-safe.
type queueMetrics struct {
	pendingJobsByUser prometheus.Gauge
	activeJobsByUser  prometheus.Gauge

	// shared across tenants
	pendingJobs          perJobTypeGauges
	activeJobs           perJobTypeGauges
	incompleteSplitBytes prometheus.Gauge
	incompleteMergeBytes prometheus.Gauge
	incompletePlanJobs   prometheus.Gauge

	// splitBytes, mergeBytes, and planJobCount track this tenant's contribution to the shared
	// incomplete gauges so we can subtract exactly the right amount on tenant removal.
	splitBytes   uint64
	mergeBytes   uint64
	planJobCount int
	clear        func()
}

func (q *queueMetrics) Pending(j TrackedJob) {
	isPlan := j.ID() == planJobId
	q.pendingJobsByUser.Inc()
	q.pendingJobs.inc(isPlan)
	if isPlan {
		q.incompletePlanJobs.Inc()
		q.planJobCount++
	} else {
		q.addBytes(j.(*TrackedCompactionJob))
	}
}

func (q *queueMetrics) Leased(j TrackedJob) {
	isPlan := j.ID() == planJobId
	q.pendingJobsByUser.Dec()
	q.pendingJobs.dec(isPlan)
	q.activeJobsByUser.Inc()
	q.activeJobs.inc(isPlan)
}

// Recover records jobs restored from persisted state on startup.
func (q *queueMetrics) Recover(pending, leased []TrackedJob) {
	for _, j := range pending {
		isPlan := j.ID() == planJobId
		q.pendingJobsByUser.Inc()
		q.pendingJobs.inc(isPlan)
		if isPlan {
			q.incompletePlanJobs.Inc()
			q.planJobCount++
		} else {
			q.addBytes(j.(*TrackedCompactionJob))
		}
	}
	for _, j := range leased {
		isPlan := j.ID() == planJobId
		q.activeJobsByUser.Inc()
		q.activeJobs.inc(isPlan)
		if isPlan {
			q.incompletePlanJobs.Inc()
			q.planJobCount++
		} else if cj, ok := j.(*TrackedCompactionJob); ok {
			q.addBytes(cj)
		}
	}
}

// Revive records a job moving from active back to pending (lease expired or cancelled).
func (q *queueMetrics) Revive(j TrackedJob) {
	isPlan := j.ID() == planJobId
	q.activeJobsByUser.Dec()
	q.activeJobs.dec(isPlan)
	q.pendingJobsByUser.Inc()
	q.pendingJobs.inc(isPlan)
}

// Complete records a job leaving the system from the active queue (success or failure).
func (q *queueMetrics) Complete(j TrackedJob) {
	isPlan := j.ID() == planJobId
	q.activeJobsByUser.Dec()
	q.activeJobs.dec(isPlan)
	if isPlan {
		q.incompletePlanJobs.Dec()
		q.planJobCount--
	} else if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

// DropPending records a job leaving the system from the pending queue.
func (q *queueMetrics) DropPending(j TrackedJob) {
	isPlan := j.ID() == planJobId
	q.pendingJobsByUser.Dec()
	q.pendingJobs.dec(isPlan)
	if isPlan {
		q.incompletePlanJobs.Dec()
		q.planJobCount--
	} else {
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
