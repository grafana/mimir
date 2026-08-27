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
	pendingJobs              *prometheus.GaugeVec
	pendingJobsByUser        *prometheus.GaugeVec
	pendingJobsLastEmpty     prometheus.Gauge
	lanePendingJobsLastEmpty map[lane]prometheus.Gauge
	incompleteJobsBytes      *prometheus.GaugeVec
	incompleteSplitBytes     map[lane]prometheus.Gauge
	incompleteMergeBytes     map[lane]prometheus.Gauge
	activeJobs               *prometheus.GaugeVec
	activeJobsByUser         *prometheus.GaugeVec
	jobsCompleted            *prometheus.CounterVec
	repeatedJobFailures      prometheus.Counter
	lanePolicy               lanePolicy
}

func newSchedulerMetrics(reg prometheus.Registerer, lanePolicy lanePolicy) *schedulerMetrics {
	allLanes := lanePolicy.AllLanes()
	compactionLanes := lanePolicy.CompactionLanes()
	m := &schedulerMetrics{
		lanePolicy: lanePolicy,
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
		}, []string{"compaction_type", "lane"}),
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
	laneLastEmpty := promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_compactor_scheduler_lane_pending_jobs_last_empty_timestamp_seconds",
		Help: "Unix timestamp of the last time there were no pending jobs remaining in this lane.",
	}, []string{"lane"})

	// Pre-initialize job type labels so we get zeros instead of no data.
	m.jobsCompleted.WithLabelValues(jobTypePlan)
	m.jobsCompleted.WithLabelValues(jobTypeCompaction)
	m.pendingJobs.WithLabelValues(jobTypePlan)
	m.pendingJobs.WithLabelValues(jobTypeCompaction)
	m.activeJobs.WithLabelValues(jobTypePlan)
	m.activeJobs.WithLabelValues(jobTypeCompaction)
	m.lanePendingJobsLastEmpty = make(map[lane]prometheus.Gauge, len(allLanes))
	for _, l := range allLanes {
		m.lanePendingJobsLastEmpty[l] = laneLastEmpty.WithLabelValues(string(l))
	}
	m.incompleteSplitBytes = make(map[lane]prometheus.Gauge, len(compactionLanes))
	m.incompleteMergeBytes = make(map[lane]prometheus.Gauge, len(compactionLanes))
	for _, l := range compactionLanes {
		m.incompleteSplitBytes[l] = m.incompleteJobsBytes.WithLabelValues(compactionTypeSplit, string(l))
		m.incompleteMergeBytes[l] = m.incompleteJobsBytes.WithLabelValues(compactionTypeMerge, string(l))
	}
	return m
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		queue: &queueMetrics{
			pendingJobsByUser:     s.pendingJobsByUser.WithLabelValues(tenant),
			activeJobsByUser:      s.activeJobsByUser.WithLabelValues(tenant),
			pendingPlanJobs:       s.pendingJobs.WithLabelValues(jobTypePlan),
			pendingCompactionJobs: s.pendingJobs.WithLabelValues(jobTypeCompaction),
			activePlanJobs:        s.activeJobs.WithLabelValues(jobTypePlan),
			activeCompactionJobs:  s.activeJobs.WithLabelValues(jobTypeCompaction),
			incompleteSplitBytes:  s.incompleteSplitBytes,
			incompleteMergeBytes:  s.incompleteMergeBytes,
			splitBytes:            make(map[lane]uint64, len(s.incompleteSplitBytes)),
			mergeBytes:            make(map[lane]uint64, len(s.incompleteMergeBytes)),
			laneForJob:            s.lanePolicy.LaneForJob,
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
	q := m.queue
	for l, bytes := range q.splitBytes {
		q.incompleteSplitBytes[l].Sub(float64(bytes))
		delete(q.splitBytes, l)
	}
	for l, bytes := range q.mergeBytes {
		q.incompleteMergeBytes[l].Sub(float64(bytes))
		delete(q.mergeBytes, l)
	}
	q.pendingPlanJobs.Sub(float64(q.pendingPlanCount))
	q.pendingCompactionJobs.Sub(float64(q.pendingCompactionCount))
	q.activePlanJobs.Sub(float64(q.activePlanCount))
	q.activeCompactionJobs.Sub(float64(q.activeCompactionCount))
	q.pendingPlanCount = 0
	q.pendingCompactionCount = 0
	q.activePlanCount = 0
	q.activeCompactionCount = 0
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
	activePlanJobs        prometheus.Gauge
	activeCompactionJobs  prometheus.Gauge
	incompleteSplitBytes  map[lane]prometheus.Gauge
	incompleteMergeBytes  map[lane]prometheus.Gauge
	laneForJob            func(TrackedJob) lane

	// This tenant's contribution to the shared gauges, tracked so Clear() can subtract exactly
	// the right amount on tenant removal.
	splitBytes             map[lane]uint64
	mergeBytes             map[lane]uint64
	pendingPlanCount       int
	pendingCompactionCount int
	activePlanCount        int
	activeCompactionCount  int
	clear                  func()
}

func (q *queueMetrics) Pending(j TrackedJob) {
	q.incPending(j.ID() == planJobId)
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.addBytes(cj)
	}
}

func (q *queueMetrics) Leased(j TrackedJob) {
	isPlan := j.ID() == planJobId
	q.decPending(isPlan)
	q.incActive(isPlan)
}

// Recover records jobs restored from persisted state on startup.
func (q *queueMetrics) Recover(pending, leased []TrackedJob) {
	for _, j := range pending {
		q.Pending(j)
	}
	for _, j := range leased {
		q.incActive(j.ID() == planJobId)
		if cj, ok := j.(*TrackedCompactionJob); ok {
			q.addBytes(cj)
		}
	}
}

// Revive records a job moving from active back to pending (lease expired or cancelled).
func (q *queueMetrics) Revive(j TrackedJob) {
	isPlan := j.ID() == planJobId
	q.decActive(isPlan)
	q.incPending(isPlan)
}

// Complete records a job leaving the system from the active queue (success or failure).
func (q *queueMetrics) Complete(j TrackedJob) {
	q.decActive(j.ID() == planJobId)
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

// DropPending records a job leaving the system from the pending queue.
func (q *queueMetrics) DropPending(j TrackedJob) {
	q.decPending(j.ID() == planJobId)
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

func (q *queueMetrics) incPending(isPlan bool) {
	q.pendingJobsByUser.Inc()
	if isPlan {
		q.pendingPlanJobs.Inc()
		q.pendingPlanCount++
	} else {
		q.pendingCompactionJobs.Inc()
		q.pendingCompactionCount++
	}
}

func (q *queueMetrics) decPending(isPlan bool) {
	q.pendingJobsByUser.Dec()
	if isPlan {
		q.pendingPlanJobs.Dec()
		q.pendingPlanCount--
	} else {
		q.pendingCompactionJobs.Dec()
		q.pendingCompactionCount--
	}
}

func (q *queueMetrics) incActive(isPlan bool) {
	q.activeJobsByUser.Inc()
	if isPlan {
		q.activePlanJobs.Inc()
		q.activePlanCount++
	} else {
		q.activeCompactionJobs.Inc()
		q.activeCompactionCount++
	}
}

func (q *queueMetrics) decActive(isPlan bool) {
	q.activeJobsByUser.Dec()
	if isPlan {
		q.activePlanJobs.Dec()
		q.activePlanCount--
	} else {
		q.activeCompactionJobs.Dec()
		q.activeCompactionCount--
	}
}

func (q *queueMetrics) addBytes(cj *TrackedCompactionJob) {
	l := q.laneForJob(cj)
	if cj.value.isSplit {
		q.splitBytes[l] += cj.totalBlockBytes
		q.incompleteSplitBytes[l].Add(float64(cj.totalBlockBytes))
	} else {
		q.mergeBytes[l] += cj.totalBlockBytes
		q.incompleteMergeBytes[l].Add(float64(cj.totalBlockBytes))
	}
}

func (q *queueMetrics) subBytes(cj *TrackedCompactionJob) {
	l := q.laneForJob(cj)
	if cj.value.isSplit {
		q.splitBytes[l] -= cj.totalBlockBytes
		q.incompleteSplitBytes[l].Sub(float64(cj.totalBlockBytes))
	} else {
		q.mergeBytes[l] -= cj.totalBlockBytes
		q.incompleteMergeBytes[l].Sub(float64(cj.totalBlockBytes))
	}
}
