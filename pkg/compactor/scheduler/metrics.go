// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/compactor/scheduler/compactorschedulerpb"
)

const (
	jobTypePlan             = "plan"
	jobTypeCompaction       = "compaction"
	jobTypeBackfillCleanup  = "backfill_cleanup"
	jobTypeBackfillValidate = "backfill_validate"
	jobTypeBackfillCopy     = "backfill_copy"

	compactionTypeSplit = "split"
	compactionTypeMerge = "merge"
)

// jobTypeLabel returns the job_type label value of a job type
func jobTypeLabel(t compactorschedulerpb.JobType) string {
	switch t {
	case compactorschedulerpb.JOB_TYPE_PLANNING:
		return jobTypePlan
	case compactorschedulerpb.JOB_TYPE_BACKFILL_CLEANUP:
		return jobTypeBackfillCleanup
	case compactorschedulerpb.JOB_TYPE_BACKFILL_VALIDATE:
		return jobTypeBackfillValidate
	case compactorschedulerpb.JOB_TYPE_BACKFILL_COPY:
		return jobTypeBackfillCopy
	}
	return jobTypeCompaction
}

// jobTypeLabels returns the job_type label values a scheduler can report in the given mode
func jobTypeLabels(backfillMode bool) []string {
	if backfillMode {
		return []string{jobTypePlan, jobTypeCompaction, jobTypeBackfillCleanup, jobTypeBackfillValidate, jobTypeBackfillCopy}
	}
	return []string{jobTypePlan, jobTypeCompaction}
}

type schedulerMetrics struct {
	pendingJobs              *prometheus.GaugeVec
	pendingJobsByType        map[string]prometheus.Gauge // cached children of pendingJobs for this mode's job types
	activeJobsByType         map[string]prometheus.Gauge // cached children of activeJobs for this mode's job types
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

func newSchedulerMetrics(reg prometheus.Registerer, lanePolicy lanePolicy, backfillMode bool) *schedulerMetrics {
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

	// Pre-initialize job type labels so we get zeros instead of no data. The gauges are also cached to avoid label lookups.
	jobTypes := jobTypeLabels(backfillMode)
	m.pendingJobsByType = make(map[string]prometheus.Gauge, len(jobTypes))
	m.activeJobsByType = make(map[string]prometheus.Gauge, len(jobTypes))
	for _, jobType := range jobTypes {
		m.jobsCompleted.WithLabelValues(jobType)
		m.pendingJobsByType[jobType] = m.pendingJobs.WithLabelValues(jobType)
		m.activeJobsByType[jobType] = m.activeJobs.WithLabelValues(jobType)
	}
	m.lanePendingJobsLastEmpty = make(map[lane]prometheus.Gauge, len(allLanes))
	for _, l := range allLanes {
		m.lanePendingJobsLastEmpty[l] = laneLastEmpty.WithLabelValues(l.String())
	}
	m.incompleteSplitBytes = make(map[lane]prometheus.Gauge, len(compactionLanes))
	m.incompleteMergeBytes = make(map[lane]prometheus.Gauge, len(compactionLanes))
	for _, l := range compactionLanes {
		m.incompleteSplitBytes[l] = m.incompleteJobsBytes.WithLabelValues(compactionTypeSplit, l.String())
		m.incompleteMergeBytes[l] = m.incompleteJobsBytes.WithLabelValues(compactionTypeMerge, l.String())
	}
	return m
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		queue: &queueMetrics{
			pendingJobsByUser:    s.pendingJobsByUser.WithLabelValues(tenant),
			activeJobsByUser:     s.activeJobsByUser.WithLabelValues(tenant),
			pendingJobs:          s.pendingJobsByType,
			activeJobs:           s.activeJobsByType,
			incompleteSplitBytes: s.incompleteSplitBytes,
			incompleteMergeBytes: s.incompleteMergeBytes,
			splitBytes:           make(map[lane]uint64, len(s.incompleteSplitBytes)),
			mergeBytes:           make(map[lane]uint64, len(s.incompleteMergeBytes)),
			pendingCounts:        make(map[string]int),
			activeCounts:         make(map[string]int),
			laneForJob:           s.lanePolicy.LaneForJob,
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
	for jobType, count := range q.pendingCounts {
		q.pendingJobs[jobType].Sub(float64(count))
		delete(q.pendingCounts, jobType)
	}
	for jobType, count := range q.activeCounts {
		q.activeJobs[jobType].Sub(float64(count))
		delete(q.activeCounts, jobType)
	}
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
	pendingJobs          map[string]prometheus.Gauge // by job type, read-only
	activeJobs           map[string]prometheus.Gauge // by job type, read-only
	incompleteSplitBytes map[lane]prometheus.Gauge
	incompleteMergeBytes map[lane]prometheus.Gauge
	laneForJob           func(TrackedJob) lane

	// This tenant's contribution to the shared gauges, tracked so Clear() can subtract exactly
	// the right amount on tenant removal.
	splitBytes    map[lane]uint64
	mergeBytes    map[lane]uint64
	pendingCounts map[string]int // by job type
	activeCounts  map[string]int // by job type
	clear         func()
}

func (q *queueMetrics) Pending(j TrackedJob) {
	q.incPending(jobTypeLabel(j.Type()))
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.addBytes(cj)
	}
}

func (q *queueMetrics) Leased(j TrackedJob) {
	jobType := jobTypeLabel(j.Type())
	q.decPending(jobType)
	q.incActive(jobType)
}

// Recover records jobs restored from persisted state on startup.
func (q *queueMetrics) Recover(pending, leased []TrackedJob) {
	for _, j := range pending {
		q.Pending(j)
	}
	for _, j := range leased {
		q.incActive(jobTypeLabel(j.Type()))
		if cj, ok := j.(*TrackedCompactionJob); ok {
			q.addBytes(cj)
		}
	}
}

// Revive records a job moving from active back to pending (lease expired or cancelled).
func (q *queueMetrics) Revive(j TrackedJob) {
	jobType := jobTypeLabel(j.Type())
	q.decActive(jobType)
	q.incPending(jobType)
}

// Complete records a job leaving the system from the active queue (success or failure).
func (q *queueMetrics) Complete(j TrackedJob) {
	q.decActive(jobTypeLabel(j.Type()))
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

// DropPending records a job leaving the system from the pending queue.
func (q *queueMetrics) DropPending(j TrackedJob) {
	q.decPending(jobTypeLabel(j.Type()))
	if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

func (q *queueMetrics) incPending(jobType string) {
	q.pendingJobsByUser.Inc()
	q.pendingJobs[jobType].Inc()
	q.pendingCounts[jobType]++
}

func (q *queueMetrics) decPending(jobType string) {
	q.pendingJobsByUser.Dec()
	q.pendingJobs[jobType].Dec()
	q.pendingCounts[jobType]--
}

func (q *queueMetrics) incActive(jobType string) {
	q.activeJobsByUser.Inc()
	q.activeJobs[jobType].Inc()
	q.activeCounts[jobType]++
}

func (q *queueMetrics) decActive(jobType string) {
	q.activeJobsByUser.Dec()
	q.activeJobs[jobType].Dec()
	q.activeCounts[jobType]--
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
