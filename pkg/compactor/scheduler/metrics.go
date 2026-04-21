// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/compactor"
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
	incompletePlanJobs  prometheus.Gauge
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
		incompleteJobsBytes: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes",
			Help: "The total bytes of blocks in compaction jobs that have not yet completed (pending or active).",
		}, []string{"compaction_type", compactor.SizeBucketLabel}),
		incompletePlanJobs: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_compactor_incomplete_plan_jobs",
			Help: "The total number of plan jobs that have not yet completed (pending or active).",
		}),
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
	// Pre-initialize all label combinations so every series exists with value zero. This gives
	// the autoscaler stable PromQL output when the queue is empty.
	m.jobsCompleted.WithLabelValues(jobTypePlan)
	m.jobsCompleted.WithLabelValues(jobTypeCompaction)
	for _, ct := range []string{compactionTypeSplit, compactionTypeMerge} {
		for _, sb := range compactor.AllSizeBuckets() {
			m.incompleteJobsBytes.WithLabelValues(ct, sb)
		}
	}
	return m
}

func (s *schedulerMetrics) newTrackerMetricsForTenant(tenant string) *trackerMetrics {
	return &trackerMetrics{
		queue: &queueMetrics{
			pendingJobs:         s.pendingJobs.WithLabelValues(tenant),
			activeJobs:          s.activeJobs.WithLabelValues(tenant),
			incompleteJobsBytes: s.incompleteJobsBytes,
			incompletePlanJobs:  s.incompletePlanJobs,
			perBucketBytes:      map[sizeBucketKey]uint64{},
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

// Clear deletes all per-tenant label values and subtracts this tenant's contribution from the
// shared gauges. Must be called when a tenant is removed.
func (m *trackerMetrics) Clear() {
	for k, v := range m.queue.perBucketBytes {
		m.queue.incompleteJobsBytes.WithLabelValues(k.compactionType, k.sizeBucket).Sub(float64(v))
	}
	m.queue.incompletePlanJobs.Sub(float64(m.queue.planJobCount))
	m.queue.perBucketBytes = map[sizeBucketKey]uint64{}
	m.queue.planJobCount = 0
	m.queue.clear()
}

// sizeBucketKey identifies a (compaction_type, size_bucket) pair for tracking per-tenant
// contributions to the shared incompleteJobsBytes gauge.
type sizeBucketKey struct {
	compactionType string
	sizeBucket     string
}

// queueMetrics encapsulates queue-level metrics for one tenant, allowing the caller to ignore
// the details of which metrics to update and how, focusing only on job state transitions.
// Callers are responsible for making valid transitions. Invalid calls (e.g. DropPending on an
// empty queue) will produce incorrect gauge values. Methods are not thread-safe.
type queueMetrics struct {
	pendingJobs prometheus.Gauge
	activeJobs  prometheus.Gauge

	// shared across tenants
	incompleteJobsBytes *prometheus.GaugeVec
	incompletePlanJobs  prometheus.Gauge

	// perBucketBytes and planJobCount track this tenant's contribution to the shared gauges
	// so Clear can subtract exactly the right amount on tenant removal.
	perBucketBytes map[sizeBucketKey]uint64
	planJobCount   int
	clear          func()
}

func (q *queueMetrics) Pending(j TrackedJob) {
	q.pendingJobs.Inc()
	if j.ID() == planJobId {
		q.incompletePlanJobs.Inc()
		q.planJobCount++
	} else {
		q.addBytes(j.(*TrackedCompactionJob))
	}
}

func (q *queueMetrics) Leased(j TrackedJob) {
	q.pendingJobs.Dec()
	q.activeJobs.Inc()
}

// Recover records jobs restored from persisted state on startup.
func (q *queueMetrics) Recover(pending, leased []TrackedJob) {
	for _, j := range pending {
		q.pendingJobs.Inc()
		if j.ID() == planJobId {
			q.incompletePlanJobs.Inc()
			q.planJobCount++
		} else {
			q.addBytes(j.(*TrackedCompactionJob))
		}
	}
	for _, j := range leased {
		q.activeJobs.Inc()
		if j.ID() == planJobId {
			q.incompletePlanJobs.Inc()
			q.planJobCount++
		} else if cj, ok := j.(*TrackedCompactionJob); ok {
			q.addBytes(cj)
		}
	}
}

// Revive records a job moving from active back to pending (lease expired or cancelled).
func (q *queueMetrics) Revive(j TrackedJob) {
	q.activeJobs.Dec()
	q.pendingJobs.Inc()
}

// Complete records a job leaving the system from the active queue (success or failure).
func (q *queueMetrics) Complete(j TrackedJob) {
	q.activeJobs.Dec()
	if j.ID() == planJobId {
		q.incompletePlanJobs.Dec()
		q.planJobCount--
	} else if cj, ok := j.(*TrackedCompactionJob); ok {
		q.subBytes(cj)
	}
}

// DropPending records a job leaving the system from the pending queue.
func (q *queueMetrics) DropPending(j TrackedJob) {
	q.pendingJobs.Dec()
	if j.ID() == planJobId {
		q.incompletePlanJobs.Dec()
		q.planJobCount--
	} else {
		q.subBytes(j.(*TrackedCompactionJob))
	}
}

func (q *queueMetrics) addBytes(cj *TrackedCompactionJob) {
	k := jobBucketKey(cj)
	q.perBucketBytes[k] += cj.totalBlockBytes
	q.incompleteJobsBytes.WithLabelValues(k.compactionType, k.sizeBucket).Add(float64(cj.totalBlockBytes))
}

func (q *queueMetrics) subBytes(cj *TrackedCompactionJob) {
	k := jobBucketKey(cj)
	q.perBucketBytes[k] -= cj.totalBlockBytes
	q.incompleteJobsBytes.WithLabelValues(k.compactionType, k.sizeBucket).Sub(float64(cj.totalBlockBytes))
}

func jobBucketKey(cj *TrackedCompactionJob) sizeBucketKey {
	ct := compactionTypeMerge
	if cj.value.isSplit {
		ct = compactionTypeSplit
	}
	return sizeBucketKey{compactionType: ct, sizeBucket: compactor.SizeBucket(cj.totalBlockBytes)}
}
