// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func at(hour, minute int) time.Time {
	return time.Date(2026, 1, 2, hour, minute, 0, 0, time.UTC)
}

func newTestSchedulerMetrics(reg prometheus.Registerer) *schedulerMetrics {
	return newSchedulerMetrics(reg, newSimpleLanePolicy())
}

func newTestJobTracker(clk clock.Clock) (*JobTracker, *prometheus.Registry) {
	return newTestJobTrackerWithPolicy(clk, newSimpleLanePolicy())
}

func newTestJobTrackerWithPolicy(clk clock.Clock, policy lanePolicy) (*JobTracker, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	metrics := newSchedulerMetrics(reg, policy)
	return NewJobTracker(&NopJobPersister{}, "test", clk, policy, infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger()), reg
}

type errJobPersister struct{ NopJobPersister }

func (e *errJobPersister) WriteAndDeleteJobs([]TrackedJob, []TrackedJob) error {
	return errors.New("write failed")
}

func TestJobTracker_Maintenance_Planning(t *testing.T) {
	leaseDuration := time.Minute // value does not matter
	planningInterval := time.Hour
	compactionWaitPeriod := 15 * time.Minute

	tests := map[string]struct {
		setup              func(jt *JobTracker)
		now                time.Time
		expectedPlan       bool
		expectedTransition bool
	}{
		"plans when there is no pending plan": {
			now:                at(3, 0),
			expectedPlan:       true,
			expectedTransition: true,
		},
		"skips when there is a pending plan": {
			setup: func(jt *JobTracker) {
				jt.toPendingBack(NewTrackedPlanJob(time.Now()))
			},
			now: at(3, 0),
		},
		"skips within compaction wait period": {
			setup: func(jt *JobTracker) {
				// (3:30 - 15m = 3:15).Truncate(1h) = 3:00, so 3:00 + 1h = 4:00 + compactionWaitPeriod
				jt.completePlanTime = at(3, 30)
			},
			now: at(4, 0).Add(compactionWaitPeriod - time.Minute),
		},
		"plans after compaction wait period": {
			setup: func(jt *JobTracker) {
				jt.completePlanTime = at(3, 30)
			},
			now:                at(4, 0).Add(compactionWaitPeriod),
			expectedPlan:       true,
			expectedTransition: true,
		},
		"plan lane transitions even when compaction lane already pending": {
			setup: func(jt *JobTracker) {
				jt.toPendingBack(NewTrackedCompactionJob("compactionId", &CompactionJob{}, 1, time.Now()))
			},
			now:                at(3, 0),
			expectedPlan:       true,
			expectedTransition: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clk := clock.NewMock()
			clk.Set(tc.now)
			jt, _ := newTestJobTracker(clk)
			if tc.setup != nil {
				tc.setup(jt)
			}

			transition, err := jt.Maintenance(leaseDuration, false, true, planningInterval, compactionWaitPeriod)
			require.NoError(t, err)

			if tc.expectedPlan {
				require.Contains(t, jt.incompleteJobs, planJobId)
				require.Equal(t, tc.expectedTransition, len(transition) > 0)
				require.True(t, jt.completePlanTime.IsZero())
			} else {
				require.Empty(t, transition)
			}
		})
	}

	t.Run("returns error on persist failure", func(t *testing.T) {
		metrics := newTestSchedulerMetrics(prometheus.NewPedanticRegistry())
		jt := NewJobTracker(&errJobPersister{}, "test", clock.New(), newSimpleLanePolicy(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

		transition, err := jt.Maintenance(leaseDuration, false, true, planningInterval, compactionWaitPeriod)
		require.Error(t, err)
		require.Empty(t, transition)
		require.NotContains(t, jt.incompleteJobs, planJobId)
	})

	t.Run("planning skipped when plan is false", func(t *testing.T) {
		metrics := newTestSchedulerMetrics(prometheus.NewPedanticRegistry())
		jt := NewJobTracker(&errJobPersister{}, "test", clock.New(), newSimpleLanePolicy(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())
		transition, err := jt.Maintenance(leaseDuration, false, false, planningInterval, compactionWaitPeriod)
		require.NoError(t, err)
		require.Empty(t, transition)
		require.NotContains(t, jt.incompleteJobs, planJobId)
	})

	t.Run("plans for next window when plan completion bleeds past interval boundary", func(t *testing.T) {
		// Previous plan: 1:30 + 25m = 1:55 start time.
		// If a plan completes at 2:03 (past the 2:00 boundary), the 2:00 + 25m = 2:25 should still be planned, not skipped.
		const planInterval = 30 * time.Minute
		const waitPeriod = 25 * time.Minute

		clk := clock.NewMock()
		clk.Set(at(2, 25))
		jt, _ := newTestJobTracker(clk)
		jt.completePlanTime = at(2, 3)

		transition, err := jt.Maintenance(leaseDuration, false, true, planInterval, waitPeriod)
		require.NoError(t, err)
		require.NotEmpty(t, transition)
		require.Contains(t, jt.incompleteJobs, planJobId)
	})
}

func TestJobTracker_recoverFrom(t *testing.T) {
	newAvailableCompaction := func(id string, order uint32) *TrackedCompactionJob {
		return NewTrackedCompactionJob(id, &CompactionJob{}, order, at(1, 0))
	}

	newLeasedCompaction := func(id string, order uint32, statusTime time.Time) *TrackedCompactionJob {
		j := NewTrackedCompactionJob(id, &CompactionJob{}, order, at(1, 0))
		j.MarkLeased(statusTime)
		return j
	}

	newCompleteCompaction := func(id string) *TrackedCompactionJob {
		j := NewTrackedCompactionJob(id, &CompactionJob{}, 0, at(1, 0))
		j.MarkComplete(at(2, 0))
		return j
	}

	newLeasedPlan := func(statusTime time.Time) *TrackedPlanJob {
		j := NewTrackedPlanJob(at(1, 0))
		j.MarkLeased(statusTime)
		return j
	}

	newCompletePlan := func(statusTime time.Time) *TrackedPlanJob {
		j := NewTrackedPlanJob(at(1, 0))
		j.MarkComplete(statusTime)
		return j
	}

	tests := map[string]struct {
		compactionJobs       []*TrackedCompactionJob
		planJob              *TrackedPlanJob
		expectedPending      []string
		expectedActive       []string
		expectedCompleteJobs []string
		expectedPlanLeased   bool
		expectedPlanTime     time.Time
	}{
		"no jobs": {},
		"available plan job goes to pending": {
			planJob:         NewTrackedPlanJob(at(1, 0)),
			expectedPending: []string{planJobId},
		},
		"leased plan job goes to active": {
			planJob:            newLeasedPlan(at(2, 0)),
			expectedActive:     []string{planJobId},
			expectedPlanLeased: true,
		},
		"complete plan job recovers completion time": {
			planJob:          newCompletePlan(at(2, 0)),
			expectedPlanTime: at(2, 0),
		},
		"compaction jobs distributed by status": {
			compactionJobs: []*TrackedCompactionJob{
				newAvailableCompaction("aa", 1),
				newLeasedCompaction("bb", 2, at(3, 0)),
				newCompleteCompaction("cc"),
			},
			expectedPending:      []string{"aa"},
			expectedActive:       []string{"bb"},
			expectedCompleteJobs: []string{"cc"},
		},
		"pending jobs sorted by order": {
			compactionJobs: []*TrackedCompactionJob{
				newAvailableCompaction("cc", 3),
				newAvailableCompaction("aa", 1),
				newAvailableCompaction("bb", 2),
			},
			expectedPending: []string{"aa", "bb", "cc"},
		},
		"active jobs sorted by status time": {
			compactionJobs: []*TrackedCompactionJob{
				newLeasedCompaction("cc", 1, at(3, 0)),
				newLeasedCompaction("aa", 2, at(1, 0)),
				newLeasedCompaction("bb", 3, at(2, 0)),
			},
			expectedActive: []string{"aa", "bb", "cc"},
		},
	}

	toSlice := func(l *list.List) []string {
		var s []string
		for e := l.Front(); e != nil; e = e.Next() {
			s = append(s, e.Value.(TrackedJob).ID())
		}
		return s
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jt, _ := newTestJobTracker(clock.NewMock())

			jt.recoverFrom(tc.compactionJobs, tc.planJob)
			pendingIDs := append(toSlice(jt.pending[planLane]), toSlice(jt.pending[compactionLane])...)
			require.Equal(t, tc.expectedPending, pendingIDs)

			require.Equal(t, tc.expectedActive, toSlice(jt.active))
			var completeIDs []string
			for _, j := range jt.completeCompactionJobs {
				completeIDs = append(completeIDs, j.ID())
			}
			require.Equal(t, tc.expectedCompleteJobs, completeIDs)
			require.Equal(t, tc.expectedPlanLeased, jt.isPlanJobLeased)
			require.Equal(t, tc.expectedPlanTime, jt.completePlanTime)
			require.Len(t, jt.incompleteJobs, len(tc.expectedPending)+len(tc.expectedActive))
		})
	}
}

type splitMerge struct{ split, merge float64 }

// trackerBytesAsserter builds an assertion over every lane of a policy; lanes left out of an
// expectation are asserted to be zero.
func trackerBytesAsserter(t *testing.T, reg *prometheus.Registry, lanes []lane) func(string, map[lane]splitMerge) {
	return func(msg string, want map[lane]splitMerge) {
		t.Helper()
		var sb strings.Builder
		sb.WriteString("# HELP cortex_compactor_scheduler_incomplete_compaction_jobs_bytes The total bytes of blocks in compaction jobs that have not yet completed (pending or active).\n")
		sb.WriteString("# TYPE cortex_compactor_scheduler_incomplete_compaction_jobs_bytes gauge\n")
		for _, l := range lanes {
			fmt.Fprintf(&sb, "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type=\"merge\",lane=%q} %g\n", l, want[l].merge)
			fmt.Fprintf(&sb, "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type=\"split\",lane=%q} %g\n", l, want[l].split)
		}
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(sb.String()), "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes"), msg)
	}
}

func TestJobTracker_ByteTracking(t *testing.T) {
	clk := clock.NewMock()
	policy := newUrgencyLanePolicy(testUrgencyConfig())
	jt, reg := newTestJobTrackerWithPolicy(clk, policy)
	assertBytes := trackerBytesAsserter(t, reg, policy.CompactionLanes())

	splitJob := NewTrackedCompactionJob("split-job", &CompactionJob{isSplit: true, totalBlockBytes: 100}, 1, clk.Now())
	// Spans 24h, so it lands in the defer lane.
	mergeJob := NewTrackedCompactionJob("merge-job", &CompactionJob{isSplit: false, maxTime: int64(24 * time.Hour / time.Millisecond), totalBlockBytes: 200}, 2, clk.Now())
	bothPending := map[lane]splitMerge{compactionUrgentLane: {split: 100}, compactionDeferLane: {merge: 200}}

	jt.recoverFrom([]*TrackedCompactionJob{splitJob, mergeJob}, nil)
	assertBytes("both jobs pending after recovery", bothPending)

	leaseResp, _, err := jt.Lease(compactionUrgentLane)
	require.NoError(t, err)
	assertBytes("split job leased (still incomplete)", bothPending)

	canceled, _, err := jt.CancelLease(leaseResp.Key.Id, leaseResp.Key.Epoch, false)
	require.NoError(t, err)
	require.True(t, canceled)
	assertBytes("split job revived to pending (bytes unchanged)", bothPending)

	leaseResp, _, err = jt.Lease(compactionUrgentLane)
	require.NoError(t, err)
	_, _, err = jt.Remove(leaseResp.Key.Id, leaseResp.Key.Epoch, true)
	require.NoError(t, err)
	assertBytes("split job complete", map[lane]splitMerge{compactionDeferLane: {merge: 200}})

	leaseResp, _, err = jt.Lease(compactionDeferLane)
	require.NoError(t, err)
	_, _, err = jt.Remove(leaseResp.Key.Id, leaseResp.Key.Epoch, true)
	require.NoError(t, err)
	assertBytes("merge job complete", nil)
}

func TestJobTracker_PlanJobTracking(t *testing.T) {
	clk := clock.NewMock()
	clk.Set(at(3, 0))
	jt, reg := newTestJobTracker(clk)

	assertPlanJobLocation := func(label string, pending, active int) {
		t.Helper()
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_compactor_scheduler_pending_jobs The number of queued pending jobs.
			# TYPE cortex_compactor_scheduler_pending_jobs gauge
			cortex_compactor_scheduler_pending_jobs{job_type="compaction"} 0
			cortex_compactor_scheduler_pending_jobs{job_type="plan"} %d
			# HELP cortex_compactor_scheduler_active_jobs The number of jobs active in workers.
			# TYPE cortex_compactor_scheduler_active_jobs gauge
			cortex_compactor_scheduler_active_jobs{job_type="compaction"} 0
			cortex_compactor_scheduler_active_jobs{job_type="plan"} %d
		`, pending, active)), "cortex_compactor_scheduler_pending_jobs", "cortex_compactor_scheduler_active_jobs"), label)
	}

	assertPlanJobLocation("no plan jobs yet", 0, 0)

	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 0)
	require.NoError(t, err)
	assertPlanJobLocation("plan job pending", 1, 0)

	leaseResp, _, err := jt.Lease(planLane)
	require.NoError(t, err)
	require.Equal(t, planJobId, leaseResp.Key.Id)
	assertPlanJobLocation("plan job active", 0, 1)

	canceled, _, err := jt.CancelLease(leaseResp.Key.Id, leaseResp.Key.Epoch, false)
	require.NoError(t, err)
	require.True(t, canceled)
	assertPlanJobLocation("plan job revived to pending", 1, 0)

	leaseResp, _, err = jt.Lease(planLane)
	require.NoError(t, err)
	_, _, err = jt.Remove(leaseResp.Key.Id, leaseResp.Key.Epoch, true)
	require.NoError(t, err)
	assertPlanJobLocation("plan job complete", 0, 0)
}

func TestJobTracker_Cleanup(t *testing.T) {
	clk := clock.NewMock()
	reg := prometheus.NewPedanticRegistry()
	sm := newTestSchedulerMetrics(reg)
	policy := newSimpleLanePolicy()
	assertBytes := trackerBytesAsserter(t, reg, policy.CompactionLanes())

	// Two tenants share the same aggregate gauges (incompleteJobsBytes, pendingJobs, activeJobs).
	jt1 := NewJobTracker(&NopJobPersister{}, "tenant1", clk, policy, infiniteLeases, infiniteLeases, sm.newTrackerMetricsForTenant("tenant1"), log.NewNopLogger())
	jt2 := NewJobTracker(&NopJobPersister{}, "tenant2", clk, policy, infiniteLeases, infiniteLeases, sm.newTrackerMetricsForTenant("tenant2"), log.NewNopLogger())

	jt1.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("split-job", &CompactionJob{isSplit: true, totalBlockBytes: 100}, 1, clk.Now()),
	}, nil)
	jt2.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("merge-job", &CompactionJob{isSplit: false, totalBlockBytes: 200}, 1, clk.Now()),
	}, nil)
	assertBytes("both tenants contributing before cleanup", map[lane]splitMerge{compactionLane: {split: 100, merge: 200}})

	// Set time past the first planning window to force planning on Maintenance()
	clk.Set(at(3, 0))
	_, err := jt1.Maintenance(time.Minute, false, true, time.Hour, 0)
	require.NoError(t, err)
	_, err = jt2.Maintenance(time.Minute, false, true, time.Hour, 0)
	require.NoError(t, err)

	// Lease both of tenant1's jobs
	for _, lane := range []lane{planLane, compactionLane} {
		_, _, err := jt1.Lease(lane)
		require.NoError(t, err)
	}

	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_pending_jobs The number of queued pending jobs.
		# TYPE cortex_compactor_scheduler_pending_jobs gauge
		cortex_compactor_scheduler_pending_jobs{job_type="compaction"} 1
		cortex_compactor_scheduler_pending_jobs{job_type="plan"} 1
		# HELP cortex_compactor_scheduler_active_jobs The number of jobs active in workers.
		# TYPE cortex_compactor_scheduler_active_jobs gauge
		cortex_compactor_scheduler_active_jobs{job_type="compaction"} 1
		cortex_compactor_scheduler_active_jobs{job_type="plan"} 1
	`), "cortex_compactor_scheduler_pending_jobs", "cortex_compactor_scheduler_active_jobs"), "tenant1 active, tenant2 pending")

	// Cleaning up tenant1 should only subtract its contribution, not zero the shared gauges.
	jt1.CleanupMetrics()
	assertBytes("only tenant1 bytes removed", map[lane]splitMerge{compactionLane: {merge: 200}})
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_pending_jobs_by_user The number of queued pending jobs, broken down by user.
		# TYPE cortex_compactor_scheduler_pending_jobs_by_user gauge
		cortex_compactor_scheduler_pending_jobs_by_user{user="tenant2"} 2
	`), "cortex_compactor_scheduler_pending_jobs_by_user"), "only tenant2 pending jobs remain")
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_pending_jobs The number of queued pending jobs.
		# TYPE cortex_compactor_scheduler_pending_jobs gauge
		cortex_compactor_scheduler_pending_jobs{job_type="compaction"} 1
		cortex_compactor_scheduler_pending_jobs{job_type="plan"} 1
		# HELP cortex_compactor_scheduler_active_jobs The number of jobs active in workers.
		# TYPE cortex_compactor_scheduler_active_jobs gauge
		cortex_compactor_scheduler_active_jobs{job_type="compaction"} 0
		cortex_compactor_scheduler_active_jobs{job_type="plan"} 0
	`), "cortex_compactor_scheduler_pending_jobs", "cortex_compactor_scheduler_active_jobs"), "tenant1's active contribution removed, tenant2's pending preserved")
}

func TestJobTracker_CancelLease_PlanJobAlwaysRevives(t *testing.T) {
	const maxLeases = 2

	clk := clock.NewMock()
	metrics := newTestSchedulerMetrics(prometheus.NewPedanticRegistry())
	jt := NewJobTracker(&NopJobPersister{}, "test", clk, newSimpleLanePolicy(), maxLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 15*time.Minute)
	require.NoError(t, err)

	for range maxLeases + 1 {
		job, _, err := jt.Lease(planLane)
		require.NoError(t, err)
		require.NotNil(t, job, "plan job should always be leaseable")

		canceled, _, err := jt.CancelLease(job.Key.Id, job.Key.Epoch, false)
		require.NoError(t, err)
		require.True(t, canceled)
	}
}

func TestJobTracker_CancelLease_Interrupted(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		planJob                  bool
		maxLeases                int
		threshold                int
		interrupted              []bool
		expectedDropped          bool
		expectedRepeatedFailures float64
	}{
		{
			name:        "interrupted reassigns never count or drop",
			maxLeases:   1,
			threshold:   1,
			interrupted: []bool{true, true},
		},
		{
			name:        "plan jobs never report interrupted reassigns",
			planJob:     true,
			threshold:   1,
			interrupted: []bool{true, true},
		},
		{
			name:                     "uninterrupted reassigns drop after maxLeases and report",
			maxLeases:                2,
			threshold:                1,
			interrupted:              []bool{false, true, false},
			expectedDropped:          true,
			expectedRepeatedFailures: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewMock()
			metrics := newTestSchedulerMetrics(prometheus.NewPedanticRegistry())
			jt := NewJobTracker(&NopJobPersister{}, "test", clk, newSimpleLanePolicy(), tc.maxLeases, tc.threshold, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

			lane := compactionLane
			if tc.planJob {
				lane = planLane
				_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 15*time.Minute)
				require.NoError(t, err)
			} else {
				jt.recoverFrom([]*TrackedCompactionJob{
					NewTrackedCompactionJob("merge-job", &CompactionJob{totalBlockBytes: 100}, 1, clk.Now()),
				}, nil)
			}

			for _, interrupted := range tc.interrupted {
				lease, _, err := jt.Lease(lane)
				require.NoError(t, err)
				require.NotNil(t, lease)

				canceled, _, err := jt.CancelLease(lease.Key.Id, lease.Key.Epoch, interrupted)
				require.NoError(t, err)
				require.True(t, canceled)
			}

			lease, _, err := jt.Lease(lane)
			require.NoError(t, err)
			require.Equal(t, tc.expectedDropped, lease == nil)
			require.Equal(t, tc.expectedRepeatedFailures, prom_testutil.ToFloat64(jt.metrics.repeatedJobFailures))
		})
	}
}
