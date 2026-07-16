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

func newTestJobTracker(clk clock.Clock) (*JobTracker, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	metrics := newSchedulerMetrics(reg)
	return NewJobTracker(&NopJobPersister{}, "test", clk, newSimpleLanePolicy(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger()), reg
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
				jt.toPendingBack(NewTrackedCompactionJob("compactionId", &CompactionJob{}, 1, 0, time.Now()))
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

			transition, err := jt.Maintenance(leaseDuration, false, true, planningInterval, 0, compactionWaitPeriod)
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
		metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
		jt := NewJobTracker(&errJobPersister{}, "test", clock.New(), newSimpleLanePolicy(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

		transition, err := jt.Maintenance(leaseDuration, false, true, planningInterval, 0, compactionWaitPeriod)
		require.Error(t, err)
		require.Empty(t, transition)
		require.NotContains(t, jt.incompleteJobs, planJobId)
	})

	t.Run("planning skipped when plan is false", func(t *testing.T) {
		metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
		jt := NewJobTracker(&errJobPersister{}, "test", clock.New(), newSimpleLanePolicy(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())
		transition, err := jt.Maintenance(leaseDuration, false, false, planningInterval, 0, compactionWaitPeriod)
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

		transition, err := jt.Maintenance(leaseDuration, false, true, planInterval, 0, waitPeriod)
		require.NoError(t, err)
		require.NotEmpty(t, transition)
		require.Contains(t, jt.incompleteJobs, planJobId)
	})
}

func TestJobTracker_Maintenance_Cleanup(t *testing.T) {
	leaseDuration := time.Minute // value does not matter
	cleanupInterval := 15 * time.Minute

	tests := map[string]struct {
		setup           func(jt *JobTracker)
		now             time.Time
		cleanupInterval time.Duration
		expectedCleanup bool
	}{
		"cleans up when there is no pending cleanup": {
			now:             at(3, 0),
			cleanupInterval: cleanupInterval,
			expectedCleanup: true,
		},
		"skips when there is a pending cleanup": {
			setup: func(jt *JobTracker) {
				jt.toPendingBack(NewTrackedCleanupJob(time.Now()))
			},
			now:             at(3, 0),
			cleanupInterval: cleanupInterval,
		},
		"skips within current cleanup window": {
			setup: func(jt *JobTracker) {
				// 3:00.Truncate(15m) = 3:00, so the window runs until 3:15.
				jt.completeCleanupTime = at(3, 0)
			},
			now:             at(3, 10),
			cleanupInterval: cleanupInterval,
		},
		"cleans up for next window after completion": {
			setup: func(jt *JobTracker) {
				jt.completeCleanupTime = at(3, 0)
			},
			now:             at(3, 15),
			cleanupInterval: cleanupInterval,
			expectedCleanup: true,
		},
		"disabled when interval is nonpositive": {
			now:             at(3, 0),
			cleanupInterval: 0,
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

			// plan is false to isolate cleanup, which is evaluated independent of the planning gate.
			transition, err := jt.Maintenance(leaseDuration, false, false, 0, tc.cleanupInterval, 0)
			require.NoError(t, err)

			if tc.expectedCleanup {
				require.Contains(t, jt.incompleteJobs, cleanupJobId)
				require.NotEmpty(t, transition)
				require.True(t, jt.completeCleanupTime.IsZero())
			} else {
				require.Empty(t, transition)
			}
		})
	}
}

func TestJobTracker_CompleteCleanup_RequiresCleanupJob(t *testing.T) {
	clk := clock.NewMock()
	clk.Set(at(3, 0))
	jt, _ := newTestJobTracker(clk)

	jt.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("aa", &CompactionJob{}, 1, 100, clk.Now()),
	}, nil, nil)

	leaseResp, _, err := jt.Lease(compactionLane)
	require.NoError(t, err)

	// A compaction job key must not be completable through the cleanup path.
	completed, err := jt.CompleteCleanup(leaseResp.Key.Id, leaseResp.Key.Epoch)
	require.NoError(t, err)
	require.False(t, completed)
	require.True(t, jt.completeCleanupTime.IsZero())
	require.Contains(t, jt.incompleteJobs, "aa")
}

// deleteRecordingPersister records the IDs of jobs deleted via WriteAndDeleteJobs.
type deleteRecordingPersister struct {
	NopJobPersister
	deleted []string
}

func (p *deleteRecordingPersister) WriteAndDeleteJobs(_, deletes []TrackedJob) error {
	for _, j := range deletes {
		p.deleted = append(p.deleted, j.ID())
	}
	return nil
}

func TestJobTracker_OfferCompactionJobs_PreservesPendingCleanup(t *testing.T) {
	clk := clock.NewMock()
	clk.Set(at(3, 0))

	persister := &deleteRecordingPersister{}
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	jt := NewJobTracker(persister, "test", clk, newSimpleLanePolicy(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

	// Create a pending plan job and a pending cleanup job, then lease the plan job.
	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 15*time.Minute, 0)
	require.NoError(t, err)
	require.Contains(t, jt.incompleteJobs, cleanupJobId)

	planLease, _, err := jt.Lease(planLane)
	require.NoError(t, err)
	require.Equal(t, planJobId, planLease.Key.Id)

	// Completing the plan replaces pending compaction work, but the independent cleanup job must survive.
	offered := []*TrackedCompactionJob{NewTrackedCompactionJob("compaction-1", &CompactionJob{}, 1, 0, clk.Now())}
	accepted, found, _, err := jt.OfferCompactionJobs(offered, planLease.Key.Epoch)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, 1, accepted)

	require.Contains(t, jt.incompleteJobs, cleanupJobId, "pending cleanup job must remain tracked")
	require.Contains(t, jt.incompleteJobs, "compaction-1")
	require.Equal(t, 1, jt.pending[cleanupLane].Len(), "cleanup job must remain pending in its lane")
	require.NotContains(t, persister.deleted, cleanupJobId, "cleanup job must not be deleted from persistence")
}

func TestJobTracker_recoverFrom(t *testing.T) {
	newAvailableCompaction := func(id string, order uint32) *TrackedCompactionJob {
		return NewTrackedCompactionJob(id, &CompactionJob{}, order, 0, at(1, 0))
	}

	newLeasedCompaction := func(id string, order uint32, statusTime time.Time) *TrackedCompactionJob {
		j := NewTrackedCompactionJob(id, &CompactionJob{}, order, 0, at(1, 0))
		j.MarkLeased(statusTime)
		return j
	}

	newCompleteCompaction := func(id string) *TrackedCompactionJob {
		j := NewTrackedCompactionJob(id, &CompactionJob{}, 0, 0, at(1, 0))
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
	newCompleteCleanup := func(statusTime time.Time) *TrackedCleanupJob {
		j := NewTrackedCleanupJob(at(1, 0))
		j.MarkComplete(statusTime)
		return j
	}

	tests := map[string]struct {
		compactionJobs       []*TrackedCompactionJob
		planJob              *TrackedPlanJob
		cleanupJob           *TrackedCleanupJob
		expectedPending      []string
		expectedActive       []string
		expectedCompleteJobs []string
		expectedPlanLeased   bool
		expectedPlanTime     time.Time
		expectedCleanupTime  time.Time
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
		"complete cleanup job recovers completion time": {
			cleanupJob:          newCompleteCleanup(at(3, 0)),
			expectedCleanupTime: at(3, 0),
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

			jt.recoverFrom(tc.compactionJobs, tc.planJob, tc.cleanupJob)
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

func assertTrackerBytes(t *testing.T, reg *prometheus.Registry, msg string, splitBytes, mergeBytes float64) {
	t.Helper()
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_compactor_scheduler_incomplete_compaction_jobs_bytes The total bytes of blocks in compaction jobs that have not yet completed (pending or active).
		# TYPE cortex_compactor_scheduler_incomplete_compaction_jobs_bytes gauge
		cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="merge"} %g
		cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="split"} %g
	`, mergeBytes, splitBytes)), "cortex_compactor_scheduler_incomplete_compaction_jobs_bytes"), msg)
}

func TestJobTracker_ByteTracking(t *testing.T) {
	clk := clock.NewMock()
	jt, reg := newTestJobTracker(clk)

	splitJob := NewTrackedCompactionJob("split-job", &CompactionJob{isSplit: true}, 1, 100, clk.Now())
	mergeJob := NewTrackedCompactionJob("merge-job", &CompactionJob{isSplit: false}, 2, 200, clk.Now())

	jt.recoverFrom([]*TrackedCompactionJob{splitJob, mergeJob}, nil, nil)
	assertTrackerBytes(t, reg, "both jobs pending after recovery", 100, 200)

	leaseResp, _, err := jt.Lease(compactionLane)
	require.NoError(t, err)
	assertTrackerBytes(t, reg, "split job leased (still incomplete)", 100, 200)

	canceled, _, err := jt.CancelLease(leaseResp.Key.Id, leaseResp.Key.Epoch, false)
	require.NoError(t, err)
	require.True(t, canceled)
	assertTrackerBytes(t, reg, "split job revived to pending (bytes unchanged)", 100, 200)

	leaseResp, _, err = jt.Lease(compactionLane)
	require.NoError(t, err)
	_, _, err = jt.Remove(leaseResp.Key.Id, leaseResp.Key.Epoch, true)
	require.NoError(t, err)
	assertTrackerBytes(t, reg, "split job complete", 0, 200)

	leaseResp, _, err = jt.Lease(compactionLane)
	require.NoError(t, err)
	_, _, err = jt.Remove(leaseResp.Key.Id, leaseResp.Key.Epoch, true)
	require.NoError(t, err)
	assertTrackerBytes(t, reg, "merge job complete", 0, 0)
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
			cortex_compactor_scheduler_pending_jobs{job_type="cleanup"} 0
			cortex_compactor_scheduler_pending_jobs{job_type="compaction"} 0
			cortex_compactor_scheduler_pending_jobs{job_type="plan"} %d
			# HELP cortex_compactor_scheduler_active_jobs The number of jobs active in workers.
			# TYPE cortex_compactor_scheduler_active_jobs gauge
			cortex_compactor_scheduler_active_jobs{job_type="cleanup"} 0
			cortex_compactor_scheduler_active_jobs{job_type="compaction"} 0
			cortex_compactor_scheduler_active_jobs{job_type="plan"} %d
		`, pending, active)), "cortex_compactor_scheduler_pending_jobs", "cortex_compactor_scheduler_active_jobs"), label)
	}

	assertPlanJobLocation("no plan jobs yet", 0, 0)

	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 0, 0)
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

func TestJobTracker_CleanupMetrics(t *testing.T) {
	clk := clock.NewMock()
	reg := prometheus.NewPedanticRegistry()
	sm := newSchedulerMetrics(reg)

	// Two tenants share the same aggregate gauges (incompleteJobsBytes, pendingJobs, activeJobs).
	jt1 := NewJobTracker(&NopJobPersister{}, "tenant1", clk, newSimpleLanePolicy(), infiniteLeases, infiniteLeases, sm.newTrackerMetricsForTenant("tenant1"), log.NewNopLogger())
	jt2 := NewJobTracker(&NopJobPersister{}, "tenant2", clk, newSimpleLanePolicy(), infiniteLeases, infiniteLeases, sm.newTrackerMetricsForTenant("tenant2"), log.NewNopLogger())

	jt1.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("split-job", &CompactionJob{isSplit: true}, 1, 100, clk.Now()),
	}, nil, nil)
	jt2.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("merge-job", &CompactionJob{isSplit: false}, 1, 200, clk.Now()),
	}, nil, nil)
	assertTrackerBytes(t, reg, "both tenants contributing before cleanup", 100, 200)

	// Set time past the first planning window to force planning on Maintenance()
	clk.Set(at(3, 0))
	_, err := jt1.Maintenance(time.Minute, false, true, time.Hour, 0, 0)
	require.NoError(t, err)
	_, err = jt2.Maintenance(time.Minute, false, true, time.Hour, 0, 0)
	require.NoError(t, err)

	// Lease both of tenant1's jobs
	for _, lane := range []lane{planLane, compactionLane} {
		_, _, err := jt1.Lease(lane)
		require.NoError(t, err)
	}

	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_pending_jobs The number of queued pending jobs.
		# TYPE cortex_compactor_scheduler_pending_jobs gauge
		cortex_compactor_scheduler_pending_jobs{job_type="cleanup"} 0
		cortex_compactor_scheduler_pending_jobs{job_type="compaction"} 1
		cortex_compactor_scheduler_pending_jobs{job_type="plan"} 1
		# HELP cortex_compactor_scheduler_active_jobs The number of jobs active in workers.
		# TYPE cortex_compactor_scheduler_active_jobs gauge
		cortex_compactor_scheduler_active_jobs{job_type="cleanup"} 0
		cortex_compactor_scheduler_active_jobs{job_type="compaction"} 1
		cortex_compactor_scheduler_active_jobs{job_type="plan"} 1
	`), "cortex_compactor_scheduler_pending_jobs", "cortex_compactor_scheduler_active_jobs"), "tenant1 active, tenant2 pending")

	// Cleaning up tenant1 should only subtract its contribution, not zero the shared gauges.
	jt1.CleanupMetrics()
	assertTrackerBytes(t, reg, "only tenant1 bytes removed", 0, 200)
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_pending_jobs_by_user The number of queued pending jobs, broken down by user.
		# TYPE cortex_compactor_scheduler_pending_jobs_by_user gauge
		cortex_compactor_scheduler_pending_jobs_by_user{user="tenant2"} 2
	`), "cortex_compactor_scheduler_pending_jobs_by_user"), "only tenant2 pending jobs remain")
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_pending_jobs The number of queued pending jobs.
		# TYPE cortex_compactor_scheduler_pending_jobs gauge
		cortex_compactor_scheduler_pending_jobs{job_type="cleanup"} 0
		cortex_compactor_scheduler_pending_jobs{job_type="compaction"} 1
		cortex_compactor_scheduler_pending_jobs{job_type="plan"} 1
		# HELP cortex_compactor_scheduler_active_jobs The number of jobs active in workers.
		# TYPE cortex_compactor_scheduler_active_jobs gauge
		cortex_compactor_scheduler_active_jobs{job_type="cleanup"} 0
		cortex_compactor_scheduler_active_jobs{job_type="compaction"} 0
		cortex_compactor_scheduler_active_jobs{job_type="plan"} 0
	`), "cortex_compactor_scheduler_pending_jobs", "cortex_compactor_scheduler_active_jobs"), "tenant1's active contribution removed, tenant2's pending preserved")
}

func TestJobTracker_CancelLease_PlanJobAlwaysRevives(t *testing.T) {
	const maxLeases = 2

	clk := clock.NewMock()
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	jt := NewJobTracker(&NopJobPersister{}, "test", clk, newSimpleLanePolicy(), maxLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 0, 15*time.Minute)
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

func TestJobTracker_CancelLease_CleanupJobAlwaysRevives(t *testing.T) {
	const maxLeases = 2

	clk := clock.NewMock()
	clk.Set(at(3, 0))
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	jt := NewJobTracker(&NopJobPersister{}, "test", clk, newSimpleLanePolicy(), maxLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

	_, err := jt.Maintenance(time.Minute, false, false, 0, 15*time.Minute, 0)
	require.NoError(t, err)

	for range maxLeases + 1 {
		job, _, err := jt.Lease(cleanupLane)
		require.NoError(t, err)
		require.NotNil(t, job, "cleanup job should always be leaseable")
		require.Equal(t, cleanupJobId, job.Key.Id)

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
			metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
			jt := NewJobTracker(&NopJobPersister{}, "test", clk, newSimpleLanePolicy(), tc.maxLeases, tc.threshold, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

			lane := compactionLane
			if tc.planJob {
				lane = planLane
				_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 0, 15*time.Minute)
				require.NoError(t, err)
			} else {
				jt.recoverFrom([]*TrackedCompactionJob{
					NewTrackedCompactionJob("merge-job", &CompactionJob{}, 1, 100, clk.Now()),
				}, nil, nil)
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
