// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"errors"
	"fmt"
	"slices"
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
	return newTestJobTrackerWithLanePolicy(clk, newSimpleLanePolicy())
}

func newTestJobTrackerWithLanePolicy(clk clock.Clock, lanePolicy lanePolicy) (*JobTracker, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	metrics := newSchedulerMetrics(reg, lanePolicy)
	jt := NewJobTracker(&NopJobPersister{}, "test", clk, lanePolicy, infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())
	jt.discoveredBy.Store(uint32(discoveredByBlocks))
	return jt, reg
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
		setup                     func(jt *JobTracker)
		now                       time.Time
		expectedPlan              bool
		expectedBackfillPhasePlan bool
		expectedTransition        bool
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
				jt.plan.completeTime = at(3, 30)
			},
			now: at(4, 0).Add(compactionWaitPeriod - time.Minute),
		},
		"plans after compaction wait period": {
			setup: func(jt *JobTracker) {
				jt.plan.completeTime = at(3, 30)
			},
			now:                at(4, 0).Add(compactionWaitPeriod),
			expectedPlan:       true,
			expectedTransition: true,
		},
		"skips when not found in the blocks bucket": {
			setup: func(jt *JobTracker) {
				jt.discoveredBy.Store(uint32(discoveredByBackfills))
			},
			now:                       at(3, 0),
			expectedBackfillPhasePlan: true,
			expectedTransition:        true,
		},
		"plans both when found in the blocks bucket and backfills": {
			setup: func(jt *JobTracker) {
				jt.discoveredBy.Store(uint32(discoveredByBlocks | discoveredByBackfills))
			},
			now:                       at(3, 0),
			expectedPlan:              true,
			expectedBackfillPhasePlan: true,
			expectedTransition:        true,
		},
		"skips backfill phase plan when there is a pending one": {
			setup: func(jt *JobTracker) {
				jt.discoveredBy.Store(uint32(discoveredByBackfills))
				jt.toPendingBack(NewTrackedBackfillPhasePlanJob(time.Now()))
			},
			now:                       at(3, 0),
			expectedBackfillPhasePlan: true, // the pending one remains
		},
		"skips backfill phase plan within the planning window": {
			setup: func(jt *JobTracker) {
				jt.discoveredBy.Store(uint32(discoveredByBackfills))
				jt.backfillPhasePlan.completeTime = at(3, 30)
			},
			now: at(4, 0).Add(-time.Minute),
		},
		"plans backfill phase plan in the next window without a compaction wait period": {
			setup: func(jt *JobTracker) {
				jt.discoveredBy.Store(uint32(discoveredByBackfills))
				jt.backfillPhasePlan.completeTime = at(3, 30)
			},
			now:                       at(4, 0),
			expectedBackfillPhasePlan: true,
			expectedTransition:        true,
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
			jt, _ := newTestJobTrackerWithLanePolicy(clk, newBackfillLanePolicy())
			if tc.setup != nil {
				tc.setup(jt)
			}

			transition, err := jt.Maintenance(leaseDuration, false, true, planningInterval, compactionWaitPeriod)
			require.NoError(t, err)

			require.Equal(t, tc.expectedTransition, len(transition) > 0)
			// Only a newly created plan job makes the plan lane transition
			require.Equal(t, tc.expectedPlan, slices.Contains(transition, planLane))
			if tc.expectedPlan {
				require.Contains(t, jt.incompleteJobs, planJobId)
				require.True(t, jt.plan.completeTime.IsZero())
			}
			if tc.expectedBackfillPhasePlan {
				require.Contains(t, jt.incompleteJobs, backfillPhasePlanJobId)
				require.True(t, jt.backfillPhasePlan.completeTime.IsZero())
			} else {
				require.NotContains(t, jt.incompleteJobs, backfillPhasePlanJobId)
			}
		})
	}

	t.Run("returns error on persist failure", func(t *testing.T) {
		jt, _ := newTestJobTracker(clock.New())
		jt.persister = &errJobPersister{}

		transition, err := jt.Maintenance(leaseDuration, false, true, planningInterval, compactionWaitPeriod)
		require.Error(t, err)
		require.Empty(t, transition)
		require.NotContains(t, jt.incompleteJobs, planJobId)
	})

	t.Run("planning skipped when plan is false", func(t *testing.T) {
		jt, _ := newTestJobTracker(clock.New())
		jt.persister = &errJobPersister{}
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
		jt.plan.completeTime = at(2, 3)

		transition, err := jt.Maintenance(leaseDuration, false, true, planInterval, waitPeriod)
		require.NoError(t, err)
		require.NotEmpty(t, transition)
		require.Contains(t, jt.incompleteJobs, planJobId)
	})
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

	newCompleteBackfillCompaction := func(id string) *TrackedCompactionJob {
		j := NewTrackedCompactionJob(id, &CompactionJob{backfillID: []byte("01K7ZQ3W8V9C2N5R6T4Y1HXJ0M")}, 0, 0, at(1, 0))
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

	newLeasedBackfillPhasePlan := func(statusTime time.Time) *TrackedBackfillPhasePlanJob {
		j := NewTrackedBackfillPhasePlanJob(at(1, 0))
		j.MarkLeased(statusTime)
		return j
	}

	newCompleteBackfillPhasePlan := func(statusTime time.Time) *TrackedBackfillPhasePlanJob {
		j := NewTrackedBackfillPhasePlanJob(at(1, 0))
		j.MarkComplete(statusTime)
		return j
	}

	type expectedPlanner struct {
		completeTime time.Time
		completeJobs []string
	}

	tests := map[string]struct {
		compactionJobs            []*TrackedCompactionJob
		planJob                   *TrackedPlanJob
		backfillPhasePlanJob      *TrackedBackfillPhasePlanJob
		expectedPending           []string
		expectedActive            []string
		expectedPlan              expectedPlanner
		expectedBackfillPhasePlan expectedPlanner
	}{
		"no jobs": {},
		"available backfill phase plan job goes to pending": {
			backfillPhasePlanJob: NewTrackedBackfillPhasePlanJob(at(1, 0)),
			expectedPending:      []string{backfillPhasePlanJobId},
		},
		"leased backfill phase plan job goes to active": {
			backfillPhasePlanJob: newLeasedBackfillPhasePlan(at(2, 0)),
			expectedActive:       []string{backfillPhasePlanJobId},
		},
		"complete backfill phase plan job recovers completion time": {
			backfillPhasePlanJob:      newCompleteBackfillPhasePlan(at(2, 0)),
			expectedBackfillPhasePlan: expectedPlanner{completeTime: at(2, 0)},
		},
		"available plan job goes to pending": {
			planJob:         NewTrackedPlanJob(at(1, 0)),
			expectedPending: []string{planJobId},
		},
		"leased plan job goes to active": {
			planJob:        newLeasedPlan(at(2, 0)),
			expectedActive: []string{planJobId},
		},
		"complete plan job recovers completion time": {
			planJob:      newCompletePlan(at(2, 0)),
			expectedPlan: expectedPlanner{completeTime: at(2, 0)},
		},
		"compaction jobs distributed by status": {
			compactionJobs: []*TrackedCompactionJob{
				newAvailableCompaction("aa", 1),
				newLeasedCompaction("bb", 2, at(3, 0)),
				newCompleteCompaction("cc"),
			},
			expectedPending: []string{"aa"},
			expectedActive:  []string{"bb"},
			expectedPlan:    expectedPlanner{completeJobs: []string{"cc"}},
		},
		"complete compaction jobs are retained by the plan job that planned them": {
			compactionJobs: []*TrackedCompactionJob{
				newCompleteCompaction("cc"),
				newCompleteBackfillCompaction("dd"),
			},
			expectedPlan:              expectedPlanner{completeJobs: []string{"cc"}},
			expectedBackfillPhasePlan: expectedPlanner{completeJobs: []string{"dd"}},
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
			jt, _ := newTestJobTrackerWithLanePolicy(clock.NewMock(), newBackfillLanePolicy())

			jt.recoverFrom(tc.compactionJobs, tc.planJob, tc.backfillPhasePlanJob)
			pendingIDs := append(toSlice(jt.pending[planLane]), toSlice(jt.pending[compactionLane])...)
			pendingIDs = append(pendingIDs, toSlice(jt.pending[backfillPhasePlanLane])...)
			require.Equal(t, tc.expectedPending, pendingIDs)

			require.Equal(t, tc.expectedActive, toSlice(jt.active))
			requirePlanner := func(expected expectedPlanner, ps *plannerState) {
				t.Helper()
				var completeIDs []string
				for _, j := range ps.completeJobs {
					completeIDs = append(completeIDs, j.ID())
				}
				require.Equal(t, expected.completeJobs, completeIDs)
				require.Equal(t, expected.completeTime, ps.completeTime)
			}
			requirePlanner(tc.expectedPlan, &jt.plan)
			requirePlanner(tc.expectedBackfillPhasePlan, &jt.backfillPhasePlan)
			require.Len(t, jt.incompleteJobs, len(tc.expectedPending)+len(tc.expectedActive))
		})
	}
}

func TestJobTracker_OfferCompactionJobs_PlanJobsAreSeparate(t *testing.T) {
	backfillID := []byte("01K7ZQ3W8V9C2N5R6T4Y1HXJ0M")
	newCompaction := func(id string, block string, order uint32) *TrackedCompactionJob {
		return NewTrackedCompactionJob(id, &CompactionJob{blocks: [][]byte{[]byte(block)}}, order, 0, at(1, 0))
	}
	newBackfillCompaction := func(id string, block string, order uint32) *TrackedCompactionJob {
		return NewTrackedCompactionJob(id, &CompactionJob{blocks: [][]byte{[]byte(block)}, backfillID: backfillID}, order, 0, at(1, 0))
	}

	tests := map[string]struct {
		planID           string
		offered          []*TrackedCompactionJob
		expectedAccepted int
		expectedPending  map[lane][]string
		otherPlanID      string
	}{
		"plan job offer leaves backfill jobs alone": {
			planID: planJobId,
			offered: []*TrackedCompactionJob{
				newCompaction("compaction-new", "block-3", 1),
				newCompaction("compaction-conflict", "block-1", 2), // shares a block with the complete compaction job
			},
			expectedAccepted: 1,
			expectedPending: map[lane][]string{
				compactionLane:         {"compaction-new"},
				backfillCompactionLane: {"backfill-pending"},
			},
			otherPlanID: backfillPhasePlanJobId,
		},
		"backfill phase plan job offer leaves compaction jobs alone": {
			planID: backfillPhasePlanJobId,
			offered: []*TrackedCompactionJob{
				newBackfillCompaction("backfill-new", "backfill-block-3", 1),
				newBackfillCompaction("backfill-conflict", "backfill-block-1", 2), // shares a block with the complete backfill compaction job
			},
			expectedAccepted: 1,
			expectedPending: map[lane][]string{
				compactionLane:         {"compaction-pending"},
				backfillCompactionLane: {"backfill-new"},
			},
			otherPlanID: planJobId,
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
			jt, _ := newTestJobTrackerWithLanePolicy(clock.NewMock(), newBackfillLanePolicy())
			jt.recoverFrom([]*TrackedCompactionJob{
				newCompaction("compaction-complete", "block-1", 1),
				newCompaction("compaction-pending", "block-2", 2),
				newBackfillCompaction("backfill-complete", "backfill-block-1", 1),
				newBackfillCompaction("backfill-pending", "backfill-block-2", 2),
			}, NewTrackedPlanJob(at(1, 0)), NewTrackedBackfillPhasePlanJob(at(1, 0)))

			planLeases := make(map[string]int64)
			for _, l := range []lane{planLane, backfillPhasePlanLane} {
				resp, _, err := jt.Lease(l)
				require.NoError(t, err)
				planLeases[resp.Key.Id] = resp.Key.Epoch
			}
			// Complete one job per plan job while both are leased so that each is retained for conflict detection
			for _, l := range []lane{compactionLane, backfillCompactionLane} {
				resp, _, err := jt.Lease(l)
				require.NoError(t, err)
				removed, _, err := jt.Remove(resp.Key.Id, resp.Key.Epoch, true)
				require.NoError(t, err)
				require.True(t, removed)
			}

			accepted, found, _, err := jt.OfferCompactionJobs(tc.planID, tc.offered, planLeases[tc.planID])
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, tc.expectedAccepted, accepted)

			for l, expected := range tc.expectedPending {
				require.Equal(t, expected, toSlice(jt.pending[l]), l.String())
			}
			require.NotContains(t, jt.incompleteJobs, tc.planID)
			require.Contains(t, jt.incompleteJobs, tc.otherPlanID)
			require.Empty(t, jt.plannerState(tc.planID).completeJobs)
			require.Len(t, jt.plannerState(tc.otherPlanID).completeJobs, 1)
		})
	}
}

func TestJobTracker_Lease_BackfillPhasePlanHasOutstandingJobs(t *testing.T) {
	leased := func(j *TrackedCompactionJob) *TrackedCompactionJob {
		j.MarkLeased(at(2, 0))
		return j
	}
	newCompaction := func() *TrackedCompactionJob {
		return NewTrackedCompactionJob("0@17241709254077376921-merge--1761004800000-1761012000000", &CompactionJob{}, 1, 0, at(1, 0))
	}
	newBackfillCompaction := func() *TrackedCompactionJob {
		return NewTrackedCompactionJob(backfillCompactionJobKeyPrefix+"0@17241709254077376921-merge--1761004800000-1761012000000", &CompactionJob{backfillID: []byte("01K7ZQ3W8V9C2N5R6T4Y1HXJ0M")}, 1, 0, at(1, 0))
	}
	newLeasedPlan := func() *TrackedPlanJob {
		j := NewTrackedPlanJob(at(1, 0))
		j.MarkLeased(at(2, 0))
		return j
	}

	tests := map[string]struct {
		compactionJobs []*TrackedCompactionJob
		planJob        *TrackedPlanJob
		expected       bool
	}{
		"no jobs": {},
		"pending compaction job": {
			compactionJobs: []*TrackedCompactionJob{newCompaction()},
		},
		"active compaction job": {
			compactionJobs: []*TrackedCompactionJob{leased(newCompaction())},
		},
		"active plan job": {
			planJob: newLeasedPlan(),
		},
		"pending backfill compaction job": {
			compactionJobs: []*TrackedCompactionJob{newBackfillCompaction()},
			expected:       true,
		},
		"active backfill compaction job": {
			compactionJobs: []*TrackedCompactionJob{leased(newBackfillCompaction())},
			expected:       true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jt, _ := newTestJobTrackerWithLanePolicy(clock.NewMock(), newBackfillLanePolicy())
			jt.recoverFrom(tc.compactionJobs, tc.planJob, NewTrackedBackfillPhasePlanJob(at(1, 0)))

			resp, _, err := jt.Lease(backfillPhasePlanLane)
			require.NoError(t, err)
			require.Equal(t, backfillPhasePlanJobId, resp.Key.Id)
			require.Equal(t, tc.expected, resp.Spec.BackfillPhasePlanning.HasOutstandingJobs)
		})
	}
}

func assertTrackerBytes(t *testing.T, reg *prometheus.Registry, msg string, splitBytes, mergeBytes float64) {
	t.Helper()
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
		# HELP cortex_compactor_scheduler_incomplete_compaction_jobs_bytes The total bytes of blocks in compaction jobs that have not yet completed (pending or active).
		# TYPE cortex_compactor_scheduler_incomplete_compaction_jobs_bytes gauge
		cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="merge",lane="compaction"} %g
		cortex_compactor_scheduler_incomplete_compaction_jobs_bytes{compaction_type="split",lane="compaction"} %g
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
	lanePolicy := newSimpleLanePolicy()
	sm := newSchedulerMetrics(reg, lanePolicy)

	// Two tenants share the same aggregate gauges (incompleteJobsBytes, pendingJobs, activeJobs).
	jt1 := NewJobTracker(&NopJobPersister{}, "tenant1", clk, lanePolicy, infiniteLeases, infiniteLeases, sm.newTrackerMetricsForTenant("tenant1"), log.NewNopLogger())
	jt1.discoveredBy.Store(uint32(discoveredByBlocks))
	jt2 := NewJobTracker(&NopJobPersister{}, "tenant2", clk, lanePolicy, infiniteLeases, infiniteLeases, sm.newTrackerMetricsForTenant("tenant2"), log.NewNopLogger())
	jt2.discoveredBy.Store(uint32(discoveredByBlocks))

	jt1.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("split-job", &CompactionJob{isSplit: true}, 1, 100, clk.Now()),
	}, nil, nil)
	jt2.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("merge-job", &CompactionJob{isSplit: false}, 1, 200, clk.Now()),
	}, nil, nil)
	assertTrackerBytes(t, reg, "both tenants contributing before cleanup", 100, 200)

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
	assertTrackerBytes(t, reg, "only tenant1 bytes removed", 0, 200)
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

	jt, _ := newTestJobTracker(clock.NewMock())
	jt.maxLeases = maxLeases

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
			jt, _ := newTestJobTracker(clk)
			jt.maxLeases = tc.maxLeases
			jt.repeatedFailureReportThreshold = tc.threshold

			lane := compactionLane
			if tc.planJob {
				lane = planLane
				_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 15*time.Minute)
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
