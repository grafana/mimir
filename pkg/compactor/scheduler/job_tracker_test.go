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

func jobIDs(l *list.List) []string {
	var ids []string
	for e := l.Front(); e != nil; e = e.Next() {
		ids = append(ids, e.Value.(TrackedJob).ID())
	}
	return ids
}

func at(hour, minute int) time.Time {
	return time.Date(2026, 1, 2, hour, minute, 0, 0, time.UTC)
}

func newTestJobTracker(clk clock.Clock) (*JobTracker, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	metrics := newSchedulerMetrics(reg)
	return NewJobTracker(&NopJobPersister{}, "test", clk, infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger()), reg
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
				jt.incompleteJobs[planJobId] = jt.pending.PushBack(NewTrackedPlanJob(time.Now()))
			},
			now: at(3, 0),
		},
		"skips within compaction wait period": {
			setup: func(jt *JobTracker) {
				// 3:30 + 1h = 4:30, truncate to 1h = 4:00, so 4:00 + compactionWaitPeriod
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
		"no transition upon creation when previously non-empty": {
			setup: func(jt *JobTracker) {
				jt.pending.PushBack(NewTrackedCompactionJob("compactionId", &CompactionJob{}, 1, 0, time.Now()))
			},
			now:                at(3, 0),
			expectedPlan:       true,
			expectedTransition: false,
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
				require.Equal(t, tc.expectedTransition, transition)
				require.True(t, jt.completePlanTime.IsZero())
			} else {
				require.False(t, transition)
			}
		})
	}

	t.Run("returns error on persist failure", func(t *testing.T) {
		metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
		jt := NewJobTracker(&errJobPersister{}, "test", clock.New(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

		transition, err := jt.Maintenance(leaseDuration, false, true, planningInterval, compactionWaitPeriod)
		require.Error(t, err)
		require.False(t, transition)
		require.NotContains(t, jt.incompleteJobs, planJobId)
	})

	t.Run("planning skipped when plan is false", func(t *testing.T) {
		metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
		jt := NewJobTracker(&errJobPersister{}, "test", clock.New(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())
		transition, err := jt.Maintenance(leaseDuration, false, false, planningInterval, compactionWaitPeriod)
		require.NoError(t, err)
		require.False(t, transition)
		require.NotContains(t, jt.incompleteJobs, planJobId)
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

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jt, _ := newTestJobTracker(clock.NewMock())

			jt.recoverFrom(tc.compactionJobs, tc.planJob)

			require.Equal(t, tc.expectedPending, jobIDs(jt.pending))
			require.Equal(t, tc.expectedActive, jobIDs(jt.active))
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

	jt.recoverFrom([]*TrackedCompactionJob{splitJob, mergeJob}, nil)
	assertTrackerBytes(t, reg, "both jobs pending after recovery", 100, 200)

	leaseResp, _, err := jt.Lease()
	require.NoError(t, err)
	assertTrackerBytes(t, reg, "split job leased (still incomplete)", 100, 200)

	canceled, _, err := jt.CancelLease(leaseResp.Key.Id, leaseResp.Key.Epoch)
	require.NoError(t, err)
	require.True(t, canceled)
	assertTrackerBytes(t, reg, "split job revived to pending (bytes unchanged)", 100, 200)

	leaseResp, _, err = jt.Lease()
	require.NoError(t, err)
	_, _, err = jt.Remove(leaseResp.Key.Id, leaseResp.Key.Epoch, true)
	require.NoError(t, err)
	assertTrackerBytes(t, reg, "split job complete", 0, 200)

	leaseResp, _, err = jt.Lease()
	require.NoError(t, err)
	_, _, err = jt.Remove(leaseResp.Key.Id, leaseResp.Key.Epoch, true)
	require.NoError(t, err)
	assertTrackerBytes(t, reg, "merge job complete", 0, 0)
}

func TestJobTracker_PlanJobTracking(t *testing.T) {
	clk := clock.NewMock()
	clk.Set(at(3, 0))
	jt, reg := newTestJobTracker(clk)

	assertIncompletePlanJobs := func(label string, expected int) {
		t.Helper()
		require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
			# HELP cortex_compactor_incomplete_plan_jobs The total number of plan jobs that have not yet completed (pending or active).
			# TYPE cortex_compactor_incomplete_plan_jobs gauge
			cortex_compactor_incomplete_plan_jobs %d
		`, expected)), "cortex_compactor_incomplete_plan_jobs"), label)
	}

	assertIncompletePlanJobs("no plan jobs yet", 0)

	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 0)
	require.NoError(t, err)
	assertIncompletePlanJobs("plan job pending", 1)

	leaseResp, _, err := jt.Lease()
	require.NoError(t, err)
	require.Equal(t, planJobId, leaseResp.Key.Id)
	assertIncompletePlanJobs("plan job active (still incomplete)", 1)

	canceled, _, err := jt.CancelLease(leaseResp.Key.Id, leaseResp.Key.Epoch)
	require.NoError(t, err)
	require.True(t, canceled)
	assertIncompletePlanJobs("plan job revived to pending (unchanged)", 1)

	leaseResp, _, err = jt.Lease()
	require.NoError(t, err)
	_, _, err = jt.Remove(leaseResp.Key.Id, leaseResp.Key.Epoch, true)
	require.NoError(t, err)
	assertIncompletePlanJobs("plan job complete", 0)
}

func TestJobTracker_Cleanup(t *testing.T) {
	clk := clock.NewMock()
	reg := prometheus.NewPedanticRegistry()
	sm := newSchedulerMetrics(reg)

	// Two tenants share the same incompleteJobsBytes and incompletePlanJobs gauges.
	jt1 := NewJobTracker(&NopJobPersister{}, "tenant1", clk, infiniteLeases, infiniteLeases, sm.newTrackerMetricsForTenant("tenant1"), log.NewNopLogger())
	jt2 := NewJobTracker(&NopJobPersister{}, "tenant2", clk, infiniteLeases, infiniteLeases, sm.newTrackerMetricsForTenant("tenant2"), log.NewNopLogger())

	jt1.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("split-job", &CompactionJob{isSplit: true}, 1, 100, clk.Now()),
	}, nil)
	jt2.recoverFrom([]*TrackedCompactionJob{
		NewTrackedCompactionJob("merge-job", &CompactionJob{isSplit: false}, 1, 200, clk.Now()),
	}, nil)
	assertTrackerBytes(t, reg, "both tenants contributing before cleanup", 100, 200)

	// Set time past the first planning window to force planning on Maintenance()
	clk.Set(at(3, 0))
	_, err := jt1.Maintenance(time.Minute, false, true, time.Hour, 0)
	require.NoError(t, err)
	_, err = jt2.Maintenance(time.Minute, false, true, time.Hour, 0)
	require.NoError(t, err)

	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_incomplete_plan_jobs The total number of plan jobs that have not yet completed (pending or active).
		# TYPE cortex_compactor_incomplete_plan_jobs gauge
		cortex_compactor_incomplete_plan_jobs 2
	`), "cortex_compactor_incomplete_plan_jobs"), "both tenants have a pending plan job")

	// Cleaning up tenant1 should only subtract its contribution, not zero the shared gauges.
	jt1.CleanupMetrics()
	assertTrackerBytes(t, reg, "only tenant1 bytes removed", 0, 200)
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_scheduler_pending_jobs The number of queued pending jobs.
		# TYPE cortex_compactor_scheduler_pending_jobs gauge
		cortex_compactor_scheduler_pending_jobs{user="tenant2"} 2
	`), "cortex_compactor_scheduler_pending_jobs"), "only tenant2 pending jobs remain")
	require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(`
		# HELP cortex_compactor_incomplete_plan_jobs The total number of plan jobs that have not yet completed (pending or active).
		# TYPE cortex_compactor_incomplete_plan_jobs gauge
		cortex_compactor_incomplete_plan_jobs 1
	`), "cortex_compactor_incomplete_plan_jobs"), "only tenant2 plan job remains")
}

func TestJobTracker_CancelLease_PlanJobAlwaysRevives(t *testing.T) {
	const maxLeases = 2

	clk := clock.NewMock()
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	jt := NewJobTracker(&NopJobPersister{}, "test", clk, maxLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())

	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 15*time.Minute)
	require.NoError(t, err)

	for range maxLeases + 1 {
		job, _, err := jt.Lease()
		require.NoError(t, err)
		require.NotNil(t, job, "plan job should always be leaseable")

		canceled, _, err := jt.CancelLease(job.Key.Id, job.Key.Epoch)
		require.NoError(t, err)
		require.True(t, canceled)
	}
}
