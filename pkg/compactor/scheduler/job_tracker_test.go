// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/prometheus/client_golang/prometheus"
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

func newTestJobTracker(t *testing.T, clk clock.Clock) *JobTracker {
	t.Helper()
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	return NewJobTracker(&NopJobPersister{}, "test", clk, infiniteLeases, metrics.newTrackerMetricsForTenant("test"))
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
				jt.pending.PushBack(NewTrackedCompactionJob("compactionId", &CompactionJob{}, 1, time.Now()))
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
			jt := newTestJobTracker(t, clk)
			if tc.setup != nil {
				tc.setup(jt)
			}

			transition, err := jt.Maintenance(leaseDuration, false, planningInterval, compactionWaitPeriod)
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
		jt := NewJobTracker(&errJobPersister{}, "test", clock.New(), infiniteLeases, metrics.newTrackerMetricsForTenant("test"))

		transition, err := jt.Maintenance(leaseDuration, false, planningInterval, compactionWaitPeriod)
		require.Error(t, err)
		require.False(t, transition)
		require.NotContains(t, jt.incompleteJobs, planJobId)
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

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			jt := newTestJobTracker(t, clock.NewMock())

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
