// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"errors"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func jobIDs(l *jobList) []string {
	var ids []string
	for e := l.Front(); e != nil; e = e.Next() {
		ids = append(ids, e.Value.(TrackedJob).ID())
	}
	return ids
}

func at(hour, minute int) time.Time {
	return time.Date(2026, 1, 2, hour, minute, 0, 0, time.UTC)
}

func newTestJobTracker(clk clock.Clock) *JobTracker {
	metrics := newSchedulerMetrics(prometheus.NewPedanticRegistry())
	return NewJobTracker(&NopJobPersister{}, "test", clk, infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("test"), log.NewNopLogger())
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
			jt := newTestJobTracker(clk)
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
			jt := newTestJobTracker(clock.NewMock())

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

func TestJobTracker_IncompleteCompactionJobBytes(t *testing.T) {
	clk := clock.NewMock()
	reg := prometheus.NewPedanticRegistry()
	m := newSchedulerMetrics(reg)
	jt := NewJobTracker(&NopJobPersister{}, "test", clk, infiniteLeases, infiniteLeases, m.newTrackerMetricsForTenant("test"), log.NewNopLogger())

	bytesGauge := func() float64 {
		return testutil.ToFloat64(m.incompleteCompactionJobBytes)
	}

	newJob := func(id string, bytes int64) *TrackedCompactionJob {
		return NewTrackedCompactionJob(id, &CompactionJob{}, 1, bytes, clk.Now())
	}

	// Plan job leased so that offered compaction jobs are accepted.
	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 0)
	require.NoError(t, err)
	planLease, _, err := jt.Lease()
	require.NoError(t, err)

	// Offer two compaction jobs with known sizes.
	_, _, _, err = jt.OfferCompactionJobs([]*TrackedCompactionJob{newJob("a", 100), newJob("b", 200)}, planLease.Key.Epoch)
	require.NoError(t, err)
	require.Equal(t, float64(300), bytesGauge(), "total bytes after offering")

	// Lease job "a" — moves to active, bytes stay the same.
	leaseA, _, err := jt.Lease()
	require.NoError(t, err)
	require.Equal(t, float64(300), bytesGauge(), "total bytes unchanged after lease")

	// Complete job "a" — bytes decrease.
	_, _, err = jt.Remove(leaseA.Key.Id, leaseA.Key.Epoch, true)
	require.NoError(t, err)
	require.Equal(t, float64(200), bytesGauge(), "total bytes after completing job a")

	// Lease and expire job "b" back to pending — bytes stay the same.
	leaseB, _, err := jt.Lease()
	require.NoError(t, err)
	clk.Add(2 * time.Minute)
	_, err = jt.Maintenance(time.Minute, true, false, time.Hour, 0)
	require.NoError(t, err)
	require.Equal(t, float64(200), bytesGauge(), "total bytes unchanged after lease expiry revival")
	_ = leaseB

	// Remove job "b" while still pending.
	pendingLease, _, err := jt.Lease()
	require.NoError(t, err)
	_, _, err = jt.Remove(pendingLease.Key.Id, pendingLease.Key.Epoch, false)
	require.NoError(t, err)
	require.Equal(t, float64(0), bytesGauge(), "total bytes zero after all jobs removed")
}

func TestJobTracker_MaxIncompleteCompactionJobBytes(t *testing.T) {
	clk := clock.NewMock()
	reg := prometheus.NewPedanticRegistry()
	m := newSchedulerMetrics(reg)
	jt := NewJobTracker(&NopJobPersister{}, "test", clk, infiniteLeases, infiniteLeases, m.newTrackerMetricsForTenant("test"), log.NewNopLogger())

	maxGauge := func() float64 {
		return testutil.ToFloat64(m.maxIncompleteCompactionJobBytes)
	}

	newJob := func(id string, bytes int64) *TrackedCompactionJob {
		return NewTrackedCompactionJob(id, &CompactionJob{}, 1, bytes, clk.Now())
	}

	_, err := jt.Maintenance(time.Minute, false, true, time.Hour, 0)
	require.NoError(t, err)
	planLease, _, err := jt.Lease()
	require.NoError(t, err)

	// Offer jobs with "largest" first in queue so it is leased first.
	_, _, _, err = jt.OfferCompactionJobs([]*TrackedCompactionJob{newJob("largest", 300), newJob("mid", 200), newJob("small", 100)}, planLease.Key.Epoch)
	require.NoError(t, err)
	require.Equal(t, float64(300), maxGauge(), "max after offering")

	// Lease "largest" (300) — still in the heap (active), max unchanged.
	leaseLargest, _, err := jt.Lease()
	require.NoError(t, err)
	require.Equal(t, float64(300), maxGauge(), "max unchanged after leasing largest job")

	// Complete "largest" — max should drop to next largest (200).
	_, _, err = jt.Remove(leaseLargest.Key.Id, leaseLargest.Key.Epoch, true)
	require.NoError(t, err)
	require.Equal(t, float64(200), maxGauge(), "max drops after completing largest job")

	// Complete all remaining jobs — max should reach 0.
	for {
		lease, _, err := jt.Lease()
		require.NoError(t, err)
		if lease == nil {
			break
		}
		_, _, err = jt.Remove(lease.Key.Id, lease.Key.Epoch, true)
		require.NoError(t, err)
	}
	require.Equal(t, float64(0), maxGauge(), "max is 0 when queue is empty")
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
