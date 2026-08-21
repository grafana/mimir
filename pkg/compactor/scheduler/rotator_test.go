// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"container/list"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	prom_testutil "github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestRotator_RecoverFrom_ColdStartDelay(t *testing.T) {
	maintenanceInterval := 10 * time.Minute
	intervalsBeforeColdStartPlanning := 5

	now := time.Now()
	clock := clock.NewMock()
	clock.Set(now)

	tests := []struct {
		name              string
		creationTime      time.Time
		jobTrackers       map[string]*JobTracker
		expectedIntervals int
	}{
		{
			name:              "no delay, past",
			creationTime:      now.Add(-time.Duration(intervalsBeforeColdStartPlanning+1) * maintenanceInterval),
			expectedIntervals: 0,
		},
		{
			name:              "partial delay, multiple",
			creationTime:      now.Add((-2 * maintenanceInterval)),
			expectedIntervals: 3, // 5 - 2 = 3
		},
		{
			name:              "partial delay, fractional",
			creationTime:      now.Add((-2 * maintenanceInterval) - maintenanceInterval/2),
			expectedIntervals: 3, // 5 - 2.5 = 2.5, ceil(2.5) = 3
		},
		{
			name:              "full delay",
			creationTime:      now,
			expectedIntervals: intervalsBeforeColdStartPlanning,
		},
		{
			name:         "existing trackers do not bypass delay",
			creationTime: now,
			jobTrackers: func() map[string]*JobTracker {
				jt, _ := newTestJobTracker(clock)
				return map[string]*JobTracker{"test": jt}
			}(),
			expectedIntervals: intervalsBeforeColdStartPlanning,
		},
		{
			name:              "clock skew, creation time in future",
			creationTime:      now.Add(maintenanceInterval),
			expectedIntervals: intervalsBeforeColdStartPlanning,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			metrics := newTestSchedulerMetrics(reg)
			r := NewRotator(0, 0, 0, maintenanceInterval, 0, intervalsBeforeColdStartPlanning, newSimpleLanePolicy(), metrics.pendingJobsLastEmpty, metrics.lanePendingJobsLastEmptyGauges, log.NewNopLogger())
			r.clock = clock

			r.RecoverFrom(tc.jobTrackers, tc.creationTime)
			require.Equal(t, tc.expectedIntervals, r.intervalsBeforeColdStartPlanning)
		})
	}
}

func newRotatorForTest() *Rotator {
	reg := prometheus.NewPedanticRegistry()
	metrics := newTestSchedulerMetrics(reg)
	return NewRotator(0, 0, 0, time.Minute, 0, 0, newSimpleLanePolicy(), metrics.pendingJobsLastEmpty, metrics.lanePendingJobsLastEmptyGauges, log.NewNopLogger())
}

// newTrackerWithPendingJobs builds a JobTracker for the named tenant holding numJobs pending
// compaction jobs (numJobs == 0 yields an empty tracker).
func newTrackerWithPendingJobs(clk clock.Clock, name string, numJobs int) *JobTracker {
	metrics := newTestSchedulerMetrics(prometheus.NewPedanticRegistry())
	jt := NewJobTracker(&NopJobPersister{}, name, clk, newSimpleLanePolicy(), infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant(name), log.NewNopLogger())
	for j := range numJobs {
		id := fmt.Sprintf("%s-%d", name, j)
		jt.toPendingBack(NewTrackedCompactionJob(id, &CompactionJob{}, uint32(j), clk.Now()))
	}
	return jt
}

// addTenantWithPendingJobs adds a tenant to the rotation with numJobs pending compaction jobs
func addTenantWithPendingJobs(r *Rotator, clk clock.Clock, name string, numJobs int) {
	r.AddTenant(name, newTrackerWithPendingJobs(clk, name, numJobs))
}

// leaseTenant leases one job and returns the tenant it was leased from.
func leaseTenant(t *testing.T, r *Rotator) string {
	t.Helper()
	resp, ok, err := r.LeaseJob(context.Background(), newSimpleLanePolicy().AllLanes())
	require.NoError(t, err)
	require.True(t, ok)
	return resp.Spec.Tenant
}

// TestRotator_RemoveTenant checks that RemoveTenant does not alter the order of the remaining tenants.
func TestRotator_RemoveTenant(t *testing.T) {
	for _, tc := range []struct {
		name       string
		tenants    []string
		leaseFirst int
		remove     string
		want       []string
	}{
		{
			name:       "tenant the cursor already passed keeps order",
			tenants:    []string{"a", "b", "c", "d", "e"},
			leaseFirst: 5, // full cycle: cursor back on "a"
			remove:     "c",
			want:       []string{"a", "b", "d", "e", "a", "b", "d", "e"},
		},
		{
			name:       "next-up middle tenant does not skip",
			tenants:    []string{"a", "b", "c", "d", "e"},
			leaseFirst: 2, // next up is "c"
			remove:     "c",
			want:       []string{"d", "e", "a", "b", "d"},
		},
		{
			name:       "next-up tail tenant wraps to front",
			tenants:    []string{"a", "b", "c"},
			leaseFirst: 2, // next up is "c"
			remove:     "c",
			want:       []string{"a", "b", "a", "b"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewMock()
			clk.Set(time.Now())
			r := newRotatorForTest()
			r.clock = clk

			for _, name := range tc.tenants {
				addTenantWithPendingJobs(r, clk, name, 10)
			}
			for range tc.leaseFirst {
				leaseTenant(t, r)
			}

			_, ok := r.RemoveTenant(tc.remove)
			require.True(t, ok)

			var seen []string
			for range tc.want {
				seen = append(seen, leaseTenant(t, r))
			}
			require.Equal(t, tc.want, seen)
		})
	}
}

// TestRotator_LeaseJob_ConcurrentLeasersDoNotSkipPendingWork is a regression test for a bug where
// LeaseJob's loop advanced the shared cursor on every iteration. Under contention, this could cause
// it to skip over tenants with pending jobs.
func TestRotator_LeaseJob_ConcurrentLeasersDoNotSkipPendingWork(t *testing.T) {
	const (
		numEmpties = 8
		jobCount   = 16
		trials     = 50
	)

	for trial := range trials {
		clk := clock.NewMock()
		clk.Set(time.Now())

		r := newRotatorForTest()
		r.clock = clk

		// lets us easily setup a rotation with tenants with no work for the test
		forceIntoRotation := func(name string, jt *JobTracker) {
			state := &tenantRotationState{tracker: jt, elements: make(map[lane]*list.Element)}
			r.tenantStateMap[name] = state
			r.addToRotation(compactionLane, name, state)
		}

		// Setup a rotation like so: [empty, ..., empty, pending(jobCount jobs), empty, ..., empty]
		// The pending tenant sits in the middle so a leaser must traverse empties before reaching it.
		for i := range numEmpties + 1 {
			name, numJobs := fmt.Sprintf("empty%d", i), 0
			if i == numEmpties/2 {
				name, numJobs = "pending", jobCount
			}
			forceIntoRotation(name, newTrackerWithPendingJobs(clk, name, numJobs))
		}

		// Spin up jobCount concurrent leasers, we should have exactly jobCount successful leases
		allLanes := newSimpleLanePolicy().AllLanes()
		var wg sync.WaitGroup
		var successes atomic.Int32
		start := make(chan struct{})
		for range jobCount {
			wg.Go(func() {
				<-start
				if _, ok, _ := r.LeaseJob(context.Background(), allLanes); ok {
					successes.Add(1)
				}
			})
		}
		close(start)
		wg.Wait()

		require.Equal(t, int32(jobCount), successes.Load(),
			"trial %d: every leaser must succeed because the rotation has at least %d pending jobs", trial, jobCount)
	}
}

func TestRotator_LeaseJob_LanePriority(t *testing.T) {
	clk := clock.New()
	lanePolicy := newSimpleLanePolicy()
	metrics := newTestSchedulerMetrics(prometheus.NewPedanticRegistry())
	r := NewRotator(0, 0, 0, time.Minute, 0, 0, lanePolicy, metrics.pendingJobsLastEmpty, metrics.lanePendingJobsLastEmptyGauges, log.NewNopLogger())

	// Add a tenant with a plan job and a compaction job
	jt := NewJobTracker(&NopJobPersister{}, "t1", clk, lanePolicy, infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("t1"), log.NewNopLogger())
	jt.toPendingBack(NewTrackedPlanJob(clk.Now()))
	firstCompactionJobId := "first"
	jt.toPendingBack(NewTrackedCompactionJob(firstCompactionJobId, &CompactionJob{totalBlockBytes: 2}, 1, clk.Now()))
	r.AddTenant("t1", jt)

	// Add a tenant with a only a compaction job
	jt2 := NewJobTracker(&NopJobPersister{}, "t2", clk, lanePolicy, infiniteLeases, infiniteLeases, metrics.newTrackerMetricsForTenant("t2"), log.NewNopLogger())
	secondCompactionJobId := "second"
	jt2.toPendingBack(NewTrackedCompactionJob(secondCompactionJobId, &CompactionJob{totalBlockBytes: 2}, 1, clk.Now()))
	r.AddTenant("t2", jt2)

	// compaction lane is checked first and first compaction job is returned
	resp, ok, err := r.LeaseJob(context.Background(), []lane{compactionLane, planLane})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, firstCompactionJobId, resp.Key.Id)
	require.Equal(t, "t1", resp.Spec.Tenant)

	// plan lane is checked first, plan job is found
	resp, ok, err = r.LeaseJob(context.Background(), []lane{planLane, compactionLane})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, planJobId, resp.Key.Id)
	require.Equal(t, "t1", resp.Spec.Tenant)

	// only plan lane is checked, nothing found
	_, ok, err = r.LeaseJob(context.Background(), []lane{planLane})
	require.NoError(t, err)
	require.False(t, ok)

	// plan lane is checked first, not found, compaction job checked next and second compaction job is found
	resp, ok, err = r.LeaseJob(context.Background(), []lane{planLane, compactionLane})
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, secondCompactionJobId, resp.Key.Id)
	require.Equal(t, "t2", resp.Spec.Tenant)

	// both lanes are checked, nothing found
	_, ok, err = r.LeaseJob(context.Background(), []lane{planLane, compactionLane})
	require.NoError(t, err)
	require.False(t, ok)
}

func TestRotator_PendingJobsLastEmpty(t *testing.T) {
	now := time.Now()

	pendingTracker := func(clk clock.Clock) *JobTracker {
		jt, _ := newTestJobTracker(clk)
		jt.toPendingBack(NewTrackedPlanJob(clk.Now()))
		return jt
	}

	tests := map[string]struct {
		action func(r *Rotator, clk *clock.Mock)
		// expected is for the whole queue, expectedByLane for each lane on its own.
		expected       int64
		expectedByLane map[lane]int64
	}{
		"recovery with no pending jobs": {
			action:         func(r *Rotator, _ *clock.Mock) { r.RecoverFrom(nil, now) },
			expected:       now.Unix(),
			expectedByLane: map[lane]int64{planLane: now.Unix(), compactionLane: now.Unix()},
		},
		"recovery with pending jobs leaves only their lane unset": {
			action: func(r *Rotator, clk *clock.Mock) {
				r.RecoverFrom(map[string]*JobTracker{"t1": pendingTracker(clk)}, now)
			},
			expected:       0,
			expectedByLane: map[lane]int64{planLane: 0, compactionLane: now.Unix()},
		},
		// The point of tracking this per lane: a compaction backlog must not hide that planning is idle.
		"recovery with a pending compaction job leaves only the compaction lane unset": {
			action: func(r *Rotator, clk *clock.Mock) {
				jt, _ := newTestJobTracker(clk)
				jt.toPendingBack(NewTrackedCompactionJob("c1", &CompactionJob{}, 1, clk.Now()))
				r.RecoverFrom(map[string]*JobTracker{"t1": jt}, now)
			},
			expected:       0,
			expectedByLane: map[lane]int64{planLane: now.Unix(), compactionLane: 0},
		},
		"maintenance over empty rotation": {
			action:         func(r *Rotator, _ *clock.Mock) { r.Maintenance(context.Background(), false, false) },
			expected:       now.Unix(),
			expectedByLane: map[lane]int64{planLane: now.Unix(), compactionLane: now.Unix()},
		},
		// The clock advances so the assertion pins when the lanes were stamped, not merely that they were.
		"removing the last tenant in rotation": {
			action: func(r *Rotator, clk *clock.Mock) {
				r.AddTenant("t1", pendingTracker(clk))
				clk.Add(time.Minute)
				_, _ = r.RemoveTenant("t1")
			},
			expected:       now.Add(time.Minute).Unix(),
			expectedByLane: map[lane]int64{planLane: now.Add(time.Minute).Unix(), compactionLane: now.Add(time.Minute).Unix()},
		},
		"maintenance mid-shutdown records nothing": {
			action: func(r *Rotator, _ *clock.Mock) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				r.Maintenance(ctx, false, false)
			},
			expected:       0,
			expectedByLane: map[lane]int64{planLane: 0, compactionLane: 0},
		},
		"maintenance moving a tenant into a rotation still records the idle lanes": {
			action: func(r *Rotator, clk *clock.Mock) {
				jt, _ := newTestJobTracker(clk)
				r.AddTenant("t1", jt)
				r.Maintenance(context.Background(), false, true)
			},
			expected:       0,
			expectedByLane: map[lane]int64{planLane: 0, compactionLane: now.Unix()},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clk := clock.NewMock()
			clk.Set(now)
			reg := prometheus.NewPedanticRegistry()
			metrics := newTestSchedulerMetrics(reg)
			r := NewRotator(0, 0, 0, 0, 0, 0, newSimpleLanePolicy(), metrics.pendingJobsLastEmpty, metrics.lanePendingJobsLastEmptyGauges, log.NewNopLogger())
			r.clock = clk

			tc.action(r, clk)

			const (
				laneMetric = "cortex_compactor_scheduler_lane_pending_jobs_last_empty_timestamp_seconds"
				anyMetric  = "cortex_compactor_scheduler_pending_jobs_last_empty_timestamp_seconds"
			)
			var expected strings.Builder
			fmt.Fprintf(&expected, "# HELP %s Unix timestamp of the last time there were no pending jobs remaining in this lane.\n# TYPE %s gauge\n", laneMetric, laneMetric)
			for _, l := range slices.Sorted(maps.Keys(tc.expectedByLane)) {
				fmt.Fprintf(&expected, "%s{lane=%q} %d\n", laneMetric, l, tc.expectedByLane[l])
			}
			fmt.Fprintf(&expected, "# HELP %s Unix timestamp of the last time there were no pending jobs remaining.\n# TYPE %s gauge\n%s %d\n", anyMetric, anyMetric, anyMetric, tc.expected)

			require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(expected.String()), laneMetric, anyMetric))
		})
	}
}
