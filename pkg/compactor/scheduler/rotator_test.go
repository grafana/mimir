// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"fmt"
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
			metrics := newSchedulerMetrics(reg)
			r := NewRotator(0, 0, 0, maintenanceInterval, 0, intervalsBeforeColdStartPlanning, metrics.pendingJobsLastEmpty, log.NewNopLogger())
			r.clock = clock

			r.RecoverFrom(tc.jobTrackers, tc.creationTime)
			require.Equal(t, tc.expectedIntervals, r.intervalsBeforeColdStartPlanning)
		})
	}
}

func newRotatorForTest() *Rotator {
	reg := prometheus.NewPedanticRegistry()
	metrics := newSchedulerMetrics(reg)
	return NewRotator(0, 0, 0, time.Minute, 0, 0, metrics.pendingJobsLastEmpty, log.NewNopLogger())
}

func seedRotation(r *Rotator, tenants ...string) map[string]*TenantRotationState {
	states := make(map[string]*TenantRotationState, len(tenants))
	for _, tenant := range tenants {
		state := &TenantRotationState{}
		r.tenantStateMap[tenant] = state
		r.addToRotation(tenant, state)
		states[tenant] = state
	}
	return states
}

func rotationOrder(r *Rotator) []string {
	tenants := make([]string, 0, r.rotation.Len())
	for e := r.rotation.Front(); e != nil; e = e.Next() {
		tenants = append(tenants, e.Value.(string))
	}
	return tenants
}

func TestRotator_RemoveFromRotation_PreservesOrder(t *testing.T) {
	r := newRotatorForTest()
	states := seedRotation(r, "a", "b", "c", "d", "e")
	require.Equal(t, []string{"a", "b", "c", "d", "e"}, rotationOrder(r))

	// Remove from the middle: remaining tenants keep their relative order.
	r.removeFromRotation(states["c"])
	require.Equal(t, []string{"a", "b", "d", "e"}, rotationOrder(r))
	require.Nil(t, states["c"].element)

	// Remove the head and the tail.
	r.removeFromRotation(states["a"])
	require.Equal(t, []string{"b", "d", "e"}, rotationOrder(r))
	r.removeFromRotation(states["e"])
	require.Equal(t, []string{"b", "d"}, rotationOrder(r))

	// Drain the rotation: cursor goes back to nil.
	r.removeFromRotation(states["b"])
	r.removeFromRotation(states["d"])
	require.Equal(t, 0, r.rotation.Len())
	require.Nil(t, r.cursor.Load())
}

func TestRotator_RemoveFromRotation_AdvancesCursor(t *testing.T) {
	r := newRotatorForTest()
	states := seedRotation(r, "a", "b", "c", "d", "e")

	// Consume two slots so the cursor lands on "c".
	require.Equal(t, "a", r.advanceCursor().Value.(string))
	require.Equal(t, "b", r.advanceCursor().Value.(string))
	require.Equal(t, "c", r.cursor.Load().Value.(string))

	// Removing the tenant the cursor points to advances it to the next tenant, preserving order.
	r.removeFromRotation(states["c"])
	require.Equal(t, "d", r.cursor.Load().Value.(string))

	// Round-robin continues in the original relative order.
	var seen []string
	for range 8 {
		seen = append(seen, r.advanceCursor().Value.(string))
	}
	require.Equal(t, []string{"d", "e", "a", "b", "d", "e", "a", "b"}, seen)
}

// TestRotator_LeaseJob_ConcurrentLeasersDoNotSkipPendingWork is a regression test for a bug where
// LeaseJob's loop advanced the shared cursor on every iteration. Under contention, a concurrent
// leaser's advances could interleave with this leaser's advances, causing it to revisit some
// tenants and skip others — including the one with pending work. With K pending jobs and K
// concurrent leasers, all K leasers must succeed.
func TestRotator_LeaseJob_ConcurrentLeasersDoNotSkipPendingWork(t *testing.T) {
	const (
		numEmpties = 8
		leasers    = 16
		trials     = 50
	)

	for trial := range trials {
		clk := clock.NewMock()
		clk.Set(time.Now())

		r := newRotatorForTest()
		r.clock = clk

		// Build [empty, ..., empty, pending(leasers jobs), empty, ..., empty]. Put the pending
		// tenant in the middle so a leaser must traverse empties before reaching it.
		midIdx := numEmpties / 2
		seeded := make([]struct {
			name string
			jt   *JobTracker
		}, 0, numEmpties+1)
		for i := range numEmpties + 1 {
			jt, _ := newTestJobTracker(clk)
			name := fmt.Sprintf("empty%d", i)
			if i == midIdx {
				name = "pending"
				for j := range leasers {
					id := fmt.Sprintf("job-%d-%d", trial, j)
					jt.incompleteJobs[id] = jt.pending.PushBack(
						NewTrackedCompactionJob(id, &CompactionJob{}, uint32(j), 0, clk.Now()))
				}
			}
			seeded = append(seeded, struct {
				name string
				jt   *JobTracker
			}{name, jt})
		}

		// Seed the rotation directly so the empties are in it (AddTenant would skip them).
		for _, s := range seeded {
			state := &TenantRotationState{tracker: s.jt}
			r.tenantStateMap[s.name] = state
			r.addToRotation(s.name, state)
		}

		var wg sync.WaitGroup
		var successes atomic.Int32
		start := make(chan struct{})
		for range leasers {
			wg.Go(func() {
				<-start
				if _, ok, _ := r.LeaseJob(context.Background()); ok {
					successes.Add(1)
				}
			})
		}
		close(start)
		wg.Wait()

		require.Equal(t, int32(leasers), successes.Load(),
			"trial %d: every leaser must succeed because the rotation has at least %d pending jobs", trial, leasers)
	}
}

func TestRotator_RemoveFromRotation_CursorWrapsOnTailRemoval(t *testing.T) {
	r := newRotatorForTest()
	states := seedRotation(r, "a", "b", "c")

	// Advance cursor to the tail ("c").
	require.Equal(t, "a", r.advanceCursor().Value.(string))
	require.Equal(t, "b", r.advanceCursor().Value.(string))
	require.Equal(t, "c", r.cursor.Load().Value.(string))

	// Removing the tail under the cursor wraps to the front.
	r.removeFromRotation(states["c"])
	require.Equal(t, "a", r.cursor.Load().Value.(string))
}

func TestRotator_PendingJobsLastEmpty(t *testing.T) {
	now := time.Now()

	pendingTracker := func(clk clock.Clock) *JobTracker {
		jt, _ := newTestJobTracker(clk)
		jt.incompleteJobs[planJobId] = jt.pending.PushBack(NewTrackedPlanJob(clk.Now()))
		return jt
	}

	tests := map[string]struct {
		action   func(r *Rotator, clk *clock.Mock)
		expected int64
	}{
		"recovery with no pending jobs": {
			action:   func(r *Rotator, _ *clock.Mock) { r.RecoverFrom(nil, now) },
			expected: now.Unix(),
		},
		"recovery with pending jobs leaves metric at 0": {
			action: func(r *Rotator, clk *clock.Mock) {
				r.RecoverFrom(map[string]*JobTracker{"t1": pendingTracker(clk)}, now)
			},
			expected: 0,
		},
		"maintenance over empty rotation": {
			action:   func(r *Rotator, _ *clock.Mock) { r.Maintenance(context.Background(), false, false) },
			expected: now.Unix(),
		},
		"removing the last tenant in rotation": {
			action: func(r *Rotator, clk *clock.Mock) {
				r.AddTenant("t1", pendingTracker(clk))
				_, _ = r.RemoveTenant("t1")
			},
			expected: now.Unix(),
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			clk := clock.NewMock()
			clk.Set(now)
			reg := prometheus.NewPedanticRegistry()
			metrics := newSchedulerMetrics(reg)
			r := NewRotator(0, 0, 0, 0, 0, 0, metrics.pendingJobsLastEmpty, log.NewNopLogger())
			r.clock = clk

			tc.action(r, clk)

			metricName := "cortex_compactor_scheduler_pending_jobs_last_empty_timestamp_seconds"
			require.NoError(t, prom_testutil.GatherAndCompare(reg, strings.NewReader(fmt.Sprintf(`
				# HELP %s Unix timestamp of the last time there were no pending jobs remaining.
				# TYPE %s gauge
				%s %d
			`, metricName, metricName, metricName, tc.expected)), metricName))
		})
	}
}
