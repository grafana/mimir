// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
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
