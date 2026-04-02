// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/go-kit/log"
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
			jobTrackers: map[string]*JobTracker{
				"test": newTestJobTracker(clock),
			},
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
			r := NewRotator(0, 0, 0, maintenanceInterval, 0, intervalsBeforeColdStartPlanning, log.NewNopLogger())
			r.clock = clock

			r.RecoverFrom(tc.jobTrackers, tc.creationTime)
			require.Equal(t, tc.expectedIntervals, r.intervalsBeforeColdStartPlanning)
		})
	}
}
