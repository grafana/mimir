// SPDX-License-Identifier: AGPL-3.0-only

package reactivelimiter

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/require"
)

// Tests that a rejection rate is computed as expected based on queue in/out stats.
func TestPrioritizer_Calibrate(t *testing.T) {
	p := NewPrioritizer(log.NewNopLogger()).(*prioritizer)
	config := createLimiterConfig()
	config.InitialInflightLimit = 1
	config.ShortWindowMinSamples = 10
	config.MaxRejectionFactor = 4
	limiter := NewPriorityLimiter(config, p, nil).(*priorityLimiter)

	acquireBlocking := func() {
		go limiter.AcquirePermit(context.Background(), PriorityLow)
	}
	assertBlocked := func(blocked int) {
		require.Eventually(t, func() bool {
			return limiter.Blocked() == blocked
		}, 300*time.Millisecond, 10*time.Millisecond)
	}

	permit, err := limiter.AcquirePermit(context.Background(), PriorityLow)
	require.NoError(t, err)
	acquireBlocking()
	assertBlocked(1)
	acquireBlocking()
	assertBlocked(2)
	acquireBlocking()
	assertBlocked(3)
	acquireBlocking()
	assertBlocked(4)
	permit.Record()

	p.Calibrate()
	require.Equal(t, .5, limiter.RejectionRate())
	require.True(t, p.priorityThreshold.Load() > 0 && p.priorityThreshold.Load() < 200, "low priority execution should be rejected")
}

func TestComputeRejectionRate(t *testing.T) {
	tests := []struct {
		name               string
		queueSize          int
		rejectionThreshold int
		maxQueueSize       int
		expectedRate       float64
	}{
		{
			name:               "queueSize below rejectionThreshold",
			queueSize:          50,
			rejectionThreshold: 60,
			maxQueueSize:       100,
			expectedRate:       0,
		},
		{
			name:               "queueSize equal to rejectionThreshold",
			queueSize:          60,
			rejectionThreshold: 60,
			maxQueueSize:       100,
			expectedRate:       0,
		},
		{
			name:               "queueSize between rejectionThreshold and maxQueueSize",
			queueSize:          80,
			rejectionThreshold: 60,
			maxQueueSize:       100,
			expectedRate:       .5,
		},
		{
			name:               "queueSize equal to maxQueueSize",
			queueSize:          100,
			rejectionThreshold: 60,
			maxQueueSize:       100,
			expectedRate:       1,
		},
		{
			name:               "queueSize above maxQueueSize",
			queueSize:          120,
			rejectionThreshold: 60,
			maxQueueSize:       100,
			expectedRate:       1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rate := computeRejectionRate(tc.queueSize, tc.rejectionThreshold, tc.maxQueueSize)
			require.Equal(t, tc.expectedRate, rate)
		})
	}
}
