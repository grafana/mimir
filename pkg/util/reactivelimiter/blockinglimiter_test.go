// SPDX-License-Identifier: AGPL-3.0-only

package reactivelimiter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test asserts that blocking requests block or are rejected and the rejection rate is updated as expected.
func TestBlockingLimiter_AcquirePermit(t *testing.T) {
	config := createLimiterConfig()
	config.InitialInflightLimit = 1
	config.MinInflightLimit = 10
	config.MaxInflightLimit = 1
	config.InitialRejectionFactor = 2
	config.MaxRejectionFactor = 4
	limiter := NewBlockingLimiter(config, nil).(*blockingLimiter)

	_, err := limiter.AcquirePermit(context.Background())
	require.NoError(t, err)

	acquireBlocking := func() {
		// Call until blocking
		for {
			_, _ = limiter.AcquirePermit(context.Background())
		}
	}
	assertBlocked := func(blocked int) {
		require.Eventually(t, func() bool {
			return limiter.Blocked() == blocked
		}, 300*time.Millisecond, 10*time.Millisecond)
	}

	go acquireBlocking()
	assertBlocked(1)
	go acquireBlocking()
	assertBlocked(2)
	go acquireBlocking()
	assertBlocked(3)
	assert.Equal(t, .5, limiter.computeRejectionRate())
	go acquireBlocking()
	assertBlocked(4)
	assert.Equal(t, 1.0, limiter.computeRejectionRate())

	// Queue is full
	permit, err := limiter.AcquirePermit(context.Background())
	require.Nil(t, permit)
	require.ErrorIs(t, err, ErrExceeded)
	assertBlocked(4)
	require.Eventually(t, func() bool {
		return limiter.computeRejectionRate() == 1.0
	}, 300*time.Millisecond, 10*time.Millisecond)
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
