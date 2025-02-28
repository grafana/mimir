// SPDX-License-Identifier: AGPL-3.0-only

package reactivelimiter

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPriorityLimiter_AcquirePermit(t *testing.T) {
	p := NewPrioritizer(nil).(*prioritizer)
	limiter := NewPriorityLimiter(createLimiterConfig(), p, nil).(*priorityLimiter)

	t.Run("with no rejection threshold", func(t *testing.T) {
		permit, err := limiter.AcquirePermit(context.Background(), PriorityLow)
		require.NotNil(t, permit)
		require.NoError(t, err)
	})

	t.Run("below prioritizer rejection threshold", func(t *testing.T) {
		p.priorityThreshold.Store(200)
		permit, err := limiter.AcquirePermit(context.Background(), PriorityLow)
		require.Nil(t, permit)
		require.Error(t, err, ErrExceeded)
	})

	t.Run("above prioritizer rejection threshold", func(t *testing.T) {
		p.priorityThreshold.Store(200)
		permit, err := limiter.AcquirePermit(context.Background(), PriorityHigh)
		require.NotNil(t, permit)
		require.NoError(t, err)
	})

	// Asserts that AcquirePermit fails after the max number of requests is rejected, even if the request exceeds the priority threshold
	t.Run("above max blocked requests", func(t *testing.T) {
		config := createLimiterConfig()
		config.InitialInflightLimit = 1
		limiter := NewPriorityLimiter(config, p, nil).(*priorityLimiter)
		p.priorityThreshold.Store(200)

		// Add a request and 3 waiters
		for i := 0; i < 4; i++ {
			go func() {
				permit, err := limiter.AcquirePermit(context.Background(), PriorityHigh)
				require.NotNil(t, permit)
				require.NoError(t, err)
			}()
		}

		require.Eventually(t, func() bool {
			return limiter.Blocked() == 3
		}, 300*time.Millisecond, 10*time.Millisecond)
		permit, err := limiter.AcquirePermit(context.Background(), PriorityHigh)
		require.Nil(t, permit)
		require.ErrorIs(t, err, ErrExceeded)
	})
}
