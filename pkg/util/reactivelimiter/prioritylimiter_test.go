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

// Test the new priority adapter methods that convert int priority to Priority enum
func TestPriorityLimiter_AcquirePermitWithPriority(t *testing.T) {
	p := NewPrioritizer(nil).(*prioritizer)
	limiter := NewPriorityLimiter(createLimiterConfig(), p, nil).(*priorityLimiter)

	testCases := []struct {
		name        string
		priority    int
		expectedEnum Priority
		description string
	}{
		{"VeryHigh priority (450)", 450, PriorityVeryHigh, "VeryHigh priority (400-499)"},
		{"High priority (350)", 350, PriorityHigh, "High priority (300-399)"},
		{"Medium priority (250)", 250, PriorityMedium, "Medium priority (200-299)"},
		{"Low priority (150)", 150, PriorityLow, "Low priority (100-199)"},
		{"VeryLow priority (50)", 50, PriorityVeryLow, "VeryLow priority (0-99)"},
		{"Boundary: 400", 400, PriorityVeryHigh, "Boundary test for VeryHigh"},
		{"Boundary: 399", 399, PriorityHigh, "Boundary test for High"},
		{"Boundary: 200", 200, PriorityMedium, "Boundary test for Medium"},
		{"Boundary: 100", 100, PriorityLow, "Boundary test for Low"},
		{"Boundary: 0", 0, PriorityVeryLow, "Boundary test for VeryLow"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// Test that AcquirePermitWithPriority works (no rejection threshold set)
			p.priorityThreshold.Store(int32(0)) // No rejection
			permit, err := limiter.AcquirePermitWithPriority(context.Background(), tc.priority)
			require.NoError(t, err)
			require.NotNil(t, permit)
			
			// Clean up the permit
			permit.Record()
		})
	}
}

func TestPriorityLimiter_CanAcquirePermitWithPriority(t *testing.T) {
	p := NewPrioritizer(nil).(*prioritizer)
	limiter := NewPriorityLimiter(createLimiterConfig(), p, nil).(*priorityLimiter)

	testCases := []struct {
		name              string
		priority          int
		rejectionThreshold int
		expectedCanAcquire bool
	}{
		{"VeryHigh priority (450) no rejection", 450, 0, true},
		{"VeryHigh priority (450) below threshold", 450, 500, false},
		{"VeryHigh priority (450) above threshold", 450, 400, true},
		{"High priority (350) above threshold", 350, 300, true},
		{"High priority (350) below threshold", 350, 400, false},
		{"Medium priority (250) above threshold", 250, 200, true},
		{"Low priority (150) above threshold", 150, 100, true},
		{"VeryLow priority (50) above threshold", 50, 0, true},
		{"VeryLow priority (50) below threshold", 50, 100, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			p.priorityThreshold.Store(int32(tc.rejectionThreshold))
			canAcquire := limiter.CanAcquirePermitWithPriority(tc.priority)
			require.Equal(t, tc.expectedCanAcquire, canAcquire)
		})
	}
}

func TestPriorityLimiter_PriorityRejectionWithIntPriority(t *testing.T) {
	p := NewPrioritizer(nil).(*prioritizer)
	limiter := NewPriorityLimiter(createLimiterConfig(), p, nil).(*priorityLimiter)

	// Set rejection threshold that should block Low and VeryLow priorities
	p.priorityThreshold.Store(int32(250))

	// VeryHigh priority (450) should be allowed
	permit1, err1 := limiter.AcquirePermitWithPriority(context.Background(), 450)
	require.NoError(t, err1)
	require.NotNil(t, permit1)

	// High priority (350) should be allowed  
	permit2, err2 := limiter.AcquirePermitWithPriority(context.Background(), 350)
	require.NoError(t, err2)
	require.NotNil(t, permit2)

	// Medium priority (250) should be allowed (equal to threshold, converted to granular priority might allow)
	canAcquire250 := limiter.CanAcquirePermitWithPriority(250)
	require.True(t, canAcquire250)

	// Low priority (150) should be rejected
	permit3, err3 := limiter.AcquirePermitWithPriority(context.Background(), 150)
	require.Error(t, err3)
	require.ErrorIs(t, err3, ErrExceeded)
	require.Nil(t, permit3)

	// VeryLow priority (50) should be rejected
	permit4, err4 := limiter.AcquirePermitWithPriority(context.Background(), 50)
	require.Error(t, err4)
	require.ErrorIs(t, err4, ErrExceeded)
	require.Nil(t, permit4)

	// Clean up permits
	permit1.Record()
	permit2.Record()
}

func TestConvertIntToPriority(t *testing.T) {
	testCases := []struct {
		intPriority      int
		expectedPriority Priority
		description      string
	}{
		{499, PriorityVeryHigh, "Max VeryHigh"},
		{450, PriorityVeryHigh, "Mid VeryHigh"},
		{400, PriorityVeryHigh, "Min VeryHigh"},
		{399, PriorityHigh, "Max High"},
		{350, PriorityHigh, "Mid High"},
		{300, PriorityHigh, "Min High"},
		{299, PriorityMedium, "Max Medium"},
		{250, PriorityMedium, "Mid Medium"},
		{200, PriorityMedium, "Min Medium"},
		{199, PriorityLow, "Max Low"},
		{150, PriorityLow, "Mid Low"},
		{100, PriorityLow, "Min Low"},
		{99, PriorityVeryLow, "Max VeryLow"},
		{50, PriorityVeryLow, "Mid VeryLow"},
		{0, PriorityVeryLow, "Min VeryLow"},
		{-10, PriorityVeryLow, "Below range"},
		{1000, PriorityVeryHigh, "Above range"},
	}

	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			result := convertIntToPriority(tc.intPriority)
			require.Equal(t, tc.expectedPriority, result)
		})
	}
}
