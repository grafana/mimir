// SPDX-License-Identifier: AGPL-3.0-only

package gcs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRateLimiter(t *testing.T) {
	t.Run("starts at initial QPS capped by max", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		limiter := newRateLimiter("test", 1000, 3200, 20*time.Minute, uploadRateLimiter, reg)

		assert.Equal(t, 1000, limiter.getCurrentQPS())
	})

	t.Run("doubles QPS each ramp period until max", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		// Use short ramp period for testing.
		limiter := newRateLimiter("test", 1000, 8000, 100*time.Millisecond, uploadRateLimiter, reg)

		assert.Equal(t, 1000, limiter.getCurrentQPS())

		// After one ramp period, should double.
		time.Sleep(110 * time.Millisecond)
		assert.Equal(t, 2000, limiter.getCurrentQPS())

		// After two ramp periods, should double again.
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, 4000, limiter.getCurrentQPS())

		// After three ramp periods, should double again and hit cap.
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, 8000, limiter.getCurrentQPS())

		// After four ramp periods, should still be capped at maxQPS.
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, 8000, limiter.getCurrentQPS())
	})

	t.Run("allows requests up to burst limit immediately", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		// maxQPS below initialQPS, so it will be capped at maxQPS=500.
		limiter := newRateLimiter("test", 1000, 500, 20*time.Minute, uploadRateLimiter, reg)

		ctx := context.Background()
		start := time.Now()

		// Should be able to consume initial burst quickly (burst size = 2 * currentQPS = 1000).
		for range 500 {
			err := limiter.Wait(ctx)
			require.NoError(t, err)
		}

		elapsed := time.Since(start)
		// Should complete in under 100ms (most tokens were pre-allocated).
		assert.Less(t, elapsed, 100*time.Millisecond)
	})

	t.Run("blocks when burst is exhausted", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		// Low QPS to test blocking (will cap initialQPS=1000 to maxQPS=10).
		limiter := newRateLimiter("test", 1000, 10, 20*time.Minute, uploadRateLimiter, reg)

		ctx := context.Background()

		// Consume all initial burst tokens (burst = 2 * 10 = 20).
		for range 20 {
			err := limiter.Wait(ctx)
			require.NoError(t, err)
		}

		// Next request should block.
		start := time.Now()
		err := limiter.Wait(ctx)
		require.NoError(t, err)
		elapsed := time.Since(start)

		// Should have waited approximately 1/10th of a second (1 token at 10 QPS).
		assert.Greater(t, elapsed, 80*time.Millisecond)
		assert.Less(t, elapsed, 150*time.Millisecond)
	})

	t.Run("returns error when context cancelled", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		// Very low QPS to ensure blocking (will cap initialQPS=1000 to maxQPS=1).
		limiter := newRateLimiter("test", 1000, 1, 20*time.Minute, uploadRateLimiter, reg)

		// Consume all initial burst tokens (burst = 2 * 1 = 2).
		for range 2 {
			err := limiter.Wait(context.Background())
			require.NoError(t, err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err := limiter.Wait(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("records metrics for allowed and limited requests", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		// maxQPS=100 caps initialQPS=1000 down to 100.
		limiter := newRateLimiter("test", 1000, 100, 20*time.Minute, uploadRateLimiter, reg)

		ctx := context.Background()

		// Make some requests that should be allowed immediately.
		for range 5 {
			err := limiter.Wait(ctx)
			require.NoError(t, err)
		}

		assert.Equal(t, 5.0, testutil.ToFloat64(limiter.requestsTotal.WithLabelValues("true")))
		assert.Equal(t, 0.0, testutil.ToFloat64(limiter.requestsTotal.WithLabelValues("false")))

		assert.Equal(t, 100.0, testutil.ToFloat64(limiter.currentQPSGauge))

		// Consume remaining burst tokens to trigger rate limiting (burst = 200, already used 5).
		for range 195 {
			_ = limiter.Wait(ctx)
		}

		err := limiter.Wait(ctx)
		require.NoError(t, err)
		// Should have at least one rate-limited request now.
		assert.GreaterOrEqual(t, testutil.ToFloat64(limiter.requestsTotal.WithLabelValues("false")), 1.0)
		assert.Greater(t, testutil.ToFloat64(limiter.rateLimitedSeconds), 0.0)
	})

	t.Run("is safe for concurrent use", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		// maxQPS=50 caps initialQPS=1000 down to 50.
		limiter := newRateLimiter("test", 1000, 50, 20*time.Minute, uploadRateLimiter, reg)

		ctx := context.Background()
		numGoroutines := 100

		start := time.Now()

		var wg sync.WaitGroup
		for range numGoroutines {
			wg.Add(1)
			go func() {
				defer wg.Done()
				err := limiter.Wait(ctx)
				require.NoError(t, err)
			}()
		}
		wg.Wait()

		elapsed := time.Since(start)

		// With 50 QPS and 100 requests:
		// - First 100 requests use initial token bucket (2 seconds worth = 100 tokens)
		// - Should complete quickly since burst covers all requests.
		assert.Less(t, elapsed, 3*time.Second)

		// Verify all requests were processed.
		totalRequests := testutil.ToFloat64(limiter.requestsTotal.WithLabelValues("true")) +
			testutil.ToFloat64(limiter.requestsTotal.WithLabelValues("false"))
		assert.Equal(t, float64(numGoroutines), totalRequests)
	})

	t.Run("upload and read limiters have separate metrics", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()

		// Create rate limiters for different operations.
		uploadLimiter := newRateLimiter("test", 100, 100, 20*time.Minute, uploadRateLimiter, reg)
		readLimiter := newRateLimiter("test", 200, 200, 20*time.Minute, readRateLimiter, reg)

		ctx := context.Background()

		// Use both limiters.
		require.NoError(t, uploadLimiter.Wait(ctx))
		require.NoError(t, readLimiter.Wait(ctx))

		// Verify separate metrics.
		assert.Equal(t, 1.0, testutil.ToFloat64(uploadLimiter.requestsTotal.WithLabelValues("true")))
		assert.Equal(t, 1.0, testutil.ToFloat64(readLimiter.requestsTotal.WithLabelValues("true")))
		assert.Equal(t, 100.0, testutil.ToFloat64(uploadLimiter.currentQPSGauge))
		assert.Equal(t, 200.0, testutil.ToFloat64(readLimiter.currentQPSGauge))
	})

	t.Run("works without metrics when registerer is nil", func(t *testing.T) {
		limiter := newRateLimiter("test", 100, 100, 20*time.Minute, uploadRateLimiter, nil)

		// Verify rate limiter works without metrics.
		ctx := context.Background()
		err := limiter.Wait(ctx)
		require.NoError(t, err)
		assert.Nil(t, limiter.requestsTotal)
		assert.Nil(t, limiter.currentQPSGauge)
		assert.Nil(t, limiter.rateLimitedSeconds)

		// Verify rate limiting still works.
		assert.Equal(t, 100, limiter.getCurrentQPS())
	})

	t.Run("rate limits without panic when registerer is nil", func(t *testing.T) {
		// Test that rate limiting works correctly even with nil registerer
		// when we need to wait (i.e., exceed the burst).
		limiter := newRateLimiter("test", 1000, 10, 20*time.Minute, uploadRateLimiter, nil)

		ctx := context.Background()

		// Consume all burst tokens (burst = 2 * 10 = 20).
		for range 20 {
			err := limiter.Wait(ctx)
			require.NoError(t, err)
		}

		// Next request should block but not panic due to nil metrics.
		start := time.Now()
		err := limiter.Wait(ctx)
		require.NoError(t, err)
		elapsed := time.Since(start)

		// Should have waited approximately 1/10th of a second (1 token at 10 QPS).
		assert.Greater(t, elapsed, 80*time.Millisecond)
	})

	t.Run("respects custom initial QPS", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		// Use a custom initial QPS of 50 with max of 200.
		limiter := newRateLimiter("test", 50, 200, 100*time.Millisecond, uploadRateLimiter, reg)

		assert.Equal(t, 50, limiter.getCurrentQPS())

		// After one ramp period, should double to 100.
		time.Sleep(110 * time.Millisecond)
		assert.Equal(t, 100, limiter.getCurrentQPS())

		// After two ramp periods, should double to 200 (max).
		time.Sleep(100 * time.Millisecond)
		assert.Equal(t, 200, limiter.getCurrentQPS())
	})

	t.Run("backoff halves QPS", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		limiter := newRateLimiter("test", 100, 100, 20*time.Minute, uploadRateLimiter, reg)

		assert.Equal(t, 100, limiter.getCurrentQPS())

		limiter.Backoff()
		assert.Equal(t, 50, limiter.getCurrentQPS())

		// Verify metric was incremented.
		assert.Equal(t, 1.0, testutil.ToFloat64(limiter.backoffsTotal))
		assert.Equal(t, 50.0, testutil.ToFloat64(limiter.currentQPSGauge))
	})

	t.Run("backoff respects cooldown", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		limiter := newRateLimiter("test", 100, 100, 20*time.Minute, uploadRateLimiter, reg)

		assert.Equal(t, 100, limiter.getCurrentQPS())

		// First backoff should work.
		limiter.Backoff()
		assert.Equal(t, 50, limiter.getCurrentQPS())

		// Second immediate backoff should be ignored due to cooldown.
		limiter.Backoff()
		assert.Equal(t, 50, limiter.getCurrentQPS())

		// Only one backoff should have been recorded.
		assert.Equal(t, 1.0, testutil.ToFloat64(limiter.backoffsTotal))
	})

	t.Run("backoff does not go below 1 QPS", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		limiter := newRateLimiter("test", 2, 2, 20*time.Minute, uploadRateLimiter, reg)

		assert.Equal(t, 2, limiter.getCurrentQPS())

		// First backoff: 2 -> 1.
		limiter.Backoff()
		assert.Equal(t, 1, limiter.getCurrentQPS())

		// Manually reset lastBackoff to allow another backoff.
		limiter.mu.Lock()
		limiter.lastBackoff = time.Time{}
		limiter.mu.Unlock()

		// Second backoff should not go below 1.
		limiter.Backoff()
		assert.Equal(t, 1, limiter.getCurrentQPS())

		// Only one backoff should have been recorded (second was a no-op).
		assert.Equal(t, 1.0, testutil.ToFloat64(limiter.backoffsTotal))
	})

	t.Run("backoff restarts ramp-up from backed-off rate", func(t *testing.T) {
		reg := prometheus.NewPedanticRegistry()
		limiter := newRateLimiter("test", 100, 200, 50*time.Millisecond, uploadRateLimiter, reg)

		assert.Equal(t, 100, limiter.getCurrentQPS())

		// Let it ramp up to 200.
		time.Sleep(60 * time.Millisecond)
		assert.Equal(t, 200, limiter.getCurrentQPS())

		// Backoff: 200 -> 100.
		limiter.Backoff()
		assert.Equal(t, 100, limiter.getCurrentQPS())

		// After one ramp period, should double to 200 again.
		time.Sleep(60 * time.Millisecond)
		assert.Equal(t, 200, limiter.getCurrentQPS())
	})

	t.Run("backoff works without metrics when registerer is nil", func(t *testing.T) {
		limiter := newRateLimiter("test", 100, 100, 20*time.Minute, uploadRateLimiter, nil)

		assert.Equal(t, 100, limiter.getCurrentQPS())

		// Should not panic.
		limiter.Backoff()
		assert.Equal(t, 50, limiter.getCurrentQPS())
	})
}
