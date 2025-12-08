// SPDX-License-Identifier: AGPL-3.0-only

package gcs

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
)

type rateLimiterMode int

const (
	uploadRateLimiter rateLimiterMode = iota
	readRateLimiter
)

const (
	// backoffCooldown is the minimum time between successive backoffs to prevent
	// oscillation when multiple concurrent requests hit rate limits.
	backoffCooldown = 5 * time.Second
)

// rateLimiter implements request rate limiting with exponential doubling
// following Google Cloud Storage best practices for ramping up request rates.
// See: https://cloud.google.com/storage/docs/request-rate#ramp-up.
//
// The rate limiter also supports adaptive backoff: when GCS returns a 429
// rate limit error, the rate is immediately halved to reduce pressure.
type rateLimiter struct {
	initialQPS int
	maxQPS     int
	rampPeriod time.Duration

	mu          sync.Mutex
	startTime   time.Time
	currentQPS  int
	limiter     *rate.Limiter
	lastBackoff time.Time // Time of last backoff to prevent rapid successive backoffs

	rateLimitedSeconds prometheus.Counter
	currentQPSGauge    prometheus.Gauge
	requestsTotal      *prometheus.CounterVec
	backoffsTotal      prometheus.Counter
}

// newRateLimiter creates a new rate limiter with exponential doubling.
// The rate starts at initialQPS (capped by maxQPS) and doubles every rampPeriod until it reaches maxQPS.
// The mode parameter configures whether to rate limit uploads or reads (used for metrics labels).
// If reg is nil, no metrics will be registered.
func newRateLimiter(name string, initialQPS, maxQPS int, rampPeriod time.Duration, mode rateLimiterMode, reg prometheus.Registerer) *rateLimiter {
	var operation string
	switch mode {
	case uploadRateLimiter:
		operation = "upload"
	case readRateLimiter:
		operation = "read"
	default:
		panic(fmt.Errorf("unrecognized rateLimiterMode %v", mode))
	}
	startQPS := min(initialQPS, maxQPS)
	rl := &rateLimiter{
		initialQPS: initialQPS,
		maxQPS:     maxQPS,
		rampPeriod: rampPeriod,
		startTime:  time.Now(),
		currentQPS: startQPS,
		limiter:    rate.NewLimiter(rate.Limit(startQPS), startQPS*2), // Burst = 2 seconds worth of requests.
	}

	if reg != nil {
		constLabels := prometheus.Labels{"name": name, "operation": operation}
		rl.rateLimitedSeconds = promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_gcs_rate_limited_seconds_total",
			Help:        "Total seconds spent waiting due to GCS rate limiting.",
			ConstLabels: constLabels,
		})
		rl.currentQPSGauge = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name:        "cortex_gcs_current_qps",
			Help:        "Current queries per second limit for GCS operations.",
			ConstLabels: constLabels,
		})
		rl.requestsTotal = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_gcs_requests_total",
			Help:        "Total GCS requests, labeled by whether they were allowed immediately or rate limited.",
			ConstLabels: constLabels,
		}, []string{"allowed"})
		rl.backoffsTotal = promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_gcs_rate_limit_backoffs_total",
			Help:        "Total number of backoffs due to GCS 429 rate limit errors.",
			ConstLabels: constLabels,
		})
		rl.currentQPSGauge.Set(float64(startQPS))
	}

	return rl
}

// Wait blocks until the rate limiter allows a request to proceed.
// It implements exponential doubling of the rate over time.
func (rl *rateLimiter) Wait(ctx context.Context) error {
	rl.mu.Lock()
	rl.maybeUpdateRate()
	rl.mu.Unlock()

	r := rl.limiter.Reserve()
	delay := r.Delay()

	if delay == 0 {
		if rl.requestsTotal != nil {
			rl.requestsTotal.WithLabelValues("true").Inc()
		}
		return nil
	}

	if rl.requestsTotal != nil {
		rl.requestsTotal.WithLabelValues("false").Inc()
	}
	if rl.rateLimitedSeconds != nil {
		rl.rateLimitedSeconds.Add(delay.Seconds())
	}

	select {
	case <-ctx.Done():
		r.Cancel()
		return ctx.Err()
	case <-time.After(delay):
		return nil
	}
}

// maybeUpdateRate updates the rate limit based on exponential doubling.
// Has to be called with rl.mu lock taken.
func (rl *rateLimiter) maybeUpdateRate() {
	elapsed := time.Since(rl.startTime)
	periodsElapsed := int(elapsed / rl.rampPeriod)

	// Calculate new QPS: initialQPS * 2^periodsElapsed, capped at maxQPS.
	newQPS := min(rl.initialQPS, rl.maxQPS)
	for range periodsElapsed {
		newQPS *= 2
		if newQPS >= rl.maxQPS {
			newQPS = rl.maxQPS
			break
		}
	}

	if newQPS != rl.currentQPS {
		rl.currentQPS = newQPS
		rl.limiter.SetLimit(rate.Limit(newQPS))
		rl.limiter.SetBurst(newQPS * 2)
		if rl.currentQPSGauge != nil {
			rl.currentQPSGauge.Set(float64(newQPS))
		}
	}
}

// getCurrentQPS returns the current QPS limit for observability.
func (rl *rateLimiter) getCurrentQPS() int {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	rl.maybeUpdateRate()
	return rl.currentQPS
}

// Backoff reduces the rate limit in response to a 429 rate limit error from GCS.
// It halves the current QPS and resets the ramp-up start time so that the rate
// will gradually increase again from the new lower value.
//
// To prevent oscillation when multiple concurrent requests hit rate limits,
// backoff is only applied if at least backoffCooldown has passed since the last backoff.
func (rl *rateLimiter) Backoff() {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	if now.Sub(rl.lastBackoff) < backoffCooldown {
		// Too soon since last backoff, skip to avoid oscillation.
		return
	}

	// Halve the current QPS, but don't go below 1.
	newQPS := max(rl.currentQPS/2, 1)
	if newQPS == rl.currentQPS {
		// Already at minimum, nothing to do.
		return
	}

	rl.currentQPS = newQPS
	rl.limiter.SetLimit(rate.Limit(newQPS))
	rl.limiter.SetBurst(newQPS * 2)

	// Reset start time so ramp-up begins from this new lower rate.
	// We set initialQPS to the new backed-off rate so doubling starts from here.
	rl.initialQPS = newQPS
	rl.startTime = now
	rl.lastBackoff = now

	if rl.currentQPSGauge != nil {
		rl.currentQPSGauge.Set(float64(newQPS))
	}
	if rl.backoffsTotal != nil {
		rl.backoffsTotal.Inc()
	}
}
