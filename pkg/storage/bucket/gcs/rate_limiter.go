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

// rateLimiter implements request rate limiting with exponential doubling
// following Google Cloud Storage best practices for ramping up request rates.
// See: https://cloud.google.com/storage/docs/request-rate#ramp-up.
type rateLimiter struct {
	initialQPS int
	maxQPS     int
	rampPeriod time.Duration

	mu         sync.Mutex
	startTime  time.Time
	currentQPS int
	limiter    *rate.Limiter

	rateLimitedSeconds prometheus.Counter
	currentQPSGauge    prometheus.Gauge
	requestsTotal      *prometheus.CounterVec
}

// newRateLimiter creates a new rate limiter with exponential doubling.
// The rate starts at initialQPS (capped by maxQPS) and doubles every rampPeriod until it reaches maxQPS.
// The mode parameter configures whether to rate limit uploads or reads (used for metrics labels).
// If reg is nil, no metrics will be registered.
func newRateLimiter(initialQPS, maxQPS int, rampPeriod time.Duration, mode rateLimiterMode, reg prometheus.Registerer) *rateLimiter {
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
		constLabels := prometheus.Labels{"operation": operation}
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
