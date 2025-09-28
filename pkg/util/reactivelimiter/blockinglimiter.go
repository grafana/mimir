// SPDX-License-Identifier: AGPL-3.0-only

package reactivelimiter

import (
	"context"
	"math/rand"

	"github.com/go-kit/log"
)

// BlockingLimiter is a reactive concurrency limiter that block requests when the limiter is full, based on a rejection
// threshold.
type BlockingLimiter interface {
	Metrics

	// AcquirePermit attempts to acquire a permit, potentially blocking based on the limiter's current queue size and
	// rejection threshold.
	AcquirePermit(ctx context.Context) (Permit, error)

	// CanAcquirePermit returns whether it's currently possible to acquire a permit, based on the number of requests
	// inflight, the current queue size, and current rejection threshold.
	CanAcquirePermit() bool

	// AcquirePermitWithPriority attempts to acquire a permit with priority-aware rejection
	AcquirePermitWithPriority(ctx context.Context, priority int) (Permit, error)

	// CanAcquirePermitWithPriority returns whether a permit can be acquired for the given priority
	CanAcquirePermitWithPriority(priority int) bool

	// Reset resets the limiter to its initial limit.
	Reset()
}

// blockingLimiter wraps an adaptiveLimiter and blocks some portion of requests when the adaptiveLimiter is at its
// limit.
type blockingLimiter struct {
	*reactiveLimiter
}

func NewBlockingLimiter(config *Config, logger log.Logger) BlockingLimiter {
	return &blockingLimiter{
		reactiveLimiter: newLimiter(config, logger),
	}
}

func (l *blockingLimiter) AcquirePermit(ctx context.Context) (Permit, error) {
	if !l.CanAcquirePermit() {
		return nil, ErrExceeded
	}

	return l.reactiveLimiter.AcquirePermit(ctx)
}

func (l *blockingLimiter) CanAcquirePermit() bool {
	if l.reactiveLimiter.CanAcquirePermit() {
		return true
	}

	rejectionRate := l.computeRejectionRate()
	if rejectionRate == 0 {
		return true
	}
	if rejectionRate >= 1 || rejectionRate >= rand.Float64() {
		return false
	}
	return true
}

func (l *blockingLimiter) AcquirePermitWithPriority(ctx context.Context, priority int) (Permit, error) {
	if !l.CanAcquirePermitWithPriority(priority) {
		return nil, ErrExceeded
	}

	return l.reactiveLimiter.AcquirePermit(ctx)
}

func (l *blockingLimiter) CanAcquirePermitWithPriority(priority int) bool {
	if l.reactiveLimiter.CanAcquirePermit() {
		return true
	}

	rejectionRate := l.computeRejectionRate()
	if rejectionRate == 0 {
		return true
	}

	var priorityThreshold float64
	switch {
	case priority >= 400:
		priorityThreshold = 0.95
	case priority >= 300:
		priorityThreshold = 0.80
	case priority >= 200:
		priorityThreshold = 0.60
	case priority >= 100:
		priorityThreshold = 0.40
	default:
		priorityThreshold = 0.20
	}

	if rejectionRate > priorityThreshold {
		return false
	}

	return true
}

func (l *blockingLimiter) computeRejectionRate() float64 {
	_, blocked, rejectionThreshold, maxQueueSize := l.queueStats()
	return computeRejectionRate(blocked, rejectionThreshold, maxQueueSize)
}

func computeRejectionRate(queueSize, rejectionThreshold, maxQueueSize int) float64 {
	if queueSize <= rejectionThreshold {
		return 0
	}
	if queueSize >= maxQueueSize {
		return 1
	}
	return float64(queueSize-rejectionThreshold) / float64(maxQueueSize-rejectionThreshold)
}
