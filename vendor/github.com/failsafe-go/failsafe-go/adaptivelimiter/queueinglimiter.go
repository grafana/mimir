package adaptivelimiter

import (
	"context"
	"math/rand"
	"time"

	"github.com/failsafe-go/failsafe-go/policy"
)

// queueingLimiter wraps an adaptiveLimiter and queues some portion of executions when the adaptiveLimiter is full.
type queueingLimiter[R any] struct {
	*adaptiveLimiter[R]
}

func (l *queueingLimiter[R]) AcquirePermit(ctx context.Context) (Permit, error) {
	if !l.CanAcquirePermit() {
		return nil, ErrExceeded
	}

	// Acquire a permit, blocking if needed
	return l.adaptiveLimiter.AcquirePermit(ctx)
}

func (l *queueingLimiter[R]) AcquirePermitWithMaxWait(ctx context.Context, maxWaitTime time.Duration) (Permit, error) {
	if !l.CanAcquirePermit() {
		return nil, ErrExceeded
	}

	// Acquire a permit, blocking if needed
	return l.adaptiveLimiter.AcquirePermitWithMaxWait(ctx, maxWaitTime)
}

// TryAcquirePermit for a queueingLimiter adds no new behavior since it needs to return immediately, even if the
// semaphore is full, regardless of the queue size.
func (l *queueingLimiter[R]) TryAcquirePermit() (Permit, bool) {
	return l.adaptiveLimiter.TryAcquirePermit()
}

// CanAcquirePermit returns whether a permit can be acquired based on the semaphore or the queue.
func (l *queueingLimiter[R]) CanAcquirePermit() bool {
	// Check with semaphore
	if l.adaptiveLimiter.CanAcquirePermit() {
		return true
	}

	// Check with queue
	rejectionRate := l.getQueueStats().ComputeRejectionRate()
	if rejectionRate == 0 {
		return true
	}
	if rejectionRate >= 1 || rejectionRate >= rand.Float64() {
		return false
	}
	return true
}

func (l *queueingLimiter[R]) ToExecutor(_ R) any {
	e := &executor[R]{
		BaseExecutor:    policy.BaseExecutor[R]{},
		blockingLimiter: l,
	}
	e.Executor = e
	return e
}

func (l *queueingLimiter[R]) canAcquirePermit(_ context.Context) bool {
	return l.CanAcquirePermit()
}

func (l *queueingLimiter[R]) configRef() *config[R] {
	return &l.config
}

func (l *queueingLimiter[R]) getQueueStats() *queueStats {
	limit := l.Limit()
	return &queueStats{
		limit:              limit,
		queued:             l.Queued(),
		rejectionThreshold: int(float64(limit) * l.initialRejectionFactor),
		maxQueue:           int(float64(limit) * l.maxRejectionFactor),
	}
}

// Implements priority.Stats.
type queueStats struct {
	limit              int
	queued             int
	rejectionThreshold int
	maxQueue           int
}

func (s *queueStats) ComputeRejectionRate() float64 {
	if s.queued < s.rejectionThreshold {
		return 0
	}
	if s.queued >= s.maxQueue {
		return 1
	}
	return float64(s.queued-s.rejectionThreshold) / float64(s.maxQueue-s.rejectionThreshold)
}

func (s *queueStats) DebugLogArgs() []any {
	return []any{"limit", s.limit, "queued", s.queued}
}
