// SPDX-License-Identifier: AGPL-3.0-only

package reactivelimiter

import (
	"context"
	"math/rand"

	"github.com/go-kit/log"
)

type Priority int

const (
	PriorityVeryLow Priority = iota
	PriorityLow
	PriorityMedium
	PriorityHigh
	PriorityVeryHigh
)

// priorityRange provides a wider range of priorities that allow for rejecting a subset of requests within a Priority.
type priorityRange struct {
	lower, upper int
}

// Defining the priority ranges as a map
var priorityRanges = map[Priority]priorityRange{
	PriorityVeryLow:  {0, 99},
	PriorityLow:      {100, 199},
	PriorityMedium:   {200, 299},
	PriorityHigh:     {300, 399},
	PriorityVeryHigh: {400, 499},
}

func randomGranularPriority(priority Priority) int {
	r := priorityRanges[priority]
	return rand.Intn(r.upper-r.lower+1) + r.lower
}

// PriorityLimiter is a reactive concurrency limiter that can prioritize request rejections via a Prioritizer.
type PriorityLimiter interface {
	Metrics

	// AcquirePermit attempts to acquire a permit, potentially blocking based on the prioriter current rejection threshold.
	// The request priority must be greater than the current priority threshold for admission.
	AcquirePermit(ctx context.Context, priority Priority) (Permit, error)

	// CanAcquirePermit returns whether it's currently possible to acquire a permit for the priority.
	CanAcquirePermit(priority Priority) bool

	// Reset resets the limiter to its initial limit.
	Reset()
}

func NewPriorityLimiter(config *Config, prioritizer Prioritizer, logger log.Logger) PriorityLimiter {
	limiter := &priorityLimiter{
		reactiveLimiter: newLimiter(config, logger),
		prioritizer:     prioritizer,
	}
	prioritizer.register(limiter)
	return limiter
}

type priorityLimiter struct {
	*reactiveLimiter
	prioritizer Prioritizer
}

func (l *priorityLimiter) AcquirePermit(ctx context.Context, priority Priority) (Permit, error) {
	// Generate a granular priority for the request and check if we can acquire a permit
	granularPriority := randomGranularPriority(priority)
	if !l.canAcquirePermit(granularPriority) {
		return nil, ErrExceeded
	}

	l.prioritizer.recordPriority(granularPriority)
	return l.reactiveLimiter.AcquirePermit(ctx)
}

func (l *priorityLimiter) CanAcquirePermit(priority Priority) bool {
	return l.canAcquirePermit(randomGranularPriority(priority))
}

func (l *priorityLimiter) canAcquirePermit(granularPriority int) bool {
	// Threshold against the limiter's max capacity
	_, _, _, maxBlocked := l.queueStats()
	if l.Blocked() >= maxBlocked {
		return false
	}

	// Threshold against the prioritizer's rejection threshold
	return granularPriority >= l.prioritizer.RejectionThreshold()
}

func (l *priorityLimiter) RejectionRate() float64 {
	return l.prioritizer.RejectionRate()
}

// AcquirePermitWithPriority adapts int priority to Priority enum for BlockingLimiter interface
func (l *priorityLimiter) AcquirePermitWithPriority(ctx context.Context, priority int) (Permit, error) {
	priorityEnum := convertIntToPriority(priority)
	return l.AcquirePermit(ctx, priorityEnum)
}

// CanAcquirePermitWithPriority adapts int priority to Priority enum for BlockingLimiter interface  
func (l *priorityLimiter) CanAcquirePermitWithPriority(priority int) bool {
	priorityEnum := convertIntToPriority(priority)
	return l.CanAcquirePermit(priorityEnum)
}

// convertIntToPriority converts int priority (0-499) to Priority enum
func convertIntToPriority(priority int) Priority {
	switch {
	case priority >= 400:
		return PriorityVeryHigh
	case priority >= 300:
		return PriorityHigh
	case priority >= 200:
		return PriorityMedium
	case priority >= 100:
		return PriorityLow
	default:
		return PriorityVeryLow
	}
}
