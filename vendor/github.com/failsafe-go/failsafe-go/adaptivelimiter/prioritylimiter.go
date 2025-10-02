package adaptivelimiter

import (
	"context"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/policy"
	"github.com/failsafe-go/failsafe-go/priority"
)

// PriorityLimiter is an adaptive concurrency limiter that can prioritize execution rejections during overload. When the
// limiter and its queue start to become full, it uses a Prioritizer to determine which priority levels should be
// rejected, allowing higher-priority executions to proceed while shedding lower-priority load.
//
// R is the execution result type. This type is concurrency safe.
type PriorityLimiter[R any] interface {
	failsafe.Policy[R]
	Metrics

	// AcquirePermit attempts to acquire a permit for an execution at the priority or level contained in the context,
	// waiting until one is available or the execution is canceled. Returns [context.Canceled] if the ctx is canceled. A
	// priority must be stored in the context using the PriorityKey, else a level must be stored in the context using the
	// LevelKey. The priority or level must be greater than the current rejection threshold for admission. Levels must be
	// between 0 and 499.
	//
	// Example usage:
	//   ctx := priority.ContextWithPriority(context.Background(), priority.High)
	//   permit, err := limiter.AcquirePermit(ctx)
	AcquirePermit(ctx context.Context) (Permit, error)

	// AcquirePermitWithMaxWait attempts to acquire a permit for an execution at the priority or level contained in the
	// context, waiting until one is available, the execution is canceled, or the maxWaitTime is exceeded. Returns
	// [context.Canceled] if the ctx is canceled. A priority must be stored in the context using the PriorityKey, else a
	// level must be stored in the context using the LevelKey. The priority or level must be greater than the current
	// rejection threshold for admission. Levels must be between 0 and 499.
	AcquirePermitWithMaxWait(ctx context.Context, maxWaitTime time.Duration) (Permit, error)

	// AcquirePermitWithPriority attempts to acquire a permit for a execution at the given priority, waiting until one is
	// available or the execution is canceled. Returns [context.Canceled] if the ctx is canceled. The execution priority must
	// be greater than the current rejection threshold for admission.
	AcquirePermitWithPriority(ctx context.Context, priority priority.Priority) (Permit, error)

	// AcquirePermitWithLevel attempts to acquire a permit for a execution at the given priority level, waiting until one is
	// available or the execution is canceled. Returns [context.Canceled] if the ctx is canceled. The execution priority level
	// must be greater than the current rejection threshold for admission, and the level must be between 0 and 499.
	AcquirePermitWithLevel(ctx context.Context, level int) (Permit, error)

	// CanAcquirePermit returns whether it's currently possible to acquire a permit for the priority or level contained in
	// the context. If a priority and level are both provided, the level takes precedent. If no priority or level are
	// provided, level 0 is used.
	CanAcquirePermit(ctx context.Context) bool

	// CanAcquirePermitWithPriority returns whether it's currently possible to acquire a permit for the priority.
	CanAcquirePermitWithPriority(ctx context.Context, priority priority.Priority) bool

	// CanAcquirePermitWithLevel returns whether it's currently possible to acquire a permit for the level. The level must
	// be between 0 and 499.
	CanAcquirePermitWithLevel(level int) bool

	// Reset resets the limiter to its initial limit.
	Reset()
}

// PriorityPermit is a permit to perform an execution that must be completed by calling Record, RecordUsage, or Drop.
type PriorityPermit interface {
	// Record records an execution completion and releases a permit back to the limiter. The execution duration will be used
	// to influence the limiter. If a priority.UsageTracker is configured with the associated Prioritizer, then usage will
	// also be recorded and will influence execution priority if the limiter becomes overloaded.
	Record()

	// RecordUsage records an execution completion and releases a permit back to the limiter. The execution duration will be used
	// to influence the limiter. If a priority.UsageTracker is configured with the associated Prioritizer, then the usage
	// for the userID will also be recorded and will influence execution priority if the limiter becomes overloaded.
	RecordUsage(userID string, usage float64)

	// Drop releases an execution permit back to the limiter without recording a completion. This should be used when an
	// execution completes prematurely, such as via a timeout, and we don't want the execution duration to influence the
	// limiter.
	Drop()
}

type priorityLimiter[R any] struct {
	*queueingLimiter[R]
	prioritizer *internal.BasePrioritizer[*queueStats]
}

func (l *priorityLimiter[R]) AcquirePermit(ctx context.Context) (Permit, error) {
	return l.AcquirePermitWithLevel(ctx, l.prioritizer.LevelFromContext(ctx))
}

func (l *priorityLimiter[R]) AcquirePermitWithMaxWait(ctx context.Context, maxWaitTime time.Duration) (Permit, error) {
	level := l.prioritizer.LevelFromContext(ctx)
	if !l.CanAcquirePermitWithLevel(level) {
		return nil, ErrExceeded
	}

	permit, err := l.adaptiveLimiter.AcquirePermitWithMaxWait(ctx, maxWaitTime)
	if err != nil {
		return nil, err
	}
	l.prioritizer.LevelTracker.RecordLevel(level)
	return l.enhancedPermit(ctx, permit), nil
}

func (l *priorityLimiter[R]) AcquirePermitWithPriority(ctx context.Context, priority priority.Priority) (Permit, error) {
	return l.AcquirePermitWithLevel(ctx, l.prioritizer.LevelFromContextWithPriority(ctx, priority))
}

func (l *priorityLimiter[R]) AcquirePermitWithLevel(ctx context.Context, level int) (Permit, error) {
	if !l.CanAcquirePermitWithLevel(level) {
		return nil, ErrExceeded
	}

	permit, err := l.adaptiveLimiter.AcquirePermit(ctx)
	if err != nil {
		return nil, err
	}
	l.prioritizer.LevelTracker.RecordLevel(level)
	return l.enhancedPermit(ctx, permit), nil
}

func (l *priorityLimiter[R]) CanAcquirePermit(ctx context.Context) bool {
	return l.CanAcquirePermitWithLevel(l.prioritizer.LevelFromContext(ctx))
}

func (l *priorityLimiter[R]) CanAcquirePermitWithPriority(ctx context.Context, priority priority.Priority) bool {
	return l.CanAcquirePermitWithLevel(l.prioritizer.LevelFromContextWithPriority(ctx, priority))
}

func (l *priorityLimiter[R]) CanAcquirePermitWithLevel(level int) bool {
	// When this is the only limiter being used by the prioritizer, we can reject based on the limiter's capacity rather
	// than wait for the next prioritizer calibration.
	if l.prioritizer.RegisteredPolicies() == 1 {
		// Return immediately if the limiter has capacity
		if l.adaptiveLimiter.CanAcquirePermit() {
			return true
		}

		// Check the limiter's max capacity
		maxQueue := int(float64(l.Limit()) * l.maxRejectionFactor)
		if l.Queued() >= maxQueue {
			return false
		}
	}

	return level >= l.prioritizer.RejectionThreshold()
}

func (l *priorityLimiter[R]) ToExecutor(_ R) any {
	e := &executor[R]{
		BaseExecutor:    &policy.BaseExecutor[R]{},
		blockingLimiter: l,
	}
	e.Executor = e
	return e
}

func (l *priorityLimiter[R]) enhancedPermit(ctx context.Context, permit Permit) Permit {
	if l.prioritizer.UsageTracker != nil && ctx != nil {
		if value := ctx.Value(priority.UserKey); value != nil {
			if userID, ok := value.(string); ok && userID != "" {
				if p, ok := permit.(*recordingPermit[R]); ok {
					p.userID = userID
					p.usageTracker = l.prioritizer.UsageTracker
				}
			}
		}
	}
	return permit
}

func (l *priorityLimiter[R]) canAcquirePermit(ctx context.Context) bool {
	return l.CanAcquirePermit(ctx)
}

func (l *priorityLimiter[R]) configRef() *config[R] {
	return &l.config
}
