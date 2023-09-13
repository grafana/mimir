package failsafe

import (
	"context"
	"sync"
	"time"
)

// ExecutionStats contains stats for an execution.
type ExecutionStats struct {
	// The number of execution attempts, including attempts that are currently in progress and attempts that were blocked before being
	// executed, such as by a CircuitBreaker or RateLimiter.
	Attempts int
	// The number of completed executions. Executions that are blocked, such as when a CircuitBreaker is open, are not counted.
	Executions int
	// The time that the initial execution attempt started at.
	StartTime time.Time
}

// IsFirstAttempt returns true when Attempts is 1, meaning this is the first execution attempt.
func (s *ExecutionStats) IsFirstAttempt() bool {
	return s.Attempts == 1
}

// IsRetry returns true when Attempts is > 1, meaning the execution is being retried.
func (s *ExecutionStats) IsRetry() bool {
	return s.Attempts > 1
}

// GetElapsedTime returns the elapsed time since initial execution attempt began.
func (s *ExecutionStats) GetElapsedTime() time.Duration {
	return time.Since(s.StartTime)
}

// ExecutionAttempt contains information about an execution attempt.
type ExecutionAttempt[R any] struct {
	ExecutionStats
	// The last error that occurred, else the zero value for R.
	LastResult R
	// The last error that occurred, else nil.
	LastErr error
	// The time that the most recent execution attempt started at.
	AttemptStartTime time.Time
}

// GetElapsedAttemptTime returns the elapsed time since the last execution attempt began.
func (e *ExecutionAttempt[_]) GetElapsedAttemptTime() time.Duration {
	return time.Since(e.AttemptStartTime)
}

// Execution contains information about an execution in progress.
type Execution[R any] struct {
	ExecutionAttempt[R]
	Context context.Context
	mtx     *sync.Mutex
	// Guarded by mtx
	canceledIndex int
}

// IsCanceled returns whether the execution has been canceled. This happens when Timeout is exceeded or a configured Context is done, where
// the [context.Context.Err] is not nil.
func (e *Execution[_]) IsCanceled() bool {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.canceledIndex > -1 || (e.Context != nil && e.Context.Err() != nil)
}

// ExecutionAttemptedEvent indicates an execution was attempted.
type ExecutionAttemptedEvent[R any] struct {
	ExecutionAttempt[R]
}

// ExecutionScheduledEvent indicates an execution was scheduled.
type ExecutionScheduledEvent[R any] struct {
	ExecutionAttempt[R]
	// The delay before the next execution attempt.
	Delay time.Duration
}

// GetDelay returns the Delay before the next execution event.
func (e *ExecutionScheduledEvent[R]) GetDelay() time.Duration {
	return e.Delay
}

// ExecutionCompletedEvent indicates an execution was completed.
type ExecutionCompletedEvent[R any] struct {
	ExecutionStats
	// The execution result, else the zero value for R
	Result R
	// The execution error, else nil
	Err error
}

func newExecutionCompletedEvent[R any](er *ExecutionResult[R], stats *ExecutionStats) ExecutionCompletedEvent[R] {
	return ExecutionCompletedEvent[R]{
		Result:         er.Result,
		Err:            er.Err,
		ExecutionStats: *stats,
	}
}
