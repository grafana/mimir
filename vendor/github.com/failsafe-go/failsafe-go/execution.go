package failsafe

import (
	"context"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go/common"
)

// ExecutionStats contains execution stats.
type ExecutionStats interface {
	// Attempts returns the number of execution attempts so far, including attempts that are currently in progress and
	// attempts that were blocked before being executed, such as by a CircuitBreaker or RateLimiter.
	Attempts() int

	// Executions returns the number of completed executions. Executions that are blocked, such as when a CircuitBreaker is
	// open, are not counted.
	Executions() int

	// StartTime returns the time that the initial execution attempt started at.
	StartTime() time.Time

	// ElapsedTime returns the elapsed time since initial execution attempt began.
	ElapsedTime() time.Duration
}

// ExecutionAttempt contains information for an execution attempt.
type ExecutionAttempt[R any] interface {
	ExecutionStats

	// LastResult returns the result, if any, from the last execution attempt.
	LastResult() R

	// LastError returns the error, if any, from the last execution attempt.
	LastError() error

	// IsFirstAttempt returns true when Attempts is 1, meaning this is the first execution attempt.
	IsFirstAttempt() bool

	// IsRetry returns true when Attempts is > 1, meaning the execution is being retried.
	IsRetry() bool

	// AttemptStartTime returns the time that the most recent execution attempt started at.
	AttemptStartTime() time.Time

	// ElapsedAttemptTime returns the elapsed time since the last execution attempt began.
	ElapsedAttemptTime() time.Duration
}

// Execution contains information about an execution.
type Execution[R any] interface {
	ExecutionAttempt[R]

	// IsCanceled returns whether the execution has been canceled by an external Context or a timeout.Timeout.
	IsCanceled() bool

	// Canceled returns a channel that is done when the execution is canceled, either by an external Context or a
	// timeout.Timeout.
	Canceled() <-chan any
}

type execution[R any] struct {
	attempts         int
	executions       int
	startTime        time.Time
	attemptStartTime time.Time
	result           *common.PolicyResult[R]
	ctx              context.Context
	mtx              *sync.Mutex

	// Guarded by mtx
	lastResult    R     // The last error that occurred, else the zero value for R.
	lastError     error // The last error that occurred, else nil.
	canceled      chan any
	canceledIndex *int
}

var _ Execution[any] = &execution[any]{}
var _ ExecutionStats = &execution[any]{}

func (e *execution[R]) Attempts() int {
	return e.attempts
}

func (e *execution[R]) Executions() int {
	return e.executions
}

func (e *execution[R]) StartTime() time.Time {
	return e.startTime
}

func (e *execution[R]) IsFirstAttempt() bool {
	return e.attempts == 1
}

func (e *execution[R]) IsRetry() bool {
	return e.attempts > 1
}

func (e *execution[R]) ElapsedTime() time.Duration {
	return time.Since(e.startTime)
}

func (e *execution[R]) LastResult() R {
	return e.lastResult
}

func (e *execution[R]) LastError() error {
	return e.lastError
}

func (e *execution[R]) Context() context.Context {
	return e.ctx
}

func (e *execution[R]) AttemptStartTime() time.Time {
	return e.attemptStartTime
}

func (e *execution[_]) ElapsedAttemptTime() time.Duration {
	return time.Since(e.attemptStartTime)
}

func (e *execution[_]) IsCanceled() bool {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return *e.canceledIndex > -1
}

func (e *execution[_]) Canceled() <-chan any {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.canceled
}

func (e *execution[R]) InitializeAttempt(policyIndex int) bool {
	// Lock to guard against a race with a Timeout canceling the execution
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if e.isCanceledForPolicy(policyIndex) {
		return false
	}
	e.attempts++
	e.attemptStartTime = time.Now()
	if e.isCanceledForPolicy(-1) {
		*e.canceledIndex = -1
		e.canceled = make(chan any)
	}
	return true
}

func (e *execution[R]) Record(result *common.PolicyResult[R]) *common.PolicyResult[R] {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	e.executions++
	if !e.isCanceledForPolicy(-1) {
		e.result = result
		e.lastResult = result.Result
		e.lastError = result.Error
	}
	return e.result
}

func (e *execution[R]) Cancel(policyIndex int, result *common.PolicyResult[R]) {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	if e.isCanceledForPolicy(-1) {
		return
	}
	e.result = result
	e.lastResult = result.Result
	e.lastError = result.Error
	*e.canceledIndex = policyIndex
	if e.canceled != nil {
		close(e.canceled)
	}
}

func (e *execution[R]) IsCanceledForPolicy(policyIndex int) bool {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.isCanceledForPolicy(policyIndex)
}

// Requires locking externally.
func (e *execution[R]) isCanceledForPolicy(policyIndex int) bool {
	return *e.canceledIndex > policyIndex
}

func (e *execution[R]) CopyWithResult(result *common.PolicyResult[R]) Execution[R] {
	e.mtx.Lock()
	c := *e
	e.mtx.Unlock()
	c.lastResult = result.Result
	c.lastError = result.Error
	return &c
}

func (e *execution[R]) Copy() Execution[R] {
	e.mtx.Lock()
	c := *e
	e.mtx.Unlock()
	return &c
}

func (e *execution[R]) Result() *common.PolicyResult[R] {
	e.mtx.Lock()
	defer e.mtx.Unlock()
	return e.result
}
