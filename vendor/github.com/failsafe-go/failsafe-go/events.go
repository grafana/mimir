package failsafe

import (
	"time"

	"github.com/failsafe-go/failsafe-go/common"
)

// ExecutionEvent indicates an execution was attempted.
type ExecutionEvent[R any] struct {
	ExecutionAttempt[R]
}

// ExecutionScheduledEvent indicates an execution was scheduled.
type ExecutionScheduledEvent[R any] struct {
	ExecutionAttempt[R]
	// The delay before the next execution attempt.
	Delay time.Duration
}

// ExecutionDoneEvent indicates an execution is done.
type ExecutionDoneEvent[R any] struct {
	ExecutionInfo
	// The execution result, else the zero value for R
	Result R
	// The execution error, else nil
	Error error
}

func newExecutionDoneEvent[R any](info ExecutionInfo, er *common.PolicyResult[R]) ExecutionDoneEvent[R] {
	return ExecutionDoneEvent[R]{
		ExecutionInfo: info,
		Result:        er.Result,
		Error:         er.Error,
	}
}
