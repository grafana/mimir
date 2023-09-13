package internal

import "github.com/failsafe-go/failsafe-go"

func NewExecutionAttempt[R any](result *failsafe.ExecutionResult[R], exec *failsafe.Execution[R]) failsafe.ExecutionAttempt[R] {
	return failsafe.ExecutionAttempt[R]{
		ExecutionStats:   exec.ExecutionStats,
		LastResult:       result.Result,
		LastErr:          result.Err,
		AttemptStartTime: exec.AttemptStartTime,
	}
}

func NewExecutionCompletedEventForExec[R any](exec *failsafe.Execution[R]) failsafe.ExecutionCompletedEvent[R] {
	return failsafe.ExecutionCompletedEvent[R]{
		Result:         exec.LastResult,
		Err:            exec.LastErr,
		ExecutionStats: exec.ExecutionStats,
	}
}

func FailureResult[R any](err error) *failsafe.ExecutionResult[R] {
	return &failsafe.ExecutionResult[R]{
		Err:      err,
		Complete: true,
	}
}
