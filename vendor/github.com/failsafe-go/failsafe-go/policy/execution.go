package policy

import (
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
)

type ExecutionInternal[R any] interface {
	failsafe.Execution[R]

	// RecordResult records an execution result, such as before a retry attempt, and returns the result or the cancel result,
	// if any.
	RecordResult(result *common.PolicyResult[R]) *common.PolicyResult[R]

	// InitializeRetry prepares a new execution retry. If the retry could not be initialized because was canceled, the associated
	// cancellation result is returned.
	InitializeRetry() *common.PolicyResult[R]

	// Cancel cancels the execution with the result.
	Cancel(result *common.PolicyResult[R])

	// IsCanceledWithResult returns whether the execution is canceled, along with the cancellation result, if any.
	IsCanceledWithResult() (bool, *common.PolicyResult[R])

	// CopyWithResult returns a copy of the failsafe.Execution with the result. If the result is nil, this will preserve a
	// copy of the lastResult and lastError. This is useful before passing the execution to an event listener, otherwise
	// these may be changed if the execution is canceled.
	CopyWithResult(result *common.PolicyResult[R]) failsafe.Execution[R]

	// CopyForCancellable creates a cancellable child copy of the execution based on the current execution's context.
	CopyForCancellable() failsafe.Execution[R]

	// CopyForHedge creates a copy of the execution marked as a hedge.
	CopyForHedge() failsafe.Execution[R]
}
