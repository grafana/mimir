package policy

import (
	"context"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
)

type ExecutionInternal[R any] interface {
	failsafe.Execution[R]

	// RecordResult records an execution result, such as before a retry attempt, and returns the result or the cancel result,
	// if any.
	RecordResult(policyIndex int, result *common.PolicyResult[R]) *common.PolicyResult[R]

	// InitializeRetry prepares a new execution retry. If the retry could not be initialized because was canceled, the associated
	// cancellation result is returned.
	InitializeRetry(policyIndex int) *common.PolicyResult[R]

	// InitializeHedge prepares a new hedge execution. If the attempt could not be initialized because was canceled, the associated
	// cancellation result is returned.
	InitializeHedge(policyIndex int) *common.PolicyResult[R]

	// Cancel marks the execution as having been canceled by the policyIndex, if it wasn't already canceled, and returns the
	// cancel result. Any pending executions of any inner policies of the policyIndex will be canceled. Outer policies of the
	// policyIndex will be unaffected.
	Cancel(policyIndex int, result *common.PolicyResult[R]) *common.PolicyResult[R]

	// IsCanceledForPolicy returns whether the execution has been canceled for the policyIndex, along with the associated
	// cancellation result if so.
	IsCanceledForPolicy(policyIndex int) (bool, *common.PolicyResult[R])

	// CopyWithResult returns a copy of the failsafe.Execution with the result. If the result is nil, this will preserve a
	// copy of the lastResult and lastError. This is useful before passing the execution to an event listener, otherwise
	// these may be changed if the execution is canceled.
	CopyWithResult(result *common.PolicyResult[R]) failsafe.Execution[R]

	// CopyWithContext returns a copy of the failsafe.Execution with the context and cancelFunc.
	CopyWithContext(ctx context.Context, cancelFunc func()) failsafe.Execution[R]
}
