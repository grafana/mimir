package policy

import (
	"context"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
)

type ExecutionInternal[R any] interface {
	failsafe.Execution[R]

	// Result returns the last recorded result. If the execution was cancelled, this should be called to fetch the last
	// recorded result.
	Result() *common.PolicyResult[R]

	// Context returns any context configured for the execution, else nil.
	Context() context.Context

	// InitializeAttempt prepares a new execution attempt. Returns false if the attempt could not be initialized since it was
	// canceled by an external Context or a timeout.Timeout composed outside the Executor.
	InitializeAttempt(policyIndex int) bool

	// Record records the result of an execution attempt, if a result was not already recorded, and returns the recorded
	// execution.
	Record(result *common.PolicyResult[R]) *common.PolicyResult[R]

	// Cancel marks the execution as having been cancelled by the policyExecutor, which will also cancel pending executions
	// of any inner policies of the policyExecutor, and also records the result. Outer policies of the policyExecutor will be
	// unaffected.
	Cancel(policyIndex int, result *common.PolicyResult[R])

	// IsCanceledForPolicy returns whether the execution has been canceled by an external Context or a policy composed
	// outside the Executor.
	IsCanceledForPolicy(policyIndex int) bool

	// Copy returns a copy of the failsafe.Execution.
	Copy() failsafe.Execution[R]

	// CopyWithResult returns a copy of the failsafe.Execution for the result.
	CopyWithResult(result *common.PolicyResult[R]) failsafe.Execution[R]
}
