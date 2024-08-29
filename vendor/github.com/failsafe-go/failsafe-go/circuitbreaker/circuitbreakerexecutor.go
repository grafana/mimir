package circuitbreaker

import (
	"github.com/failsafe-go/failsafe-go/common"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/policy"
)

// executor is a policy.Executor that handles failures according to a CircuitBreaker.
type executor[R any] struct {
	*policy.BaseExecutor[R]
	*circuitBreaker[R]
}

var _ policy.Executor[any] = &executor[any]{}

func (e *executor[R]) PreExecute(_ policy.ExecutionInternal[R]) *common.PolicyResult[R] {
	if !e.TryAcquirePermit() {
		return internal.FailureResult[R](ErrOpen)
	}
	return nil
}

func (e *executor[R]) OnSuccess(exec policy.ExecutionInternal[R], result *common.PolicyResult[R]) {
	e.BaseExecutor.OnSuccess(exec, result)
	e.RecordSuccess()
}

func (e *executor[R]) OnFailure(exec policy.ExecutionInternal[R], result *common.PolicyResult[R]) *common.PolicyResult[R] {
	// Wrap the result in the execution so it's available when computing a delay
	exec = exec.CopyWithResult(result).(policy.ExecutionInternal[R])
	e.BaseExecutor.OnFailure(exec, result)
	e.mtx.Lock()
	defer e.mtx.Unlock()
	e.recordFailure(exec)
	return result
}
