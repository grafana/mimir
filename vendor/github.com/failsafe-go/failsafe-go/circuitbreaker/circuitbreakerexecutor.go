package circuitbreaker

import (
	"github.com/failsafe-go/failsafe-go/common"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/policy"
)

// circuitBreakerExecutor is a failsafe.Executor that handles failures according to a CircuitBreaker.
type circuitBreakerExecutor[R any] struct {
	*policy.BaseExecutor[R]
	*circuitBreaker[R]
}

var _ policy.Executor[any] = &circuitBreakerExecutor[any]{}

func (cbe *circuitBreakerExecutor[R]) PreExecute(_ policy.ExecutionInternal[R]) *common.PolicyResult[R] {
	if !cbe.circuitBreaker.TryAcquirePermit() {
		return internal.FailureResult[R](ErrOpen)
	}
	return nil
}

func (cbe *circuitBreakerExecutor[R]) OnSuccess(exec policy.ExecutionInternal[R], result *common.PolicyResult[R]) {
	cbe.BaseExecutor.OnSuccess(exec, result)
	cbe.RecordSuccess()
}

func (cbe *circuitBreakerExecutor[R]) OnFailure(exec policy.ExecutionInternal[R], result *common.PolicyResult[R]) *common.PolicyResult[R] {
	cbe.BaseExecutor.OnFailure(exec, result)
	cbe.mtx.Lock()
	defer cbe.mtx.Unlock()
	cbe.recordFailure(exec)
	return result
}
