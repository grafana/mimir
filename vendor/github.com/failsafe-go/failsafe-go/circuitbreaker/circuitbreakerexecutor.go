package circuitbreaker

import (
	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/spi"
)

// circuitBreakerExecutor is a failsafe.PolicyExecutor that handles failures according to a CircuitBreaker.
type circuitBreakerExecutor[R any] struct {
	*spi.BasePolicyExecutor[R]
	*circuitBreaker[R]
}

var _ failsafe.PolicyExecutor[any] = &circuitBreakerExecutor[any]{}

func (cbe *circuitBreakerExecutor[R]) PreExecute(_ *failsafe.ExecutionInternal[R]) *failsafe.ExecutionResult[R] {
	if !cbe.circuitBreaker.TryAcquirePermit() {
		return internal.FailureResult[R](ErrCircuitBreakerOpen)
	}
	return nil
}

func (cbe *circuitBreakerExecutor[R]) OnSuccess(_ *failsafe.ExecutionResult[R]) {
	cbe.RecordSuccess()
}

func (cbe *circuitBreakerExecutor[R]) OnFailure(exec *failsafe.Execution[R], result *failsafe.ExecutionResult[R]) *failsafe.ExecutionResult[R] {
	cbe.mtx.Lock()
	defer cbe.mtx.Unlock()
	cbe.recordFailure(exec)
	return result
}
