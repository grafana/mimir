package adaptivelimiter

import (
	"context"
	"errors"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/policy"
)

type blockingLimiter[R any] interface {
	canAcquirePermit(ctx context.Context) bool

	AcquirePermit(ctx context.Context) (Permit, error)

	AcquirePermitWithMaxWait(ctx context.Context, maxWaitTime time.Duration) (Permit, error)

	configRef() *config[R]
}

// executor is a policy.Executor that handles failures according to an AdaptiveLimiter or PriorityLimiter.
type executor[R any] struct {
	policy.BaseExecutor[R]
	blockingLimiter[R]
}

var _ policy.Executor[any] = &executor[any]{}

func (e *executor[R]) Apply(innerFn func(failsafe.Execution[R]) *common.PolicyResult[R]) func(failsafe.Execution[R]) *common.PolicyResult[R] {
	return func(exec failsafe.Execution[R]) *common.PolicyResult[R] {
		execInternal := exec.(policy.ExecutionInternal[R])
		c := e.blockingLimiter.configRef()

		var permit Permit
		var err error
		if exec.Context().Value(policy.CheckOnlyKey) != nil {
			if !e.canAcquirePermit(exec.Context()) {
				err = ErrExceeded
			}
		} else if c.maxWaitTime == -1 {
			permit, err = e.AcquirePermit(exec.Context())
		} else {
			permit, err = e.AcquirePermitWithMaxWait(exec.Context(), c.maxWaitTime)
		}

		if err != nil {
			// Check for cancellation while waiting for a permit
			if canceled, cancelResult := execInternal.IsCanceledWithResult(); canceled {
				return cancelResult
			}

			// Handle exceeded
			if c.onLimitExceeded != nil && errors.Is(err, ErrExceeded) {
				c.onLimitExceeded(failsafe.ExecutionEvent[R]{
					ExecutionAttempt: exec,
				})
			}
			return internal.FailureResult[R](err)
		}

		result := innerFn(exec)
		result = e.PostExecute(execInternal, result)
		if permit != nil {
			permit.Record()
		}
		return result
	}
}
