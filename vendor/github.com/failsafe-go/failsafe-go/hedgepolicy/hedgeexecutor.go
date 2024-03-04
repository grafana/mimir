package hedgepolicy

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/common"
	"github.com/failsafe-go/failsafe-go/policy"
)

// hedgeExecutor is a failsafe.Executor that handles failures according to a HedgePolicy.
type hedgeExecutor[R any] struct {
	*policy.BaseExecutor[R]
	*hedgePolicy[R]
}

var _ policy.Executor[any] = &hedgeExecutor[any]{}

func (e *hedgeExecutor[R]) Apply(innerFn func(failsafe.Execution[R]) *common.PolicyResult[R]) func(failsafe.Execution[R]) *common.PolicyResult[R] {
	return func(exec failsafe.Execution[R]) *common.PolicyResult[R] {
		execInternal := exec.(policy.ExecutionInternal[R])

		// Create child context
		execInternal = execInternal.CopyWithContext(context.WithCancel(exec.Context())).(policy.ExecutionInternal[R])

		// Guard against a race between execution results
		done := atomic.Bool{}
		resultCount := atomic.Int32{}
		hedgeExec := execInternal
		resultChan := make(chan *common.PolicyResult[R], 1) // Only the first result is sent

		for attempts := 1; ; attempts++ {
			go func(hedgeExec policy.ExecutionInternal[R]) {
				result := innerFn(hedgeExec)
				isFinalResult := int(resultCount.Add(1)) == e.config.maxHedges+1
				isCancellable := e.config.IsAbortable(result.Result, result.Error)

				if (isFinalResult || isCancellable) && done.CompareAndSwap(false, true) {
					// Close canceled channel without recording a result, and get existing cancel result if any
					if cancelResult := execInternal.Cancel(e.PolicyIndex, nil); cancelResult != nil {
						result = cancelResult
					}
					resultChan <- result
				}
			}(hedgeExec)

			if attempts-1 < e.config.maxHedges {
				// Wait for hedge delay or result
				timer := time.NewTimer(e.config.delayFunc(exec))
				select {
				case <-timer.C:
				case result := <-resultChan:
					timer.Stop()
					return result
				}
			} else {
				// All hedges have been started, wait for a result
				select {
				case result := <-resultChan:
					return result
				}
			}

			// Prepare for hedge execution
			if hedgeExec == execInternal {
				hedgeExec = execInternal.CopyWithResult(nil).(policy.ExecutionInternal[R])
			}
			if cancelResult := hedgeExec.InitializeHedge(e.PolicyIndex); cancelResult != nil {
				return cancelResult
			}

			// Call hedge listener
			if e.config.onHedge != nil {
				e.config.onHedge(failsafe.ExecutionEvent[R]{ExecutionAttempt: hedgeExec.CopyWithResult(nil)})
			}
		}
	}
}
