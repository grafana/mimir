package failsafe

import (
	"time"
)

// Policy handles execution failures.
type Policy[R any] interface {
	// ToExecutor returns a PolicyExecutor capable of handling an execution for the Policy.
	ToExecutor(policyIndex int) PolicyExecutor[R]
}

// ListenablePolicyBuilder configures listeners for a Policy execution result.
type ListenablePolicyBuilder[S any, R any] interface {
	// OnSuccess registers the listener to be called when the policy succeeds in handling an execution. This means that the supplied
	// execution either succeeded, or if it failed, the policy was able to produce a successful result.
	OnSuccess(func(event ExecutionCompletedEvent[R])) S

	// OnFailure registers the listener to be called when the policy fails to handle an error. This means that not only was the supplied
	// execution considered a failure by the policy, but that the policy was unable to produce a successful result.
	OnFailure(func(event ExecutionCompletedEvent[R])) S
}

/*
FailurePolicyBuilder builds a Policy that allows configurable conditions to determine whether an execution is a failure.
  - By default, any error is considered a failure and will be handled by the policy. You can override this by specifying your own handle
    conditions. The default error handling condition will only be overridden by another condition that handles errors such as Handle
    or HandleIf. Specifying a condition that only handles results, such as HandleResult will not replace the default error handling condition.
  - If multiple handle conditions are specified, any condition that matches an execution result or error will trigger policy handling.
*/
type FailurePolicyBuilder[S any, R any] interface {
	// HandleErrors specifies the errors to handle as failures. Any errors that evaluate to true for errors.Is and the execution error will
	// be handled.
	HandleErrors(errors ...error) S

	// HandleResult specifies the results to handle as failures. Any result that evaluates to true for reflect.DeepEqual and the execution
	// result will be handled. This method is only considered when a result is returned from an execution, not when an error is returned.
	HandleResult(result R) S

	// HandleIf specifies that a failure has occurred if the predicate matches the execution result or error.
	HandleIf(predicate func(R, error) bool) S
}

// DelayFunction returns a duration to delay for, given an Execution.
type DelayFunction[R any] func(exec *Execution[R]) time.Duration

// DelayablePolicyBuilder builds policies that can be delayed between executions.
type DelayablePolicyBuilder[S any, R any] interface {
	// WithDelay configures the time to delay between execution attempts.
	WithDelay(delay time.Duration) S

	// WithDelayFn accepts a function that configures the time to delay before the next execution attempt.
	WithDelayFn(delayFn DelayFunction[R]) S
}
