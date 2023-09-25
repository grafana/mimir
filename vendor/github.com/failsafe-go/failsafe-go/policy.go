package failsafe

import (
	"time"
)

// Policy handles execution failures.
type Policy[R any] interface {
	// ToExecutor returns a policy.Executor capable of handling an execution for the Policy.
	// The typeToken parameter helps catch mismatches between R types when composing policies.
	ToExecutor(policyIndex int, typeToken R) any
}

/*
FailurePolicyBuilder builds a Policy that allows configurable conditions to determine whether an execution is a failure.
  - By default, any error is considered a failure and will be handled by the policy. You can override this by specifying
    your own handle conditions. The default error handling condition will only be overridden by another condition that
    handles errors such as Handle or HandleIf. Specifying a condition that only handles results, such as HandleResult
    will not replace the default error handling condition.
  - If multiple handle conditions are specified, any condition that matches an execution result or error will trigger
    policy handling.
*/
type FailurePolicyBuilder[S any, R any] interface {
	// HandleErrors specifies the errors to handle as failures. Any errors that evaluate to true for errors.Is and the
	// execution error will be handled.
	HandleErrors(errors ...error) S

	// HandleResult specifies the results to handle as failures. Any result that evaluates to true for reflect.DeepEqual and
	// the execution result will be handled. This method is only considered when a result is returned from an execution, not
	// when an error is returned.
	HandleResult(result R) S

	// HandleIf specifies that a failure has occurred if the predicate matches the execution result or error.
	HandleIf(predicate func(R, error) bool) S

	// OnSuccess registers the listener to be called when the policy determines an execution attempt was a success.
	OnSuccess(listener func(ExecutionEvent[R])) S

	// OnFailure registers the listener to be called when the policy determines an execution attempt was a failure, and may
	// be handled.
	OnFailure(listener func(ExecutionEvent[R])) S
}

// DelayFunction returns a duration to delay for given the ExecutionAttempt.
type DelayFunction[R any] func(exec ExecutionAttempt[R]) time.Duration

// DelayablePolicyBuilder builds policies that can be delayed between executions.
type DelayablePolicyBuilder[S any, R any] interface {
	// WithDelay configures the time to delay between execution attempts.
	WithDelay(delay time.Duration) S

	// WithDelayFn accepts a function that configures the time to delay before the next execution attempt.
	WithDelayFn(delayFn DelayFunction[R]) S
}
