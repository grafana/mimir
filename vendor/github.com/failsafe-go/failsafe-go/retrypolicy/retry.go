package retrypolicy

import (
	"errors"
	"fmt"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/budget"
	"github.com/failsafe-go/failsafe-go/internal"
	"github.com/failsafe-go/failsafe-go/policy"
)

const defaultMaxRetries = 2

// ErrExceeded is a convenience error sentinel that can be used to build policies that handle ExceededError, such as via
// HandleErrors(retrypolicy.ErrExceeded). It can also be used with Errors.Is to determine whether an error is a
// retrypolicy.ExceededError.
var ErrExceeded = errors.New("retries exceeded")

// ExceededError is returned when a RetryPolicy's max attempts or max duration are exceeded. This type can be used with
// HandleErrorTypes(retrypolicy.ExceededError{}).
type ExceededError struct {
	LastResult any
	LastError  error
}

func (e ExceededError) Error() string {
	return fmt.Sprintf("retries exceeded. last result: %v, last error: %v", e.LastResult, e.LastError)
}

func (e ExceededError) Is(err error) bool {
	return err == ErrExceeded
}

func (e ExceededError) Unwrap() error {
	if e.LastError != nil {
		return e.LastError
	}
	return fmt.Errorf("failure: %v", e.LastResult)
}

// IsExceededError returns whether the err is an ExceededError or ErrExceeded.
func IsExceededError(err error) bool {
	return errors.Is(err, ErrExceeded)
}

// AsExceededError returns a *ExceededError if err is an ExceededError value, else nil.
func AsExceededError(err error) *ExceededError {
	var e ExceededError
	if errors.As(err, &e) {
		return &e
	}
	return nil
}

// RetryPolicy is a policy that defines when retries should be performed. See Builder for configuration options.
//
// R is the execution result type. This type is concurrency safe.
type RetryPolicy[R any] interface {
	failsafe.Policy[R]
}

/*
Builder builds RetryPolicy instances.

  - By default, a RetryPolicy will retry a failed execution up to 2 times when any error is returned, with no delay between
    retry attempts. If retries are exceeded, ExceededError is returned by default. Alternatively, ReturnLastFailure
    can be used to configure the policy to return the last execution failure.
  - You can change the default number of retry attempts and delay between retries by using the with configuration methods.
  - By default, any error is considered a failure and will be handled by the policy. You can override this by specifying
    your own HandleErrors conditions. The default error handling condition will only be overridden by another condition
    that handles error such as HandleErrors or HandleIf. Specifying a condition that only handles results, such as
    HandleResult or HandleResultIf will not replace the default error handling condition.
  - If multiple HandleErrors conditions are specified, any condition that matches an execution result or error will
    trigger policy handling.
  - The AbortOn, AbortWhen and AbortIf methods describe when retries should be aborted.

This class extends failsafe.FailurePolicyBuilder and failsafe.DelayablePolicyBuilder which offer additional configuration.

R is the execution result type. This type is not concurrency safe.
*/
type Builder[R any] interface {
	failsafe.FailurePolicyBuilder[Builder[R], R]
	failsafe.DelayablePolicyBuilder[Builder[R], R]

	// AbortOnResult specifies that retries should be aborted if the execution result matches the result using
	// reflect.DeepEqual.
	AbortOnResult(result R) Builder[R]

	// AbortOnErrors specifies that retries should be aborted if the execution error matches any of the errs using errors.Is.
	AbortOnErrors(errs ...error) Builder[R]

	// AbortOnErrorTypes specifies the errors whose types should cause retries to be aborted. Any execution errors or their
	// Unwrapped parents whose type matches any of the errs' types will cause to be aborted. This is similar to the check
	// that errors.As performs.
	AbortOnErrorTypes(errs ...any) Builder[R]

	// AbortIf specifies that retries should be aborted if the predicate matches the result or error.
	AbortIf(predicate func(R, error) bool) Builder[R]

	// ReturnLastFailure configures the policy to return the last failure result or error after attempts are exceeded,
	// rather than returning ExceededError.
	ReturnLastFailure() Builder[R]

	// WithMaxAttempts sets the max number of execution attempts to perform. -1 indicates no limit. This method has the same
	// effect as setting 1 more than WithMaxRetries. For example, 2 retries equal 3 attempts.
	WithMaxAttempts(maxAttempts int) Builder[R]

	// WithMaxRetries sets the max number of retries to perform when an execution attempt fails. -1 indicates no limit. This
	// method has the same effect as setting 1 less than WithMaxAttempts. For example, 2 retries equal 3 attempts.
	WithMaxRetries(maxRetries int) Builder[R]

	// WithMaxDuration sets the max duration to perform retries for, else the execution will be failed.
	WithMaxDuration(maxDuration time.Duration) Builder[R]

	// WithBackoff sets the delay between retries, exponentially backing off to the maxDelay and multiplying consecutive
	// delays by a factor of 2. Replaces any previously configured fixed or random delays.
	WithBackoff(delay time.Duration, maxDelay time.Duration) Builder[R]

	// WithBackoffFactor sets the delay between retries, exponentially backing off to the maxDelay and multiplying
	// consecutive delays by the delayFactor. Replaces any previously configured fixed or random delays.
	WithBackoffFactor(delay time.Duration, maxDelay time.Duration, delayFactor float64) Builder[R]

	// WithRandomDelay sets a random delay between the delayMin and delayMax (inclusive) to occur between retries.
	// Replaces any previously configured delay or backoff delay.
	WithRandomDelay(delayMin time.Duration, delayMax time.Duration) Builder[R]

	// WithJitter sets the jitter to randomly vary retry delays by. For each retry delay, a random portion of the jitter will
	// be added or subtracted to the delay. For example: a jitter of 100 milliseconds will randomly add between -100 and 100
	// milliseconds to each retry delay. Replaces any previously configured jitter factor.
	//
	// Jitter should be combined with fixed, random, or exponential backoff delays. If no delays are configured, this setting
	// is ignored.
	WithJitter(jitter time.Duration) Builder[R]

	// WithJitterFactor sets the jitterFactor to randomly vary retry delays by. For each retry delay, a random portion of the
	// delay multiplied by the jitterFactor will be added or subtracted to the delay. For example: a retry delay of 100
	// milliseconds and a jitterFactor of .25 will result in a random retry delay between 75 and 125 milliseconds. Replaces
	// any previously configured jitter duration.
	//
	// Jitter should be combined with fixed, random, or exponential backoff delays. If no delays are configured, this setting
	// is ignored.
	WithJitterFactor(jitterFactor float64) Builder[R]

	// WithBudget configures a retry budget. When the retryBudget is exceeded, retries will stop with budget.ErrExceeded.
	WithBudget(retryBudget budget.Budget) Builder[R]

	// OnAbort registers the listener to be called when an execution is aborted.
	OnAbort(listener func(failsafe.ExecutionEvent[R])) Builder[R]

	// OnRetryScheduled registers the listener to be called when a retry is about to be scheduled. This method differs from
	// OnRetry since it is called when a retry is initially scheduled but before any configured delay, whereas OnRetry is
	// called after a delay, just before the retry attempt takes place.
	OnRetryScheduled(listener func(failsafe.ExecutionScheduledEvent[R])) Builder[R]

	// OnRetry registers the listener to be called when a retry is about to be attempted.
	OnRetry(listener func(failsafe.ExecutionEvent[R])) Builder[R]

	// OnRetriesExceeded registers the listener to be called when an execution fails and the max retry attempts or max
	// duration are exceeded. The provided event will contain the last execution result and error.
	OnRetriesExceeded(listener func(failsafe.ExecutionEvent[R])) Builder[R]

	// Build returns a new RetryPolicy using the builder's configuration.
	Build() RetryPolicy[R]
}

type config[R any] struct {
	policy.BaseFailurePolicy[R]
	policy.BaseDelayablePolicy[R]
	policy.BaseAbortablePolicy[R]

	returnLastFailure bool
	delayMin          time.Duration
	delayMax          time.Duration
	delayFactor       float64
	maxDelay          time.Duration
	jitter            time.Duration
	jitterFactor      float64
	maxDuration       time.Duration
	maxRetries        int
	budget            internal.Budget

	onAbort           func(failsafe.ExecutionEvent[R])
	onRetry           func(failsafe.ExecutionEvent[R])
	onRetryScheduled  func(failsafe.ExecutionScheduledEvent[R])
	onRetriesExceeded func(failsafe.ExecutionEvent[R])
}

var _ Builder[any] = &config[any]{}

type retryPolicy[R any] struct {
	config[R]
}

// NewWithDefaults creates a RetryPolicy for execution result type R that allows 3 execution attempts max with no delay. To
// configure additional options on a RetryPolicy, use NewBuilder instead.
func NewWithDefaults[R any]() RetryPolicy[R] {
	return NewBuilder[R]().Build()
}

// NewBuilder creates a Builder for execution result type R, which by default will build a RetryPolicy that
// allows 3 execution attempts max with no delay, unless configured otherwise.
func NewBuilder[R any]() Builder[R] {
	return &config[R]{
		BaseFailurePolicy:   policy.BaseFailurePolicy[R]{},
		BaseDelayablePolicy: policy.BaseDelayablePolicy[R]{},
		BaseAbortablePolicy: policy.BaseAbortablePolicy[R]{},
		maxRetries:          defaultMaxRetries,
	}
}

func (c *config[R]) AbortOnResult(result R) Builder[R] {
	c.BaseAbortablePolicy.AbortOnResult(result)
	return c
}

func (c *config[R]) AbortOnErrors(errs ...error) Builder[R] {
	c.BaseAbortablePolicy.AbortOnErrors(errs...)
	return c
}

func (c *config[R]) AbortOnErrorTypes(errs ...any) Builder[R] {
	c.BaseAbortablePolicy.AbortOnErrorTypes(errs...)
	return c
}

func (c *config[R]) AbortIf(predicate func(R, error) bool) Builder[R] {
	c.BaseAbortablePolicy.AbortIf(predicate)
	return c
}

func (c *config[R]) HandleErrors(errs ...error) Builder[R] {
	c.BaseFailurePolicy.HandleErrors(errs...)
	return c
}

func (c *config[R]) HandleErrorTypes(errs ...any) Builder[R] {
	c.BaseFailurePolicy.HandleErrorTypes(errs...)
	return c
}

func (c *config[R]) HandleResult(result R) Builder[R] {
	c.BaseFailurePolicy.HandleResult(result)
	return c
}

func (c *config[R]) HandleIf(predicate func(R, error) bool) Builder[R] {
	c.BaseFailurePolicy.HandleIf(predicate)
	return c
}

func (c *config[R]) ReturnLastFailure() Builder[R] {
	c.returnLastFailure = true
	return c
}

func (c *config[R]) WithMaxAttempts(maxAttempts int) Builder[R] {
	if maxAttempts == -1 {
		c.maxRetries = -1
	} else {
		c.maxRetries = maxAttempts - 1
	}
	return c
}

func (c *config[R]) WithMaxRetries(maxRetries int) Builder[R] {
	c.maxRetries = maxRetries
	return c
}

func (c *config[R]) WithMaxDuration(maxDuration time.Duration) Builder[R] {
	c.maxDuration = maxDuration
	return c
}

func (c *config[R]) WithDelay(delay time.Duration) Builder[R] {
	c.BaseDelayablePolicy.WithDelay(delay)
	return c
}

func (c *config[R]) WithDelayFunc(delayFunc failsafe.DelayFunc[R]) Builder[R] {
	c.BaseDelayablePolicy.WithDelayFunc(delayFunc)
	return c
}

func (c *config[R]) WithBackoff(delay time.Duration, maxDelay time.Duration) Builder[R] {
	return c.WithBackoffFactor(delay, maxDelay, 2)
}

func (c *config[R]) WithBackoffFactor(delay time.Duration, maxDelay time.Duration, delayFactor float64) Builder[R] {
	c.BaseDelayablePolicy.WithDelay(delay)
	c.maxDelay = maxDelay
	c.delayFactor = delayFactor

	// Clear random delay
	c.delayMin = 0
	c.delayMax = 0
	return c
}

func (c *config[R]) WithRandomDelay(delayMin time.Duration, delayMax time.Duration) Builder[R] {
	c.delayMin = delayMin
	c.delayMax = delayMax

	// Clear non-random delay
	c.Delay = 0
	c.maxDelay = 0
	return c
}

func (c *config[R]) WithJitter(jitter time.Duration) Builder[R] {
	c.jitter = jitter
	return c
}

func (c *config[R]) WithJitterFactor(jitterFactor float64) Builder[R] {
	c.jitterFactor = jitterFactor
	return c
}

func (c *config[R]) WithBudget(retryBudget budget.Budget) Builder[R] {
	c.budget = retryBudget.(internal.Budget)
	return c.AbortOnErrors(budget.ErrExceeded)
}

func (c *config[R]) OnSuccess(listener func(event failsafe.ExecutionEvent[R])) Builder[R] {
	c.BaseFailurePolicy.OnSuccess(listener)
	return c
}

func (c *config[R]) OnFailure(listener func(event failsafe.ExecutionEvent[R])) Builder[R] {
	c.BaseFailurePolicy.OnFailure(listener)
	return c
}

func (c *config[R]) OnAbort(listener func(failsafe.ExecutionEvent[R])) Builder[R] {
	c.onAbort = listener
	return c
}

func (c *config[R]) OnRetry(listener func(failsafe.ExecutionEvent[R])) Builder[R] {
	c.onRetry = listener
	return c
}

func (c *config[R]) OnRetryScheduled(listener func(failsafe.ExecutionScheduledEvent[R])) Builder[R] {
	c.onRetryScheduled = listener
	return c
}

func (c *config[R]) OnRetriesExceeded(listener func(failsafe.ExecutionEvent[R])) Builder[R] {
	c.onRetriesExceeded = listener
	return c
}

func (c *config[R]) allowsRetries() bool {
	return c.maxRetries == -1 || c.maxRetries > 0
}

func (c *config[R]) Build() RetryPolicy[R] {
	return &retryPolicy[R]{
		config: *c, // TODO copy base fields
	}
}

func (rp *retryPolicy[R]) ToExecutor(_ R) any {
	rpe := &executor[R]{
		BaseExecutor: policy.BaseExecutor[R]{
			BaseFailurePolicy: &rp.BaseFailurePolicy,
		},
		retryPolicy: rp,
	}
	rpe.Executor = rpe
	return rpe
}
