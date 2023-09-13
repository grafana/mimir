package circuitbreaker

import (
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/internal/util"
	"github.com/failsafe-go/failsafe-go/spi"
)

/*
CircuitBreakerBuilder builds CircuitBreaker instances.

  - By default, any error is considered a failure and will be handled by the policy. You can override this by specifying your own handle
    conditions. The default error handling condition will only be overridden by another condition that handles error such as
    HandleErrors or HandleIf. Specifying a condition that only handles results, such as HandleResult or HandleResultIf will not replace the default
    error handling condition.
  - If multiple handle conditions are specified, any condition that matches an execution result or error will trigger policy handling.

This type is not concurrency safe.
*/
type CircuitBreakerBuilder[R any] interface {
	failsafe.ListenablePolicyBuilder[CircuitBreakerBuilder[R], R]
	failsafe.FailurePolicyBuilder[CircuitBreakerBuilder[R], R]
	failsafe.DelayablePolicyBuilder[CircuitBreakerBuilder[R], R]

	// OnClose calls the listener when the CircuitBreaker is closed.
	OnClose(listener func(StateChangedEvent)) CircuitBreakerBuilder[R]

	// OnOpen calls the listener when the CircuitBreaker is opened.
	OnOpen(listener func(StateChangedEvent)) CircuitBreakerBuilder[R]

	// OnHalfOpen calls the listener when the CircuitBreaker is half-opened.
	OnHalfOpen(listener func(StateChangedEvent)) CircuitBreakerBuilder[R]

	// WithFailureThreshold configures count based failure thresholding by setting the number of consecutive failures that must occur when
	// in a ClosedState in order to open the circuit.
	//
	// If WithSuccessThreshold is not configured, the failureThreshold will also be used when the circuit breaker is in a HalfOpenState to
	// determine whether to transition back to OpenState or ClosedState.
	WithFailureThreshold(failureThreshold uint) CircuitBreakerBuilder[R]

	// WithFailureThresholdRatio configures count based failure thresholding by setting the ratio of failures to executions that must
	// occur when in a ClosedState in order to open the circuit. For example: 5, 10 would open the circuit if 5 out of the last 10
	// executions result in a failure.
	//
	// If WithSuccessThreshold is not configured, the failureThreshold and failureThresholdingCapacity will also be used when the circuit
	// breaker is in a HalfOpenState to determine whether to transition back to OpenState or ClosedState.
	WithFailureThresholdRatio(failureThreshold uint, failureThresholdingCapacity uint) CircuitBreakerBuilder[R]

	// WithFailureThresholdPeriod configures time based failure thresholding by setting the number of failures that must occur within the
	// failureThresholdingPeriod when in a ClosedState in order to open the circuit.
	//
	// If WithSuccessThreshold is not configured, the failureThreshold will also be used when the circuit breaker is in a HalfOpenState to
	// determine whether to transition back to OpenState or ClosedState.
	WithFailureThresholdPeriod(failureThreshold uint, failureThresholdingPeriod time.Duration) CircuitBreakerBuilder[R]

	// WithFailureExecutionThreshold configures the mininum number of executions that must occur when in the ClosedState or HalfOpenState
	// before the circuit breaker can transition. This is often used with WithFailureThresholdPeriod.
	WithFailureExecutionThreshold(failureExecutionThreshold uint) CircuitBreakerBuilder[R]

	// WithFailureRateThreshold configures time based failure rate thresholding by setting the percentage rate of failures, from 1 to 100,
	// that must occur within the rolling failureThresholdingPeriod when in a ClosedState in order to open the circuit. The number of
	// executions must also exceed the failureExecutionThreshold within the failureThresholdingPeriod before the circuit will be opened.
	//
	// If WithSuccessThreshold is not configured, the failureExecutionThreshold will also be used when the circuit breaker is in a
	// HalfOpenSttate state to determine whether to transition back to open or closed.
	WithFailureRateThreshold(failureRateThreshold uint, failureExecutionThreshold uint, failureThresholdingPeriod time.Duration) CircuitBreakerBuilder[R]

	// WithSuccessThreshold configures count based success thresholding by setting the number of consecutive successful executions that
	// must occur when in a HalfOpenState in order to close the circuit, else the circuit is re-opened when a failure occurs.
	WithSuccessThreshold(successThreshold uint) CircuitBreakerBuilder[R]

	// WithSuccessThresholdRatio configures count based success thresholding by setting the ratio of successful executions that must
	// occur when in a HalfOpenState in order to close the circuit. For example: 5, 10 would close the circuit if 5 out of the last 10
	// executions were successful.
	WithSuccessThresholdRatio(successThreshold uint, successThresholdingCapacity uint) CircuitBreakerBuilder[R]

	// Build returns a new CircuitBreaker using the builder's configuration.
	Build() CircuitBreaker[R]
}

type circuitBreakerConfig[R any] struct {
	*spi.BaseListenablePolicy[R]
	*spi.BaseFailurePolicy[R]
	*spi.BaseDelayablePolicy[R]
	clock            util.Clock
	openListener     func(StateChangedEvent)
	halfOpenListener func(StateChangedEvent)
	closeListener    func(StateChangedEvent)

	// Failure config
	failureThreshold            uint
	failureRateThreshold        uint
	failureThresholdingCapacity uint
	failureExecutionThreshold   uint
	failureThresholdingPeriod   time.Duration

	// Success config
	successThreshold            uint
	successThresholdingCapacity uint
}

var _ CircuitBreakerBuilder[any] = &circuitBreakerConfig[any]{}

// WithDefaults creates a count based CircuitBreaker for execution result type R that opens after a single failure, closes after a single
// success, and has a 1 minute delay by default. To configure additional options on a CircuitBreaker, use Builder() instead.
func WithDefaults[R any]() CircuitBreaker[R] {
	return Builder[R]().Build()
}

// Builder creates a CircuitBreakerBuilder for execution result type R which by default will build a count based circuit breaker that opens
// after a single failure, closes after a single success, and has a 1 minute delay, unless configured otherwise.
func Builder[R any]() CircuitBreakerBuilder[R] {
	return &circuitBreakerConfig[R]{
		BaseListenablePolicy: &spi.BaseListenablePolicy[R]{},
		BaseFailurePolicy:    &spi.BaseFailurePolicy[R]{},
		BaseDelayablePolicy: &spi.BaseDelayablePolicy[R]{
			Delay: 1 * time.Minute,
		},
		clock:                       util.NewClock(),
		failureThreshold:            1,
		failureThresholdingCapacity: 1,
	}
}

func (c *circuitBreakerConfig[R]) Build() CircuitBreaker[R] {
	breaker := &circuitBreaker[R]{
		config: c,
	}
	breaker.state = newClosedState[R](breaker)
	return breaker
}

func (c *circuitBreakerConfig[R]) HandleErrors(errs ...error) CircuitBreakerBuilder[R] {
	c.BaseFailurePolicy.HandleErrors(errs...)
	return c
}

func (c *circuitBreakerConfig[R]) HandleResult(result R) CircuitBreakerBuilder[R] {
	c.BaseFailurePolicy.HandleResult(result)
	return c
}

func (c *circuitBreakerConfig[R]) HandleIf(predicate func(R, error) bool) CircuitBreakerBuilder[R] {
	c.BaseFailurePolicy.HandleIf(predicate)
	return c
}

func (c *circuitBreakerConfig[R]) WithFailureThreshold(failureThreshold uint) CircuitBreakerBuilder[R] {
	return c.WithFailureThresholdRatio(failureThreshold, failureThreshold)
}

func (c *circuitBreakerConfig[R]) WithFailureThresholdRatio(failureThreshold uint, failureThresholdingCapacity uint) CircuitBreakerBuilder[R] {
	c.failureThreshold = failureThreshold
	c.failureThresholdingCapacity = failureThresholdingCapacity
	return c
}

func (c *circuitBreakerConfig[R]) WithFailureThresholdPeriod(failureThreshold uint, failureThresholdingPeriod time.Duration) CircuitBreakerBuilder[R] {
	c.failureThreshold = failureThreshold
	c.failureThresholdingCapacity = failureThreshold
	c.failureExecutionThreshold = failureThreshold
	c.failureThresholdingPeriod = failureThresholdingPeriod
	return c
}

func (c *circuitBreakerConfig[R]) WithFailureExecutionThreshold(failureExecutionThreshold uint) CircuitBreakerBuilder[R] {
	c.failureExecutionThreshold = failureExecutionThreshold
	return c
}

func (c *circuitBreakerConfig[R]) WithFailureRateThreshold(failureRateThreshold uint, failureExecutionThreshold uint, failureThresholdingPeriod time.Duration) CircuitBreakerBuilder[R] {
	c.failureRateThreshold = failureRateThreshold
	c.failureExecutionThreshold = failureExecutionThreshold
	c.failureThresholdingPeriod = failureThresholdingPeriod
	return c
}

func (c *circuitBreakerConfig[R]) WithSuccessThreshold(successThreshold uint) CircuitBreakerBuilder[R] {
	return c.WithSuccessThresholdRatio(successThreshold, successThreshold)
}

func (c *circuitBreakerConfig[R]) WithSuccessThresholdRatio(successThreshold uint, successThresholdingCapacity uint) CircuitBreakerBuilder[R] {
	c.successThreshold = successThreshold
	c.successThresholdingCapacity = successThresholdingCapacity
	return c
}

func (c *circuitBreakerConfig[R]) WithDelay(delay time.Duration) CircuitBreakerBuilder[R] {
	c.BaseDelayablePolicy.WithDelay(delay)
	return c
}

func (c *circuitBreakerConfig[R]) WithDelayFn(delayFn failsafe.DelayFunction[R]) CircuitBreakerBuilder[R] {
	c.BaseDelayablePolicy.WithDelayFn(delayFn)
	return c
}

func (c *circuitBreakerConfig[R]) OnClose(listener func(event StateChangedEvent)) CircuitBreakerBuilder[R] {
	c.closeListener = listener
	return c
}

func (c *circuitBreakerConfig[R]) OnOpen(listener func(event StateChangedEvent)) CircuitBreakerBuilder[R] {
	c.openListener = listener
	return c
}

func (c *circuitBreakerConfig[R]) OnHalfOpen(listener func(event StateChangedEvent)) CircuitBreakerBuilder[R] {
	c.halfOpenListener = listener
	return c
}

func (c *circuitBreakerConfig[R]) OnSuccess(listener func(event failsafe.ExecutionCompletedEvent[R])) CircuitBreakerBuilder[R] {
	c.BaseListenablePolicy.OnSuccess(listener)
	return c
}

func (c *circuitBreakerConfig[R]) OnFailure(listener func(event failsafe.ExecutionCompletedEvent[R])) CircuitBreakerBuilder[R] {
	c.BaseListenablePolicy.OnFailure(listener)
	return c
}
