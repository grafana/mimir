package circuitbreaker

import (
	"errors"
	"sync"
	"time"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/policy"
)

// ErrOpen is returned when an execution is attempted against a circuit breaker that is open.
var ErrOpen = errors.New("circuit breaker open")

// State of a CircuitBreaker.
type State int

func (s State) String() string {
	switch s {
	case ClosedState:
		return "closed"
	case OpenState:
		return "open"
	case HalfOpenState:
		return "half-open"
	default:
		return "unknown"
	}
}

const (
	// ClosedState indicates the circuit is closed and fully functional, allowing executions to occur.
	ClosedState State = iota

	// OpenState indicates the circuit is opened and not allowing executions to occur.
	OpenState

	// HalfOpenState indicates the circuit is temporarily allowing executions to occur.
	HalfOpenState
)

/*
CircuitBreaker is a policy that temporarily blocks execution when a configured number of failures are exceeded. Circuit
breakers have three states: closed, open, and half-open. When a circuit breaker is in the ClosedState (default),
executions are allowed. If a configurable number of failures occur, optionally over some time period, the circuit
breaker transitions to OpenState. In the OpenState a circuit breaker will fail executions with ErrOpen.
After a configurable delay, the circuit breaker will transition to HalfOpenState. In the HalfOpenState a configurable
number of trial executions will be allowed, after which the circuit breaker will transition to either ClosedState or
OpenState depending on how many were successful.

A circuit breaker can be count based or time based:

  - Count based circuit breakers will transition between states when recent execution results exceed a threshold.
  - Time based circuit breakers will transition between states when recent execution results exceed a threshold within a time
    period. A minimum number of executions must be performed in order for a state transition to occur. Time based circuit breakers
    use a sliding window to aggregate execution results. The window is divided into 10 time slices, each representing 1/10th
    of the failureThresholdingPeriod. As time progresses, statistics for old time slices are gradually discarded, which
    smoothes the calculation of success and failure rates.

This type is concurrency safe.
*/
type CircuitBreaker[R any] interface {
	failsafe.Policy[R]
	// Open opens the CircuitBreaker.
	Open()

	// HalfOpen half-opens the CircuitBreaker.
	HalfOpen()

	// Close closes the CircuitBreaker.
	Close()

	// IsOpen returns whether the CircuitBreaker is open.
	IsOpen() bool

	// IsHalfOpen returns whether the CircuitBreaker is half-open.
	IsHalfOpen() bool

	// IsClosed returns whether the CircuitBreaker is closed.
	IsClosed() bool

	// State returns the State of the CircuitBreaker.
	State() State

	// RemainingDelay returns the remaining delay until the circuit is half-opened and allows another execution, when in the
	// OpenState, else returns 0 when in other states.
	RemainingDelay() time.Duration

	// Metrics returns metrics for the CircuitBreaker.
	Metrics() Metrics

	// TryAcquirePermit tries to acquire a permit to use the circuit breaker and returns whether a permit was acquired.
	// Permission will be automatically released when a result or failure is recorded.
	TryAcquirePermit() bool

	// RecordResult records an execution result as a success or failure based on the failure handling configuration.
	RecordResult(result R)

	// RecordError records an error as a success or failure based on the failure handling configuration.
	RecordError(err error)

	// RecordSuccess records an execution success.
	RecordSuccess()

	// RecordFailure records an execution failure.
	RecordFailure()
}

type Metrics interface {
	// Executions returns the number of executions recorded in the current state when the state is ClosedState or
	// HalfOpenState. When the state is OpenState, this returns the executions recorded during the previous ClosedState.
	//
	// For count based thresholding, the max number of executions is limited to the execution threshold. For time based
	// thresholds, the number of executions may vary within the thresholding period.
	Executions() uint

	// Failures returns the number of failures recorded in the current state when in a ClosedState or HalfOpenState. When
	// in OpenState, this returns the failures recorded during the previous ClosedState.
	//
	// For count based thresholds, the max number of failures is based on the failure threshold. For time based thresholds,
	// the number of failures may vary within the failure thresholding period.
	Failures() uint

	// FailureRate returns the percentage rate of failed executions, from 0 to 100, in the current state when in a
	// ClosedState or HalfOpenState. When in OpenState, this returns the rate recorded during the previous ClosedState.
	//
	// The rate is based on the configured failure thresholding capacity.
	FailureRate() uint

	// Successes returns the number of successes recorded in the current state when in a ClosedState or HalfOpenState.
	// When in OpenState, this returns the successes recorded during the previous ClosedState.
	//
	// The max number of successes is based on the success threshold.
	Successes() uint

	// SuccessRate returns percentage rate of successful executions, from 0 to 100, in the current state when in a
	// ClosedState or HalfOpenState. When in OpenState, this returns the successes recorded during the previous ClosedState.
	//
	// The rate is based on the configured success thresholding capacity.
	SuccessRate() uint
}

// StateChangedEvent indicates a CircuitBreaker's state has changed.
type StateChangedEvent struct {
	OldState State
	NewState State
}

type circuitBreaker[R any] struct {
	config *circuitBreakerConfig[R]
	mtx    sync.Mutex
	// Guarded by mtx
	state circuitState[R]
}

func (cb *circuitBreaker[R]) TryAcquirePermit() bool {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb.tryAcquirePermit()
}

func (cb *circuitBreaker[R]) Open() {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	cb.open(nil)
}

func (cb *circuitBreaker[R]) HalfOpen() {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	cb.halfOpen()
}

func (cb *circuitBreaker[R]) Close() {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	cb.close()
}

func (cb *circuitBreaker[R]) State() State {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb.state.getState()
}

func (cb *circuitBreaker[R]) RemainingDelay() time.Duration {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb.state.getRemainingDelay()
}

func (cb *circuitBreaker[R]) Metrics() Metrics {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb
}

func (cb *circuitBreaker[R]) IsOpen() bool {
	return cb.State() == OpenState
}

func (cb *circuitBreaker[R]) IsHalfOpen() bool {
	return cb.State() == HalfOpenState
}

func (cb *circuitBreaker[R]) IsClosed() bool {
	return cb.State() == ClosedState
}

func (cb *circuitBreaker[R]) Executions() uint {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb.state.getStats().getExecutionCount()
}

func (cb *circuitBreaker[R]) Failures() uint {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb.state.getStats().getFailureCount()
}

func (cb *circuitBreaker[R]) FailureRate() uint {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb.state.getStats().getFailureRate()
}

func (cb *circuitBreaker[R]) Successes() uint {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb.state.getStats().getSuccessCount()
}

func (cb *circuitBreaker[R]) SuccessRate() uint {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	return cb.state.getStats().getSuccessRate()
}

func (cb *circuitBreaker[R]) RecordFailure() {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	cb.recordFailure(nil)
}

func (cb *circuitBreaker[R]) RecordError(err error) {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	cb.recordResult(*(new(R)), err)
}

func (cb *circuitBreaker[R]) RecordResult(result R) {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	cb.recordResult(result, nil)
}

func (cb *circuitBreaker[R]) RecordSuccess() {
	cb.mtx.Lock()
	defer cb.mtx.Unlock()
	cb.recordSuccess()
}

func (cb *circuitBreaker[R]) ToExecutor(policyIndex int, _ R) any {
	cbe := &circuitBreakerExecutor[R]{
		BaseExecutor: &policy.BaseExecutor[R]{
			BaseFailurePolicy: cb.config.BaseFailurePolicy,
			PolicyIndex:       policyIndex,
		},
		circuitBreaker: cb,
	}
	cbe.Executor = cbe
	return cbe
}

// Transitions to the newState if not already in that state and calls listener after transitioning.
//
// Requires external locking.
func (cb *circuitBreaker[R]) transitionTo(newState State, exec failsafe.Execution[R], listener func(StateChangedEvent)) {
	transitioned := false
	currentState := cb.state.getState()
	if currentState != newState {
		switch newState {
		case ClosedState:
			cb.state = newClosedState(cb)
		case OpenState:
			delay := cb.config.ComputeDelay(exec)
			if delay == -1 {
				delay = cb.config.Delay
			}
			cb.state = newOpenState(cb, cb.state, delay)
		case HalfOpenState:
			cb.state = newHalfOpenState(cb)
		}
		transitioned = true
	}

	if transitioned {
		event := StateChangedEvent{
			OldState: currentState,
			NewState: newState,
		}
		if cb.config.stateChangedListener != nil {
			cb.config.stateChangedListener(event)
		}
		if listener != nil {
			listener(event)
		}
	}
}

// Requires external locking.
func (cb *circuitBreaker[R]) tryAcquirePermit() bool {
	return cb.state.tryAcquirePermit()
}

// Opens the circuit breaker and considers the execution when computing the delay before the circuit breaker
// will transition to half open.
//
// Requires external locking.
func (cb *circuitBreaker[R]) open(execution failsafe.Execution[R]) {
	cb.transitionTo(OpenState, execution, cb.config.openListener)
}

// Requires external locking.
func (cb *circuitBreaker[R]) close() {
	cb.transitionTo(ClosedState, nil, cb.config.closeListener)
}

// Requires external locking.
func (cb *circuitBreaker[R]) halfOpen() {
	cb.transitionTo(HalfOpenState, nil, cb.config.halfOpenListener)
}

// Requires external locking.
func (cb *circuitBreaker[R]) recordResult(result R, err error) {
	if cb.config.IsFailure(result, err) {
		cb.recordFailure(nil)
	} else {
		cb.recordSuccess()
	}
}

// Requires external locking.
func (cb *circuitBreaker[R]) recordSuccess() {
	cb.state.getStats().recordSuccess()
	cb.state.checkThresholdAndReleasePermit(nil)
}

// Requires external locking.
func (cb *circuitBreaker[R]) recordFailure(exec failsafe.Execution[R]) {
	cb.state.getStats().recordFailure()
	cb.state.checkThresholdAndReleasePermit(exec)
}

func (cb *circuitBreaker[R]) Reset() {
	cb.close()
	cb.state.getStats().reset()
}
