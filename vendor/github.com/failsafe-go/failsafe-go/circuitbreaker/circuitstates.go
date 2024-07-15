package circuitbreaker

import (
	"time"

	"github.com/failsafe-go/failsafe-go"
)

// State of a CircuitBreaker.
type circuitState[R any] interface {
	getState() State
	getStats() circuitStats
	getRemainingDelay() time.Duration
	tryAcquirePermit() bool
	checkThresholdAndReleasePermit(exec failsafe.Execution[R])
}

type closedState[R any] struct {
	breaker *circuitBreaker[R]
	stats   circuitStats
}

func newClosedState[R any](breaker *circuitBreaker[R]) *closedState[R] {
	var capacity uint
	if breaker.config.failureExecutionThreshold != 0 {
		capacity = breaker.config.failureExecutionThreshold
	} else {
		capacity = breaker.config.failureThresholdingCapacity
	}
	return &closedState[R]{
		breaker: breaker,
		stats:   newStats(breaker.config, true, capacity),
	}
}

func (s *closedState[R]) getState() State {
	return ClosedState
}

func (s *closedState[R]) getStats() circuitStats {
	return s.stats
}

func (s *closedState[R]) getRemainingDelay() time.Duration {
	return 0
}

func (s *closedState[R]) tryAcquirePermit() bool {
	return true
}

// Checks to see if the executions and failure thresholds have been exceeded, opening the circuit if so.
func (s *closedState[R]) checkThresholdAndReleasePermit(exec failsafe.Execution[R]) {
	// Execution threshold can only be set for time based thresholding
	if s.stats.getExecutionCount() >= s.breaker.config.failureExecutionThreshold {
		// Failure rate threshold can only be set for time based thresholding
		failureRateThreshold := s.breaker.config.failureRateThreshold
		if (failureRateThreshold != 0 && s.stats.getFailureRate() >= failureRateThreshold) ||
			(failureRateThreshold == 0 && s.stats.getFailureCount() >= s.breaker.config.failureThreshold) {
			s.breaker.open(exec)
		}
	}
}

type openState[R any] struct {
	breaker   *circuitBreaker[R]
	stats     circuitStats
	startTime int64
	delay     time.Duration
}

func newOpenState[R any](breaker *circuitBreaker[R], previousState circuitState[R], delay time.Duration) *openState[R] {
	return &openState[R]{
		breaker:   breaker,
		stats:     previousState.getStats(),
		startTime: breaker.config.clock.CurrentUnixNano(),
		delay:     delay,
	}
}

func (s *openState[R]) getState() State {
	return OpenState
}

func (s *openState[R]) getStats() circuitStats {
	return s.stats
}

func (s *openState[R]) getRemainingDelay() time.Duration {
	elapsedTime := s.breaker.config.clock.CurrentUnixNano() - s.startTime
	return max(0, s.delay-time.Duration(elapsedTime))
}

func (s *openState[R]) tryAcquirePermit() bool {
	if s.breaker.config.clock.CurrentUnixNano()-s.startTime >= s.delay.Nanoseconds() {
		s.breaker.halfOpen()
		return s.breaker.tryAcquirePermit()
	}
	return false
}

func (s *openState[R]) checkThresholdAndReleasePermit(_ failsafe.Execution[R]) {
}

type halfOpenState[R any] struct {
	breaker             *circuitBreaker[R]
	stats               circuitStats
	permittedExecutions uint
}

func newHalfOpenState[R any](breaker *circuitBreaker[R]) *halfOpenState[R] {
	capacity := breaker.config.successThresholdingCapacity
	if capacity == 0 {
		capacity = breaker.config.failureExecutionThreshold
	}
	if capacity == 0 {
		capacity = breaker.config.failureThresholdingCapacity
	}
	return &halfOpenState[R]{
		breaker:             breaker,
		stats:               newStats[R](breaker.config, false, capacity),
		permittedExecutions: capacity,
	}
}

func (s *halfOpenState[R]) getState() State {
	return HalfOpenState
}

func (s *halfOpenState[R]) getStats() circuitStats {
	return s.stats
}

func (s *halfOpenState[R]) getRemainingDelay() time.Duration {
	return 0
}

func (s *halfOpenState[R]) tryAcquirePermit() bool {
	if s.permittedExecutions > 0 {
		s.permittedExecutions--
		return true
	}
	return false
}

/*
Checks to determine if a threshold has been met and the circuit should be opened or closed.
  - If a success threshold is configured, the circuit is opened or closed based on whether the ratio was exceeded.
  - Else the circuit is opened or closed based on whether the failure threshold was exceeded.

A permit is released before returning.
*/
func (s *halfOpenState[R]) checkThresholdAndReleasePermit(exec failsafe.Execution[R]) {
	var successesExceeded bool
	var failuresExceeded bool

	successThreshold := s.breaker.config.successThreshold
	if successThreshold != 0 {
		successThresholdingCapacity := s.breaker.config.successThresholdingCapacity
		successesExceeded = s.stats.getSuccessCount() >= successThreshold
		failuresExceeded = s.stats.getFailureCount() > successThresholdingCapacity-successThreshold
	} else {
		// Failure rate threshold can only be set for time based thresholding
		failureRateThreshold := s.breaker.config.failureRateThreshold
		if failureRateThreshold != 0 {
			// Execution threshold can only be set for time based thresholding
			executionThresholdExceeded := s.stats.getExecutionCount() >= s.breaker.config.failureExecutionThreshold
			failuresExceeded = executionThresholdExceeded && s.stats.getFailureRate() >= failureRateThreshold
			successesExceeded = executionThresholdExceeded && s.stats.getSuccessRate() > 100-failureRateThreshold
		} else {
			failureThresholdingCapacity := s.breaker.config.failureThresholdingCapacity
			failureThreshold := s.breaker.config.failureThreshold
			failuresExceeded = s.stats.getFailureCount() >= failureThreshold
			successesExceeded = s.stats.getSuccessCount() > failureThresholdingCapacity-failureThreshold
		}
	}

	if successesExceeded {
		s.breaker.close()
	} else if failuresExceeded {
		s.breaker.open(exec)
	}
	s.permittedExecutions++
}
