// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestCircuitBreaker_TryRecordFailure(t *testing.T) {
	cfg := CircuitBreakerConfig{Enabled: true}
	cb := newCircuitBreaker(cfg, prometheus.NewRegistry(), "test-request-type", log.NewNopLogger())
	t.Run("no error", func(t *testing.T) {
		require.False(t, cb.tryRecordFailure(nil))
	})

	t.Run("context cancelled", func(t *testing.T) {
		require.False(t, cb.tryRecordFailure(context.Canceled))
		require.False(t, cb.tryRecordFailure(fmt.Errorf("%w", context.Canceled)))
	})

	t.Run("gRPC context cancelled", func(t *testing.T) {
		err := status.Error(codes.Canceled, "cancelled!")
		require.False(t, cb.tryRecordFailure(err))
		require.False(t, cb.tryRecordFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC deadline exceeded", func(t *testing.T) {
		err := status.Error(codes.DeadlineExceeded, "broken!")
		require.True(t, cb.tryRecordFailure(err))
		require.True(t, cb.tryRecordFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable with INSTANCE_LIMIT details is not a failure", func(t *testing.T) {
		err := newErrorWithStatus(newInstanceLimitReachedError("broken"), codes.Unavailable)
		require.False(t, cb.tryRecordFailure(err))
		require.False(t, cb.tryRecordFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable with SERVICE_UNAVAILABLE details is not a failure", func(t *testing.T) {
		stat := status.New(codes.Unavailable, "broken!")
		stat, err := stat.WithDetails(&mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE})
		require.NoError(t, err)
		err = stat.Err()
		require.False(t, cb.tryRecordFailure(err))
		require.False(t, cb.tryRecordFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable without details is not a failure", func(t *testing.T) {
		err := status.Error(codes.Unavailable, "broken!")
		require.False(t, cb.tryRecordFailure(err))
		require.False(t, cb.tryRecordFailure(fmt.Errorf("%w", err)))
	})
}

func requireCircuitBreakerState(t *testing.T, cb *circuitBreaker, state circuitBreakerState) {
	require.Equal(t, state, cb.state.Load())
}

func TestCircuitBreaker_NilImplementation(t *testing.T) {
	var cb *circuitBreaker

	// A nil *circuitBreaker is a valid implementation
	require.False(t, cb.tryRecordFailure(errors.New("error")))
	require.False(t, cb.isActive())
	cb.activate()
	cb.deactivate()
	require.False(t, cb.isOpen())
	cb.scheduleActivation()
	f, err := cb.tryAcquirePermit()
	require.NotNil(t, f)
	require.NoError(t, err)
	require.Nil(t, cb.finishRequest(0, 0, nil))
	require.Nil(t, cb.recordResult(errors.New("error")))
}

func TestCircuitBreaker_IsActive(t *testing.T) {
	var cb *circuitBreaker

	cfg := CircuitBreakerConfig{Enabled: true}
	cb = newCircuitBreaker(cfg, prometheus.NewRegistry(), "test-request-type", log.NewNopLogger())

	// Inactive by default
	requireCircuitBreakerState(t, cb, circuitBreakerInactive)
	require.False(t, cb.isActive())

	cb.activate()

	// Should be active immediately
	requireCircuitBreakerState(t, cb, circuitBreakerActive)
	require.True(t, cb.isActive())

	cb.deactivate()

	// Should be inactive immediately
	requireCircuitBreakerState(t, cb, circuitBreakerInactive)
	require.False(t, cb.isActive())
}

func TestCircuitBreaker_IsActiveWithDelay(t *testing.T) {
	var cb *circuitBreaker

	cfg := CircuitBreakerConfig{Enabled: true, InitialDelay: 5 * time.Millisecond}
	cb = newCircuitBreaker(cfg, prometheus.NewRegistry(), "test-request-type", log.NewNopLogger())

	// Inactive by default
	requireCircuitBreakerState(t, cb, circuitBreakerInactive)
	require.False(t, cb.isActive())

	cb.activate()

	// When InitialDelay is set, circuit breaker is not immediately activated, but activation is pending
	requireCircuitBreakerState(t, cb, circuitBreakerPending)
	require.False(t, cb.isActive())

	// InitalDelay starts when we first get a request, not when we activate the circuit breaker
	require.Never(t, cb.isActive, 10*time.Millisecond, 1*time.Millisecond)

	_, err := cb.tryAcquirePermit()
	require.NoError(t, err)

	// Once we get a request, circuit breaker becomes active after InitialDelay passes
	requireCircuitBreakerState(t, cb, circuitBreakerActivating)
	require.False(t, cb.isActive())
	require.Eventually(t, cb.isActive, 10*time.Millisecond, 1*time.Millisecond)
	requireCircuitBreakerState(t, cb, circuitBreakerActive)

	cb.deactivate()

	// Should be inactive immediately
	requireCircuitBreakerState(t, cb, circuitBreakerInactive)
	require.False(t, cb.isActive())

	_, err = cb.tryAcquirePermit()
	require.NoError(t, err)

	// A request while deactivated shouldn't queue an activation
	requireCircuitBreakerState(t, cb, circuitBreakerInactive)
	require.False(t, cb.isActive())
	require.Never(t, cb.isActive, 10*time.Millisecond, 1*time.Millisecond)
}

func TestCircuitBreaker_IsActiveWithDelay_Cancel(t *testing.T) {
	var cb *circuitBreaker

	cfg := CircuitBreakerConfig{Enabled: true, InitialDelay: 5 * time.Millisecond}
	cb = newCircuitBreaker(cfg, prometheus.NewRegistry(), "test-request-type", log.NewNopLogger())

	cb.activate()

	// When InitialDelay is set, circuit breaker is not immediately activated, but activation is pending
	requireCircuitBreakerState(t, cb, circuitBreakerPending)
	require.False(t, cb.isActive())

	cb.deactivate()

	// Should be inactive immediately, and cancel any pending activation
	requireCircuitBreakerState(t, cb, circuitBreakerInactive)
	require.False(t, cb.isActive())
	require.Never(t, cb.isActive, 10*time.Millisecond, 1*time.Millisecond)

	cb.activate()
	_, err := cb.tryAcquirePermit()
	require.NoError(t, err)

	// Once we get a request, circuit breaker queues the activation
	requireCircuitBreakerState(t, cb, circuitBreakerActivating)
	require.False(t, cb.isActive())

	cb.deactivate()

	// Should be inactive immediately, and cancel any queued activation
	requireCircuitBreakerState(t, cb, circuitBreakerInactive)
	require.False(t, cb.isActive())
	require.Never(t, cb.isActive, 10*time.Millisecond, 1*time.Millisecond)
}

func TestCircuitBreaker_IsActiveWithDelay_CancelThenRestart(t *testing.T) {
	var cb *circuitBreaker

	cfg := CircuitBreakerConfig{Enabled: true, InitialDelay: 150 * time.Millisecond}
	cb = newCircuitBreaker(cfg, prometheus.NewRegistry(), "test-request-type", log.NewNopLogger())

	cb.activate()
	_, err := cb.tryAcquirePermit()
	require.NoError(t, err)

	// t = 0ms, queue an activation
	requireCircuitBreakerState(t, cb, circuitBreakerActivating)
	require.False(t, cb.isActive())

	// We're going to cancel this activation, so it shouldn't trigger at t=150ms
	go require.Never(t, cb.isActive, 200*time.Millisecond, 1*time.Millisecond)

	time.Sleep(50 * time.Millisecond)

	// t = 50ms, cancel the activation
	cb.deactivate()

	// Should be inactive immediately, and cancel any queued activation
	requireCircuitBreakerState(t, cb, circuitBreakerInactive)
	require.False(t, cb.isActive())

	time.Sleep(50 * time.Millisecond)

	// t = 100ms, queue another activation
	cb.activate()
	_, err = cb.tryAcquirePermit()
	require.NoError(t, err)

	// A new activation should be queued, and activate the breaker at t=250ms
	requireCircuitBreakerState(t, cb, circuitBreakerActivating)
	require.False(t, cb.isActive())
	require.Eventually(t, cb.isActive, 200*time.Millisecond, 1*time.Millisecond)
	requireCircuitBreakerState(t, cb, circuitBreakerActive)
}

func TestCircuitBreaker_TryAcquirePermit(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_transitions_total",
		"cortex_ingester_circuit_breaker_current_state",
	}
	testCases := map[string]struct {
		initialDelay                time.Duration
		circuitBreakerSetup         func(*circuitBreaker)
		expectedCircuitBreakerError bool
		expectedMetrics             string
	}{
		"if circuit breaker is not active, finish function and no error are returned": {
			initialDelay: 1 * time.Minute,
			circuitBreakerSetup: func(cb *circuitBreaker) {
				cb.deactivate()
			},
			expectedCircuitBreakerError: false,
		},
		"if circuit breaker closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *circuitBreaker) {
				cb.activate()
				cb.cb.Close()
			},
			expectedCircuitBreakerError: false,
		},
		"if circuit breaker open, no finish function and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup: func(cb *circuitBreaker) {
				cb.activate()
				cb.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
        	    # HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="test-request-type",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="test-request-type",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="test-request-type",state="open"} 1
        	    # HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="test-request-type",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="test-request-type",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="test-request-type",state="open"} 1
			`,
		},
		"if circuit breaker half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *circuitBreaker) {
				cb.activate()
				cb.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cfg := CircuitBreakerConfig{
				Enabled:                    true,
				FailureThresholdPercentage: 20,
				CooldownPeriod:             10 * time.Second,
				testModeEnabled:            true,
			}
			cb := newCircuitBreaker(cfg, registry, "test-request-type", log.NewNopLogger())
			testCase.circuitBreakerSetup(cb)
			finish, err := cb.tryAcquirePermit()
			if testCase.expectedCircuitBreakerError {
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
				require.Nil(t, finish)
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			} else {
				require.NoError(t, err)
				require.NotNil(t, finish)
			}
		})
	}
}

func TestCircuitBreaker_RecordResult(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_request_timeouts_total",
	}
	testCases := map[string]struct {
		errs            []error
		expectedErr     error
		expectedMetrics string
	}{
		"successful execution records a success": {
			errs:        nil,
			expectedErr: nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 1
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 0
			`,
		},
		"erroneous execution not passing the failure check records a success": {
			errs:        []error{context.Canceled},
			expectedErr: nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 1
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 0
			`,
		},
		"erroneous execution passing the failure check records an error": {
			errs:        []error{context.DeadlineExceeded},
			expectedErr: context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 1
			`,
		},
		"erroneous execution with multiple errors records the first error passing the failure check": {
			errs:        []error{context.Canceled, context.DeadlineExceeded},
			expectedErr: context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 1
			`,
		},
	}
	cfg := CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cb := newCircuitBreaker(cfg, registry, "test-request-type", log.NewNopLogger())
			cb.activate()
			err := cb.recordResult(testCase.errs...)
			require.Equal(t, testCase.expectedErr, err)
			assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
		})
	}
}

func TestCircuitBreaker_FinishRequest(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_request_timeouts_total",
	}
	maxRequestDuration := 2 * time.Second
	testCases := map[string]struct {
		requestDuration time.Duration
		isActive        bool
		err             error
		expectedErr     error
		expectedMetrics string
	}{
		"with a permit acquired, requestDuration lower than maxRequestDuration and no input error, finishRequest gives success": {
			requestDuration: 1 * time.Second,
			isActive:        true,
			err:             nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 1
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 0
			`,
		},
		"with circuit breaker not active, requestDuration lower than maxRequestDuration and no input error, finishRequest does nothing": {
			requestDuration: 1 * time.Second,
			isActive:        false,
			err:             nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 0
			`,
		},
		"with circuit breaker active, requestDuration higher than maxRequestDuration and no input error, finishRequest gives context deadline exceeded error": {
			requestDuration: 3 * time.Second,
			isActive:        true,
			err:             nil,
			expectedErr:     context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 1
			`,
		},
		"with circuit breaker not active, requestDuration higher than maxRequestDuration and no input error, finishRequest does nothing": {
			requestDuration: 3 * time.Second,
			isActive:        false,
			err:             nil,
			expectedErr:     nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 0
			`,
		},
		"with circuit breaker not active, requestDuration higher than maxRequestDuration and an input error relevant for circuit breakers, finishRequest does nothing": {
			requestDuration: 3 * time.Second,
			isActive:        false,
			err:             context.DeadlineExceeded,
			expectedErr:     nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 0
			`,
		},
		"with circuit breaker not active, requestDuration higher than maxRequestDuration and an input error irrelevant for circuit breakers, finishRequest does nothing": {
			requestDuration: 3 * time.Second,
			isActive:        false,
			err:             context.Canceled,
			expectedErr:     nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 0
			`,
		},
		"with circuit breaker active, requestDuration higher than maxRequestDuration and an input error irrelevant for circuit breakers, finishRequest gives context deadline exceeded error": {
			requestDuration: 3 * time.Second,
			isActive:        true,
			err:             newInstanceLimitReachedError("error"),
			expectedErr:     context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{request_type="test-request-type",result="circuit_breaker_open"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="test-request-type"} 1
			`,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cfg := CircuitBreakerConfig{
				Enabled:        true,
				RequestTimeout: 2 * time.Second,
			}
			cb := newCircuitBreaker(cfg, registry, "test-request-type", log.NewNopLogger())
			if testCase.isActive {
				cb.activate()
			}
			err := cb.finishRequest(testCase.requestDuration, maxRequestDuration, testCase.err)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, testCase.expectedErr)
			}
			assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
		})
	}
}

func TestIngester_IngestStorage_PushToStorage_CircuitBreaker(t *testing.T) {
	pushTimeout := 100 * time.Millisecond
	tests := map[string]struct {
		expectedErrorWhenCircuitBreakerClosed error
		pushRequestDelay                      time.Duration
		limits                                InstanceLimits
		expectedMetrics                       string
	}{
		"deadline exceeded": {
			expectedErrorWhenCircuitBreakerClosed: nil,
			limits:                                InstanceLimits{MaxInMemoryTenants: 3},
			pushRequestDelay:                      pushTimeout,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 2
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 2
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	    # TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 2
			`,
		},
	}

	for initialDelayEnabled, initialDelayStatus := range map[bool]string{false: "disabled", true: "enabled"} {
		for testName, testCase := range tests {
			t.Run(fmt.Sprintf("%s with initial delay %s", testName, initialDelayStatus), func(t *testing.T) {
				metricLabelAdapters := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test"}}}
				metricNames := []string{
					"cortex_ingester_circuit_breaker_results_total",
					"cortex_ingester_circuit_breaker_transitions_total",
					"cortex_ingester_circuit_breaker_current_state",
					"cortex_ingester_circuit_breaker_request_timeouts_total",
				}

				registry := prometheus.NewRegistry()

				// Create a mocked ingester
				cfg := defaultIngesterTestConfig(t)
				cfg.ActiveSeriesMetrics.IdleTimeout = 100 * time.Millisecond
				cfg.InstanceLimitsFn = func() *InstanceLimits {
					return &testCase.limits
				}
				failureThreshold := 2
				var initialDelay time.Duration
				if initialDelayEnabled {
					initialDelay = time.Hour
				}
				cfg.PushCircuitBreaker = CircuitBreakerConfig{
					Enabled:                    true,
					FailureThresholdPercentage: uint(failureThreshold),
					CooldownPeriod:             10 * time.Second,
					InitialDelay:               initialDelay,
					RequestTimeout:             pushTimeout,
					testModeEnabled:            true,
				}

				overrides, err := validation.NewOverrides(defaultLimitsTestConfig(), nil)
				require.NoError(t, err)
				i, _, _ := createTestIngesterWithIngestStorage(t, &cfg, overrides, registry)
				require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
				defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

				// Wait until the ingester is healthy
				test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
					return i.lifecycler.HealthyInstancesCount()
				})

				// the first request is successful
				ctx := user.InjectOrgID(context.Background(), "test-0")
				req := mimirpb.ToWriteRequest(
					metricLabelAdapters,
					[]mimirpb.Sample{{Value: 1, TimestampMs: 8}},
					nil,
					nil,
					mimirpb.API,
				)
				err = i.PushToStorage(ctx, req)
				require.NoError(t, err)

				count := 0

				// Push timeseries for each user
				for _, userID := range []string{"test-1", "test-2"} {
					reqs := []*mimirpb.WriteRequest{
						mimirpb.ToWriteRequest(
							metricLabelAdapters,
							[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
							nil,
							nil,
							mimirpb.API,
						),
						mimirpb.ToWriteRequest(
							metricLabelAdapters,
							[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
							nil,
							nil,
							mimirpb.API,
						),
					}

					for _, req := range reqs {
						ctx := user.InjectOrgID(context.Background(), userID)
						count++
						i.circuitBreaker.push.testRequestDelay = testCase.pushRequestDelay
						err = i.PushToStorage(ctx, req)
						if initialDelayEnabled {
							if testCase.expectedErrorWhenCircuitBreakerClosed != nil {
								require.ErrorAs(t, err, &testCase.expectedErrorWhenCircuitBreakerClosed)
							} else {
								require.NoError(t, err)
							}
						} else {
							if count <= failureThreshold {
								if testCase.expectedErrorWhenCircuitBreakerClosed != nil {
									require.ErrorAs(t, err, &testCase.expectedErrorWhenCircuitBreakerClosed)
								}
							} else {
								require.ErrorAs(t, err, &circuitBreakerOpenError{})
							}
						}
					}
				}

				// Check tracked Prometheus metrics
				var expectedMetrics string
				if initialDelayEnabled {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
						# TYPE cortex_ingester_circuit_breaker_results_total counter
        	            cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	            cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	            cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	            cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
        	            cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	            cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
						# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
						# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
						cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 0
    				`
				} else {
					expectedMetrics = testCase.expectedMetrics
				}
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
			})
		}
	}
}

func TestIngester_StartPushRequest_CircuitBreakerOpen(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	cfg := defaultIngesterTestConfig(t)
	cfg.PushCircuitBreaker = CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second}

	i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
	defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

	// Wait until the ingester is healthy
	test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
		return i.lifecycler.HealthyInstancesCount()
	})

	ctx := user.InjectOrgID(context.Background(), "test")

	// If i's circuit breaker is closed, StartPushRequest is successful.
	i.circuitBreaker.push.cb.Close()
	_, err = i.StartPushRequest(ctx, 0)
	require.NoError(t, err)

	// If i's circuit breaker is open, StartPushRequest returns a circuitBreakerOpenError.
	i.circuitBreaker.push.cb.Open()
	_, err = i.StartPushRequest(ctx, 0)
	require.Error(t, err)
	require.ErrorAs(t, err, &circuitBreakerOpenError{})

	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_transitions_total",
		"cortex_ingester_circuit_breaker_current_state",
	}
	expectedMetrics := `
		# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
		# TYPE cortex_ingester_circuit_breaker_results_total counter
        cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 1
        cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
		# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
		# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 1
		# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        # TYPE cortex_ingester_circuit_breaker_current_state gauge
        cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 1
	`
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
}

func TestIngester_FinishPushRequest(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_request_timeouts_total",
	}
	testCases := map[string]struct {
		pushRequestDuration          time.Duration
		acquiredCircuitBreakerPermit bool
		err                          error
		expectedMetrics              string
	}{
		"with a permit acquired, pushRequestDuration lower than RequestTimeout and no input err, FinishPushRequest records a success": {
			pushRequestDuration:          1 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 0
			`,
		},
		"when a permit not acquired, pushRequestDuration lower than RequestTimeout and no input err, FinishPushRequest does nothing": {
			pushRequestDuration:          1 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 0
			`,
		},
		"with a permit acquired, pushRequestDuration higher than RequestTimeout and no input error, FinishPushRequest records a failure": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 1
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 1
			`,
		},
		"with a permit not acquired, pushRequestDuration higher than RequestTimeout and no input error, FinishPushRequest does nothing": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 0
			`,
		},
		"with a permit acquired, pushRequestDuration higher than RequestTimeout and an input error irrelevant for the circuit breakers, FinishPushRequest records a failure": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          newInstanceLimitReachedError("error"),
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 1
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 1
			`,
		},
		"with a permit not acquired, pushRequestDuration higher than RequestTimeout and an input error relevant for the circuit breakers, FinishPushRequest does nothing": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          newInstanceLimitReachedError("error"),
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 0
			`,
		},
		"with a permit not acquired, pushRequestDuration higher than RequestTimeout and an input error irrelevant for the circuit breakers, FinishPushRequest does nothing": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          context.Canceled,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 0
			`,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			cfg := defaultIngesterTestConfig(t)
			cfg.PushCircuitBreaker = CircuitBreakerConfig{
				Enabled:        true,
				RequestTimeout: 2 * time.Second,
			}

			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, reg)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			ctx := user.InjectOrgID(context.Background(), "test")

			st := &pushRequestState{
				requestDuration: testCase.pushRequestDuration,
				requestFinish: func(duration time.Duration, err error) {
					if testCase.acquiredCircuitBreakerPermit {
						_ = i.circuitBreaker.push.finishRequest(duration, cfg.PushCircuitBreaker.RequestTimeout, err)
					}
				},
				pushErr: testCase.err,
			}
			ctx = context.WithValue(ctx, pushReqCtxKey, st)

			i.FinishPushRequest(ctx)
			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), metricNames...))
		})
	}
}

func TestIngester_Push_CircuitBreaker_DeadlineExceeded(t *testing.T) {
	pushTimeout := 100 * time.Millisecond
	for initialDelayEnabled, initialDelayStatus := range map[bool]string{false: "disabled", true: "enabled"} {
		t.Run(fmt.Sprintf("test slow push with initial delay %s", initialDelayStatus), func(t *testing.T) {
			metricLabelAdapters := [][]mimirpb.LabelAdapter{{{Name: labels.MetricName, Value: "test"}}}
			metricNames := []string{
				"cortex_ingester_circuit_breaker_results_total",
				"cortex_ingester_circuit_breaker_transitions_total",
				"cortex_ingester_circuit_breaker_current_state",
				"cortex_ingester_circuit_breaker_request_timeouts_total",
			}

			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.ActiveSeriesMetrics.IdleTimeout = 100 * time.Millisecond
			failureThreshold := 2
			var initialDelay time.Duration
			if initialDelayEnabled {
				initialDelay = time.Hour
			}
			cfg.PushCircuitBreaker = CircuitBreakerConfig{
				Enabled:                    true,
				FailureThresholdPercentage: uint(failureThreshold),
				CooldownPeriod:             10 * time.Second,
				InitialDelay:               initialDelay,
				RequestTimeout:             pushTimeout,
				testModeEnabled:            true,
			}

			i, err := prepareIngesterWithBlocksStorage(t, cfg, nil, registry)
			require.NoError(t, err)

			require.NoError(t, services.StartAndAwaitRunning(context.Background(), i))
			defer services.StopAndAwaitTerminated(context.Background(), i) //nolint:errcheck

			// Wait until the ingester is healthy
			test.Poll(t, 100*time.Millisecond, 1, func() interface{} {
				return i.lifecycler.HealthyInstancesCount()
			})

			// the first request is successful
			ctx := user.InjectOrgID(context.Background(), "test-0")
			req := mimirpb.ToWriteRequest(
				metricLabelAdapters,
				[]mimirpb.Sample{{Value: 1, TimestampMs: 8}},
				nil,
				nil,
				mimirpb.API,
			)
			ctx, err = i.StartPushRequest(ctx, int64(req.Size()))
			require.NoError(t, err)
			err = i.PushToStorage(ctx, req)
			require.NoError(t, err)
			i.FinishPushRequest(ctx)

			count := 0

			// Push timeseries for each user
			for _, userID := range []string{"test-1", "test-2"} {
				reqs := []*mimirpb.WriteRequest{
					mimirpb.ToWriteRequest(
						metricLabelAdapters,
						[]mimirpb.Sample{{Value: 1, TimestampMs: 9}},
						nil,
						nil,
						mimirpb.API,
					),
					mimirpb.ToWriteRequest(
						metricLabelAdapters,
						[]mimirpb.Sample{{Value: 2, TimestampMs: 10}},
						nil,
						nil,
						mimirpb.API,
					),
				}

				for _, req := range reqs {
					ctx := user.InjectOrgID(context.Background(), userID)
					// Configure circuit breaker to delay push requests.
					i.circuitBreaker.push.testRequestDelay = pushTimeout
					count++

					ctx, err = i.StartPushRequest(ctx, int64(req.Size()))
					if initialDelayEnabled || count <= failureThreshold {
						// If initial delay is enabled we expect no deadline exceeded errors
						// to be registered with the circuit breaker.
						// If initial delay is disabled, and the circuit breaker registered
						// less than failureThreshold deadline exceeded errors, it is still
						// closed.
						require.NoError(t, err)
						require.Equal(t, circuitbreaker.ClosedState, i.circuitBreaker.push.cb.State())
						st, ok := ctx.Value(pushReqCtxKey).(*pushRequestState)
						require.True(t, ok)
						require.Equal(t, int64(req.Size()), st.requestSize)
						_, err = i.Push(ctx, req)
						require.NoError(t, err)
						i.FinishPushRequest(ctx)
					} else {
						require.Equal(t, circuitbreaker.OpenState, i.circuitBreaker.push.cb.State())
						require.Nil(t, ctx)
						require.ErrorAs(t, err, &circuitBreakerOpenError{})
					}
				}
			}

			// Check tracked Prometheus metrics
			var expectedMetrics string
			if initialDelayEnabled {
				expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
						# TYPE cortex_ingester_circuit_breaker_results_total counter
        	            cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	            cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	            cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	            cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
        	            cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	            cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
						# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
						# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
						cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 0
    				`
			} else {
				expectedMetrics = `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 2
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 2
				cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
				cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
				cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
				cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
				cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
				# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
				cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 2
    				`
			}
			assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
		})
	}
}

type mockedCircuitBreaker struct {
	circuitbreaker.CircuitBreaker[any]
	mock.Mock

	acquiredPermitCount *atomic.Int64
	recordSuccessCount  *atomic.Int64
	recordFailureCount  *atomic.Int64
}

func (cb *mockedCircuitBreaker) TryAcquirePermit() bool {
	result := cb.CircuitBreaker.TryAcquirePermit()
	if result {
		cb.acquiredPermitCount.Inc()
	}
	return result
}

func (cb *mockedCircuitBreaker) RecordSuccess() {
	cb.CircuitBreaker.RecordSuccess()
	cb.recordSuccessCount.Inc()
	cb.acquiredPermitCount.Dec()
}

func (cb *mockedCircuitBreaker) RecordFailure() {
	cb.CircuitBreaker.RecordSuccess()
	cb.recordFailureCount.Inc()
	cb.acquiredPermitCount.Dec()
}

func TestPRCircuitBreaker_NewPRCircuitBreaker(t *testing.T) {
	pushCfg := CircuitBreakerConfig{
		Enabled:         true,
		CooldownPeriod:  10 * time.Second,
		testModeEnabled: true,
	}
	readCfg := CircuitBreakerConfig{
		Enabled:         true,
		CooldownPeriod:  10 * time.Second,
		testModeEnabled: true,
	}
	registerer := prometheus.NewRegistry()
	prCB := newIngesterCircuitBreaker(pushCfg, readCfg, log.NewNopLogger(), registerer)
	require.NotNil(t, prCB)
	require.NotNil(t, prCB.push)
	require.NotNil(t, prCB.read)

	expectedMetrics := `
		# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
		# TYPE cortex_ingester_circuit_breaker_results_total counter
		cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
		cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
		cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
		cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
		cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
		cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
		# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
		# TYPE cortex_ingester_circuit_breaker_transitions_total counter
		cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
		cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
		cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
		cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
		cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
		cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
		# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
		# TYPE cortex_ingester_circuit_breaker_current_state gauge
		cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
		cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
		cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
		cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
		cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
		cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
		# HELP cortex_ingester_circuit_breaker_request_timeouts_total Number of times the circuit breaker recorded a request that reached timeout.
		# TYPE cortex_ingester_circuit_breaker_request_timeouts_total counter
		cortex_ingester_circuit_breaker_request_timeouts_total{request_type="push"} 0
		cortex_ingester_circuit_breaker_request_timeouts_total{request_type="read"} 0
	`
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_transitions_total",
		"cortex_ingester_circuit_breaker_current_state",
		"cortex_ingester_circuit_breaker_request_timeouts_total",
	}
	assert.NoError(t, testutil.GatherAndCompare(registerer, strings.NewReader(expectedMetrics), metricNames...))
}

func TestPRCircuitBreaker_TryPushAcquirePermit(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_transitions_total",
		"cortex_ingester_circuit_breaker_current_state",
	}
	testCases := map[string]struct {
		circuitBreakerSetup         func(breaker ingesterCircuitBreaker)
		expectedCircuitBreakerError bool
		expectedMetrics             string
	}{
		"if push circuit breaker is not active, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.push.deactivate()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
			`,
		},
		"if push circuit breaker is closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.push.activate()
				cb.push.cb.Close()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
			`,
		},
		"if push circuit breaker is open, no finish function and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.push.activate()
				cb.push.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 1
			`,
		},
		"if push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.push.activate()
				cb.push.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
			`,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cfg := CircuitBreakerConfig{
				Enabled:                    true,
				FailureThresholdPercentage: 20,
				CooldownPeriod:             10 * time.Second,
				RequestTimeout:             circuitBreakerDefaultPushTimeout,
				testModeEnabled:            true,
			}
			acquiredPermitCount := atomic.NewInt64(0)
			recordedSuccessCount := atomic.NewInt64(0)
			recordedFailureCount := atomic.NewInt64(0)
			cb := newIngesterCircuitBreaker(cfg, CircuitBreakerConfig{}, log.NewNopLogger(), registry)
			cb.push.cb = &mockedCircuitBreaker{
				CircuitBreaker:      cb.push.cb,
				acquiredPermitCount: acquiredPermitCount,
				recordSuccessCount:  recordedSuccessCount,
				recordFailureCount:  recordedFailureCount,
			}
			testCase.circuitBreakerSetup(cb)
			finish, err := cb.tryAcquirePushPermit()

			if testCase.expectedCircuitBreakerError {
				require.Nil(t, finish)
				require.Error(t, err)
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			} else {
				require.NoError(t, err)
				require.NotNil(t, finish)
				var expectedVal int64
				if cb.push.isActive() {
					expectedVal = 1
				}
				require.Equal(t, expectedVal, acquiredPermitCount.Load())
				require.Equal(t, int64(0), recordedSuccessCount.Load())
				require.Equal(t, int64(0), recordedFailureCount.Load())
				finish(0, err)
				require.Equal(t, int64(0), acquiredPermitCount.Load())
				require.Equal(t, expectedVal, recordedSuccessCount.Load())
				require.Equal(t, int64(0), recordedFailureCount.Load())
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			}
		})
	}
}

func TestPRCircuitBreaker_TryReadAcquirePermit(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_transitions_total",
		"cortex_ingester_circuit_breaker_current_state",
	}
	testCases := map[string]struct {
		circuitBreakerSetup         func(breaker ingesterCircuitBreaker)
		expectedCircuitBreakerError bool
		expectedMetrics             string
	}{
		"if read circuit breaker is not active and push circuit breaker is not active, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.deactivate()
				cb.push.deactivate()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is not active and push circuit breaker is closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.deactivate()
				cb.push.activate()
				cb.push.cb.Close()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is not active and push circuit breaker is open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.deactivate()
				cb.push.activate()
				cb.push.cb.Open()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is not active and push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.deactivate()
				cb.push.activate()
				cb.push.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is closed and push circuit breaker is is not active, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.Close()
				cb.push.deactivate()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is closed and push circuit breaker is closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.Close()
				cb.push.activate()
				cb.push.cb.Close()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is closed and push circuit breaker is open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.Close()
				cb.push.activate()
				cb.push.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is closed and push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.Close()
				cb.push.activate()
				cb.push.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is open and push circuit breaker is not active, no finish function and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.Open()
				cb.push.deactivate()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 1
			`,
		},
		"if read circuit breaker is open and push circuit breaker is closed, no finish function and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.Open()
				cb.push.activate()
				cb.push.cb.Close()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 1
			`,
		},
		"if read circuit breaker is open and push circuit breaker is open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.Open()
				cb.push.activate()
				cb.push.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 1
			`,
		},
		"if read circuit breaker is open and push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.Open()
				cb.push.activate()
				cb.push.cb.HalfOpen()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 1
			`,
		},
		"if read circuit breaker is half-open and push circuit breaker is not active, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.HalfOpen()
				cb.push.deactivate()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is half-open and push circuit breaker is closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.HalfOpen()
				cb.push.activate()
				cb.push.cb.Close()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is half-open and push circuit breaker is open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.HalfOpen()
				cb.push.activate()
				cb.push.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
		"if read circuit breaker is half-open and push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb ingesterCircuitBreaker) {
				cb.read.activate()
				cb.read.cb.HalfOpen()
				cb.push.activate()
				cb.push.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="push",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{request_type="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="push",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{request_type="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_current_state{request_type="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{request_type="push",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{request_type="read",state="open"} 0
			`,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			pushCfg := CircuitBreakerConfig{
				Enabled:                    true,
				FailureThresholdPercentage: 20,
				CooldownPeriod:             10 * time.Second,
				RequestTimeout:             circuitBreakerDefaultPushTimeout,
				testModeEnabled:            true,
			}
			readCfg := CircuitBreakerConfig{
				Enabled:                    true,
				FailureThresholdPercentage: 20,
				CooldownPeriod:             10 * time.Second,
				RequestTimeout:             circuitBreakerDefaultReadTimeout,
				testModeEnabled:            true,
			}
			pushAcquiredPermitCount := atomic.NewInt64(0)
			pushRecordedSuccessCount := atomic.NewInt64(0)
			pushRecordedFailureCount := atomic.NewInt64(0)
			readAcquiredPermitCount := atomic.NewInt64(0)
			readRecordedSuccessCount := atomic.NewInt64(0)
			readRecordedFailureCount := atomic.NewInt64(0)
			cb := newIngesterCircuitBreaker(pushCfg, readCfg, log.NewNopLogger(), registry)
			cb.push.cb = &mockedCircuitBreaker{
				CircuitBreaker:      cb.push.cb,
				acquiredPermitCount: pushAcquiredPermitCount,
				recordSuccessCount:  pushRecordedSuccessCount,
				recordFailureCount:  pushRecordedFailureCount,
			}
			cb.read.cb = &mockedCircuitBreaker{
				CircuitBreaker:      cb.read.cb,
				acquiredPermitCount: readAcquiredPermitCount,
				recordSuccessCount:  readRecordedSuccessCount,
				recordFailureCount:  readRecordedFailureCount,
			}
			testCase.circuitBreakerSetup(cb)
			finish, err := cb.tryAcquireReadPermit()

			if testCase.expectedCircuitBreakerError {
				require.Nil(t, finish)
				require.Error(t, err)
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			} else {
				require.NoError(t, err)
				require.NotNil(t, finish)
				var expectedReadVal int64
				if cb.read.isActive() {
					expectedReadVal = 1
				}
				require.Equal(t, int64(0), pushAcquiredPermitCount.Load())
				require.Equal(t, int64(0), pushRecordedSuccessCount.Load())
				require.Equal(t, int64(0), pushRecordedFailureCount.Load())
				require.Equal(t, expectedReadVal, readAcquiredPermitCount.Load())
				require.Equal(t, int64(0), readRecordedSuccessCount.Load())
				require.Equal(t, int64(0), readRecordedFailureCount.Load())
				finish(0, err)
				require.Equal(t, int64(0), pushAcquiredPermitCount.Load())
				require.Equal(t, int64(0), pushRecordedSuccessCount.Load())
				require.Equal(t, int64(0), pushRecordedFailureCount.Load())
				require.Equal(t, int64(0), readAcquiredPermitCount.Load())
				require.Equal(t, expectedReadVal, readRecordedSuccessCount.Load())
				require.Equal(t, int64(0), readRecordedFailureCount.Load())
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			}
		})
	}
}
