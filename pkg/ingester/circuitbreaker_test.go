// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
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

func TestIsFailure(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		require.False(t, isCircuitBreakerFailure(nil))
	})

	t.Run("context cancelled", func(t *testing.T) {
		require.False(t, isCircuitBreakerFailure(context.Canceled))
		require.False(t, isCircuitBreakerFailure(fmt.Errorf("%w", context.Canceled)))
	})

	t.Run("gRPC context cancelled", func(t *testing.T) {
		err := status.Error(codes.Canceled, "cancelled!")
		require.False(t, isCircuitBreakerFailure(err))
		require.False(t, isCircuitBreakerFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC deadline exceeded", func(t *testing.T) {
		err := status.Error(codes.DeadlineExceeded, "broken!")
		require.True(t, isCircuitBreakerFailure(err))
		require.True(t, isCircuitBreakerFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable with INSTANCE_LIMIT details", func(t *testing.T) {
		err := newInstanceLimitReachedError("broken")
		require.True(t, isCircuitBreakerFailure(err))
		require.True(t, isCircuitBreakerFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable with SERVICE_UNAVAILABLE details is not a failure", func(t *testing.T) {
		stat := status.New(codes.Unavailable, "broken!")
		stat, err := stat.WithDetails(&mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE})
		require.NoError(t, err)
		err = stat.Err()
		require.False(t, isCircuitBreakerFailure(err))
		require.False(t, isCircuitBreakerFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable without details is not a failure", func(t *testing.T) {
		err := status.Error(codes.Unavailable, "broken!")
		require.False(t, isCircuitBreakerFailure(err))
		require.False(t, isCircuitBreakerFailure(fmt.Errorf("%w", err)))
	})
}

func TestCircuitBreaker_IsActive(t *testing.T) {
	var cb *circuitBreaker

	require.False(t, cb.isActive())

	cfg := CircuitBreakerConfig{Enabled: true, InitialDelay: 10 * time.Millisecond}
	cb = createCircuitBreaker(cfg, prometheus.NewRegistry(), log.NewNopLogger())
	cb.activate()

	// When InitialDelay is set, circuit breaker is not immediately active.
	require.False(t, cb.isActive())

	// After InitialDelay passed, circuit breaker becomes active.
	require.Eventually(t, func() bool {
		return cb.isActive()
	}, time.Second, 10*time.Millisecond)
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
		expectedSuccess             bool
		expectedCircuitBreakerError bool
		expectedMetrics             string
	}{
		"if circuit breaker is not active, status false and no error are returned": {
			initialDelay: 1 * time.Minute,
			circuitBreakerSetup: func(cb *circuitBreaker) {
				cb.active.Store(false)
			},
			expectedSuccess:             false,
			expectedCircuitBreakerError: false,
		},
		"if circuit breaker closed, status true and no error are returned": {
			circuitBreakerSetup: func(cb *circuitBreaker) {
				cb.activate()
				cb.cb.Close()
			},
			expectedSuccess:             true,
			expectedCircuitBreakerError: false,
		},
		"if circuit breaker open, status false and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup: func(cb *circuitBreaker) {
				cb.activate()
				cb.cb.Open()
			},
			expectedSuccess:             false,
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
        	    # HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="test-path",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="test-path",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="test-path",state="open"} 1
        	    # HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="test-path",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="test-path",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="test-path",state="open"} 1
			`,
		},
		"if circuit breaker half-open, status true and no error are returned": {
			circuitBreakerSetup: func(cb *circuitBreaker) {
				cb.activate()
				cb.cb.HalfOpen()
			},
			expectedSuccess:             true,
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
			cb := createCircuitBreaker(cfg, registry, log.NewNopLogger())
			testCase.circuitBreakerSetup(cb)
			status, err := cb.tryAcquirePermit()
			require.Equal(t, testCase.expectedSuccess, status)
			if testCase.expectedCircuitBreakerError {
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCircuitBreaker_RecordResult(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
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
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 1
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
			`,
		},
		"erroneous execution not passing the failure check records a success": {
			errs:        []error{context.Canceled},
			expectedErr: nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 1
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
			`,
		},
		"erroneous execution passing the failure check records an error": {
			errs:        []error{context.DeadlineExceeded},
			expectedErr: context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
			`,
		},
		"erroneous execution with multiple errors records the first error passing the failure check": {
			errs:        []error{context.Canceled, context.DeadlineExceeded},
			expectedErr: context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
			`,
		},
	}
	cfg := CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cb := createCircuitBreaker(cfg, registry, log.NewNopLogger())
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
	}
	instanceLimitReachedErr := newInstanceLimitReachedError("error")
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
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 1
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
			`,
		},
		"with circuit breaker not active, requestDuration lower than maxRequestDuration and no input error, finishRequest does nothing": {
			requestDuration: 1 * time.Second,
			isActive:        false,
			err:             nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
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
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
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
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
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
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
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
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
			`,
		},
		"with circuit breaker active, requestDuration higher than maxRequestDuration and an input error relevant for circuit breakers, finishRequest gives the input error": {
			requestDuration: 3 * time.Second,
			isActive:        true,
			err:             instanceLimitReachedErr,
			expectedErr:     instanceLimitReachedErr,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
			`,
		},
		"with circuit breaker active, requestDuration higher than maxRequestDuration and an input error irrelevant for circuit breakers, finishRequest gives context deadline exceeded error": {
			requestDuration: 3 * time.Second,
			isActive:        true,
			err:             context.Canceled,
			expectedErr:     context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="success"} 0
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="error"} 1
				cortex_ingester_circuit_breaker_results_total{path="test-path",result="circuit_breaker_open"} 0
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
			cb := createCircuitBreaker(cfg, registry, log.NewNopLogger())
			cb.active.Store(testCase.isActive)
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

func TestIngester_PushToStorage_CircuitBreaker(t *testing.T) {
	pushTimeout := 100 * time.Millisecond
	tests := map[string]struct {
		expectedErrorWhenCircuitBreakerClosed error
		pushRequestDelay                      time.Duration
		limits                                InstanceLimits
	}{
		"deadline exceeded": {
			expectedErrorWhenCircuitBreakerClosed: nil,
			limits:                                InstanceLimits{MaxInMemoryTenants: 3},
			pushRequestDelay:                      pushTimeout,
		},
		"instance limit hit": {
			expectedErrorWhenCircuitBreakerClosed: instanceLimitReachedError{},
			limits:                                InstanceLimits{MaxInMemoryTenants: 1},
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
					initialDelay = 200 * time.Millisecond
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
						i.circuitBreaker.pushCircuitBreaker.testRequestDelay = testCase.pushRequestDelay
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
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
    				`
				} else {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
						# TYPE cortex_ingester_circuit_breaker_results_total counter
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 2
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 2
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 1
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 1
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
    				`
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
	i.circuitBreaker.pushCircuitBreaker.cb.Close()
	_, err = i.StartPushRequest(ctx, 0)
	require.NoError(t, err)

	// If i's circuit breaker is open, StartPushRequest returns a circuitBreakerOpenError.
	i.circuitBreaker.pushCircuitBreaker.cb.Open()
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
        cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 1
        cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
		# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
		# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 1
        cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
		# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        # TYPE cortex_ingester_circuit_breaker_current_state gauge
        cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 1
        cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
	`
	assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(expectedMetrics), metricNames...))
}

func TestIngester_FinishPushRequest(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
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
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
			`,
		},
		"when a permit not acquired, pushRequestDuration lower than RequestTimeout and no input err, FinishPushRequest does nothing": {
			pushRequestDuration:          1 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
			`,
		},
		"with a permit acquired, pushRequestDuration higher than RequestTimeout and no input error, FinishPushRequest records a failure": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
			`,
		},
		"with a permit not acquired, pushRequestDuration higher than RequestTimeout and no input error, FinishPushRequest does nothing": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
			`,
		},
		"with a permit acquired, pushRequestDuration higher than RequestTimeout and an input error relevant for the circuit breakers, FinishPushRequest records a failure": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          newInstanceLimitReachedError("error"),
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
			`,
		},
		"with a permit acquired, pushRequestDuration higher than RequestTimeout and an input error irrelevant for the circuit breakers, FinishPushRequest records a failure": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          context.Canceled,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
			`,
		},
		"with a permit not acquired, pushRequestDuration higher than RequestTimeout and an input error relevant for the circuit breakers, FinishPushRequest does nothing": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          newInstanceLimitReachedError("error"),
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
			`,
		},
		"with a permit not acquired, pushRequestDuration higher than RequestTimeout and an input error irrelevant for the circuit breakers, FinishPushRequest does nothing": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          context.Canceled,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
				cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
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
						_ = i.circuitBreaker.pushCircuitBreaker.finishRequest(duration, cfg.PushCircuitBreaker.RequestTimeout, err)
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
			}

			registry := prometheus.NewRegistry()

			// Create a mocked ingester
			cfg := defaultIngesterTestConfig(t)
			cfg.ActiveSeriesMetrics.IdleTimeout = 100 * time.Millisecond
			failureThreshold := 2
			var initialDelay time.Duration
			if initialDelayEnabled {
				initialDelay = 200 * time.Millisecond
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
					i.circuitBreaker.pushCircuitBreaker.testRequestDelay = pushTimeout
					count++

					ctx, err = i.StartPushRequest(ctx, int64(req.Size()))
					if initialDelayEnabled || count <= failureThreshold {
						// If initial delay is enabled we expect no deadline exceeded errors
						// to be registered with the circuit breaker.
						// If initial delay is disabled, and the circuit breaker registered
						// less than failureThreshold deadline exceeded errors, it is still
						// closed.
						require.NoError(t, err)
						require.Equal(t, circuitbreaker.ClosedState, i.circuitBreaker.pushCircuitBreaker.cb.State())
						st, ok := ctx.Value(pushReqCtxKey).(*pushRequestState)
						require.True(t, ok)
						require.Equal(t, int64(req.Size()), st.requestSize)
						_, err = i.Push(ctx, req)
						require.NoError(t, err)
						i.FinishPushRequest(ctx)
					} else {
						require.Equal(t, circuitbreaker.OpenState, i.circuitBreaker.pushCircuitBreaker.cb.State())
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
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
    				`
			} else {
				expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
						# TYPE cortex_ingester_circuit_breaker_results_total counter
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 2
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 2
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	            cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	            cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 1
        	            cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	            cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 1
        	            cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
    				`
			}
			assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
		})
	}
}

func createCircuitBreaker(cfg CircuitBreakerConfig, registerer prometheus.Registerer, logger log.Logger) *circuitBreaker {
	var cb *circuitBreaker
	state := func(path string) circuitbreaker.State {
		return cb.cb.State()
	}
	metrics := newCircuitBreakerMetrics(registerer, state, []string{"test-path"})
	cb = newCircuitBreaker(cfg, metrics, "test-path", logger)
	return cb
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
	prCB := newPRCircuitBreaker(pushCfg, readCfg, log.NewNopLogger(), registerer)
	require.NotNil(t, prCB)
	require.NotNil(t, prCB.pushCircuitBreaker)
	require.NotNil(t, prCB.readCircuitBreaker)

	expectedMetrics := `
		# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
		# TYPE cortex_ingester_circuit_breaker_results_total counter
		cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
		cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
		cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
		cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
		cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
		cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
		# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
		# TYPE cortex_ingester_circuit_breaker_transitions_total counter
		cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
		cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
		cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
		cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
		cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
		cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
		# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
		# TYPE cortex_ingester_circuit_breaker_current_state gauge
		cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
		cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
		cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
		cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
		cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
		cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
	`
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_transitions_total",
		"cortex_ingester_circuit_breaker_current_state",
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
		circuitBreakerSetup         func(breaker *prCircuitBreaker)
		expectedCircuitBreakerError bool
		expectedMetrics             string
	}{
		"if push circuit breaker is not active, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.pushCircuitBreaker.active.Store(false)
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if push circuit breaker is closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Close()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if push circuit breaker is open, no finish function and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
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
			cb := newPRCircuitBreaker(cfg, cfg, log.NewNopLogger(), registry)
			cb.pushCircuitBreaker.cb = &mockedCircuitBreaker{
				CircuitBreaker:      cb.pushCircuitBreaker.cb,
				acquiredPermitCount: acquiredPermitCount,
				recordSuccessCount:  recordedSuccessCount,
				recordFailureCount:  recordedFailureCount,
			}
			testCase.circuitBreakerSetup(cb)
			finish, err := cb.tryPushAcquirePermit()

			if testCase.expectedCircuitBreakerError {
				require.Nil(t, finish)
				require.Error(t, err)
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			} else {
				require.NoError(t, err)
				require.NotNil(t, finish)
				var expectedVal int64
				if cb.pushCircuitBreaker.isActive() {
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
		circuitBreakerSetup         func(breaker *prCircuitBreaker)
		expectedCircuitBreakerError bool
		expectedMetrics             string
	}{
		"if read circuit breaker is not active and push circuit breaker is not active, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.active.Store(false)
				cb.pushCircuitBreaker.active.Store(false)
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is not active and push circuit breaker is closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.active.Store(false)
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Close()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is not active and push circuit breaker is open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.active.Store(false)
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is not active and push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.active.Store(false)
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is closed and push circuit breaker is is not active, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.Close()
				cb.pushCircuitBreaker.active.Store(false)
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is closed and push circuit breaker is closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.Close()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Close()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is closed and push circuit breaker is open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.Close()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is closed and push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.Close()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is open and push circuit breaker is not active, no finish function and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.Open()
				cb.pushCircuitBreaker.active.Store(false)
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 1
			`,
		},
		"if read circuit breaker is open and push circuit breaker is closed, no finish function and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.Open()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Close()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 1
			`,
		},
		"if read circuit breaker is open and push circuit breaker is open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.Open()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 1
			`,
		},
		"if read circuit breaker is open and push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.Open()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.HalfOpen()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 0
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 1
			`,
		},
		"if read circuit breaker is half-open and push circuit breaker is not active, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.HalfOpen()
				cb.pushCircuitBreaker.active.Store(false)
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is half-open and push circuit breaker is closed, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.HalfOpen()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Close()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is half-open and push circuit breaker is open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.HalfOpen()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.Open()
			},
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 1
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
			`,
		},
		"if read circuit breaker is half-open and push circuit breaker is half-open, finish function and no error are returned": {
			circuitBreakerSetup: func(cb *prCircuitBreaker) {
				cb.readCircuitBreaker.activate()
				cb.readCircuitBreaker.cb.HalfOpen()
				cb.pushCircuitBreaker.activate()
				cb.pushCircuitBreaker.cb.HalfOpen()
			},
			expectedCircuitBreakerError: false,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="error"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="circuit_breaker_open"} 0
        	    cortex_ingester_circuit_breaker_results_total{path="write",result="success"} 1
        	    cortex_ingester_circuit_breaker_results_total{path="read",result="success"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="half-open"} 1
        	    cortex_ingester_circuit_breaker_transitions_total{path="write",state="open"} 0
        	    cortex_ingester_circuit_breaker_transitions_total{path="read",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="closed"} 1
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="read",state="half-open"} 0
        	    cortex_ingester_circuit_breaker_current_state{path="write",state="open"} 0
				cortex_ingester_circuit_breaker_current_state{path="read",state="open"} 0
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
			cb := newPRCircuitBreaker(pushCfg, readCfg, log.NewNopLogger(), registry)
			cb.pushCircuitBreaker.cb = &mockedCircuitBreaker{
				CircuitBreaker:      cb.pushCircuitBreaker.cb,
				acquiredPermitCount: pushAcquiredPermitCount,
				recordSuccessCount:  pushRecordedSuccessCount,
				recordFailureCount:  pushRecordedFailureCount,
			}
			cb.readCircuitBreaker.cb = &mockedCircuitBreaker{
				CircuitBreaker:      cb.readCircuitBreaker.cb,
				acquiredPermitCount: readAcquiredPermitCount,
				recordSuccessCount:  readRecordedSuccessCount,
				recordFailureCount:  readRecordedFailureCount,
			}
			testCase.circuitBreakerSetup(cb)
			finish, err := cb.tryReadAcquirePermit()

			if testCase.expectedCircuitBreakerError {
				require.Nil(t, finish)
				require.Error(t, err)
				require.ErrorAs(t, err, &circuitBreakerOpenError{})
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			} else {
				require.NoError(t, err)
				require.NotNil(t, finish)
				var (
					expectedPushVal int64
					expectedReadVal int64
				)
				if cb.pushCircuitBreaker.isActive() {
					expectedPushVal = 1
				}
				if cb.readCircuitBreaker.isActive() {
					expectedReadVal = 1
				}
				require.Equal(t, expectedPushVal, pushAcquiredPermitCount.Load())
				require.Equal(t, int64(0), pushRecordedSuccessCount.Load())
				require.Equal(t, int64(0), pushRecordedFailureCount.Load())
				require.Equal(t, expectedReadVal, readAcquiredPermitCount.Load())
				require.Equal(t, int64(0), readRecordedSuccessCount.Load())
				require.Equal(t, int64(0), readRecordedFailureCount.Load())
				finish(0, err)
				require.Equal(t, int64(0), pushAcquiredPermitCount.Load())
				require.Equal(t, expectedPushVal, pushRecordedSuccessCount.Load())
				require.Equal(t, int64(0), pushRecordedFailureCount.Load())
				require.Equal(t, int64(0), readAcquiredPermitCount.Load())
				require.Equal(t, expectedReadVal, readRecordedSuccessCount.Load())
				require.Equal(t, int64(0), readRecordedFailureCount.Load())
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			}
		})
	}
}
