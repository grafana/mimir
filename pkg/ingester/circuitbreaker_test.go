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
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIsFailure(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		require.False(t, isFailure(nil))
	})

	t.Run("context cancelled", func(t *testing.T) {
		require.False(t, isFailure(context.Canceled))
		require.False(t, isFailure(fmt.Errorf("%w", context.Canceled)))
	})

	t.Run("gRPC context cancelled", func(t *testing.T) {
		err := status.Error(codes.Canceled, "cancelled!")
		require.False(t, isFailure(err))
		require.False(t, isFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC deadline exceeded", func(t *testing.T) {
		err := status.Error(codes.DeadlineExceeded, "broken!")
		require.True(t, isFailure(err))
		require.True(t, isFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable with INSTANCE_LIMIT details", func(t *testing.T) {
		err := newInstanceLimitReachedError("broken")
		require.True(t, isFailure(err))
		require.True(t, isFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable with SERVICE_UNAVAILABLE details is not a failure", func(t *testing.T) {
		stat := status.New(codes.Unavailable, "broken!")
		stat, err := stat.WithDetails(&mimirpb.ErrorDetails{Cause: mimirpb.SERVICE_UNAVAILABLE})
		require.NoError(t, err)
		err = stat.Err()
		require.False(t, isFailure(err))
		require.False(t, isFailure(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable without details is not a failure", func(t *testing.T) {
		err := status.Error(codes.Unavailable, "broken!")
		require.False(t, isFailure(err))
		require.False(t, isFailure(fmt.Errorf("%w", err)))
	})
}

func TestCircuitBreaker_IsActive(t *testing.T) {
	var cb *circuitBreaker

	require.False(t, cb.isActive())

	registry := prometheus.NewRegistry()
	cfg := CircuitBreakerConfig{Enabled: true, InitialDelay: 10 * time.Millisecond}
	cb = newCircuitBreaker(cfg, false, log.NewNopLogger(), registry)
	time.AfterFunc(cfg.InitialDelay, func() {
		cb.setActive()
	})
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
			initialDelay:                1 * time.Minute,
			circuitBreakerSetup:         func(cb *circuitBreaker) { cb.cb.Close() },
			expectedSuccess:             false,
			expectedCircuitBreakerError: false,
		},
		"if circuit breaker closed, status true and no error are returned": {
			circuitBreakerSetup:         func(cb *circuitBreaker) { cb.cb.Close() },
			expectedSuccess:             true,
			expectedCircuitBreakerError: false,
		},
		"if circuit breaker open, status false and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup:         func(cb *circuitBreaker) { cb.cb.Open() },
			expectedSuccess:             false,
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
				cortex_ingester_circuit_breaker_transitions_total{state="closed"} 0
				cortex_ingester_circuit_breaker_transitions_total{state="half-open"} 0
				cortex_ingester_circuit_breaker_transitions_total{state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
				cortex_ingester_circuit_breaker_current_state{state="open"} 1
				cortex_ingester_circuit_breaker_current_state{state="half-open"} 0
				cortex_ingester_circuit_breaker_current_state{state="closed"} 0
			`,
		},
		"if circuit breaker half-open, status false and a circuitBreakerErrorOpen are returned": {
			circuitBreakerSetup:         func(cb *circuitBreaker) { cb.cb.HalfOpen() },
			expectedSuccess:             false,
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
				cortex_ingester_circuit_breaker_transitions_total{state="closed"} 0
				cortex_ingester_circuit_breaker_transitions_total{state="half-open"} 1
				cortex_ingester_circuit_breaker_transitions_total{state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
				cortex_ingester_circuit_breaker_current_state{state="open"} 0
				cortex_ingester_circuit_breaker_current_state{state="half-open"} 1
				cortex_ingester_circuit_breaker_current_state{state="closed"} 0
			`,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cfg := CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second, InitialDelay: testCase.initialDelay}
			cb := newCircuitBreaker(cfg, cfg.InitialDelay == 0, log.NewNopLogger(), registry)
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
		err             error
		expectedMetrics string
	}{
		"successful execution records a success": {
			err: nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 1
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"erroneous execution not passing the failure check records a success": {
			err: context.Canceled,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 1
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"erroneous execution passing the failure check records an error": {
			err: context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 1
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
	}
	cfg := CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cb := newCircuitBreaker(cfg, true, log.NewNopLogger(), registry)
			cb.recordResult(testCase.err)
			assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
		})
	}
}

func TestCircuitBreaker_FinishPushRequest(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
	}
	testCases := map[string]struct {
		pushRequestDuration time.Duration
		initialDelay        time.Duration
		err                 error
		expectedErr         error
		expectedMetrics     string
	}{
		"with a permit acquired, pushRequestDuration lower than PushTimeout and no input error, finishPushRequest gives success": {
			pushRequestDuration: 1 * time.Second,
			err:                 nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 1
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with circuit breaker not active, pushRequestDuration lower than PushTimeout and no input error, finishPushRequest does nothing": {
			pushRequestDuration: 1 * time.Second,
			initialDelay:        1 * time.Minute,
			err:                 nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with circuit breaker active, pushRequestDuration higher than PushTimeout and no input error, finishPushRequest gives context deadline exceeded error": {
			pushRequestDuration: 3 * time.Second,
			err:                 nil,
			expectedErr:         context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 1
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with circuit breaker not active, pushRequestDuration higher than PushTimeout and no input error, finishPushRequest does nothing": {
			pushRequestDuration: 3 * time.Second,
			initialDelay:        1 * time.Minute,
			err:                 nil,
			expectedErr:         nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with circuit breaker active, pushRequestDuration higher than PushTimeout and an input error different from context deadline exceeded, finishPushRequest gives context deadline exceeded error": {
			pushRequestDuration: 3 * time.Second,
			err:                 newInstanceLimitReachedError("error"),
			expectedErr:         context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 1
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with circuit breaker not active, pushRequestDuration higher than PushTimeout and an input error different from context deadline exceeded, finishPushRequest does nothing": {
			pushRequestDuration: 3 * time.Second,
			initialDelay:        1 * time.Minute,
			err:                 newInstanceLimitReachedError("error"),
			expectedErr:         nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cfg := CircuitBreakerConfig{
				Enabled:      true,
				InitialDelay: testCase.initialDelay,
				PushTimeout:  2 * time.Second,
			}
			cb := newCircuitBreaker(cfg, cfg.InitialDelay == 0, log.NewNopLogger(), registry)
			err := cb.finishPushRequest(testCase.pushRequestDuration, testCase.err)
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
				cfg.CircuitBreakerConfig = CircuitBreakerConfig{
					Enabled:                    true,
					FailureThresholdPercentage: uint(failureThreshold),
					CooldownPeriod:             10 * time.Second,
					InitialDelay:               initialDelay,
					PushTimeout:                pushTimeout,
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
						i.circuitBreaker.testRequestDelay = testCase.pushRequestDelay
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
						cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
						cortex_ingester_circuit_breaker_results_total{result="error"} 0
						cortex_ingester_circuit_breaker_results_total{result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{state="open"} 0
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
						cortex_ingester_circuit_breaker_current_state{state="open"} 0
						cortex_ingester_circuit_breaker_current_state{state="half-open"} 0
						cortex_ingester_circuit_breaker_current_state{state="closed"} 1
    				`
				} else {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
						# TYPE cortex_ingester_circuit_breaker_results_total counter
						cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 2
						cortex_ingester_circuit_breaker_results_total{result="error"} 2
						cortex_ingester_circuit_breaker_results_total{result="success"} 1
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{state="open"} 1
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
						cortex_ingester_circuit_breaker_current_state{state="open"} 1
						cortex_ingester_circuit_breaker_current_state{state="half-open"} 0
						cortex_ingester_circuit_breaker_current_state{state="closed"} 0
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
	cfg.CircuitBreakerConfig = CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second}

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
	i.circuitBreaker.cb.Close()
	_, err = i.StartPushRequest(ctx, 0)
	require.NoError(t, err)

	// If i's circuit breaker is open, StartPushRequest returns a circuitBreakerOpenError.
	i.circuitBreaker.cb.Open()
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
		cortex_ingester_circuit_breaker_results_total{result="success"} 0
		cortex_ingester_circuit_breaker_results_total{result="error"} 0
		cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 1
		# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
		# TYPE cortex_ingester_circuit_breaker_transitions_total counter
		cortex_ingester_circuit_breaker_transitions_total{state="closed"} 0
		cortex_ingester_circuit_breaker_transitions_total{state="half-open"} 0
		cortex_ingester_circuit_breaker_transitions_total{state="open"} 1
		# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
		# TYPE cortex_ingester_circuit_breaker_current_state gauge
		cortex_ingester_circuit_breaker_current_state{state="open"} 1
		cortex_ingester_circuit_breaker_current_state{state="half-open"} 0
		cortex_ingester_circuit_breaker_current_state{state="closed"} 0
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
		"with a permit acquired, pushRequestDuration lower than PushTimeout and no input err, finishPushRequest gives success": {
			pushRequestDuration:          1 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 1
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"when a permit not acquired, pushRequestDuration lower than PushTimeout and no input err, finishPusRequest does nothing": {
			pushRequestDuration:          1 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with a permit acquired, pushRequestDuration higher than PushTimeout and no input error, finishPushRequest gives context deadline exceeded error": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 1
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with a permit not acquired, pushRequestDuration higher than PushTimeout and no input error, finishPushRequest does nothing": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with a permit acquired, pushRequestDuration higher than PushTimeout and an input error different from context deadline exceeded, finishPushRequest gives context deadline exceeded error": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: true,
			err:                          newInstanceLimitReachedError("error"),
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 1
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
		"with a permit not acquired, pushRequestDuration higher than PushTimeout and an input error different from context deadline exceeded, finishPushRequest does nothing": {
			pushRequestDuration:          3 * time.Second,
			acquiredCircuitBreakerPermit: false,
			err:                          newInstanceLimitReachedError("error"),
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{result="success"} 0
				cortex_ingester_circuit_breaker_results_total{result="error"} 0
				cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
			`,
		},
	}
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			reg := prometheus.NewPedanticRegistry()
			cfg := defaultIngesterTestConfig(t)
			cfg.CircuitBreakerConfig = CircuitBreakerConfig{
				Enabled:     true,
				PushTimeout: 2 * time.Second,
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
				requestDuration:              testCase.pushRequestDuration,
				acquiredCircuitBreakerPermit: testCase.acquiredCircuitBreakerPermit,
				pushErr:                      testCase.err,
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
			cfg.CircuitBreakerConfig = CircuitBreakerConfig{
				Enabled:                    true,
				FailureThresholdPercentage: uint(failureThreshold),
				CooldownPeriod:             10 * time.Second,
				InitialDelay:               initialDelay,
				PushTimeout:                pushTimeout,
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
					// Configure circuit breaker to delay push requests.
					i.circuitBreaker.testRequestDelay = pushTimeout
					count++

					ctx, err = i.StartPushRequest(ctx, int64(req.Size()))
					if initialDelayEnabled || count <= failureThreshold {
						// If initial delay is enabled we expect no deadline exceeded errors
						// to be registered with the circuit breaker.
						// If initial delay is disabled, and the circuit breaker registered
						// less than failureThreshold deadline exceeded errors, it is still
						// closed.
						require.NoError(t, err)
						require.Equal(t, circuitbreaker.ClosedState, i.circuitBreaker.cb.State())
						st, ok := ctx.Value(pushReqCtxKey).(*pushRequestState)
						require.True(t, ok)
						require.Equal(t, int64(req.Size()), st.requestSize)
						_, err = i.Push(ctx, req)
						require.NoError(t, err)
						i.FinishPushRequest(ctx)
					} else {
						require.Equal(t, circuitbreaker.OpenState, i.circuitBreaker.cb.State())
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
						cortex_ingester_circuit_breaker_results_total{result="success"} 0
						cortex_ingester_circuit_breaker_results_total{result="error"} 0
						cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{state="open"} 0
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
						cortex_ingester_circuit_breaker_current_state{state="open"} 0
						cortex_ingester_circuit_breaker_current_state{state="half-open"} 0
						cortex_ingester_circuit_breaker_current_state{state="closed"} 1
    				`
			} else {
				expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
						# TYPE cortex_ingester_circuit_breaker_results_total counter
						cortex_ingester_circuit_breaker_results_total{result="success"} 0
						cortex_ingester_circuit_breaker_results_total{result="error"} 2
						cortex_ingester_circuit_breaker_results_total{result="circuit_breaker_open"} 2
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{state="open"} 1
						# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
        	            # TYPE cortex_ingester_circuit_breaker_current_state gauge
						cortex_ingester_circuit_breaker_current_state{state="open"} 1
						cortex_ingester_circuit_breaker_current_state{state="half-open"} 0
						cortex_ingester_circuit_breaker_current_state{state="closed"} 0
    				`
			}
			assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
		})
	}
}
