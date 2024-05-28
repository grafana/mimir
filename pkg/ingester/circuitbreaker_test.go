// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/gogo/status"
	"github.com/grafana/dskit/middleware"
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
	cb = newCircuitBreaker(cfg, "ingester", log.NewNopLogger(), registry)
	// When InitialDelay is set, circuit breaker is not immediately active.
	require.False(t, cb.isActive())

	// After InitialDelay passed, circuit breaker becomes active.
	time.Sleep(10 * time.Millisecond)
	require.True(t, cb.isActive())
}

func TestCircuitBreaker_TryAcquirePermit(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
		"cortex_ingester_circuit_breaker_transitions_total",
		"cortex_ingester_circuit_breaker_current_state",
	}
	cfg := CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second}
	testCases := map[string]struct {
		circuitBreakerSetup         func(*circuitBreaker)
		expectedCircuitBreakerError bool
		expectedMetrics             string
	}{
		"if circuit breaker closed, no error returned": {
			circuitBreakerSetup:         func(cb *circuitBreaker) { cb.Close() },
			expectedCircuitBreakerError: false,
		},
		"if circuit breaker open, a circuitBreakerErrorOpen is returned": {
			circuitBreakerSetup:         func(cb *circuitBreaker) { cb.Open() },
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{ingester="ingester",result="circuit_breaker_open"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
				cortex_ingester_circuit_breaker_transitions_total{ingester="ingester",state="closed"} 0
				cortex_ingester_circuit_breaker_transitions_total{ingester="ingester",state="half-open"} 0
				cortex_ingester_circuit_breaker_transitions_total{ingester="ingester",state="open"} 1
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
				cortex_ingester_circuit_breaker_current_state{state="open"} 1
				cortex_ingester_circuit_breaker_current_state{state="half-open"} 0
				cortex_ingester_circuit_breaker_current_state{state="closed"} 0
			`,
		},
		"if circuit breaker half-open, a circuitBreakerErrorOpen is returned": {
			circuitBreakerSetup:         func(cb *circuitBreaker) { cb.HalfOpen() },
			expectedCircuitBreakerError: true,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{ingester="ingester",result="circuit_breaker_open"} 1
				# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
				# TYPE cortex_ingester_circuit_breaker_transitions_total counter
				cortex_ingester_circuit_breaker_transitions_total{ingester="ingester",state="closed"} 0
				cortex_ingester_circuit_breaker_transitions_total{ingester="ingester",state="half-open"} 1
				cortex_ingester_circuit_breaker_transitions_total{ingester="ingester",state="open"} 0
				# HELP cortex_ingester_circuit_breaker_current_state Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.
				# TYPE cortex_ingester_circuit_breaker_current_state gauge
				cortex_ingester_circuit_breaker_current_state{state="open"} 0
				cortex_ingester_circuit_breaker_current_state{state="half-open"} 1
				cortex_ingester_circuit_breaker_current_state{state="closed"} 0
			`,
		},
	}
	ctx := context.Background()
	for testName, testCase := range testCases {
		t.Run(testName, func(t *testing.T) {
			registry := prometheus.NewRegistry()
			cb := newCircuitBreaker(cfg, "ingester", log.NewNopLogger(), registry)
			testCase.circuitBreakerSetup(cb)
			err := cb.tryAcquirePermit()
			if testCase.expectedCircuitBreakerError {
				checkCircuitBreakerOpenErr(ctx, err, t)
				assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestCircuitBreaker_RecordSuccess(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
	}
	cfg := CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second}
	cb := newCircuitBreaker(cfg, "ingester", log.NewNopLogger(), registry)
	cb.recordSuccess()
	expectedMetrics := `
		# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
		# TYPE cortex_ingester_circuit_breaker_results_total counter
		cortex_ingester_circuit_breaker_results_total{ingester="ingester",result="success"} 1
	`
	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func TestCircuitBreaker_RecordError(t *testing.T) {
	registry := prometheus.NewRegistry()
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
	}
	cfg := CircuitBreakerConfig{Enabled: true, CooldownPeriod: 10 * time.Second}
	cb := newCircuitBreaker(cfg, "ingester", log.NewNopLogger(), registry)
	cb.recordError(context.DeadlineExceeded)
	expectedMetrics := `
		# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
		# TYPE cortex_ingester_circuit_breaker_results_total counter
		cortex_ingester_circuit_breaker_results_total{ingester="ingester",result="error"} 1
	`
	assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), metricNames...))
}

func TestCircuitBreaker_FinishPushRequest(t *testing.T) {
	metricNames := []string{
		"cortex_ingester_circuit_breaker_results_total",
	}
	cfg := CircuitBreakerConfig{
		Enabled:     true,
		PushTimeout: 2 * time.Second,
	}
	testCases := map[string]struct {
		delay           time.Duration
		err             error
		expectedErr     error
		expectedMetrics string
	}{
		"delay lower than PushTimeout and no input err give success": {
			delay: 1 * time.Second,
			err:   nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{ingester="ingester",result="success"} 1
			`,
		},
		"delay higher than PushTimeout and no input error give context deadline exceeded error": {
			delay:       3 * time.Second,
			err:         nil,
			expectedErr: context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{ingester="ingester",result="error"} 1
			`,
		},
		"delay higher than PushTimeout and an input error different from context deadline exceeded give context deadline exceeded error": {
			delay:       3 * time.Second,
			err:         newInstanceLimitReachedError("error"),
			expectedErr: context.DeadlineExceeded,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{ingester="ingester",result="error"} 1
			`,
		},
	}
	for testName, testCase := range testCases {
		registry := prometheus.NewRegistry()
		ctx := context.Background()
		cb := newCircuitBreaker(cfg, "ingester", log.NewNopLogger(), registry)
		t.Run(testName, func(t *testing.T) {
			err := cb.finishPushRequest(ctx, testCase.delay, testCase.err)
			if testCase.expectedErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, testCase.expectedErr)
			}
			assert.NoError(t, testutil.GatherAndCompare(registry, strings.NewReader(testCase.expectedMetrics), metricNames...))
		})
	}
}

func TestIngester_Push_CircuitBreaker(t *testing.T) {
	pushTimeout := 100 * time.Millisecond
	tests := map[string]struct {
		expectedErrorWhenCircuitBreakerClosed error
		ctx                                   func(context.Context) context.Context
		limits                                InstanceLimits
	}{
		"deadline exceeded": {
			expectedErrorWhenCircuitBreakerClosed: nil,
			limits:                                InstanceLimits{MaxInMemoryTenants: 3},
			ctx: func(ctx context.Context) context.Context {
				return context.WithValue(ctx, testDelayKey, 2*pushTimeout)
			},
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
					Enabled:          true,
					FailureThreshold: uint(failureThreshold),
					CooldownPeriod:   10 * time.Second,
					InitialDelay:     initialDelay,
					PushTimeout:      pushTimeout,
					testModeEnabled:  true,
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
						if testCase.ctx != nil {
							ctx = testCase.ctx(ctx)
						}
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
								checkCircuitBreakerOpenErr(ctx, err, t)
							}
						}
					}
				}

				// Check tracked Prometheus metrics
				var expectedMetrics string
				if initialDelayEnabled {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{ingester="ingester-zone-a-0",state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{ingester="ingester-zone-a-0",state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{ingester="ingester-zone-a-0",state="open"} 0
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
						cortex_ingester_circuit_breaker_results_total{ingester="ingester-zone-a-0",result="circuit_breaker_open"} 2
						cortex_ingester_circuit_breaker_results_total{ingester="ingester-zone-a-0",result="error"} 2
						cortex_ingester_circuit_breaker_results_total{ingester="ingester-zone-a-0",result="success"} 1
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{ingester="ingester-zone-a-0",state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{ingester="ingester-zone-a-0",state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{ingester="ingester-zone-a-0",state="open"} 1
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

func checkCircuitBreakerOpenErr(ctx context.Context, err error, t *testing.T) {
	var cbOpenErr circuitBreakerOpenError
	require.ErrorAs(t, err, &cbOpenErr)

	var optional middleware.OptionalLogging
	require.ErrorAs(t, err, &optional)

	shouldLog, _ := optional.ShouldLog(ctx)
	require.False(t, shouldLog, "expected not to log via .ShouldLog()")
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
	i.circuitBreaker.Close()
	_, err = i.StartPushRequest(ctx, 0)
	require.NoError(t, err)

	// If i's circuit breaker is open, StartPushRequest returns a circuitBreakerOpenError.
	i.circuitBreaker.Open()
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
		cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="circuit_breaker_open"} 1
		# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state.
		# TYPE cortex_ingester_circuit_breaker_transitions_total counter
		cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
		cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
		cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 1
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
		delay           time.Duration
		err             error
		expectedMetrics string
	}{
		"delay lower than PushTimeout and no input err give success": {
			delay: 1 * time.Second,
			err:   nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="success"} 1
			`,
		},
		"delay higher than PushTimeout and no input error give context deadline exceeded error": {
			delay: 3 * time.Second,
			err:   nil,
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="error"} 1
			`,
		},
		"delay higher than PushTimeout and an input error different from context deadline exceeded give context deadline exceeded error": {
			delay: 3 * time.Second,
			err:   newInstanceLimitReachedError("error"),
			expectedMetrics: `
				# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker.
				# TYPE cortex_ingester_circuit_breaker_results_total counter
				cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="error"} 1
			`,
		},
	}
	for testName, testCase := range testCases {
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
			requestDuration: testCase.delay,
			err:             testCase.err,
		}
		ctx = context.WithValue(ctx, pushReqCtxKey, st)

		t.Run(testName, func(t *testing.T) {
			i.FinishPushRequest(ctx)
			assert.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(testCase.expectedMetrics), metricNames...))
		})
	}
}
