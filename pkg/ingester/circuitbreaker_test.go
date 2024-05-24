// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/grafana/dskit/grpcutil"
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
)

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
				return context.WithValue(ctx, testDelayKey, (2 * pushTimeout).String())
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
				_, err = i.Push(ctx, req)
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
						_, err = i.Push(ctx, req)
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
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker
						# TYPE cortex_ingester_circuit_breaker_results_total counter
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="error"} 0
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="success"} 0
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 0
    				`
				} else {
					expectedMetrics = `
						# HELP cortex_ingester_circuit_breaker_results_total Results of executing requests via the circuit breaker
						# TYPE cortex_ingester_circuit_breaker_results_total counter
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="circuit_breaker_open"} 2
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="error"} 2
						cortex_ingester_circuit_breaker_results_total{ingester="localhost",result="success"} 1
						# HELP cortex_ingester_circuit_breaker_transitions_total Number of times the circuit breaker has entered a state
						# TYPE cortex_ingester_circuit_breaker_transitions_total counter
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="closed"} 0
						cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="half-open"} 0
        				cortex_ingester_circuit_breaker_transitions_total{ingester="localhost",state="open"} 1
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

	s, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "expected to be able to convert to gRPC status")
	require.Equal(t, codes.Unavailable, s.Code())
}
