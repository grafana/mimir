// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/util/test"
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

	t.Run("gRPC unavailable", func(t *testing.T) {
		err := status.Error(codes.Unavailable, "broken!")
		require.True(t, isFailure(err))
		require.True(t, isFailure(fmt.Errorf("%w", err)))
	})
}

func TestNewCircuitBreaker(t *testing.T) {
	// gRPC invoker that does not return an error
	success := func(currentCtx context.Context, currentMethod string, currentReq, currentRepl interface{}, currentConn *grpc.ClientConn, currentOpts ...grpc.CallOption) error {
		return nil
	}

	// gRPC invoker that returns an error that will be treated as an error by the circuit breaker
	failure := func(currentCtx context.Context, currentMethod string, currentReq, currentRepl interface{}, currentConn *grpc.ClientConn, currentOpts ...grpc.CallOption) error {
		return status.Error(codes.Unavailable, "failed")
	}

	conn := grpc.ClientConn{}
	reg := prometheus.NewPedanticRegistry()
	breaker := NewCircuitBreaker("test-1", CircuitBreakerConfig{
		Enabled:                   true,
		FailureThreshold:          1,
		FailureExecutionThreshold: 1,
		CooldownPeriod:            60 * time.Second,
	}, NewMetrics(reg), test.NewTestingLogger(t))

	// Initial request that should succeed because the circuit breaker is "closed"
	err := breaker(context.Background(), "/cortex.Ingester/Push", "", "", &conn, success)
	require.NoError(t, err)

	// Failed request that should put the circuit breaker into "open"
	err = breaker(context.Background(), "/cortex.Ingester/Push", "", "", &conn, failure)
	s, ok := status.FromError(err)
	require.True(t, ok, "expected to get gRPC status from error")
	require.Equal(t, codes.Unavailable, s.Code())

	// Subsequent requests should fail with this specific error once "open"
	err = breaker(context.Background(), "/cortex.Ingester/Push", "", "", &conn, success)
	require.ErrorIs(t, err, circuitbreaker.ErrCircuitBreakerOpen)

	// Non-ingester methods shouldn't be short-circuited
	err = breaker(context.Background(), "Different.Method", "", "", &conn, success)
	require.NoError(t, err)

	// Make sure the metrics match the behavior
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP cortex_ingester_client_circuit_breaker_results_total Results of executing requests via the circuit breaker
# TYPE cortex_ingester_client_circuit_breaker_results_total counter
cortex_ingester_client_circuit_breaker_results_total{result="circuit_breaker_open"} 1
cortex_ingester_client_circuit_breaker_results_total{result="error"} 1
cortex_ingester_client_circuit_breaker_results_total{result="success"} 1
`), "cortex_ingester_client_circuit_breaker_results_total"))
}
