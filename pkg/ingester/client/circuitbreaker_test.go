// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsSuccessful(t *testing.T) {
	t.Run("no error", func(t *testing.T) {
		require.True(t, isSuccessful(nil))
	})

	t.Run("context cancelled", func(t *testing.T) {
		require.True(t, isSuccessful(context.Canceled))
		require.True(t, isSuccessful(fmt.Errorf("%w", context.Canceled)))
	})

	t.Run("gRPC context cancelled", func(t *testing.T) {
		err := status.Error(codes.Canceled, "cancelled!")
		require.True(t, isSuccessful(err))
		require.True(t, isSuccessful(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC deadline exceeded", func(t *testing.T) {
		err := status.Error(codes.DeadlineExceeded, "broken!")
		require.False(t, isSuccessful(err))
		require.False(t, isSuccessful(fmt.Errorf("%w", err)))
	})

	t.Run("gRPC unavailable", func(t *testing.T) {
		err := status.Error(codes.Unavailable, "broken!")
		require.False(t, isSuccessful(err))
		require.False(t, isSuccessful(fmt.Errorf("%w", err)))
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
		Enabled:                true,
		MaxHalfOpenRequests:    1,
		MaxConsecutiveFailures: 1,
		OpenTimeout:            60 * time.Second,
		ClosedInterval:         60 * time.Second,
	}, NewMetrics(reg))

	// Initial request that should succeed because the circuit breaker is "closed"
	err := breaker(context.Background(), "methodName", "", "", &conn, success)
	require.NoError(t, err)

	// Failed request that should put the circuit breaker into "open"
	err = breaker(context.Background(), "methodName", "", "", &conn, failure)
	s, ok := status.FromError(err)
	require.True(t, ok, "expected to get gRPC status from error")
	require.Equal(t, codes.Unavailable, s.Code())

	// Subsequent requests should fail with this specific error once "open"
	err = breaker(context.Background(), "methodName", "", "", &conn, success)
	require.ErrorIs(t, err, gobreaker.ErrOpenState)

	// Make sure the metrics match the behavior
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP cortex_ingester_client_circuit_breaker_results_total Results of executing requests via the circuit breaker
# TYPE cortex_ingester_client_circuit_breaker_results_total counter
cortex_ingester_client_circuit_breaker_results_total{result="circuit_breaker_open"} 1
cortex_ingester_client_circuit_breaker_results_total{result="error"} 1
cortex_ingester_client_circuit_breaker_results_total{result="success"} 1
`), "cortex_ingester_client_circuit_breaker_results_total"))
}
