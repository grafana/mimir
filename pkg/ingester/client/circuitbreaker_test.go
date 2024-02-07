// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/gogo/status"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/ring"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
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

	t.Run("gRPC unavailable with INSTANCE_LIMIT details", func(t *testing.T) {
		err := perInstanceLimitError(t)
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

func perInstanceLimitError(t *testing.T) error {
	stat := status.New(codes.Unavailable, "broken!")
	stat, err := stat.WithDetails(&mimirpb.ErrorDetails{Cause: mimirpb.INSTANCE_LIMIT})
	require.NoError(t, err)
	return stat.Err()
}

func TestNewCircuitBreaker(t *testing.T) {
	// gRPC invoker that does not return an error
	success := func(currentCtx context.Context, currentMethod string, currentReq, currentRepl interface{}, currentConn *grpc.ClientConn, currentOpts ...grpc.CallOption) error {
		return nil
	}

	// gRPC invoker that returns an error that will be treated as an error by the circuit breaker
	failure := func(currentCtx context.Context, currentMethod string, currentReq, currentRepl interface{}, currentConn *grpc.ClientConn, currentOpts ...grpc.CallOption) error {
		return perInstanceLimitError(t)
	}

	conn := grpc.ClientConn{}
	reg := prometheus.NewPedanticRegistry()
	inst := ring.InstanceDesc{Id: "test", Addr: "localhost:8080"}
	coolDown := 10 * time.Second
	breaker := NewCircuitBreaker(inst, CircuitBreakerConfig{
		Enabled:                   true,
		FailureThreshold:          1,
		FailureExecutionThreshold: 1,
		ThresholdingPeriod:        60 * time.Second,
		CooldownPeriod:            coolDown,
	}, NewMetrics(reg), test.NewTestingLogger(t))

	// Initial request that should succeed because the circuit breaker is "closed"
	err := breaker(context.Background(), "/cortex.Ingester/Push", "", "", &conn, success)
	require.NoError(t, err)

	// Failed request that should put the circuit breaker into "open"
	err = breaker(context.Background(), "/cortex.Ingester/Push", "", "", &conn, failure)
	s, ok := grpcutil.ErrorToStatus(err)
	require.True(t, ok, "expected to get gRPC status from error")
	require.Equal(t, codes.Unavailable, s.Code())

	// Subsequent requests should fail with this specific error once "open"
	err = breaker(context.Background(), "/cortex.Ingester/Push", "", "", &conn, success)
	require.ErrorIs(t, err, circuitbreaker.ErrOpen)
	var errCBOpen1 ErrCircuitBreakerOpen
	require.ErrorAs(t, err, &errCBOpen1)
	require.Less(t, errCBOpen1.RemainingDelay(), coolDown)

	err = breaker(context.Background(), "/cortex.Ingester/Push", "", "", &conn, success)
	require.ErrorIs(t, err, circuitbreaker.ErrOpen)
	var errCBOpen2 ErrCircuitBreakerOpen
	require.ErrorAs(t, err, &errCBOpen2)
	require.Less(t, errCBOpen2.RemainingDelay(), errCBOpen1.RemainingDelay())

	// Non-ingester methods shouldn't be short-circuited
	err = breaker(context.Background(), "Different.Method", "", "", &conn, success)
	require.NoError(t, err)

	// Make sure the metrics match the behavior
	require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
# HELP cortex_ingester_client_circuit_breaker_results_total Results of executing requests via the circuit breaker
# TYPE cortex_ingester_client_circuit_breaker_results_total counter
cortex_ingester_client_circuit_breaker_results_total{ingester="test",result="circuit_breaker_open"} 2
cortex_ingester_client_circuit_breaker_results_total{ingester="test",result="error"} 1
cortex_ingester_client_circuit_breaker_results_total{ingester="test",result="success"} 1
`), "cortex_ingester_client_circuit_breaker_results_total"))
}
