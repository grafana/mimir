// SPDX-License-Identifier: AGPL-3.0-only

package protection

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/cmd/go-kafka-ingester/internal/metrics"
)

func config() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		FailureThresholdPercentage: 20,
		FailureExecutionThreshold:  10,
		ThresholdingPeriod:         time.Minute,
		CooldownPeriod:             10 * time.Second,
		RequestTimeout:             30 * time.Second,
	}
}

func run(t *testing.T, breaker *CircuitBreaker, at time.Time, failed bool) {
	t.Helper()
	permit, err := breaker.tryAcquireAt(at)
	require.NoError(t, err)
	require.True(t, permit.Active())
	breaker.finishAt(permit, failed, at)
}

func TestCircuitBreakerIsInactiveUntilActivated(t *testing.T) {
	breaker := NewCircuitBreaker(config(), ReadRequestType)
	permit, err := breaker.TryAcquire()
	require.NoError(t, err)
	require.False(t, permit.Active())
	delayedConfig := config()
	delayedConfig.InitialDelay = time.Hour
	delayed := NewCircuitBreaker(delayedConfig, ReadRequestType)
	delayed.Activate()
	permit, err = delayed.TryAcquire()
	require.NoError(t, err)
	require.False(t, permit.Active())
}

func TestPushCircuitBreakerCountsEveryHeadAppendOfABatch(t *testing.T) {
	pushConfig := config()
	pushConfig.RequestTimeout = 0
	breaker := NewCircuitBreaker(pushConfig, PushRequestType)
	breaker.Activate()
	errorsCount := func() float64 {
		return testutil.ToFloat64(metrics.CircuitBreakerResults.WithLabelValues(PushRequestType, "error"))
	}
	before := errorsCount()
	permit, err := breaker.TryAcquire()
	require.NoError(t, err)
	time.Sleep(2 * time.Millisecond)
	// One slow batch of ten flushes reaches the execution threshold on its own.
	breaker.FinishAll(permit, 10)
	require.Equal(t, 10.0, errorsCount()-before)
	require.Equal(t, Open, breaker.State())
}

func TestCircuitBreakerOpensOnTimeoutsAndRecoversThroughHalfOpen(t *testing.T) {
	breaker := NewCircuitBreaker(config(), ReadRequestType)
	breaker.Activate()
	start := time.Now()
	// Below the execution threshold nothing opens, even with failures.
	for range 5 {
		run(t, breaker, start, true)
	}
	require.Equal(t, Closed, breaker.State())
	for range 4 {
		run(t, breaker, start, false)
	}
	require.Equal(t, Closed, breaker.State())
	run(t, breaker, start, false)
	require.Equal(t, Open, breaker.State())
	_, err := breaker.tryAcquireAt(start.Add(4 * time.Second))
	var open *OpenError
	require.ErrorAs(t, err, &open)
	require.Equal(t, 6*time.Second, open.Remaining)
	// After the cooldown a half-open trial of successes closes it again.
	later := start.Add(11 * time.Second)
	var permits []Permit
	for range 10 {
		permit, err := breaker.tryAcquireAt(later)
		require.NoError(t, err)
		permits = append(permits, permit)
	}
	require.Equal(t, HalfOpen, breaker.State())
	_, err = breaker.tryAcquireAt(later)
	require.Error(t, err)
	for _, permit := range permits {
		breaker.finishAt(permit, false, later)
	}
	require.Equal(t, Closed, breaker.State())
}

func TestSlowRequestsCountAsFailuresAndOldOnesExpire(t *testing.T) {
	breaker := NewCircuitBreaker(config(), ReadRequestType)
	breaker.Activate()
	start := time.Now()
	for range 3 {
		permit, err := breaker.tryAcquireAt(start)
		require.NoError(t, err)
		breaker.finishAt(permit, false, start.Add(31*time.Second))
	}
	// The slow requests fall out of the one-minute window before enough executions arrive.
	later := start.Add(120 * time.Second)
	for range 10 {
		run(t, breaker, later, false)
	}
	require.Equal(t, Closed, breaker.State())
	// 3 slow requests out of 13 cross the 20% threshold; 2 out of 12 do not.
	for slow := 1; slow <= 3; slow++ {
		permit, err := breaker.tryAcquireAt(later)
		require.NoError(t, err)
		breaker.finishAt(permit, false, later.Add(31*time.Second))
		expected := Closed
		if slow == 3 {
			expected = Open
		}
		require.Equal(t, expected, breaker.State(), "slow request %d", slow)
	}
}

func TestUtilizationLimiterLimitsOnMemoryAtOnceAndCPUAfterWarmup(t *testing.T) {
	limiter := NewUtilizationLimiter(2, 1000)
	reason := limiter.Reason()
	start := time.Now()
	limiter.Compute(start, 0, 2000, true)
	require.Equal(t, "memory", reason.Load())
	// Four cores busy: the CPU average only counts after the one-minute warmup.
	cpu := 0.0
	for second := 1; second <= 59; second++ {
		cpu += 4
		limiter.Compute(start.Add(time.Duration(second)*time.Second), cpu, 0, true)
		require.Equal(t, "", reason.Load(), "second %d", second)
	}
	cpu += 4
	load, _ := limiter.Compute(start.Add(60*time.Second), cpu, 0, true)
	require.InDelta(t, 4.0, load, 1e-9)
	require.Equal(t, "cpu", reason.Load())
	// Without stats nothing is limited.
	limiter.Compute(start.Add(61*time.Second), 0, 0, false)
	require.Equal(t, "", reason.Load())
}

func TestProcessScannerReadsThisProcess(t *testing.T) {
	scanner := NewProcessScanner()
	cpu, memory, ok := scanner.Scan()
	require.True(t, ok)
	require.Positive(t, memory)
	require.GreaterOrEqual(t, cpu, 0.0)
}

func TestProtectionRejectsWhenTooBusyAndSkipsTheBreakerForOnlyReplicaRequests(t *testing.T) {
	breaker := NewCircuitBreaker(config(), ReadRequestType)
	breaker.Activate()
	reason := &Reason{}
	reason.store("cpu")
	protection := ReadProtection{LimitingReason: reason, CircuitBreaker: breaker}
	_, err := protection.start(false)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, TooBusyMessage, status.Convert(err).Message())
	reason.store("")
	permit, err := protection.start(false)
	require.NoError(t, err)
	require.True(t, permit.Active())
	permit, err = protection.start(true)
	require.NoError(t, err)
	require.False(t, permit.Active())
}

type fakeStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s fakeStream) Context() context.Context { return s.ctx }

func observed(t *testing.T, route, code string) uint64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, metrics.RequestDuration.WithLabelValues("gRPC", route, code, "false").(prometheus.Metric).Write(&metric))
	return metric.GetHistogram().GetSampleCount()
}

func TestInterceptorsRejectOverloadedReadsAndInstrumentStreamsByStatus(t *testing.T) {
	breaker := NewCircuitBreaker(config(), ReadRequestType)
	breaker.Activate()
	reason := &Reason{}
	reason.store("memory")
	protection := ReadProtection{LimitingReason: reason, CircuitBreaker: breaker}
	stream := protection.StreamServerInterceptor()
	calls := 0
	handler := func(_ any, stream grpc.ServerStream) error {
		calls++
		info, _ := grpc.Method(stream.Context())
		switch {
		case strings.HasSuffix(info, "Slow"):
			return status.Error(codes.DeadlineExceeded, "slow")
		case strings.HasSuffix(info, "Canceled"):
			return stream.Context().Err()
		}
		return nil
	}
	call := func(route string, onlyReplica bool, cancel bool) error {
		ctx := grpc.NewContextWithServerTransportStream(context.Background(), methodStream(route))
		if onlyReplica {
			ctx = metadata.NewIncomingContext(ctx, metadata.Pairs(onlyReplicaHeader, "true"))
		}
		if cancel {
			var stop context.CancelFunc
			ctx, stop = context.WithCancel(ctx)
			stop()
		}
		return stream(nil, fakeStream{ctx: ctx}, &grpc.StreamServerInfo{FullMethod: route}, handler)
	}

	route := "/cortex.Ingester/InterceptorTestBusy"
	require.Equal(t, codes.Unavailable, status.Code(call(route, false, false)))
	require.Zero(t, calls)
	require.Equal(t, uint64(1), observed(t, route, "Unavailable"))

	reason.store("")
	route = "/cortex.Ingester/InterceptorTestSlow"
	timeouts := func() float64 {
		return testutil.ToFloat64(metrics.CircuitBreakerRequestTimeouts.WithLabelValues(ReadRequestType))
	}
	before := timeouts()
	require.Equal(t, codes.DeadlineExceeded, status.Code(call(route, false, false)))
	require.Equal(t, uint64(1), observed(t, route, "DeadlineExceeded"))
	require.Greater(t, timeouts(), before)

	// Requests only this replica can serve skip the breaker but are still instrumented.
	route = "/cortex.Ingester/InterceptorTestOnlyReplica"
	require.NoError(t, call(route, true, false))
	require.Equal(t, uint64(1), observed(t, route, "OK"))

	// A stream whose client went away, like a canceled stream.
	route = "/cortex.Ingester/InterceptorTestCanceled"
	require.True(t, errors.Is(call(route, false, true), context.Canceled))
	require.Equal(t, uint64(1), observed(t, route, "Canceled"))
	require.Equal(t, 3, calls)

	// Calls outside the ingester's API are timed but never limited.
	reason.store("cpu")
	unary := protection.UnaryServerInterceptor()
	_, err := unary(context.Background(), nil, &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"},
		func(context.Context, any) (any, error) { return "ok", nil })
	require.NoError(t, err)
	require.Equal(t, uint64(1), observed(t, "/grpc.health.v1.Health/Check", "OK"))
}

func TestGRPCCodeNamesMatchGo(t *testing.T) {
	require.Equal(t, "Canceled", GRPCCodeName(codes.Canceled))
	require.Equal(t, "DeadlineExceeded", GRPCCodeName(codes.DeadlineExceeded))
	require.Equal(t, "Unknown", GRPCCodeName(codes.Code(99)))
}

type methodStream string

func (m methodStream) Method() string             { return string(m) }
func (methodStream) SetHeader(metadata.MD) error  { return nil }
func (methodStream) SendHeader(metadata.MD) error { return nil }
func (methodStream) SetTrailer(metadata.MD) error { return nil }
