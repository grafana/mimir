// SPDX-License-Identifier: AGPL-3.0-only

// Package protection is read-path protection like the Go ingester's `StartReadRequest`: a CPU
// and memory utilization based limiter and the read circuit breaker, applied to every
// `/cortex.Ingester/*` gRPC call, plus the `cortex_request_duration_seconds` instrumentation with
// gRPC status codes. It also holds the push circuit breaker the Kafka path applies to each batch.
package protection

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/grafana/mimir/pkg/storage/seriesstore/limits"
	"github.com/grafana/mimir/pkg/storage/seriesstore/metrics"
)

const (
	TooBusyMessage    = "ingester is currently too busy to process queries, try again later"
	ReadRequestType   = "read"
	PushRequestType   = "push"
	onlyReplicaHeader = "__only_replica__"
)

// Args are the protection flags, spelled like Mimir's.
type Args struct {
	CircuitBreakerEnabled      bool
	FailureThresholdPercentage uint
	FailureExecutionThreshold  uint
	ThresholdingPeriod         string
	CooldownPeriod             string
	InitialDelay               string
	RequestTimeout             string
	// The push circuit breaker, which the Go ingester applies to each head append of records.
	PushCircuitBreakerEnabled      bool
	PushFailureThresholdPercentage uint
	PushFailureExecutionThreshold  uint
	PushThresholdingPeriod         string
	PushCooldownPeriod             string
	PushInitialDelay               string
	PushRequestTimeout             string
	// CPU cores; 0 disables the CPU part of the utilization limiter.
	CPUUtilizationLimit float64
	// Bytes of allocated memory; 0 disables the memory part of the utilization limiter.
	MemoryUtilizationLimit   uint64
	GRPCMaxConcurrentStreams uint
}

func (a *Args) RegisterFlags(f *flag.FlagSet) {
	f.BoolVar(&a.CircuitBreakerEnabled, "ingester.read-circuit-breaker.enabled", false, "")
	f.UintVar(&a.FailureThresholdPercentage, "ingester.read-circuit-breaker.failure-threshold-percentage", 10, "")
	f.UintVar(&a.FailureExecutionThreshold, "ingester.read-circuit-breaker.failure-execution-threshold", 100, "")
	f.StringVar(&a.ThresholdingPeriod, "ingester.read-circuit-breaker.thresholding-period", "1m", "")
	f.StringVar(&a.CooldownPeriod, "ingester.read-circuit-breaker.cooldown-period", "10s", "")
	f.StringVar(&a.InitialDelay, "ingester.read-circuit-breaker.initial-delay", "0", "")
	f.StringVar(&a.RequestTimeout, "ingester.read-circuit-breaker.request-timeout", "30s", "")
	f.BoolVar(&a.PushCircuitBreakerEnabled, "ingester.push-circuit-breaker.enabled", false, "")
	f.UintVar(&a.PushFailureThresholdPercentage, "ingester.push-circuit-breaker.failure-threshold-percentage", 10, "")
	f.UintVar(&a.PushFailureExecutionThreshold, "ingester.push-circuit-breaker.failure-execution-threshold", 100, "")
	f.StringVar(&a.PushThresholdingPeriod, "ingester.push-circuit-breaker.thresholding-period", "1m", "")
	f.StringVar(&a.PushCooldownPeriod, "ingester.push-circuit-breaker.cooldown-period", "10s", "")
	f.StringVar(&a.PushInitialDelay, "ingester.push-circuit-breaker.initial-delay", "0", "")
	f.StringVar(&a.PushRequestTimeout, "ingester.push-circuit-breaker.request-timeout", "2s", "")
	f.Float64Var(&a.CPUUtilizationLimit, "ingester.read-path-cpu-utilization-limit", 0, "")
	f.Uint64Var(&a.MemoryUtilizationLimit, "ingester.read-path-memory-utilization-limit", 0, "")
	f.UintVar(&a.GRPCMaxConcurrentStreams, "server.grpc-max-concurrent-streams", 100, "")
}

// CircuitBreakerConfig is a breaker's thresholds and periods.
type CircuitBreakerConfig struct {
	FailureThresholdPercentage uint
	FailureExecutionThreshold  uint
	ThresholdingPeriod         time.Duration
	CooldownPeriod             time.Duration
	InitialDelay               time.Duration
	RequestTimeout             time.Duration
}

func breakerConfig(enabled bool, percentage, executions uint, periods [4]string) (*CircuitBreakerConfig, error) {
	if !enabled {
		return nil, nil
	}
	var durations [4]time.Duration
	for i, period := range periods {
		ms, err := limits.ParseDurationMs(period)
		if err != nil {
			return nil, err
		}
		durations[i] = time.Duration(ms) * time.Millisecond
	}
	return &CircuitBreakerConfig{
		FailureThresholdPercentage: percentage,
		FailureExecutionThreshold:  executions,
		ThresholdingPeriod:         durations[0],
		CooldownPeriod:             durations[1],
		InitialDelay:               durations[2],
		RequestTimeout:             durations[3],
	}, nil
}

// CircuitBreaker is the read breaker's config, or nil when disabled.
func (a *Args) CircuitBreaker() (*CircuitBreakerConfig, error) {
	return breakerConfig(a.CircuitBreakerEnabled, a.FailureThresholdPercentage, a.FailureExecutionThreshold,
		[4]string{a.ThresholdingPeriod, a.CooldownPeriod, a.InitialDelay, a.RequestTimeout})
}

// PushCircuitBreaker is the push breaker's config, or nil when disabled.
func (a *Args) PushCircuitBreaker() (*CircuitBreakerConfig, error) {
	return breakerConfig(a.PushCircuitBreakerEnabled, a.PushFailureThresholdPercentage, a.PushFailureExecutionThreshold,
		[4]string{a.PushThresholdingPeriod, a.PushCooldownPeriod, a.PushInitialDelay, a.PushRequestTimeout})
}

type BreakerState int

const (
	Closed BreakerState = iota
	Open
	HalfOpen
)

func (s BreakerState) Label() string {
	switch s {
	case Open:
		return "open"
	case HalfOpen:
		return "half-open"
	default:
		return "closed"
	}
}

const breakerBuckets = 10

type breakerBucket struct {
	start      time.Time
	executions uint
	failures   uint
}

// CircuitBreaker is a time-based failure-rate circuit breaker with the behavior of failsafe-go's,
// as configured by the Go ingester: only requests that time out count as failures.
type CircuitBreaker struct {
	config      CircuitBreakerConfig
	requestType string
	// 0 inactive, 1 pending the initial delay, 2 active.
	activation  atomic.Uint32
	activatedAt atomic.Int64

	mu       sync.Mutex
	state    BreakerState
	openedAt time.Time
	// Closed state: executions and failures in time buckets covering the thresholding period.
	buckets [breakerBuckets]breakerBucket
	// Half-open state: permits handed out and results recorded.
	halfOpenPermits    uint
	halfOpenExecutions uint
	halfOpenFailures   uint
}

// Permit is a request the breaker let through. A zero Permit means the breaker was not active.
type Permit struct {
	started  time.Time
	halfOpen bool
	active   bool
}

// Active reports whether the breaker counts this request.
func (p Permit) Active() bool { return p.active }

// OpenError is returned while the breaker is open, with the remaining cooldown.
type OpenError struct{ Remaining time.Duration }

func (e *OpenError) Error() string {
	return fmt.Sprintf("circuit breaker open with remaining delay %v", e.Remaining)
}

func NewCircuitBreaker(config CircuitBreakerConfig, requestType string) *CircuitBreaker {
	now := time.Now()
	for _, state := range []BreakerState{Closed, Open, HalfOpen} {
		value := 0.0
		if state == Closed {
			value = 1
		}
		metrics.CircuitBreakerCurrentState.WithLabelValues(requestType, state.Label()).Set(value)
		metrics.CircuitBreakerTransitions.WithLabelValues(requestType, state.Label())
	}
	for _, result := range []string{"success", "error", "circuit_breaker_open"} {
		metrics.CircuitBreakerResults.WithLabelValues(requestType, result)
	}
	breaker := &CircuitBreaker{config: config, requestType: requestType, state: Closed, openedAt: now}
	for i := range breaker.buckets {
		breaker.buckets[i].start = now
	}
	return breaker
}

// Activate is called once the ingester serves reads; the breaker starts counting after the
// initial delay.
func (b *CircuitBreaker) Activate() {
	b.activatedAt.Store(time.Now().UnixNano())
	b.activation.Store(1)
}

func (b *CircuitBreaker) isActive(now time.Time) bool {
	switch b.activation.Load() {
	case 2:
		return true
	case 1:
		if now.Sub(time.Unix(0, b.activatedAt.Load())) >= b.config.InitialDelay {
			b.activation.Store(2)
			return true
		}
		return false
	default:
		return false
	}
}

func (b *CircuitBreaker) State() BreakerState {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *CircuitBreaker) transition(state BreakerState, now time.Time) {
	metrics.CircuitBreakerCurrentState.WithLabelValues(b.requestType, b.state.Label()).Set(0)
	metrics.CircuitBreakerCurrentState.WithLabelValues(b.requestType, state.Label()).Set(1)
	metrics.CircuitBreakerTransitions.WithLabelValues(b.requestType, state.Label()).Inc()
	log.Printf("phase=circuit_breaker previous=%s current=%s", b.state.Label(), state.Label())
	b.state = state
	b.openedAt = now
	for i := range b.buckets {
		b.buckets[i] = breakerBucket{start: now}
	}
	b.halfOpenPermits, b.halfOpenExecutions, b.halfOpenFailures = 0, 0, 0
}

// TryAcquire returns an inactive Permit when the breaker is not active yet, and an *OpenError
// with the remaining delay when open.
func (b *CircuitBreaker) TryAcquire() (Permit, error) {
	return b.tryAcquireAt(time.Now())
}

func (b *CircuitBreaker) tryAcquireAt(now time.Time) (Permit, error) {
	if !b.isActive(now) {
		return Permit{}, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state == Open {
		elapsed := max(now.Sub(b.openedAt), 0)
		if elapsed < b.config.CooldownPeriod {
			metrics.CircuitBreakerResults.WithLabelValues(b.requestType, "circuit_breaker_open").Inc()
			return Permit{}, &OpenError{Remaining: b.config.CooldownPeriod - elapsed}
		}
		b.transition(HalfOpen, now)
	}
	if b.state == HalfOpen {
		if b.halfOpenPermits >= max(b.config.FailureExecutionThreshold, 1) {
			metrics.CircuitBreakerResults.WithLabelValues(b.requestType, "circuit_breaker_open").Inc()
			return Permit{}, &OpenError{}
		}
		b.halfOpenPermits++
		return Permit{started: now, halfOpen: true, active: true}, nil
	}
	return Permit{started: now, active: true}, nil
}

// Finish records the end of a request; deadlineExceeded is its gRPC status.
func (b *CircuitBreaker) Finish(permit Permit, deadlineExceeded bool) {
	b.finishAt(permit, deadlineExceeded, time.Now())
}

// FinishAll records executions requests that ran together under permit, like the head appends
// of the Go ingester's flushes for one batch of records.
func (b *CircuitBreaker) FinishAll(permit Permit, executions int) {
	now := time.Now()
	for range max(executions, 1) {
		b.finishAt(permit, false, now)
	}
}

func (b *CircuitBreaker) finishAt(permit Permit, deadlineExceeded bool, now time.Time) {
	if !permit.active {
		return
	}
	failed := deadlineExceeded || now.Sub(permit.started) > b.config.RequestTimeout
	result := "success"
	if failed {
		metrics.CircuitBreakerRequestTimeouts.WithLabelValues(b.requestType).Inc()
		result = "error"
	}
	metrics.CircuitBreakerResults.WithLabelValues(b.requestType, result).Inc()
	b.mu.Lock()
	defer b.mu.Unlock()
	threshold := float64(b.config.FailureThresholdPercentage) / 100
	switch b.state {
	case Open:
	case HalfOpen:
		if !permit.halfOpen {
			return
		}
		b.halfOpenExecutions++
		if failed {
			b.halfOpenFailures++
		}
		capacity := max(b.config.FailureExecutionThreshold, 1)
		failures := float64(b.halfOpenFailures)
		if failures/float64(capacity) >= threshold && failures > 0 {
			b.transition(Open, now)
		} else if b.halfOpenExecutions >= capacity {
			b.transition(Closed, now)
		}
	case Closed:
		width := max(b.config.ThresholdingPeriod/breakerBuckets, time.Millisecond)
		sinceOpen := max(now.Sub(b.openedAt), 0)
		index := int(sinceOpen / width)
		bucketStart := b.openedAt.Add(width * time.Duration(index))
		slot := &b.buckets[index%breakerBuckets]
		if !slot.start.Equal(bucketStart) {
			*slot = breakerBucket{start: bucketStart}
		}
		slot.executions++
		if failed {
			slot.failures++
		}
		windowStart := now.Add(-b.config.ThresholdingPeriod)
		if windowStart.Before(b.openedAt) {
			windowStart = b.openedAt
		}
		var executions, failures uint
		for _, bucket := range b.buckets {
			if bucket.start.Add(width).After(windowStart) {
				executions += bucket.executions
				failures += bucket.failures
			}
		}
		if executions >= b.config.FailureExecutionThreshold && executions > 0 &&
			float64(failures)/float64(executions) >= threshold {
			b.transition(Open, now)
		}
	}
}

// ReadProtection is the read-path checks shared by every gRPC call.
type ReadProtection struct {
	LimitingReason *Reason
	CircuitBreaker *CircuitBreaker
}

func (p ReadProtection) start(onlyReplica bool) (Permit, error) {
	if p.LimitingReason != nil {
		if reason := p.LimitingReason.Load(); reason != "" {
			metrics.UtilizationLimitedReads.WithLabelValues(reason).Inc()
			return Permit{}, status.Error(codes.Unavailable, TooBusyMessage)
		}
	}
	// A request no other replica can serve is never rejected for overload.
	if onlyReplica || p.CircuitBreaker == nil {
		return Permit{}, nil
	}
	permit, err := p.CircuitBreaker.TryAcquire()
	var open *OpenError
	if errors.As(err, &open) {
		return Permit{}, status.Errorf(codes.Unavailable,
			"circuit breaker open on read request type with remaining delay %v", open.Remaining)
	}
	return permit, err
}

func isOnlyReplica(ctx context.Context) bool {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, value := range md.Get(onlyReplicaHeader) {
		if value == "true" {
			return true
		}
	}
	return false
}

// requestCode is a finished call's gRPC code: a call that ended with its context, like a canceled
// stream, reports why the context ended.
func requestCode(ctx context.Context, err error) codes.Code {
	if err == nil {
		return codes.OK
	}
	if _, ok := status.FromError(err); ok {
		return status.Code(err)
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		return status.FromContextError(err).Code()
	}
	return codes.Unknown
}

type requestTracker struct {
	route   string
	started time.Time
}

func newRequestTracker(route string) requestTracker {
	metrics.InflightRequests.WithLabelValues("gRPC", route).Inc()
	return requestTracker{route: route, started: time.Now()}
}

func (t requestTracker) finish(code codes.Code) {
	metrics.InflightRequests.WithLabelValues("gRPC", t.route).Dec()
	metrics.RequestDuration.WithLabelValues("gRPC", t.route, GRPCCodeName(code), "false").
		Observe(time.Since(t.started).Seconds())
}

// call runs a gRPC call under the protection: reads may be rejected, every call is timed and
// classified by its status.
func (p ReadProtection) call(ctx context.Context, route string, handle func() error) error {
	tracker := newRequestTracker(route)
	var permit Permit
	if strings.HasPrefix(route, "/cortex.Ingester/") {
		var err error
		permit, err = p.start(isOnlyReplica(ctx))
		if err != nil {
			tracker.finish(status.Code(err))
			return err
		}
	}
	err := handle()
	code := requestCode(ctx, err)
	tracker.finish(code)
	if permit.active && p.CircuitBreaker != nil {
		p.CircuitBreaker.Finish(permit, code == codes.DeadlineExceeded)
	}
	return err
}

// UnaryServerInterceptor applies the protection to unary calls.
func (p ReadProtection) UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		var response any
		err := p.call(ctx, info.FullMethod, func() error {
			var err error
			response, err = handler(ctx, request)
			return err
		})
		return response, err
	}
}

// StreamServerInterceptor applies the protection to streaming calls, which are timed and
// classified when they end.
func (p ReadProtection) StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return p.call(stream.Context(), info.FullMethod, func() error {
			return handler(server, stream)
		})
	}
}

// GRPCCodeName is a gRPC code's name as Go's `codes.Code.String()` prints it, with codes it
// doesn't know as Unknown.
func GRPCCodeName(code codes.Code) string {
	if code > codes.Unauthenticated {
		return "Unknown"
	}
	return code.String()
}
