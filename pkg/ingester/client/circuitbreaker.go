// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"errors"

	"github.com/failsafe-go/failsafe-go"
	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	resultSuccess = "success"
	resultError   = "error"
	resultOpen    = "circuit_breaker_open"
)

var (
	// Only apply circuit breaking to these methods (all IngesterClient methods).
	circuitBreakMethods = map[string]struct{}{
		"/cortex.Ingester/Push":                    {},
		"/cortex.Ingester/QueryStream":             {},
		"/cortex.Ingester/QueryExemplars":          {},
		"/cortex.Ingester/LabelValues":             {},
		"/cortex.Ingester/LabelNames":              {},
		"/cortex.Ingester/UserStats":               {},
		"/cortex.Ingester/AllUserStats":            {},
		"/cortex.Ingester/MetricsForLabelMatchers": {},
		"/cortex.Ingester/MetricsMetadata":         {},
		"/cortex.Ingester/LabelNamesAndValues":     {},
		"/cortex.Ingester/LabelValuesCardinality":  {},
	}
)

func NewCircuitBreaker(inst ring.InstanceDesc, cfg CircuitBreakerConfig, metrics *Metrics, logger log.Logger) grpc.UnaryClientInterceptor {
	// Initialize each of the known labels for circuit breaker metrics for this particular ingester
	transitionOpen := metrics.circuitBreakerTransitions.WithLabelValues(inst.Id, circuitbreaker.OpenState.String())
	transitionHalfOpen := metrics.circuitBreakerTransitions.WithLabelValues(inst.Id, circuitbreaker.HalfOpenState.String())
	transitionClosed := metrics.circuitBreakerTransitions.WithLabelValues(inst.Id, circuitbreaker.ClosedState.String())
	countSuccess := metrics.circuitBreakerResults.WithLabelValues(inst.Id, resultSuccess)
	countError := metrics.circuitBreakerResults.WithLabelValues(inst.Id, resultError)
	countOpen := metrics.circuitBreakerResults.WithLabelValues(inst.Id, resultOpen)

	breaker := circuitbreaker.Builder[any]().
		WithFailureRateThreshold(cfg.FailureThreshold, cfg.FailureExecutionThreshold, cfg.ThresholdingPeriod).
		WithDelay(cfg.CooldownPeriod).
		OnFailure(func(event failsafe.ExecutionEvent[any]) {
			countError.Inc()
		}).
		OnSuccess(func(event failsafe.ExecutionEvent[any]) {
			countSuccess.Inc()
		}).
		OnClose(func(event circuitbreaker.StateChangedEvent) {
			transitionClosed.Inc()
			level.Info(logger).Log("msg", "circuit breaker is closed", "ingester", inst.Id, "previous", event.OldState, "current", event.NewState)
		}).
		OnOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionOpen.Inc()
			level.Info(logger).Log("msg", "circuit breaker is open", "ingester", inst.Id, "previous", event.OldState, "current", event.NewState)
		}).
		OnHalfOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionHalfOpen.Inc()
			level.Info(logger).Log("msg", "circuit breaker is half-open", "ingester", inst.Id, "previous", event.OldState, "current", event.NewState)
		}).
		HandleIf(func(r any, err error) bool {
			isFail := isFailure(err)
			if isFail {
				level.Warn(logger).Log("msg", "circuit breaker", "ingester", inst.Id, "err", err)
			}
			return isFail
		}).
		Build()

	executor := failsafe.NewExecutor[any](breaker)

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Don't circuit break non-ingester things like health check endpoints
		if _, ok := circuitBreakMethods[method]; !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		err := executor.Run(func() error {
			return invoker(ctx, method, req, reply, cc, opts...)
		})

		if err != nil && errors.Is(err, circuitbreaker.ErrCircuitBreakerOpen) {
			countOpen.Inc()
		}

		return err
	}
}

func isFailure(err error) bool {
	if err == nil {
		return false
	}

	// We only consider timeouts or the ingester being unavailable (returned when hitting
	// per-instance limits) to be errors worthy of tripping the circuit breaker since these
	// are specific to a particular ingester, not a user or request.
	code := status.Code(err)
	return code == codes.Unavailable || code == codes.DeadlineExceeded
}
