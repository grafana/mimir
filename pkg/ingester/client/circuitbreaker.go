// SPDX-License-Identifier: AGPL-3.0-only

package client

import (
	"context"
	"errors"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/sony/gobreaker"
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

func NewCircuitBreaker(addr string, cfg CircuitBreakerConfig, metrics *Metrics, logger log.Logger) grpc.UnaryClientInterceptor {
	breaker := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        addr,
		Timeout:     cfg.CooldownPeriod,
		MaxRequests: uint32(cfg.FailureThreshold),
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return uint64(counts.ConsecutiveFailures) >= cfg.FailureThreshold
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			level.Info(logger).Log("msg", "circuit breaker changing state", "addr", addr, "from", from, "to", to)
			metrics.circuitBreakerTransitions.WithLabelValues(to.String()).Inc()
		},
		IsSuccessful: isSuccessful,
	})

	// Initialize each of the known labels for circuit breaker metrics
	metrics.circuitBreakerTransitions.WithLabelValues(gobreaker.StateOpen.String())
	metrics.circuitBreakerTransitions.WithLabelValues(gobreaker.StateHalfOpen.String())
	metrics.circuitBreakerTransitions.WithLabelValues(gobreaker.StateClosed.String())
	metrics.circuitBreakerResults.WithLabelValues(resultSuccess)
	metrics.circuitBreakerResults.WithLabelValues(resultError)
	metrics.circuitBreakerResults.WithLabelValues(resultOpen)

	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Don't circuit break non-ingester things like health check endpoints
		if _, ok := circuitBreakMethods[method]; !ok {
			return invoker(ctx, method, req, reply, cc, opts...)
		}

		_, err := breaker.Execute(func() (interface{}, error) {
			err := invoker(ctx, method, req, reply, cc, opts...)
			return nil, err
		})

		if err != nil && (errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests)) {
			metrics.circuitBreakerResults.WithLabelValues(resultOpen).Inc()
		} else if !isSuccessful(err) {
			metrics.circuitBreakerResults.WithLabelValues(resultError).Inc()
		} else {
			metrics.circuitBreakerResults.WithLabelValues(resultSuccess).Inc()
		}

		return err
	}
}

func isSuccessful(err error) bool {
	if err == nil {
		return true
	}

	// We only consider timeouts or the ingester being unavailable (returned when hitting
	// per-instance limits) to be errors worthy of tripping the circuit breaker since these
	// are specific to a particular ingester, not a user or request.
	code := status.Code(err)
	return !(code == codes.Unavailable || code == codes.DeadlineExceeded)
}
