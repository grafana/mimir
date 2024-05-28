// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"flag"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/grpcutil"
	"github.com/grafana/dskit/middleware"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type testCtxKey string

const (
	resultSuccess            = "success"
	resultError              = "error"
	resultOpen               = "circuit_breaker_open"
	testDelayKey  testCtxKey = "test-delay"

	circuitBreakerCurrentStateGaugeName  = "cortex_ingester_circuit_breaker_current_state"
	circuitBreakerCurrentStateGaugeHelp  = "Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name."
	circuitBreakerCurrentStateGaugeLabel = "state"
)

type circuitBreakerMetrics struct {
	circuitBreakerCurrentState *prometheus.GaugeVec

	circuitBreakerOpenStateGauge     prometheus.GaugeFunc
	circuitBreakerHalfOpenStateGauge prometheus.GaugeFunc
	circuitBreakerClosedStateGauge   prometheus.GaugeFunc

	circuitBreakerTransitions *prometheus.CounterVec
	circuitBreakerResults     *prometheus.CounterVec
}

func newCircuitBreakerMetrics(r prometheus.Registerer, currentStateFn func() circuitbreaker.State) *circuitBreakerMetrics {
	return &circuitBreakerMetrics{
		circuitBreakerOpenStateGauge: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        circuitBreakerCurrentStateGaugeName,
			Help:        circuitBreakerCurrentStateGaugeHelp,
			ConstLabels: map[string]string{circuitBreakerCurrentStateGaugeLabel: circuitbreaker.OpenState.String()},
		}, func() float64 {
			if currentStateFn() == circuitbreaker.OpenState {
				return 1
			} else {
				return 0
			}
		}),
		circuitBreakerHalfOpenStateGauge: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        circuitBreakerCurrentStateGaugeName,
			Help:        circuitBreakerCurrentStateGaugeHelp,
			ConstLabels: map[string]string{circuitBreakerCurrentStateGaugeLabel: circuitbreaker.HalfOpenState.String()},
		}, func() float64 {
			if currentStateFn() == circuitbreaker.HalfOpenState {
				return 1
			} else {
				return 0
			}
		}),
		circuitBreakerClosedStateGauge: promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        circuitBreakerCurrentStateGaugeName,
			Help:        circuitBreakerCurrentStateGaugeHelp,
			ConstLabels: map[string]string{circuitBreakerCurrentStateGaugeLabel: circuitbreaker.ClosedState.String()},
		}, func() float64 {
			if currentStateFn() == circuitbreaker.ClosedState {
				return 1
			} else {
				return 0
			}
		}),
		circuitBreakerTransitions: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_circuit_breaker_transitions_total",
			Help: "Number of times the circuit breaker has entered a state.",
		}, []string{"ingester", "state"}),
		circuitBreakerResults: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_circuit_breaker_results_total",
			Help: "Results of executing requests via the circuit breaker.",
		}, []string{"ingester", "result"}),
	}
}

type CircuitBreakerConfig struct {
	Enabled                   bool          `yaml:"enabled" category:"experimental"`
	FailureThreshold          uint          `yaml:"failure_threshold" category:"experimental"`
	FailureExecutionThreshold uint          `yaml:"failure_execution_threshold" category:"experimental"`
	ThresholdingPeriod        time.Duration `yaml:"thresholding_period" category:"experimental"`
	CooldownPeriod            time.Duration `yaml:"cooldown_period" category:"experimental"`
	InitialDelay              time.Duration `yaml:"initial_delay" category:"experimental"`
	PushTimeout               time.Duration `yaml:"push_timeout" category:"experiment"`
	testModeEnabled           bool          `yaml:"-"`
}

func (cfg *CircuitBreakerConfig) RegisterFlags(f *flag.FlagSet) {
	prefix := "ingester.circuit-breaker."
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable circuit breaking when making requests to ingesters")
	f.UintVar(&cfg.FailureThreshold, prefix+"failure-threshold", 10, "Max percentage of requests that can fail over period before the circuit breaker opens")
	f.UintVar(&cfg.FailureExecutionThreshold, prefix+"failure-execution-threshold", 100, "How many requests must have been executed in period for the circuit breaker to be eligible to open for the rate of failures")
	f.DurationVar(&cfg.ThresholdingPeriod, prefix+"thresholding-period", time.Minute, "Moving window of time that the percentage of failed requests is computed over")
	f.DurationVar(&cfg.CooldownPeriod, prefix+"cooldown-period", 10*time.Second, "How long the circuit breaker will stay in the open state before allowing some requests")
	f.DurationVar(&cfg.InitialDelay, prefix+"initial-delay", 0, "How long the circuit breaker should wait between creation and starting up. During that time both failures and successes will not be counted.")
	f.DurationVar(&cfg.PushTimeout, prefix+"push-timeout", 0, "How long is execution of ingester's Push supposed to last before it is reported as timeout in a circuit breaker. This configuration is used for circuit breakers only, and timeout expirations are not reported as errors")
}

func (cfg *CircuitBreakerConfig) Validate() error {
	return nil
}

type circuitBreaker struct {
	circuitbreaker.CircuitBreaker[any]
	cfg        CircuitBreakerConfig
	ingesterID string
	logger     log.Logger
	metrics    *circuitBreakerMetrics
	startTime  time.Time
}

func newCircuitBreaker(cfg CircuitBreakerConfig, ingesterID string, logger log.Logger, registerer prometheus.Registerer) *circuitBreaker {
	cb := circuitBreaker{
		cfg:        cfg,
		ingesterID: ingesterID,
		logger:     logger,
		startTime:  time.Now().Add(cfg.InitialDelay),
	}

	cb.metrics = newCircuitBreakerMetrics(registerer, cb.State)
	// Initialize each of the known labels for circuit breaker metrics for this particular ingester.
	transitionOpen := cb.metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.OpenState.String())
	transitionHalfOpen := cb.metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.HalfOpenState.String())
	transitionClosed := cb.metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.ClosedState.String())

	cbBuilder := circuitbreaker.Builder[any]().
		WithFailureThreshold(cfg.FailureThreshold).
		WithDelay(cfg.CooldownPeriod).
		OnClose(func(event circuitbreaker.StateChangedEvent) {
			transitionClosed.Inc()
			level.Info(logger).Log("msg", "circuit breaker is closed", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		OnOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionOpen.Inc()
			level.Warn(logger).Log("msg", "circuit breaker is open", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		OnHalfOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionHalfOpen.Inc()
			level.Info(logger).Log("msg", "circuit breaker is half-open", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		HandleIf(func(_ any, err error) bool { return isFailure(err) })

	if cfg.testModeEnabled {
		// In case of testing purposes, we initialize the circuit breaker with count based failure thresholding,
		// since it is more deterministic, and therefore it is easier to predict the outcome.
		cbBuilder = cbBuilder.WithFailureThreshold(cfg.FailureThreshold)
	} else {
		// In case of production code, we prefer time based failure thresholding.
		cbBuilder = cbBuilder.WithFailureRateThreshold(cfg.FailureThreshold, cfg.FailureExecutionThreshold, cfg.ThresholdingPeriod)
	}

	cb.CircuitBreaker = cbBuilder.Build()
	return &cb
}

func isFailure(err error) bool {
	if err == nil {
		return false
	}

	// We only consider timeouts or ingester hitting a per-instance limit
	// to be errors worthy of tripping the circuit breaker since these
	// are specific to a particular ingester, not a user or request.

	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	statusCode := grpcutil.ErrorToStatusCode(err)
	if statusCode == codes.DeadlineExceeded {
		return true
	}

	var ingesterErr ingesterError
	if errors.As(err, &ingesterErr) {
		return ingesterErr.errorCause() == mimirpb.INSTANCE_LIMIT
	}

	return false
}

func (cb *circuitBreaker) State() circuitbreaker.State {
	return cb.CircuitBreaker.State()
}

func (cb *circuitBreaker) isActive() bool {
	if cb == nil {
		return false
	}
	return cb.startTime.Before(time.Now())
}

func (cb *circuitBreaker) tryAcquirePermit() error {
	if !cb.isActive() {
		return nil
	}
	if !cb.CircuitBreaker.TryAcquirePermit() {
		cb.metrics.circuitBreakerResults.WithLabelValues(cb.ingesterID, resultOpen).Inc()
		return middleware.DoNotLogError{Err: newCircuitBreakerOpenError(cb.RemainingDelay())}
	}
	return nil
}

func (cb *circuitBreaker) recordSuccess() {
	if !cb.isActive() {
		return
	}
	cb.CircuitBreaker.RecordSuccess()
	cb.metrics.circuitBreakerResults.WithLabelValues(cb.ingesterID, resultSuccess).Inc()
}

func (cb *circuitBreaker) recordError(err error) {
	if !cb.isActive() {
		return
	}
	cb.CircuitBreaker.RecordError(err)
	cb.metrics.circuitBreakerResults.WithLabelValues(cb.ingesterID, resultError).Inc()
}

func (cb *circuitBreaker) finishPushRequest(ctx context.Context, duration time.Duration, err error) error {
	if !cb.isActive() {
		return nil
	}
	if cb.cfg.testModeEnabled {
		if initialDelay, ok := ctx.Value(testDelayKey).(time.Duration); ok {
			duration += initialDelay
		}
	}
	if cb.cfg.PushTimeout < duration {
		err = context.DeadlineExceeded
	}
	if err == nil {
		cb.recordSuccess()
	} else {
		cb.recordError(err)
	}
	return err
}
