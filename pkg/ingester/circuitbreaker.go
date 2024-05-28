// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"context"
	"flag"
	"time"

	"github.com/failsafe-go/failsafe-go/circuitbreaker"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type testCtxKey string

const (
	resultSuccess            = "success"
	resultError              = "error"
	resultOpen               = "circuit_breaker_open"
	testDelayKey  testCtxKey = "test-delay"
)

type circuitBreakerMetrics struct {
	circuitBreakerCurrentState *prometheus.GaugeVec

	circuitBreakerTransitions *prometheus.CounterVec
	circuitBreakerResults     *prometheus.CounterVec
}

func newCircuitBreakerMetrics(r prometheus.Registerer) *circuitBreakerMetrics {
	return &circuitBreakerMetrics{
		circuitBreakerCurrentState: promauto.With(r).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ingester_circuit_breaker_current_state",
			Help: "Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.",
		}, []string{"state"}),
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
	metrics := newCircuitBreakerMetrics(registerer)
	// Initialize each of the known labels for circuit breaker metrics for this particular ingester.
	transitionOpen := metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.OpenState.String())
	transitionHalfOpen := metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.HalfOpenState.String())
	transitionClosed := metrics.circuitBreakerTransitions.WithLabelValues(ingesterID, circuitbreaker.ClosedState.String())
	gaugeOpen := metrics.circuitBreakerCurrentState.WithLabelValues(circuitbreaker.OpenState.String())
	gaugeHalfOpen := metrics.circuitBreakerCurrentState.WithLabelValues(circuitbreaker.HalfOpenState.String())
	gaugeClosed := metrics.circuitBreakerCurrentState.WithLabelValues(circuitbreaker.ClosedState.String())

	cbBuilder := circuitbreaker.Builder[any]().
		WithFailureThreshold(cfg.FailureThreshold).
		WithDelay(cfg.CooldownPeriod).
		OnClose(func(event circuitbreaker.StateChangedEvent) {
			transitionClosed.Inc()
			gaugeOpen.Set(0)
			gaugeHalfOpen.Set(0)
			gaugeClosed.Set(1)
			level.Info(logger).Log("msg", "circuit breaker is closed", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		OnOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionOpen.Inc()
			gaugeOpen.Set(1)
			gaugeHalfOpen.Set(0)
			gaugeClosed.Set(0)
			level.Warn(logger).Log("msg", "circuit breaker is open", "ingester", ingesterID, "previous", event.OldState, "current", event.NewState)
		}).
		OnHalfOpen(func(event circuitbreaker.StateChangedEvent) {
			transitionHalfOpen.Inc()
			gaugeOpen.Set(0)
			gaugeHalfOpen.Set(1)
			gaugeClosed.Set(0)
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

	cb := cbBuilder.Build()
	return &circuitBreaker{
		cfg:            cfg,
		CircuitBreaker: cb,
		ingesterID:     ingesterID,
		logger:         logger,
		metrics:        metrics,
		startTime:      time.Now().Add(cfg.InitialDelay),
	}
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

	var ingesterErr ingesterError
	if errors.As(err, &ingesterErr) {
		return ingesterErr.errorCause() == mimirpb.INSTANCE_LIMIT
	}

	return false
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
		return newCircuitBreakerOpenError(cb.RemainingDelay())
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

func (cb *circuitBreaker) finishPushRequest(ctx context.Context, startTimestamp time.Time, err error) {
	if !cb.isActive() {
		return
	}
	if cb.cfg.testModeEnabled {
		if initialDelay, ok := ctx.Value(testDelayKey).(time.Duration); ok {
			time.Sleep(initialDelay)
		}
	}
	if cb.cfg.PushTimeout < time.Since(startTimestamp) {
		err = context.DeadlineExceeded
	}
	if err == nil {
		cb.recordSuccess()
	} else {
		cb.recordError(err)
	}
}
