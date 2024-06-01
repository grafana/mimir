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
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc/codes"

	"github.com/grafana/mimir/pkg/mimirpb"
)

type testCtxKey string

const (
	resultSuccess                 = "success"
	resultError                   = "error"
	resultOpen                    = "circuit_breaker_open"
	defaultPushTimeout            = 2 * time.Second
	testDelayKey       testCtxKey = "test-delay"
)

type circuitBreakerMetrics struct {
	circuitBreakerTransitions *prometheus.CounterVec
	circuitBreakerResults     *prometheus.CounterVec
}

func newCircuitBreakerMetrics(r prometheus.Registerer, currentStateFn func() circuitbreaker.State) *circuitBreakerMetrics {
	cbMetrics := &circuitBreakerMetrics{
		circuitBreakerTransitions: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_circuit_breaker_transitions_total",
			Help: "Number of times the circuit breaker has entered a state.",
		}, []string{"state"}),
		circuitBreakerResults: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ingester_circuit_breaker_results_total",
			Help: "Results of executing requests via the circuit breaker.",
		}, []string{"result"}),
	}
	circuitBreakerCurrentStateGaugeFn := func(state circuitbreaker.State) prometheus.GaugeFunc {
		return promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "cortex_ingester_circuit_breaker_current_state",
			Help:        "Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.",
			ConstLabels: map[string]string{"state": state.String()},
		}, func() float64 {
			if currentStateFn() == state {
				return 1
			}
			return 0
		})
	}
	for _, s := range []circuitbreaker.State{circuitbreaker.OpenState, circuitbreaker.HalfOpenState, circuitbreaker.ClosedState} {
		circuitBreakerCurrentStateGaugeFn(s)
		// We initialize all possible states for the circuitBreakerTransitions metrics
		cbMetrics.circuitBreakerTransitions.WithLabelValues(s.String())
	}
	for _, r := range []string{resultSuccess, resultError, resultOpen} {
		// We initialize all possible results for the circuitBreakerResults metrics
		cbMetrics.circuitBreakerResults.WithLabelValues(r)
	}
	return cbMetrics
}

type CircuitBreakerConfig struct {
	Enabled                    bool          `yaml:"enabled" category:"experimental"`
	FailureThresholdPercentage uint          `yaml:"failure_threshold_percentage" category:"experimental"`
	FailureExecutionThreshold  uint          `yaml:"failure_execution_threshold" category:"experimental"`
	ThresholdingPeriod         time.Duration `yaml:"thresholding_period" category:"experimental"`
	CooldownPeriod             time.Duration `yaml:"cooldown_period" category:"experimental"`
	InitialDelay               time.Duration `yaml:"initial_delay" category:"experimental"`
	PushTimeout                time.Duration `yaml:"push_timeout" category:"experiment"`
	testModeEnabled            bool          `yaml:"-"`
}

func (cfg *CircuitBreakerConfig) RegisterFlags(f *flag.FlagSet) {
	prefix := "ingester.circuit-breaker."
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable circuit breaking when making requests to ingesters")
	f.UintVar(&cfg.FailureThresholdPercentage, prefix+"failure-threshold-percentage", 10, "Max percentage of requests that can fail over period before the circuit breaker opens")
	f.UintVar(&cfg.FailureExecutionThreshold, prefix+"failure-execution-threshold", 100, "How many requests must have been executed in period for the circuit breaker to be eligible to open for the rate of failures")
	f.DurationVar(&cfg.ThresholdingPeriod, prefix+"thresholding-period", time.Minute, "Moving window of time that the percentage of failed requests is computed over")
	f.DurationVar(&cfg.CooldownPeriod, prefix+"cooldown-period", 10*time.Second, "How long the circuit breaker will stay in the open state before allowing some requests")
	f.DurationVar(&cfg.InitialDelay, prefix+"initial-delay", 0, "How long the circuit breaker should wait between creation and starting up. During that time both failures and successes will not be counted.")
	f.DurationVar(&cfg.PushTimeout, prefix+"push-timeout", defaultPushTimeout, "How long is execution of ingester's Push supposed to last before it is reported as timeout in a circuit breaker. This configuration is used for circuit breakers only, and timeout expirations are not reported as errors")
}

type circuitBreaker struct {
	cfg       CircuitBreakerConfig
	logger    log.Logger
	metrics   *circuitBreakerMetrics
	startTime time.Time
	cb        circuitbreaker.CircuitBreaker[any]
}

func newCircuitBreaker(cfg CircuitBreakerConfig, logger log.Logger, registerer prometheus.Registerer) *circuitBreaker {
	cb := circuitBreaker{
		cfg:       cfg,
		logger:    logger,
		startTime: time.Now().Add(cfg.InitialDelay),
	}

	circuitBreakerTransitionsCounterFn := func(metrics *circuitBreakerMetrics, state circuitbreaker.State) prometheus.Counter {
		return metrics.circuitBreakerTransitions.WithLabelValues(state.String())
	}

	cbBuilder := circuitbreaker.Builder[any]().
		WithFailureThreshold(cfg.FailureThresholdPercentage).
		WithDelay(cfg.CooldownPeriod).
		OnClose(func(event circuitbreaker.StateChangedEvent) {
			circuitBreakerTransitionsCounterFn(cb.metrics, circuitbreaker.ClosedState).Inc()
			level.Info(logger).Log("msg", "circuit breaker is closed", "previous", event.OldState, "current", event.NewState)
		}).
		OnOpen(func(event circuitbreaker.StateChangedEvent) {
			circuitBreakerTransitionsCounterFn(cb.metrics, circuitbreaker.OpenState).Inc()
			level.Warn(logger).Log("msg", "circuit breaker is open", "previous", event.OldState, "current", event.NewState)
		}).
		OnHalfOpen(func(event circuitbreaker.StateChangedEvent) {
			circuitBreakerTransitionsCounterFn(cb.metrics, circuitbreaker.HalfOpenState).Inc()
			level.Info(logger).Log("msg", "circuit breaker is half-open", "previous", event.OldState, "current", event.NewState)
		}).
		HandleIf(func(_ any, err error) bool { return isFailure(err) })

	if cfg.testModeEnabled {
		// In case of testing purposes, we initialize the circuit breaker with count based failure thresholding,
		// since it is more deterministic, and therefore it is easier to predict the outcome.
		cbBuilder = cbBuilder.WithFailureThreshold(cfg.FailureThresholdPercentage)
	} else {
		// In case of production code, we prefer time based failure thresholding.
		cbBuilder = cbBuilder.WithFailureRateThreshold(cfg.FailureThresholdPercentage, cfg.FailureExecutionThreshold, cfg.ThresholdingPeriod)
	}

	cb.cb = cbBuilder.Build()
	cb.metrics = newCircuitBreakerMetrics(registerer, cb.cb.State)
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

func (cb *circuitBreaker) isActive() bool {
	if cb == nil {
		return false
	}
	return cb.startTime.Before(time.Now())
}

// TODO: fix the comment
// tryAcquirePermit tries to acquire a permit to use the circuit breaker and returns whether a permit was acquired.
// If the circuit breaker is not yet active, a status false and no error are returned.
// If it was not possible to acquire a permit, this means that the circuit breaker is open. In this case, a status
// false and an circuitBreakerOpenError are returned.
// If it was possible to acquire a permit, a status true and no error are returned. In this case, the permission
// will be automatically released once when a result is recorded by calling resultRecorded.
func (cb *circuitBreaker) tryAcquirePermit() (bool, error) {
	if !cb.isActive() {
		return false, nil
	}
	if !cb.cb.TryAcquirePermit() {
		cb.metrics.circuitBreakerResults.WithLabelValues(resultOpen).Inc()
		return false, newCircuitBreakerOpenError(cb.cb.RemainingDelay())
	}
	return true, nil
}

func (cb *circuitBreaker) recordResult(err error) {
	if !cb.isActive() {
		return
	}
	if err != nil && isFailure(err) {
		cb.cb.RecordFailure()
		cb.metrics.circuitBreakerResults.WithLabelValues(resultError).Inc()
	} else {
		cb.metrics.circuitBreakerResults.WithLabelValues(resultSuccess).Inc()
		cb.cb.RecordSuccess()
	}
}

// TODO: fix the comment
// finishPushRequest records the result of a push request with this circuit breaker.
// If the circuit breaker is not active, finishPushRequest does nothing.
// If the passed duration of a push request exceeds the configured maximal push request duration,
// a context.DeadlineExceeded error is recorded and returned, independently of the passed pushErr,
// the actual error that occurred during the push request.
// Otherwise, the given pushErr is recorded and returned.
// The returned error is needed only for testing purposes.
func (cb *circuitBreaker) finishPushRequest(ctx context.Context, duration time.Duration, pushErr error) error {
	if !cb.isActive() {
		return nil
	}
	if cb.cfg.testModeEnabled {
		if testDelay, ok := ctx.Value(testDelayKey).(time.Duration); ok {
			duration += testDelay
		}
	}
	if cb.cfg.PushTimeout < duration {
		pushErr = context.DeadlineExceeded
	}
	cb.recordResult(pushErr)
	return pushErr
}
