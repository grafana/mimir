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
	"go.uber.org/atomic"
	"google.golang.org/grpc/codes"
)

const (
	circuitBreakerResultSuccess      = "success"
	circuitBreakerResultError        = "error"
	circuitBreakerResultOpen         = "circuit_breaker_open"
	circuitBreakerDefaultPushTimeout = 2 * time.Second
	circuitBreakerDefaultReadTimeout = 30 * time.Second
	circuitBreakerRequestTypeLabel   = "request_type"
	circuitBreakerPushRequestType    = "push"
	circuitBreakerReadRequestType    = "read"
)

type circuitBreakerMetrics struct {
	circuitBreakerTransitions     *prometheus.CounterVec
	circuitBreakerResults         *prometheus.CounterVec
	circuitBreakerRequestTimeouts prometheus.Counter
}

func newCircuitBreakerMetrics(r prometheus.Registerer, currentState func() circuitbreaker.State, requestType string) *circuitBreakerMetrics {
	cbMetrics := &circuitBreakerMetrics{
		circuitBreakerTransitions: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_ingester_circuit_breaker_transitions_total",
			Help:        "Number of times the circuit breaker has entered a state.",
			ConstLabels: map[string]string{circuitBreakerRequestTypeLabel: requestType},
		}, []string{"state"}),
		circuitBreakerResults: promauto.With(r).NewCounterVec(prometheus.CounterOpts{
			Name:        "cortex_ingester_circuit_breaker_results_total",
			Help:        "Results of executing requests via the circuit breaker.",
			ConstLabels: map[string]string{circuitBreakerRequestTypeLabel: requestType},
		}, []string{"result"}),
		circuitBreakerRequestTimeouts: promauto.With(r).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_ingester_circuit_breaker_request_timeouts_total",
			Help:        "Number of times the circuit breaker recorded a request that reached timeout.",
			ConstLabels: map[string]string{circuitBreakerRequestTypeLabel: requestType},
		}),
	}
	circuitBreakerCurrentStateGauge := func(state circuitbreaker.State) prometheus.GaugeFunc {
		return promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
			Name:        "cortex_ingester_circuit_breaker_current_state",
			Help:        "Boolean set to 1 whenever the circuit breaker is in a state corresponding to the label name.",
			ConstLabels: map[string]string{circuitBreakerRequestTypeLabel: requestType, "state": state.String()},
		}, func() float64 {
			if currentState() == state {
				return 1
			}
			return 0
		})
	}
	for _, s := range []circuitbreaker.State{circuitbreaker.OpenState, circuitbreaker.HalfOpenState, circuitbreaker.ClosedState} {
		circuitBreakerCurrentStateGauge(s)
		// We initialize all possible states for the circuitBreakerTransitions metrics
		cbMetrics.circuitBreakerTransitions.WithLabelValues(s.String())
	}

	for _, r := range []string{circuitBreakerResultSuccess, circuitBreakerResultError, circuitBreakerResultOpen} {
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
	RequestTimeout             time.Duration `yaml:"request_timeout" category:"experimental"`
	testModeEnabled            bool          `yaml:"-"`
}

func (cfg *CircuitBreakerConfig) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet, defaultRequestDuration time.Duration) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable circuit breaking when making requests to ingesters")
	f.UintVar(&cfg.FailureThresholdPercentage, prefix+"failure-threshold-percentage", 10, "Max percentage of requests that can fail over period before the circuit breaker opens")
	f.UintVar(&cfg.FailureExecutionThreshold, prefix+"failure-execution-threshold", 100, "How many requests must have been executed in period for the circuit breaker to be eligible to open for the rate of failures")
	f.DurationVar(&cfg.ThresholdingPeriod, prefix+"thresholding-period", time.Minute, "Moving window of time that the percentage of failed requests is computed over")
	f.DurationVar(&cfg.CooldownPeriod, prefix+"cooldown-period", 10*time.Second, "How long the circuit breaker will stay in the open state before allowing some requests")
	f.DurationVar(&cfg.InitialDelay, prefix+"initial-delay", 0, "How long the circuit breaker should wait between an activation request and becoming effectively active. During that time both failures and successes will not be counted.")
	f.DurationVar(&cfg.RequestTimeout, prefix+"request-timeout", defaultRequestDuration, "The maximum duration of an ingester's request before it triggers a timeout. This configuration is used for circuit breakers only, and its timeouts aren't reported as errors.")
}

// circuitBreaker abstracts the ingester's server-side circuit breaker functionality.
// A nil *circuitBreaker is a valid noop implementation.
type circuitBreaker struct {
	cfg         CircuitBreakerConfig
	requestType string
	logger      log.Logger
	metrics     *circuitBreakerMetrics
	active      atomic.Bool
	cb          circuitbreaker.CircuitBreaker[any]

	// testRequestDelay is needed for testing purposes to simulate long-lasting requests
	testRequestDelay time.Duration
}

func newCircuitBreaker(cfg CircuitBreakerConfig, registerer prometheus.Registerer, requestType string, logger log.Logger) *circuitBreaker {
	if !cfg.Enabled {
		return nil
	}
	active := atomic.NewBool(false)
	cb := circuitBreaker{
		cfg:         cfg,
		requestType: requestType,
		logger:      logger,
		active:      *active,
	}

	circuitBreakerTransitionsCounter := func(metrics *circuitBreakerMetrics, state circuitbreaker.State) prometheus.Counter {
		return metrics.circuitBreakerTransitions.WithLabelValues(state.String())
	}

	cbBuilder := circuitbreaker.Builder[any]().
		WithDelay(cfg.CooldownPeriod).
		OnClose(func(event circuitbreaker.StateChangedEvent) {
			circuitBreakerTransitionsCounter(cb.metrics, circuitbreaker.ClosedState).Inc()
			level.Info(logger).Log("msg", "circuit breaker is closed", "previous", event.OldState, "current", event.NewState, "requestType", requestType)
		}).
		OnOpen(func(event circuitbreaker.StateChangedEvent) {
			circuitBreakerTransitionsCounter(cb.metrics, circuitbreaker.OpenState).Inc()
			level.Warn(logger).Log("msg", "circuit breaker is open", "previous", event.OldState, "current", event.NewState, "requestType", requestType)
		}).
		OnHalfOpen(func(event circuitbreaker.StateChangedEvent) {
			circuitBreakerTransitionsCounter(cb.metrics, circuitbreaker.HalfOpenState).Inc()
			level.Info(logger).Log("msg", "circuit breaker is half-open", "previous", event.OldState, "current", event.NewState, "requestType", requestType)
		})

	if cfg.testModeEnabled {
		// In case of testing purposes, we initialize the circuit breaker with count based failure thresholding,
		// since it is more deterministic, and therefore it is easier to predict the outcome.
		cbBuilder = cbBuilder.WithFailureThreshold(cfg.FailureThresholdPercentage)
	} else {
		// In case of production code, we prefer time based failure thresholding.
		cbBuilder = cbBuilder.WithFailureRateThreshold(cfg.FailureThresholdPercentage, cfg.FailureExecutionThreshold, cfg.ThresholdingPeriod)
	}

	cb.cb = cbBuilder.Build()
	cb.metrics = newCircuitBreakerMetrics(registerer, cb.cb.State, requestType)
	return &cb
}

func isDeadlineExceeded(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	statusCode := grpcutil.ErrorToStatusCode(err)
	return statusCode == codes.DeadlineExceeded
}

func (cb *circuitBreaker) tryRecordFailure(err error) bool {
	if err == nil {
		return false
	}

	// We only consider timeouts to be errors worthy of tripping the circuit breaker
	// since these are specific to a particular ingester, not a user or request.
	if isDeadlineExceeded(err) {
		cb.metrics.circuitBreakerRequestTimeouts.Inc()
		cb.metrics.circuitBreakerResults.WithLabelValues(circuitBreakerResultError).Inc()
		cb.cb.RecordFailure()
		return true
	}
	return false
}

func (cb *circuitBreaker) isActive() bool {
	return cb != nil && cb.active.Load()
}

func (cb *circuitBreaker) activate() {
	if cb == nil {
		return
	}
	if cb.cfg.InitialDelay == 0 {
		level.Info(cb.logger).Log("msg", "activating circuit breaker", "requestType", cb.requestType)
		cb.active.Store(true)
		return
	}
	time.AfterFunc(cb.cfg.InitialDelay, func() {
		level.Info(cb.logger).Log("msg", "activating circuit breaker", "requestType", cb.requestType)
		cb.active.Store(true)
	})
}

func (cb *circuitBreaker) deactivate() {
	if cb == nil {
		return
	}
	level.Info(cb.logger).Log("msg", "deactivating circuit breaker", "requestType", cb.requestType)
	cb.active.Store(false)
}

func (cb *circuitBreaker) isOpen() bool {
	if !cb.isActive() {
		return false
	}
	return cb.cb.IsOpen()
}

// tryAcquirePermit tries to acquire a permit to use the circuit breaker and returns whether a permit was acquired.
// If it was possible to acquire a permit, it returns a function that should be called to release the acquired permit.
// If it was not possible, the causing error is returned.
func (cb *circuitBreaker) tryAcquirePermit() (func(time.Duration, error), error) {
	if !cb.isActive() {
		return func(time.Duration, error) {}, nil
	}

	if !cb.cb.TryAcquirePermit() {
		cb.metrics.circuitBreakerResults.WithLabelValues(circuitBreakerResultOpen).Inc()
		return nil, newCircuitBreakerOpenError(cb.requestType, cb.cb.RemainingDelay())
	}

	return func(duration time.Duration, err error) {
		_ = cb.finishRequest(duration, cb.cfg.RequestTimeout, err)
	}, nil
}

// finishRequest completes a request executed upon a successfully acquired circuit breaker permit.
// It records the result of the request with the circuit breaker. Requests that lasted longer than
// the given maximumAllowedDuration are treated as a failure.
// The returned error is only used for testing purposes.
func (cb *circuitBreaker) finishRequest(actualDuration time.Duration, maximumAllowedDuration time.Duration, err error) error {
	if !cb.isActive() {
		return nil
	}
	if cb.cfg.testModeEnabled {
		actualDuration += cb.testRequestDelay
	}
	var deadlineErr error
	if maximumAllowedDuration < actualDuration {
		deadlineErr = context.DeadlineExceeded
	}
	return cb.recordResult(err, deadlineErr)
}

func (cb *circuitBreaker) recordResult(errs ...error) error {
	if !cb.isActive() {
		return nil
	}

	for _, err := range errs {
		if cb.tryRecordFailure(err) {
			return err
		}
	}
	cb.cb.RecordSuccess()
	cb.metrics.circuitBreakerResults.WithLabelValues(circuitBreakerResultSuccess).Inc()
	return nil
}

type ingesterCircuitBreaker struct {
	push *circuitBreaker
	read *circuitBreaker
}

func newIngesterCircuitBreaker(pushCfg CircuitBreakerConfig, readCfg CircuitBreakerConfig, logger log.Logger, registerer prometheus.Registerer) ingesterCircuitBreaker {
	return ingesterCircuitBreaker{
		push: newCircuitBreaker(pushCfg, registerer, circuitBreakerPushRequestType, logger),
		read: newCircuitBreaker(readCfg, registerer, circuitBreakerReadRequestType, logger),
	}
}

func (cb *ingesterCircuitBreaker) activate() {
	cb.push.activate()
	cb.read.activate()
}

// tryAcquirePushPermit tries to acquire a permit to use the push circuit breaker and returns whether a permit was acquired.
// If it was possible, tryAcquirePushPermit returns a function that should be called to release the acquired permit.
// If it was not possible, the causing error is returned.
func (cb *ingesterCircuitBreaker) tryAcquirePushPermit() (func(time.Duration, error), error) {
	return cb.push.tryAcquirePermit()
}

// tryAcquireReadPermit tries to acquire a permit to use the read circuit breaker and returns whether a permit was acquired.
// If it was possible, tryAcquireReadPermit returns a function that should be called to release the acquired permit.
// If it was not possible, the causing error is returned.
func (cb *ingesterCircuitBreaker) tryAcquireReadPermit() (func(time.Duration, error), error) {
	// If the read circuit breaker is not active, we don't try to acquire a permit.
	if !cb.read.isActive() {
		return func(time.Duration, error) {}, nil
	}

	// We don't want to allow read requests if the push circuit breaker is open.
	if cb.push.isOpen() {
		return nil, newCircuitBreakerOpenError(cb.push.requestType, cb.push.cb.RemainingDelay())
	}

	return cb.read.tryAcquirePermit()
}
