// SPDX-License-Identifier: AGPL-3.0-only

package reactivelimiter

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/failsafe-go/failsafe-go/priority"
	"github.com/go-kit/log"

	util_log "github.com/grafana/mimir/pkg/util/log"
)

const (
	defaultInitialRejectionFactor = 2.0
	defaultMaxRejectionFactor     = 3.0
)

type Config struct {
	Enabled bool `yaml:"enabled" category:"experimental"`

	MinLimit            uint    `yaml:"min_limit" category:"experimental"`
	MaxLimit            uint    `yaml:"max_limit" category:"experimental"`
	InitialLimit        uint    `yaml:"initial_limit" category:"experimental"`
	MaxLimitFactor      float64 `yaml:"max_limit_factor" category:"experimental"`
	MaxLimitFactorDecay float64 `yaml:"max_limit_factor_decay" category:"experimental"`
	MinLimitFactor      float64 `yaml:"min_limit_factor" category:"experimental"`

	RecentWindowMinDuration time.Duration `yaml:"recent_window_min_duration" category:"experimental"`
	RecentWindowMaxDuration time.Duration `yaml:"recent_window_max_duration" category:"experimental"`
	RecentWindowMinSamples  uint          `yaml:"recent_window_min_samples" category:"experimental"`
	RecentQuantile          float64       `yaml:"recent_quantile" category:"experimental"`
	BaselienWindowAge       uint          `yaml:"baseline_window_age" category:"experimental"`
	CorrelationWindow       uint          `yaml:"correlation_window" category:"experimental"`

	InitialRejectionFactor float64 `yaml:"initial_rejection_factor" category:"experimental"`
	MaxRejectionFactor     float64 `yaml:"max_rejection_factor" category:"experimental"`
}

func (cfg *Config) Validate() error {
	if cfg.MinLimitFactor < 1 {
		return fmt.Errorf("min_limit_factor must be >= 1")
	}
	return nil
}

func (cfg *Config) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefixAndRejectionFactors(prefix, f, defaultInitialRejectionFactor, defaultMaxRejectionFactor)
}

func (cfg *Config) RegisterFlagsWithPrefixAndRejectionFactors(prefix string, f *flag.FlagSet, initRejectionFactor float64, maxRejectionFactor float64) {
	f.BoolVar(&cfg.Enabled, prefix+"enabled", false, "Enable reactive limiting when making requests to a service")

	f.UintVar(&cfg.MinLimit, prefix+"min-limit", 2, "Minimum inflight requests limit")
	f.UintVar(&cfg.MaxLimit, prefix+"max-limit", 200, "Maximum inflight requests limit")
	f.UintVar(&cfg.InitialLimit, prefix+"initial-limit", 20, "Initial inflight requests limit")
	f.Float64Var(&cfg.MaxLimitFactor, prefix+"max-limit-factor", 5, "The maximum inflight limit as a multiple of current inflight requests")
	f.Float64Var(&cfg.MaxLimitFactorDecay, prefix+"max-limit-factor-decay", 1, "Logarithmic decay applied to the maxLimitFactor based on current inflight requests")
	f.Float64Var(&cfg.MinLimitFactor, prefix+"min-limit-factor", 1.2, "Minimum limit factor when max-limit-factor-decay is applied")

	f.DurationVar(&cfg.RecentWindowMinDuration, prefix+"recent-window-min-duration", time.Second, "Minimum duration of the window that is used to collect recent response time samples")
	f.DurationVar(&cfg.RecentWindowMaxDuration, prefix+"recent-window-max-duration", 30*time.Second, "Maximum duration of the window that is used to collect recent response time samples")
	f.UintVar(&cfg.RecentWindowMinSamples, prefix+"recent-window-min-samples", 50, "Minimum number of samples that must be recorded in the recent window before updating the limit")
	f.Float64Var(&cfg.RecentQuantile, prefix+"recent-quantile", .9, "The quantile of recent recorded response times to consider when adjusting the concurrency limit")
	f.UintVar(&cfg.BaselienWindowAge, prefix+"baseline-window-age", 10, "The average age of baseline samples aggregated recent samples are added to")
	f.UintVar(&cfg.CorrelationWindow, prefix+"correlation-window", 50, "How many recent limit and inflight time measurements are stored to detect whether increases in limits correlate with increases in inflight times")

	f.Float64Var(&cfg.InitialRejectionFactor, prefix+"initial-rejection-factor", initRejectionFactor, "The number of allowed queued requests, as a multiple of current inflight requests, after which rejections start")
	f.Float64Var(&cfg.MaxRejectionFactor, prefix+"max-rejection-factor", maxRejectionFactor, "The number of allowed queued requests, as a multiple of current inflight requests, after which all requests are rejected")
}

// New returns a new ReactiveLimiter.
func New(c *Config, logger log.Logger) ReactiveLimiter {
	return &reactiveLimiter{
		limiter: buildBase(c, logger).Build(),
	}
}

// NewWithPriority returns a new ReactiveLimiter that prioritizes executions, and always tries to acquire a permit with
// the priority.
func NewWithPriority(c *Config, logger log.Logger, priority priority.Priority, prioritizer priority.Prioritizer) ReactiveLimiter {
	return &priorityLimiter{
		limiter:  buildBase(c, logger).BuildPrioritized(prioritizer),
		Priority: priority,
	}
}

func buildBase(c *Config, logger log.Logger) adaptivelimiter.Builder[any] {
	return adaptivelimiter.NewBuilder[any]().
		WithLimits(c.MinLimit, c.MaxLimit, c.InitialLimit).
		WithMaxLimitFactor(c.MaxLimitFactor).
		WithMaxLimitFactorDecay(c.MaxLimitFactorDecay, c.MinLimitFactor).
		WithRecentWindow(c.RecentWindowMinDuration, c.RecentWindowMaxDuration, c.RecentWindowMinSamples).
		WithRecentQuantile(c.RecentQuantile).
		WithBaselineWindow(c.BaselienWindowAge).
		WithCorrelationWindow(c.CorrelationWindow).
		WithQueueing(c.InitialRejectionFactor, c.MaxRejectionFactor).
		WithLogger(util_log.SlogFromGoKit(logger))
}

type ReactiveLimiter interface {
	AcquirePermit(ctx context.Context) (adaptivelimiter.Permit, error)

	CanAcquirePermit() bool

	Metrics() adaptivelimiter.Metrics

	Reset()
}

type reactiveLimiter struct {
	limiter adaptivelimiter.AdaptiveLimiter[any]
}

func (l *reactiveLimiter) AcquirePermit(ctx context.Context) (adaptivelimiter.Permit, error) {
	return l.limiter.AcquirePermit(ctx)
}

func (l *reactiveLimiter) CanAcquirePermit() bool {
	return l.limiter.CanAcquirePermit()
}

func (l *reactiveLimiter) Metrics() adaptivelimiter.Metrics {
	return l.limiter
}

func (l *reactiveLimiter) Reset() {
	l.limiter.Reset()
}

type priorityLimiter struct {
	limiter adaptivelimiter.PriorityLimiter[any]
	priority.Priority
}

func (l *priorityLimiter) AcquirePermit(ctx context.Context) (adaptivelimiter.Permit, error) {
	return l.limiter.AcquirePermitWithPriority(ctx, l.Priority)
}

func (l *priorityLimiter) CanAcquirePermit() bool {
	return l.limiter.CanAcquirePermitWithPriority(context.Background(), l.Priority)
}

func (l *priorityLimiter) Metrics() adaptivelimiter.Metrics {
	return l.limiter
}

func (l *priorityLimiter) Reset() {
	l.limiter.Reset()
}
