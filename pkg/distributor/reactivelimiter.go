// SPDX-License-Identifier: AGPL-3.0-only

package distributor

import (
	"context"
	"errors"
	"flag"
	"time"

	"github.com/failsafe-go/failsafe-go/adaptivelimiter"
	"github.com/failsafe-go/failsafe-go/priority"
	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/limiter"
	"github.com/grafana/mimir/pkg/util/reactivelimiter"
)

const memoryUtilizationLimitFlag = "distributor.reactive-limiter.memory-utilization-limit"

var (
	errReactiveLimiterLimitExceeded = newReactiveLimiterExceededError(adaptivelimiter.ErrExceeded)

	errReactiveLimiterMemoryUtilizationExceeded = newReactiveLimiterExceededError(errors.New(globalerror.DistributorMemoryUtilizationLimit.MessageWithPerInstanceLimitConfig(
		"the write request has been rejected because the distributor memory utilization is above the configured limit", memoryUtilizationLimitFlag)))
)

// ReactiveLimiterConfig extends the reactive limiter config with distributor-specific admission control options.
type ReactiveLimiterConfig struct {
	reactivelimiter.Config `yaml:",inline"`

	MemoryUtilizationLimit uint64 `yaml:"memory_utilization_limit" category:"experimental"`

	TenantFairnessEnabled             bool          `yaml:"tenant_fairness_enabled" category:"experimental"`
	TenantFairnessWindow              time.Duration `yaml:"tenant_fairness_window" category:"experimental"`
	TenantFairnessMaxTenants          int           `yaml:"tenant_fairness_max_tenants" category:"experimental"`
	TenantFairnessCalibrationInterval time.Duration `yaml:"tenant_fairness_calibration_interval" category:"experimental"`
}

func (cfg *ReactiveLimiterConfig) RegisterFlags(f *flag.FlagSet) {
	cfg.RegisterFlagsWithPrefixAndRejectionFactors("distributor.reactive-limiter.", f, 1.0, 2.0)
	f.Uint64Var(&cfg.MemoryUtilizationLimit, memoryUtilizationLimitFlag, 0, "Go heap size, in bytes, above which the distributor rejects new push requests instead of queueing them. 0 to disable.")
	f.BoolVar(&cfg.TenantFairnessEnabled, "distributor.reactive-limiter.tenant-fairness-enabled", false, "When the reactive limiter is overloaded, reject requests from tenants with the highest recent in-flight bytes first. Requires the reactive limiter to be enabled.")
	f.DurationVar(&cfg.TenantFairnessWindow, "distributor.reactive-limiter.tenant-fairness-window", time.Minute, "Window over which per-tenant usage is tracked for fair rejection.")
	f.IntVar(&cfg.TenantFairnessMaxTenants, "distributor.reactive-limiter.tenant-fairness-max-tenants", 20000, "Maximum number of tenants tracked for fair rejection. The least recently active tenants are evicted first.")
	f.DurationVar(&cfg.TenantFairnessCalibrationInterval, "distributor.reactive-limiter.tenant-fairness-calibration-interval", time.Second, "Interval at which per-tenant usage and the rejection threshold are recalibrated.")
}

func (cfg *ReactiveLimiterConfig) Validate() error {
	if err := cfg.Config.Validate(); err != nil {
		return err
	}
	if cfg.TenantFairnessEnabled && !cfg.Enabled {
		return errors.New("distributor reactive limiter tenant fairness requires the reactive limiter to be enabled")
	}
	return nil
}

// distributorReactiveLimiter is the distributor's admission control. It combines the latency-based reactive limiter,
// an optional memory utilization guard, and optional fair rejection of the tenants using the most in-flight bytes.
type distributorReactiveLimiter struct {
	limiter      reactivelimiter.ReactiveLimiter
	usageTracker priority.UsageTracker
	memory       interface{ LimitingReason() string }
	services     []services.Service
}

func newDistributorReactiveLimiter(cfg ReactiveLimiterConfig, logger log.Logger, registerer prometheus.Registerer) *distributorReactiveLimiter {
	l := &distributorReactiveLimiter{}

	if cfg.MemoryUtilizationLimit > 0 {
		memory := limiter.NewUtilizationBasedLimiter(0, cfg.MemoryUtilizationLimit, false, log.With(logger, "context", "distributor reactive limiter"),
			prometheus.WrapRegistererWithPrefix("cortex_distributor_reactive_limiter_", registerer))
		l.memory = memory
		l.services = append(l.services, memory)
	}

	if cfg.Enabled {
		if cfg.TenantFairnessEnabled {
			l.usageTracker = priority.NewUsageTracker(cfg.TenantFairnessWindow, cfg.TenantFairnessMaxTenants)
			prioritizer := reactivelimiter.NewPrioritizerWithUsageTracker(logger, l.usageTracker)
			l.limiter = reactivelimiter.NewWithPriority(&cfg.Config, logger, priority.High, prioritizer.Prioritizer)
			l.services = append(l.services, services.NewTimerService(cfg.TenantFairnessCalibrationInterval, nil, func(context.Context) error {
				l.usageTracker.Calibrate()
				prioritizer.Calibrate()
				return nil
			}, nil))
		} else {
			l.limiter = reactivelimiter.New(&cfg.Config, logger)
		}
		registerReactiveLimiterMetrics(l.limiter.Metrics(), registerer)
	}

	if l.limiter == nil && l.memory == nil {
		return nil
	}
	return l
}

// overMemoryLimit returns true when the distributor should reject new requests outright because of memory pressure.
func (l *distributorReactiveLimiter) overMemoryLimit() bool {
	return l != nil && l.memory != nil && l.memory.LimitingReason() != ""
}

// canAcquirePermit is a cheap pre-check, run before the request body is read.
func (l *distributorReactiveLimiter) canAcquirePermit() bool {
	return l.limiter == nil || l.limiter.CanAcquirePermit()
}

// acquirePermit acquires a permit for the tenant, blocking if the limiter is full.
// The returned release function records the request's in-flight bytes as the tenant's usage when fairness is enabled.
func (l *distributorReactiveLimiter) acquirePermit(ctx context.Context, userID string) (release func(dropped bool, usageBytes int64), _ error) {
	if l.limiter == nil {
		return func(bool, int64) {}, nil
	}
	if l.usageTracker != nil {
		ctx = priority.ContextWithUser(ctx, userID)
	}
	permit, err := l.limiter.AcquirePermit(ctx)
	if err != nil {
		return nil, err
	}
	return func(dropped bool, usageBytes int64) {
		if dropped {
			permit.Drop()
			return
		}
		if up, ok := permit.(interface{ RecordUsage(string, int64) }); ok && l.usageTracker != nil {
			up.RecordUsage(userID, usageBytes)
			return
		}
		permit.Record()
	}, nil
}

func registerReactiveLimiterMetrics(limiterMetrics adaptivelimiter.Metrics, r prometheus.Registerer) {
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_reactive_limiter_inflight_limit",
		Help: "Distributor reactive limiter inflight request limit.",
	}, func() float64 {
		return float64(limiterMetrics.Limit())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_reactive_limiter_inflight_requests",
		Help: "Distributor reactive limiter inflight requests.",
	}, func() float64 {
		return float64(limiterMetrics.MaxInflight())
	})
	promauto.With(r).NewGaugeFunc(prometheus.GaugeOpts{
		Name: "cortex_distributor_reactive_limiter_queued_requests",
		Help: "Distributor reactive limiter queued requests.",
	}, func() float64 {
		return float64(limiterMetrics.Queued())
	})
}
