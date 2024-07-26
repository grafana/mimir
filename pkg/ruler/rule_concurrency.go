// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/rules"
	"golang.org/x/sync/semaphore"
)

type MultiTenantRuleConcurrencyController interface {
	rules.RuleConcurrencyController
}

const IntervalToEvaluationThreshold = 50.00

// DynamicSemaphore is a semaphore that can dynamically change its max concurrency.
// It is necessary as the max concurrency is defined by the user limits which can be changed at runtime.
type DynamicSemaphore struct {
	maxConcurrency func() int64

	mtx      sync.Mutex
	acquired int64
}

// NewDynamicSemaphore creates a new DynamicSemaphore
func NewDynamicSemaphore(maxConcurrency func() int64) *DynamicSemaphore {
	return &DynamicSemaphore{
		maxConcurrency: maxConcurrency,
		acquired:       0,
	}
}

// TryAcquire tries to acquire a token from the semaphore.
func (ds *DynamicSemaphore) TryAcquire() bool {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()
	maxTokens := ds.maxConcurrency()
	if ds.acquired < maxTokens {
		ds.acquired++
		return true
	}

	return false
}

// Release releases a token back to the semaphore.
func (ds *DynamicSemaphore) Release() {
	ds.mtx.Lock()
	defer ds.mtx.Unlock()
	if ds.acquired > 0 {
		ds.acquired--
	} else {
		panic("dynamic semaphore: release called without corresponding acquire")
	}
}

type MultiTenantConcurrencyControllerMetrics struct {
	CurrentConcurrency prometheus.Gauge
	AcquireTotal       prometheus.Counter
	AcquireFailedTotal prometheus.Counter
	ReleaseTotal       prometheus.Counter
}

func newMultiTenantConcurrencyControllerMetrics(reg prometheus.Registerer) *MultiTenantConcurrencyControllerMetrics {
	return &MultiTenantConcurrencyControllerMetrics{
		CurrentConcurrency: promauto.With(reg).NewGauge(prometheus.GaugeOpts{
			Name: "cortex_ruler_global_rule_evaluation_concurrency_current",
			Help: "Current number of active global concurrency slots",
		}),
		AcquireTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ruler_global_rule_evaluation_concurrency_acquire_total",
			Help: "Total number of acquired concurrency slots",
		}),
		AcquireFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total",
			Help: "Total number of failed attempts to acquire concurrency slots",
		}),
		ReleaseTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ruler_global_rule_evaluation_concurrency_release_total",
			Help: "Total number of released concurrency slots",
		}),
	}
}

// MultiTenantConcurrencyController is a concurrency controller that limits the number of concurrent rule evaluations both global and per tenant.
type MultiTenantConcurrencyController struct {
	logger               log.Logger
	limits               RulesLimits
	maxGlobalConcurrency int64
	metrics              *MultiTenantConcurrencyControllerMetrics

	globalConcurrency semaphore.Weighted
	mtx               sync.Mutex
	tenantConcurrency map[string]*DynamicSemaphore
}

// NewMultiTenantConcurrencyController creates a new MultiTenantConcurrencyController.
func NewMultiTenantConcurrencyController(logger log.Logger, maxGlobalConcurrency int64, limits RulesLimits, reg prometheus.Registerer) *MultiTenantConcurrencyController {
	return &MultiTenantConcurrencyController{
		metrics:              newMultiTenantConcurrencyControllerMetrics(reg),
		logger:               log.With(logger, "component", "ruler.MultiTenantConcurrencyController"),
		maxGlobalConcurrency: maxGlobalConcurrency,
		limits:               limits,
		globalConcurrency:    *semaphore.NewWeighted(maxGlobalConcurrency),
		tenantConcurrency:    make(map[string]*DynamicSemaphore),
	}
}

// Done releases a slot from the concurrency controller.
func (c *MultiTenantConcurrencyController) Done(ctx context.Context) {
	c.globalConcurrency.Release(1)
	c.metrics.CurrentConcurrency.Dec()
	c.metrics.ReleaseTotal.Inc()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		level.Error(c.logger).Log("msg", "error extracting org id from context while releasing a slot", "err", err)
		return
	}

	c.mtx.Lock()
	tc, ok := c.tenantConcurrency[userID]
	c.mtx.Unlock()

	if ok {
		tc.Release()
	} else {
		c.logger.Log("msg", "tenant concurrency controller: tenant not found", "tenant", userID)
	}
}

// Allow tries to acquire a slot from the concurrency controller.
func (c *MultiTenantConcurrencyController) Allow(ctx context.Context, group *rules.Group, rule rules.Rule) bool {
	c.metrics.AcquireTotal.Inc()
	// To allow a rule to be executed concurrently, we need 3 conditions:
	// 1. The rule group must be at risk of missing its evaluation.
	// 2. The rule must not have any rules that depend on it.
	// 3. The rule itself must not depend on any other rules.
	if !isGroupAtRisk(group) {
		return false
	}

	if !isRuleIndependent(rule) {
		return false
	}

	// Next, try to acquire a global concurrency slot.
	if !c.globalConcurrency.TryAcquire(1) {
		c.metrics.AcquireFailedTotal.Inc()
		return false
	}
	c.metrics.CurrentConcurrency.Inc()

	userID, err := tenant.TenantID(ctx)
	if err != nil {
		level.Error(c.logger).Log("msg", "error extracting org id from context while acquiring a slot", "err", err)
		c.globalConcurrency.Release(1)
		c.metrics.CurrentConcurrency.Dec()
		c.metrics.AcquireFailedTotal.Inc()
		return false
	}

	c.mtx.Lock()
	tc, ok := c.tenantConcurrency[userID]
	if !ok {
		tc = NewDynamicSemaphore(func() int64 {
			return c.limits.RulerMaxIndependentRuleEvaluationConcurrencyPerTenant(userID)
		})
		c.tenantConcurrency[userID] = tc
	}
	c.mtx.Unlock()

	if tc.TryAcquire() {
		return true
	}

	// If we can't acquire a tenant slot, release the global slot.
	c.globalConcurrency.Release(1)
	c.metrics.CurrentConcurrency.Dec()
	c.metrics.AcquireFailedTotal.Inc()
	return false
}

// isRuleIndependent checks if the rule is independent of other rules.
func isRuleIndependent(rule rules.Rule) bool {
	return rule.NoDependentRules() && rule.NoDependencyRules()
}

// isGroupAtRisk checks if the rule group's last evaluation time is within the risk threshold.
func isGroupAtRisk(group *rules.Group) bool {
	interval := group.Interval().Seconds()
	lastEvaluation := group.GetEvaluationTime().Seconds()

	return lastEvaluation < interval*IntervalToEvaluationThreshold/100
}

// NoopConcurrencyController is a concurrency controller that does not allow for concurrency.
type NoopConcurrencyController struct{}

func (n *NoopConcurrencyController) Done(_ context.Context) {}
func (n *NoopConcurrencyController) Allow(_ context.Context, _ *rules.Group, _ rules.Rule) bool {
	return false
}
