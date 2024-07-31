// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"sync"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/rules"
	"golang.org/x/sync/semaphore"
)

type MultiTenantRuleConcurrencyController interface {
	// NewTenantConcurrencyController returns a new rules.RuleConcurrencyController to use for the input tenantID.
	NewTenantConcurrencyController(tenantID string) rules.RuleConcurrencyController
}

// DynamicSemaphore is a semaphore that can dynamically change its max concurrency.
// It is necessary as the max concurrency is defined by the user limits which can be changed at runtime.
type DynamicSemaphore struct {
	maxConcurrency func() int64

	acquiredMtx sync.Mutex
	acquired    int64
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
	maxTokens := ds.maxConcurrency()

	ds.acquiredMtx.Lock()
	defer ds.acquiredMtx.Unlock()

	if ds.acquired < maxTokens {
		ds.acquired++
		return true
	}

	return false
}

// Release releases a token back to the semaphore.
func (ds *DynamicSemaphore) Release() {
	ds.acquiredMtx.Lock()
	defer ds.acquiredMtx.Unlock()
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
			Name: "cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use",
			Help: "Current number of concurrency slots currently in use across all tenants",
		}),
		AcquireTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total",
			Help: "Total number of started attempts to acquire concurrency slots across all tenants",
		}),
		AcquireFailedTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total",
			Help: "Total number of incomplete attempts to acquire concurrency slots across all tenants",
		}),
		ReleaseTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total",
			Help: "Total number of concurrency slots we're done using across all tenants",
		}),
	}
}

// MultiTenantConcurrencyController is a concurrency controller that limits the number of concurrent rule evaluations both global and per tenant.
// TODO update doc
type MultiTenantConcurrencyController struct {
	logger                   log.Logger
	limits                   RulesLimits
	thresholdRuleConcurrency float64 // Percentage of the rule interval at which we consider the rule group at risk of missing its evaluation.
	metrics                  *MultiTenantConcurrencyControllerMetrics

	globalConcurrency *semaphore.Weighted
}

// NewMultiTenantConcurrencyController creates a new MultiTenantConcurrencyController.
func NewMultiTenantConcurrencyController(logger log.Logger, maxGlobalConcurrency int64, ThresholdRuleConcurrency float64, reg prometheus.Registerer, limits RulesLimits) *MultiTenantConcurrencyController {
	return &MultiTenantConcurrencyController{
		metrics:                  newMultiTenantConcurrencyControllerMetrics(reg),
		logger:                   log.With(logger, "component", "concurrency-controller"),
		thresholdRuleConcurrency: ThresholdRuleConcurrency,
		limits:                   limits,
		globalConcurrency:        semaphore.NewWeighted(maxGlobalConcurrency),
	}
}

func (c *MultiTenantConcurrencyController) NewTenantConcurrencyController(tenantID string) rules.RuleConcurrencyController {
	return &TenantConcurrencyController{
		thresholdRuleConcurrency: c.thresholdRuleConcurrency,
		metrics:                  c.metrics,
		globalConcurrency:        c.globalConcurrency,
		tenantConcurrency: NewDynamicSemaphore(func() int64 {
			return c.limits.RulerMaxIndependentRuleEvaluationConcurrencyPerTenant(tenantID)
		}),
	}
}

// TODO document what this is
type TenantConcurrencyController struct {
	thresholdRuleConcurrency float64 // Percentage of the rule interval at which we consider the rule group at risk of missing its evaluation.
	metrics                  *MultiTenantConcurrencyControllerMetrics

	globalConcurrency *semaphore.Weighted
	tenantConcurrency *DynamicSemaphore
}

// Done releases a slot from the concurrency controller.
func (c *TenantConcurrencyController) Done(_ context.Context) {
	c.globalConcurrency.Release(1)
	c.metrics.CurrentConcurrency.Dec()
	c.metrics.ReleaseTotal.Inc()

	c.tenantConcurrency.Release()
}

// Allow tries to acquire a slot from the concurrency controller.
func (c *TenantConcurrencyController) Allow(_ context.Context, group *rules.Group, rule rules.Rule) bool {
	// To allow a rule to be executed concurrently, we need 3 conditions:
	// 1. The rule group must be at risk of missing its evaluation.
	// 2. The rule must not have any rules that depend on it.
	// 3. The rule itself must not depend on any other rules.
	if !c.isGroupAtRisk(group) {
		return false
	}

	if !isRuleIndependent(rule) {
		return false
	}

	// Next, try to acquire a global concurrency slot.
	c.metrics.AcquireTotal.Inc()
	if !c.globalConcurrency.TryAcquire(1) {
		c.metrics.AcquireFailedTotal.Inc()
		return false
	}
	c.metrics.CurrentConcurrency.Inc()

	if c.tenantConcurrency.TryAcquire() {
		return true
	}

	// If we can't acquire a tenant slot, release the global slot.
	c.globalConcurrency.Release(1)
	c.metrics.CurrentConcurrency.Dec()
	c.metrics.AcquireFailedTotal.Inc()
	return false
}

// isGroupAtRisk checks if the rule group's last evaluation time is within the risk threshold.
func (c *TenantConcurrencyController) isGroupAtRisk(group *rules.Group) bool {
	interval := group.Interval().Seconds()
	lastEvaluation := group.GetEvaluationTime().Seconds()

	return lastEvaluation >= interval*c.thresholdRuleConcurrency/100
}

// isRuleIndependent checks if the rule is independent of other rules.
func isRuleIndependent(rule rules.Rule) bool {
	return rule.NoDependentRules() && rule.NoDependencyRules()
}

// NoopMultiTenantConcurrencyController is a concurrency controller that does not allow for concurrency.
type NoopMultiTenantConcurrencyController struct{}

func (n *NoopMultiTenantConcurrencyController) NewTenantConcurrencyController(_ string) rules.RuleConcurrencyController {
	return &NoopTenantConcurrencyController{}
}

// TODO doc
type NoopTenantConcurrencyController struct{}

func (n *NoopTenantConcurrencyController) Done(_ context.Context) {}
func (n *NoopTenantConcurrencyController) Allow(_ context.Context, _ *rules.Group, _ rules.Rule) bool {
	return false
}
