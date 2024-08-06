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
	// NewTenantConcurrencyControllerFor returns a new rules.RuleConcurrencyController to use for the input tenantID.
	NewTenantConcurrencyControllerFor(tenantID string) rules.RuleConcurrencyController
}

// DynamicSemaphore is a semaphore that can dynamically change its max concurrency.
// It is necessary as the max concurrency is defined by the tenant limits which can be changed at runtime.
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
	SlotsInUse              *prometheus.GaugeVec
	AttemptsStartedTotal    *prometheus.CounterVec
	AttemptsIncompleteTotal *prometheus.CounterVec
	AttemptsCompletedTotal  *prometheus.CounterVec
}

func newMultiTenantConcurrencyControllerMetrics(reg prometheus.Registerer) *MultiTenantConcurrencyControllerMetrics {
	m := &MultiTenantConcurrencyControllerMetrics{
		SlotsInUse: promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use",
			Help: "Current number of concurrency slots currently in use across all tenants",
		}, []string{"user"}),
		AttemptsStartedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total",
			Help: "Total number of started attempts to acquire concurrency slots across all tenants",
		}, []string{"user"}),
		AttemptsIncompleteTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total",
			Help: "Total number of incomplete attempts to acquire concurrency slots across all tenants",
		}, []string{"user"}),
		AttemptsCompletedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total",
			Help: "Total number of concurrency slots we're done using across all tenants",
		}, []string{"user"}),
	}

	return m
}

// MultiTenantConcurrencyController instantiates concurrency controllers per tenant that limits the number of concurrent rule evaluations both global and per tenant.
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

// NewTenantConcurrencyControllerFor returns a new rules.RuleConcurrencyController to use for the input tenantID.
func (c *MultiTenantConcurrencyController) NewTenantConcurrencyControllerFor(tenantID string) rules.RuleConcurrencyController {
	return &TenantConcurrencyController{
		slotsInUse:              c.metrics.SlotsInUse.WithLabelValues(tenantID),
		attemptsStartedTotal:    c.metrics.AttemptsStartedTotal.WithLabelValues(tenantID),
		attemptsIncompleteTotal: c.metrics.AttemptsIncompleteTotal.WithLabelValues(tenantID),
		attemptsCompletedTotal:  c.metrics.AttemptsCompletedTotal.WithLabelValues(tenantID),

		tenantID:                 tenantID,
		thresholdRuleConcurrency: c.thresholdRuleConcurrency,
		globalConcurrency:        c.globalConcurrency,
		tenantConcurrency: NewDynamicSemaphore(func() int64 {
			return c.limits.RulerMaxIndependentRuleEvaluationConcurrencyPerTenant(tenantID)
		}),
	}
}

// TenantConcurrencyController is a concurrency controller that limits the number of concurrent rule evaluations per tenant.
// It also takes into account the global concurrency limit.
type TenantConcurrencyController struct {
	tenantID                 string
	thresholdRuleConcurrency float64 // Percentage of the rule interval at which we consider the rule group at risk of missing its evaluation.

	// Metrics with the tenant label already in them. This avoids having to call WithLabelValues on every metric change.
	slotsInUse              prometheus.Gauge
	attemptsStartedTotal    prometheus.Counter
	attemptsIncompleteTotal prometheus.Counter
	attemptsCompletedTotal  prometheus.Counter

	globalConcurrency *semaphore.Weighted
	tenantConcurrency *DynamicSemaphore
}

// Done releases a slot from the concurrency controller.
func (c *TenantConcurrencyController) Done(_ context.Context) {
	c.globalConcurrency.Release(1)
	c.slotsInUse.Dec()
	c.attemptsCompletedTotal.Inc()

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
	c.attemptsStartedTotal.Inc()
	if !c.globalConcurrency.TryAcquire(1) {
		c.attemptsIncompleteTotal.Inc()
		return false
	}
	c.slotsInUse.Inc()

	if c.tenantConcurrency.TryAcquire() {
		return true
	}

	// If we can't acquire a tenant slot, release the global slot.
	c.globalConcurrency.Release(1)
	c.slotsInUse.Dec()
	c.attemptsIncompleteTotal.Inc()
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

func (n *NoopMultiTenantConcurrencyController) NewTenantConcurrencyControllerFor(_ string) rules.RuleConcurrencyController {
	return &NoopTenantConcurrencyController{}
}

// NoopTenantConcurrencyController is a concurrency controller that does not allow for concurrency.
type NoopTenantConcurrencyController struct{}

func (n *NoopTenantConcurrencyController) Done(_ context.Context) {}
func (n *NoopTenantConcurrencyController) Allow(_ context.Context, _ *rules.Group, _ rules.Rule) bool {
	return false
}
