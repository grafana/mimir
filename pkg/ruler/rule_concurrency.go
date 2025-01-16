// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
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
		logger:                  log.With(c.logger, "tenant", tenantID),
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
	logger                   log.Logger
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
func (c *TenantConcurrencyController) Allow(_ context.Context, _ *rules.Group, _ rules.Rule) bool {
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

// stringableConcurrentRules is a type that allows us to print a slice of rules.ConcurrentRules.
// This prevents premature evaluation, it will only be evaluated when the logger needs to print it.
type stringableConcurrentRules []rules.ConcurrentRules

func (p stringableConcurrentRules) String() string {
	return fmt.Sprintf("%v", []rules.ConcurrentRules(p))
}

var _ fmt.Stringer = stringableConcurrentRules{}

// SplitGroupIntoBatches splits the group into batches of rules that can be evaluated concurrently.
// It tries to batch rules that have no dependencies together and rules that have dependencies in separate batches.
// Returning no batches or nil means that the group should be evaluated sequentially.
func (c *TenantConcurrencyController) SplitGroupIntoBatches(_ context.Context, g *rules.Group) []rules.ConcurrentRules {
	if !c.isGroupAtRisk(g) {
		// If the group is not at risk, we can evaluate the rules sequentially.
		return nil
	}

	logger := log.With(c.logger, "group", g.Name())

	type ruleInfo struct {
		ruleIdx                 int
		unevaluatedDependencies map[rules.Rule]struct{}
	}
	remainingRules := make(map[rules.Rule]ruleInfo)

	// This batch holds the rules that have no dependencies and will be run first.
	firstBatch := rules.ConcurrentRules{}
	for i, r := range g.Rules() {
		dependencies := r.DependencyRules()
		if dependencies == nil {
			// This means that dependencies were not calculated.
			level.Warn(logger).Log("msg", "Dependencies were not calculated for at least one rule, falling back to sequential rule evaluation.")
			return nil
		}

		// Initialize the rule info with the rule's dependencies.
		// Use a copy of the dependencies to avoid mutating the rule.
		info := ruleInfo{ruleIdx: i, unevaluatedDependencies: map[rules.Rule]struct{}{}}
		for _, dep := range dependencies {
			if dep == r {
				// Ignore self-references.
				continue
			}
			info.unevaluatedDependencies[dep] = struct{}{}
		}

		if len(info.unevaluatedDependencies) == 0 {
			firstBatch = append(firstBatch, i)
			continue
		}

		remainingRules[r] = info
	}
	if len(firstBatch) == 0 {
		// There are no rules without dependencies.
		level.Warn(logger).Log("msg", "No rules without dependencies found, falling back to sequential rule evaluation.")
		return nil
	}
	result := []rules.ConcurrentRules{firstBatch}

	// Build the order of rules to evaluate based on dependencies.
	for len(remainingRules) > 0 {
		previousBatch := result[len(result)-1]
		// Remove the batch's rules from the dependencies of its dependents.
		for _, idx := range previousBatch {
			rule := g.Rules()[idx]
			for _, dependent := range rule.DependentRules() {
				dependentInfo := remainingRules[dependent]
				delete(dependentInfo.unevaluatedDependencies, rule)
			}
		}

		var batch rules.ConcurrentRules
		// Find rules that have no remaining dependencies.
		for name, info := range remainingRules {
			if len(info.unevaluatedDependencies) == 0 {
				batch = append(batch, info.ruleIdx)
				delete(remainingRules, name)
			}
		}

		if len(batch) == 0 {
			// There is a cycle in the rules' dependencies.
			// We can't evaluate them concurrently.
			level.Warn(logger).Log("msg", "Cyclic rule dependencies detected, falling back to sequential rule evaluation")
			return nil
		}

		result = append(result, batch)
	}

	level.Info(logger).Log("msg", "Batched rules into concurrent blocks", "rules", len(g.Rules()), "batches", len(result))
	level.Debug(logger).Log("msg", "Batched rules into concurrent blocks", "batches", stringableConcurrentRules(result))

	return result
}

// isGroupAtRisk checks if the rule group's last evaluation time is within the risk threshold.
func (c *TenantConcurrencyController) isGroupAtRisk(group *rules.Group) bool {
	interval := group.Interval().Seconds()
	runtimeThreshold := interval * c.thresholdRuleConcurrency / 100

	// If the group evaluation time is greater than the threshold, the group is at risk.
	if group.GetEvaluationTime().Seconds() >= runtimeThreshold {
		return true
	}

	// If the total rule evaluation time is greater than the threshold, the group is at risk.
	if group.GetRuleEvaluationTimeSum().Seconds() >= runtimeThreshold {
		return true
	}

	return false
}

// NoopMultiTenantConcurrencyController is a concurrency controller that does not allow for concurrency.
type NoopMultiTenantConcurrencyController struct{}

func (n *NoopMultiTenantConcurrencyController) NewTenantConcurrencyControllerFor(_ string) rules.RuleConcurrencyController {
	return &NoopTenantConcurrencyController{}
}

// NoopTenantConcurrencyController is a concurrency controller that does not allow for concurrency.
type NoopTenantConcurrencyController struct{}

func (n *NoopTenantConcurrencyController) Done(_ context.Context) {}
func (n *NoopTenantConcurrencyController) SplitGroupIntoBatches(_ context.Context, _ *rules.Group) []rules.ConcurrentRules {
	return nil
}

func (n *NoopTenantConcurrencyController) Allow(_ context.Context, _ *rules.Group, _ rules.Rule) bool {
	return false
}
