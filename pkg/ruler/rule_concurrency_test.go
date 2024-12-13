// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
	"golang.org/x/sync/semaphore"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestDynamicSemaphore(t *testing.T) {
	t.Run("TestDynamicSemaphore", func(t *testing.T) {
		limitFunc := func() int64 { return 2 }
		sema := NewDynamicSemaphore(limitFunc)

		require.True(t, sema.TryAcquire())
		require.True(t, sema.TryAcquire())
		require.False(t, sema.TryAcquire()) // Should fail, limit reached.

		sema.Release()
		require.True(t, sema.TryAcquire()) // Should succeed after releasing one.
	})

	t.Run("TestDynamicSemaphoreWithChangingLimits", func(t *testing.T) {
		var limit atomic.Int64
		limit.Store(2)
		limitFunc := func() int64 { return limit.Load() }
		sema := NewDynamicSemaphore(limitFunc)

		require.True(t, sema.TryAcquire())
		require.True(t, sema.TryAcquire())
		require.False(t, sema.TryAcquire()) // Should fail, limit reached.

		limit.Store(3)

		require.True(t, sema.TryAcquire()) // Should succeed, new limit.

		sema.Release()
		require.True(t, sema.TryAcquire()) // Should succeed after releasing one.

		// Now make sure we release all the slots.
		sema.Release()
		sema.Release()
		sema.Release()
		sema.acquiredMtx.Lock()
		require.Equal(t, int64(0), sema.acquired)
	})

	t.Run("TestDynamicSemaphoreWithChangingLimitsToLowerValue", func(t *testing.T) {
		var limit atomic.Int64
		limit.Store(2)
		limitFunc := func() int64 { return limit.Load() }
		sema := NewDynamicSemaphore(limitFunc)

		require.True(t, sema.TryAcquire())
		require.True(t, sema.TryAcquire())
		require.False(t, sema.TryAcquire()) // Should fail, limit reached.

		// Change the limit with slots acquired.
		limit.Store(1)
		require.False(t, sema.TryAcquire()) // Should fail, limit reached.
		sema.Release()
		sema.Release()

		// Now try to acquire again, should succeed.
		require.True(t, sema.TryAcquire())

		// Now trying to acquire should fail because we have a new limit.
		require.False(t, sema.TryAcquire())
	})
}

func TestMultiTenantConcurrencyController(t *testing.T) {
	logger := log.NewNopLogger()
	reg := prometheus.NewPedanticRegistry()
	limits := validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
		tenantLimits["user1"] = validation.MockDefaultLimits()
		tenantLimits["user1"].RulerMaxIndependentRuleEvaluationConcurrencyPerTenant = 2
		tenantLimits["user2"] = validation.MockDefaultLimits()
		tenantLimits["user2"].RulerMaxIndependentRuleEvaluationConcurrencyPerTenant = 2
	})

	rg := rules.NewGroup(rules.GroupOptions{
		File:     "test.rules",
		Name:     "test",
		Interval: -1 * time.Minute, // by default, groups should be at risk.
		Opts:     &rules.ManagerOptions{},
	})

	exp, err := parser.ParseExpr("vector(1)")
	require.NoError(t, err)
	rule1 := rules.NewRecordingRule("test", exp, labels.Labels{})
	rule1.SetNoDependencyRules(true)
	rule1.SetNoDependentRules(true)

	globalController := NewMultiTenantConcurrencyController(logger, 3, 50.0, reg, limits)
	user1Controller := globalController.NewTenantConcurrencyControllerFor("user1")
	user2Controller := globalController.NewTenantConcurrencyControllerFor("user2")
	ctx := context.Background()

	require.True(t, user1Controller.Allow(ctx, rg, rule1))
	require.True(t, user1Controller.Allow(ctx, rg, rule1))

	require.False(t, user1Controller.Allow(ctx, rg, rule1)) // Should fail, tenant limit reached.
	require.True(t, user2Controller.Allow(ctx, rg, rule1))  // Should succeed for another tenant as we have global slots available.
	require.False(t, user2Controller.Allow(ctx, rg, rule1)) // Should fail for another tenant as we do not have global slots available even though the tenant has slots available.

	// Let's check the metrics up until this point.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total Total number of concurrency slots we're done using across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total{user="user1"} 0
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total{user="user2"} 0
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total Total number of incomplete attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total{user="user1"} 1
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total{user="user2"} 1
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total Total number of started attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total{user="user1"} 3
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total{user="user2"} 2
# HELP cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use Current number of concurrency slots currently in use across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use gauge
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use{user="user1"} 2
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use{user="user2"} 1
`)))

	// Now let's release some slots and acquire one for tenant 2 which previously failed.
	user1Controller.Done(ctx)
	require.True(t, user2Controller.Allow(ctx, rg, rule1))
	require.False(t, user1Controller.Allow(ctx, rg, rule1))

	// Let's look at the metrics again.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total Total number of incomplete attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total{user="user1"} 2
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total{user="user2"} 1
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total Total number of started attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total{user="user1"} 4
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total{user="user2"} 3
# HELP cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use Current number of concurrency slots currently in use across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use gauge
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use{user="user1"} 1
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use{user="user2"} 2
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total Total number of concurrency slots we're done using across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total{user="user1"} 1
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total{user="user2"} 0
`)))

	// Release all slots, to make sure there is room for the next set of edge cases.
	user1Controller.Done(ctx)
	user2Controller.Done(ctx)
	user2Controller.Done(ctx)

	// Finally, let's try a few edge cases.
	rg2 := rules.NewGroup(rules.GroupOptions{
		File:     "test.rules",
		Name:     "test",
		Interval: 1 * time.Minute, // group not at risk.
		Opts:     &rules.ManagerOptions{},
	})
	require.False(t, user1Controller.Allow(ctx, rg2, rule1)) // Should not be allowed with a group that is not at risk.
	rule1.SetNoDependencyRules(false)
	require.False(t, user1Controller.Allow(ctx, rg, rule1)) // Should not be allowed as the rule is no longer independent.

	// Check the metrics one final time to ensure there are no active slots in use.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total Total number of incomplete attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total{user="user1"} 2
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total{user="user2"} 1
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total Total number of started attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total{user="user1"} 4
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total{user="user2"} 3
# HELP cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use Current number of concurrency slots currently in use across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use gauge
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use{user="user1"} 0
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use{user="user2"} 0
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total Total number of concurrency slots we're done using across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total{user="user1"} 2
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total{user="user2"} 2
`)))

	// Make the rule independent again.
	rule1.SetNoDependencyRules(true)

	// Now let's test having a controller two times for the same tenant.
	user3Controller := globalController.NewTenantConcurrencyControllerFor("user3")
	user3ControllerTwo := globalController.NewTenantConcurrencyControllerFor("user3")

	// They should not interfere with each other.
	require.True(t, user3Controller.Allow(ctx, rg, rule1))
	require.True(t, user3Controller.Allow(ctx, rg, rule1))
	require.True(t, user3ControllerTwo.Allow(ctx, rg, rule1))
}

func TestIsRuleIndependent(t *testing.T) {
	tests := map[string]struct {
		rule     rules.Rule
		expected bool
	}{
		"rule has neither dependencies nor dependents": {
			rule: func() rules.Rule {
				r := rules.NewRecordingRule("test", nil, labels.Labels{})
				r.SetNoDependentRules(true)
				r.SetNoDependencyRules(true)
				return r
			}(),
			expected: true,
		},
		"rule has both dependencies and dependents": {
			rule: func() rules.Rule {
				r := rules.NewRecordingRule("test", nil, labels.Labels{})
				r.SetNoDependentRules(false)
				r.SetNoDependencyRules(false)
				return r
			}(),
			expected: false,
		},
		"rule has dependents": {
			rule: func() rules.Rule {
				r := rules.NewRecordingRule("test", nil, labels.Labels{})
				r.SetNoDependentRules(false)
				r.SetNoDependencyRules(true)
				return r
			}(),
			expected: false,
		},
		"rule has dependencies": {
			rule: func() rules.Rule {
				r := rules.NewRecordingRule("test", nil, labels.Labels{})
				r.SetNoDependentRules(true)
				r.SetNoDependencyRules(false)
				return r
			}(),
			expected: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := isRuleIndependent(tc.rule)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestGroupAtRisk(t *testing.T) {
	createAndEvalTestGroup := func(interval time.Duration, evalConcurrently bool) *rules.Group {
		st := teststorage.New(t)
		defer st.Close()
		var createdRules []rules.Rule
		ruleCt := 100
		ruleWaitTime := 1 * time.Millisecond
		for i := 0; i < ruleCt; i++ {
			q, err := parser.ParseExpr("vector(1)")
			require.NoError(t, err)
			rule := rules.NewRecordingRule(fmt.Sprintf("test_rule%d", i), q, labels.Labels{})
			rule.SetNoDependencyRules(true)
			rule.SetNoDependentRules(true)
			createdRules = append(createdRules, rule)
		}

		opts := rules.GroupOptions{
			Interval: interval,
			Opts: &rules.ManagerOptions{
				Appendable: st,
				QueryFunc: func(ctx context.Context, q string, t time.Time) (promql.Vector, error) {
					time.Sleep(ruleWaitTime)
					return promql.Vector{}, nil
				},
			},
			Rules: createdRules,
		}
		if evalConcurrently {
			opts.Opts.RuleConcurrencyController = &allowAllConcurrencyController{}
		}
		g := rules.NewGroup(opts)
		rules.DefaultEvalIterationFunc(context.Background(), g, time.Now())

		if evalConcurrently {
			// Sanity check that we're actually running the rules concurrently.
			require.Less(t, g.GetEvaluationTime(), time.Duration(ruleCt)*ruleWaitTime)
		} else {
			require.Greater(t, g.GetEvaluationTime(), time.Duration(ruleCt)*ruleWaitTime)
		}

		return g
	}

	m := newMultiTenantConcurrencyControllerMetrics(prometheus.NewPedanticRegistry())
	controller := &TenantConcurrencyController{
		slotsInUse:               m.SlotsInUse.WithLabelValues("user1"),
		attemptsStartedTotal:     m.AttemptsStartedTotal.WithLabelValues("user1"),
		attemptsIncompleteTotal:  m.AttemptsIncompleteTotal.WithLabelValues("user1"),
		attemptsCompletedTotal:   m.AttemptsCompletedTotal.WithLabelValues("user1"),
		thresholdRuleConcurrency: 50.0,
		globalConcurrency:        semaphore.NewWeighted(3),
		tenantConcurrency: NewDynamicSemaphore(func() int64 {
			return 2
		}),
	}

	tc := map[string]struct {
		groupInterval    time.Duration
		evalConcurrently bool
		expected         bool
	}{
		"group last evaluation greater than interval": {
			// Total runtime: 100x1ms ~ 100ms (run sequentially), > 1ms -> Not at risk
			groupInterval:    1 * time.Millisecond,
			evalConcurrently: false,
			expected:         true,
		},
		"group last evaluation less than interval": {
			// Total runtime: 100x1ms ~ 100ms (run sequentially), < 1s -> Not at risk
			groupInterval:    1 * time.Second,
			evalConcurrently: false,
			expected:         false,
		},
		"group total rule evaluation duration of last evaluation greater than threshold": {
			// Total runtime: 100x1ms ~ 100ms, > 50ms -> Group isn't at risk for its runtime, but it is for the sum of all rules.
			groupInterval:    50 * time.Millisecond,
			evalConcurrently: true,
			expected:         true,
		},
		"group total rule evaluation duration of last evaluation less than threshold": {
			// Total runtime: 100x1ms ~ 100ms, < 1s -> Not at risk
			groupInterval:    1 * time.Second,
			evalConcurrently: true,
			expected:         false,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			group := createAndEvalTestGroup(tt.groupInterval, tt.evalConcurrently)
			require.Equal(t, tt.expected, controller.isGroupAtRisk(group))
		})
	}
}

type allowAllConcurrencyController struct{}

func (a *allowAllConcurrencyController) Allow(_ context.Context, _ *rules.Group, _ rules.Rule) bool {
	return true
}

func (a *allowAllConcurrencyController) Done(_ context.Context) {}
