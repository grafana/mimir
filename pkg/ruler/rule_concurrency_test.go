// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"

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

	user1Ctx := user.InjectOrgID(context.Background(), "user1")
	user2Ctx := user.InjectOrgID(context.Background(), "user2")

	controller := NewMultiTenantConcurrencyController(logger, 3, 50.0, reg, limits)

	require.True(t, controller.Allow(user1Ctx, rg, rule1))
	require.True(t, controller.Allow(user1Ctx, rg, rule1))

	require.False(t, controller.Allow(user1Ctx, rg, rule1)) // Should fail, tenant limit reached.
	require.True(t, controller.Allow(user2Ctx, rg, rule1))  // Should succeed for another tenant as we have global slots available.
	require.False(t, controller.Allow(user2Ctx, rg, rule1)) // Should fail for another tenant as we do not have global slots available even though the tenant has slots available.

	// Let's check the metrics up until this point.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total Total number of incomplete attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total 2
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total Total number of started attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total 5
# HELP cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use Current number of concurrency slots currently in use across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use gauge
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use 3
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total Total number of concurrency slots we're done using across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total 0
`)))

	// Now let's release some slots and acquire one for tenant 2 which previously failed.
	controller.Done(user1Ctx)
	require.True(t, controller.Allow(user2Ctx, rg, rule1))
	require.False(t, controller.Allow(user1Ctx, rg, rule1))

	// Let's look at the metrics again.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total Total number of incomplete attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total 3
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total Total number of started attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total 7
# HELP cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use Current number of concurrency slots currently in use across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use gauge
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use 3
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total Total number of concurrency slots we're done using across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total 1
`)))

	// Release all slots, to make sure there is room for the next set of edge cases.
	controller.Done(user1Ctx)
	controller.Done(user2Ctx)
	controller.Done(user2Ctx)

	// Finally, let's try a few edge cases.
	rg2 := rules.NewGroup(rules.GroupOptions{
		File:     "test.rules",
		Name:     "test",
		Interval: 1 * time.Minute, // group not at risk.
		Opts:     &rules.ManagerOptions{},
	})
	require.False(t, controller.Allow(user1Ctx, rg2, rule1)) // Should not be allowed with a group that is not at risk.
	rule1.SetNoDependencyRules(false)
	require.False(t, controller.Allow(user1Ctx, rg, rule1))             // Should not be allowed as the rule is no longer independent.
	require.False(t, controller.Allow(context.Background(), rg, rule1)) // Should fail with a context that holds no userID.

	// Check the metrics one final time to ensure there are no active slots in use.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total Total number of incomplete attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_incomplete_total 3
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total Total number of started attempts to acquire concurrency slots across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_started_total 7
# HELP cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use Current number of concurrency slots currently in use across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use gauge
cortex_ruler_independent_rule_evaluation_concurrency_slots_in_use 0
# HELP cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total Total number of concurrency slots we're done using across all tenants
# TYPE cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total counter
cortex_ruler_independent_rule_evaluation_concurrency_attempts_completed_total 4
`)))

	// Make the rule independent again.
	rule1.SetNoDependencyRules(true)

	// Let's test marking an user for removal.
	done := make(chan struct{})

	// first acquire a slot.
	require.True(t, controller.Allow(user1Ctx, rg, rule1))

	// Now, mark the tenant for removal whilst the slot is still acquired.
	controller.MarkTenantForRemoval("user1", done)

	// If the user was marked for removal and tries to acquire a new slot, it should fail.
	require.False(t, controller.Allow(user1Ctx, rg, rule1))

	// finish using the previous slot.
	require.NotPanics(t, func() {
		controller.Done(user1Ctx)
	})

	close(done)

	// Remove both tenants.
	done = make(chan struct{})
	controller.MarkTenantForRemoval("user2", done)
	close(done)

	require.Eventually(t, func() bool {
		controller.tenantConcurrencyMtx.Lock()
		defer controller.tenantConcurrencyMtx.Unlock()
		return len(controller.tenantConcurrency) == 0 && len(controller.tenantConcurrencyMarkedForRemoval) == 0
	}, 2*time.Second, 100*time.Millisecond)
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
	logger := log.NewNopLogger()
	reg := prometheus.NewPedanticRegistry()
	limits := validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
		tenantLimits["user1"] = validation.MockDefaultLimits()
		tenantLimits["user1"].RulerMaxIndependentRuleEvaluationConcurrencyPerTenant = 2
		tenantLimits["user2"] = validation.MockDefaultLimits()
		tenantLimits["user2"].RulerMaxIndependentRuleEvaluationConcurrencyPerTenant = 2
	})

	exp, err := parser.ParseExpr("vector(1)")
	require.NoError(t, err)
	rule1 := rules.NewRecordingRule("test", exp, labels.Labels{})
	rule1.SetNoDependencyRules(true)
	rule1.SetNoDependentRules(true)

	controller := NewMultiTenantConcurrencyController(logger, 3, 50.0, reg, limits)

	tc := map[string]struct {
		group    *rules.Group
		expected bool
	}{
		"group last evaluation greater than interval": {
			group: func() *rules.Group {
				g := rules.NewGroup(rules.GroupOptions{
					Interval: -1 * time.Minute,
					Opts:     &rules.ManagerOptions{},
				})
				return g
			}(),
			expected: true,
		},
		"group last evaluation less than interval": {
			group: func() *rules.Group {
				g := rules.NewGroup(rules.GroupOptions{
					Interval: 1 * time.Minute,
					Opts:     &rules.ManagerOptions{},
				})
				return g
			}(),
			expected: false,
		},
		"group last evaluation exactly at concurrency trigger threshold": {
			group: func() *rules.Group {
				g := rules.NewGroup(rules.GroupOptions{
					Interval: 0 * time.Minute,
					Opts:     &rules.ManagerOptions{},
				})
				return g
			}(),
			expected: true,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tt.expected, controller.isGroupAtRisk(tt.group))
		})
	}
}
