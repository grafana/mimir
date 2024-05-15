package ruler

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/rules"
	"github.com/stretchr/testify/require"
)

func TestDynamicSemaphore(t *testing.T) {
	logger := log.NewNopLogger()

	t.Run("TestDynamicSemaphore", func(t *testing.T) {
		limitFunc := func() int64 { return 2 }
		sema := NewDynamicSemaphore(logger, limitFunc)

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
		sema := NewDynamicSemaphore(logger, limitFunc)

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
		sema.mtx.Lock()
		require.Equal(t, int64(0), sema.acquired)
	})

	t.Run("TestDynamicSemaphoreWithChangingLimitsToLowerValue", func(t *testing.T) {
		var limit atomic.Int64
		limit.Store(2)
		limitFunc := func() int64 { return limit.Load() }
		sema := NewDynamicSemaphore(logger, limitFunc)

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
	limits := validation.MockOverrides(func(defaults *validation.Limits, tenantLimits map[string]*validation.Limits) {
		tenantLimits["user1"] = validation.MockDefaultLimits()
		tenantLimits["user1"].RulerMaxConcurrentRuleEvaluationsPerTenant = 2
		tenantLimits["user2"] = validation.MockDefaultLimits()
		tenantLimits["user2"].RulerMaxConcurrentRuleEvaluationsPerTenant = 2
	})

	rg := rules.NewGroup(rules.GroupOptions{
		File:     "test.rules",
		Name:     "test",
		Interval: 1 * time.Minute,
		Opts:     &rules.ManagerOptions{},
	})

	exp, err := parser.ParseExpr("vector(1)")
	require.NoError(t, err)
	rule1 := rules.NewRecordingRule("test", exp, labels.Labels{})
	rule1.SetNoDependencyRules(true)
	rule1.SetNoDependentRules(true)

	user1Ctx := user.InjectOrgID(context.Background(), "user1")
	user2Ctx := user.InjectOrgID(context.Background(), "user2")

	controller := NewMultiTenantConcurrencyController(logger, 3, limits, reg)

	require.True(t, controller.Allow(user1Ctx, rg, rule1))
	require.True(t, controller.Allow(user1Ctx, rg, rule1))

	require.False(t, controller.Allow(user1Ctx, rg, rule1)) // Should fail, tenant limit reached.
	require.True(t, controller.Allow(user2Ctx, rg, rule1))  // Should succeed for another tenant as we have global slots available.
	require.False(t, controller.Allow(user2Ctx, rg, rule1)) // Should fail for another tenant as we do not have global slots available even though the tenant has slots available.

	// Let's check the metrics up until this point.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total Total number of failed attempts to acquire concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total counter
cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total 2
# HELP cortex_ruler_global_rule_evaluation_concurrency_acquire_total Total number of acquired concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_acquire_total counter
cortex_ruler_global_rule_evaluation_concurrency_acquire_total 5
# HELP cortex_ruler_global_rule_evaluation_concurrency_current Current number of active global concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_current gauge
cortex_ruler_global_rule_evaluation_concurrency_current 3
# HELP cortex_ruler_global_rule_evaluation_concurrency_release_total Total number of released concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_release_total counter
cortex_ruler_global_rule_evaluation_concurrency_release_total 0
`)))

	// Now let's release some slots and acquire one for tenant 2 which previously failed.
	controller.Done(user1Ctx)
	require.True(t, controller.Allow(user2Ctx, rg, rule1))
	require.False(t, controller.Allow(user1Ctx, rg, rule1))

	// Let's look at the metrics again.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total Total number of failed attempts to acquire concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total counter
cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total 3
# HELP cortex_ruler_global_rule_evaluation_concurrency_acquire_total Total number of acquired concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_acquire_total counter
cortex_ruler_global_rule_evaluation_concurrency_acquire_total 7
# HELP cortex_ruler_global_rule_evaluation_concurrency_current Current number of active global concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_current gauge
cortex_ruler_global_rule_evaluation_concurrency_current 3
# HELP cortex_ruler_global_rule_evaluation_concurrency_release_total Total number of released concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_release_total counter
cortex_ruler_global_rule_evaluation_concurrency_release_total 1
`)))

	// Release all slots, to make sure there is room for the next set of edge cases.
	controller.Done(user1Ctx)
	controller.Done(user2Ctx)
	controller.Done(user2Ctx)

	// Finally, let's try a few edge cases.
	require.False(t, controller.Allow(user1Ctx, &rules.Group{}, rule1)) // Should not be allowed with a group that is not at risk.
	rule1.SetNoDependencyRules(false)
	require.False(t, controller.Allow(user1Ctx, rg, rule1))             // Should not be allowed as the rule is no longer independent.
	require.False(t, controller.Allow(context.Background(), rg, rule1)) // Should fail with a context that holds no userID.

	// Check the metrics one final time to ensure there are no active slots in use.
	require.NoError(t, testutil.GatherAndCompare(reg, bytes.NewBufferString(`
# HELP cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total Total number of failed attempts to acquire concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total counter
cortex_ruler_global_rule_evaluation_concurrency_acquire_failed_total 3
# HELP cortex_ruler_global_rule_evaluation_concurrency_acquire_total Total number of acquired concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_acquire_total counter
cortex_ruler_global_rule_evaluation_concurrency_acquire_total 10
# HELP cortex_ruler_global_rule_evaluation_concurrency_current Current number of active global concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_current gauge
cortex_ruler_global_rule_evaluation_concurrency_current 0
# HELP cortex_ruler_global_rule_evaluation_concurrency_release_total Total number of released concurrency slots
# TYPE cortex_ruler_global_rule_evaluation_concurrency_release_total counter
cortex_ruler_global_rule_evaluation_concurrency_release_total 4
`)))
}
