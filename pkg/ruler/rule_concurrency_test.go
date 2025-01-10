// SPDX-License-Identifier: AGPL-3.0-only

package ruler

import (
	"bytes"
	"context"
	"fmt"
	"os"
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
	"gopkg.in/yaml.v3"

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
	rule1.SetDependencyRules([]rules.Rule{})
	rule1.SetDependentRules([]rules.Rule{})

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

	// Now let's test having a controller two times for the same tenant.
	user3Controller := globalController.NewTenantConcurrencyControllerFor("user3")
	user3ControllerTwo := globalController.NewTenantConcurrencyControllerFor("user3")

	// They should not interfere with each other.
	require.True(t, user3Controller.Allow(ctx, rg, rule1))
	require.True(t, user3Controller.Allow(ctx, rg, rule1))
	require.True(t, user3ControllerTwo.Allow(ctx, rg, rule1))
}

func TestSplitGroupIntoBatches(t *testing.T) {
	limits := validation.MockOverrides(func(_ *validation.Limits, tenantLimits map[string]*validation.Limits) {
		tenantLimits["user1"] = validation.MockDefaultLimits()
		tenantLimits["user1"].RulerMaxIndependentRuleEvaluationConcurrencyPerTenant = 2
	})

	mtController := NewMultiTenantConcurrencyController(log.NewNopLogger(), 3, 50.0, prometheus.NewPedanticRegistry(), limits)
	controller := mtController.NewTenantConcurrencyControllerFor("user1")

	ruleManager := rules.NewManager(&rules.ManagerOptions{
		RuleConcurrencyController: controller,
	})

	tests := map[string]struct {
		inputFile      string
		expectedGroups []rules.ConcurrentRules
	}{
		"chained": {
			inputFile: "fixtures/rules_chain.yaml",
			expectedGroups: []rules.ConcurrentRules{
				{0, 1},
				{2},
				{3, 4},
				{5, 6},
			},
		},
		"indeterminates": {
			inputFile:      "fixtures/rules_indeterminates.yaml",
			expectedGroups: nil,
		},
		"all independent": {
			inputFile: "fixtures/rules_multiple_independent.yaml",
			expectedGroups: []rules.ConcurrentRules{
				{0, 1, 2, 3, 4, 5},
			},
		},
		"topological sort": {
			inputFile: "fixtures/rules_topological_sort_needed.json",
			expectedGroups: []rules.ConcurrentRules{
				{0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 37, 38, 58},
				{1, 2, 5, 6, 9, 10, 13, 14, 17, 18, 21, 22, 25, 26, 29, 30, 33, 34, 39, 40, 41, 42, 45, 46, 51, 52, 55, 56},
				{3, 7, 11, 15, 19, 23, 27, 31, 35},
				{43, 44, 47, 48, 49, 50, 53, 54, 57},
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			// Load group with a -1 interval so it's always at risk.
			groups, errs := ruleManager.LoadGroups(-1*time.Second, labels.EmptyLabels(), "", nil, []string{tc.inputFile}...)
			require.Empty(t, errs)
			require.Len(t, groups, 1)

			var group *rules.Group
			for _, g := range groups {
				group = g
			}

			batches := controller.SplitGroupIntoBatches(context.Background(), group)
			requireConcurrentRulesEqual(t, tc.expectedGroups, batches)

			// Make sure the group is not mutated and will still return the same batches when called again.
			batches = controller.SplitGroupIntoBatches(context.Background(), group)
			requireConcurrentRulesEqual(t, tc.expectedGroups, batches)
		})
	}
}

func requireConcurrentRulesEqual(t *testing.T, expected, actual []rules.ConcurrentRules) {
	t.Helper()

	if expected == nil {
		require.Nil(t, actual)
		return
	}

	// Like require.Equals but ignores the order of elements in the slices.
	require.Len(t, actual, len(expected))
	for i, expectedBatch := range expected {
		actualBatch := actual[i]
		require.ElementsMatch(t, expectedBatch, actualBatch)
	}
}

func TestGroupAtRisk(t *testing.T) {
	// Write group file with 100 independent rules.
	ruleCt := 100
	dummyRules := []map[string]interface{}{}
	for i := 0; i < ruleCt; i++ {
		dummyRules = append(dummyRules, map[string]interface{}{
			"record": fmt.Sprintf("test_rule%d", i),
			"expr":   "vector(1)",
		})
	}

	groupFileContent := map[string]interface{}{
		"groups": []map[string]interface{}{
			{
				"name":  "test",
				"rules": dummyRules,
			},
		},
	}

	groupFile := t.TempDir() + "/test.rules"
	f, err := os.Create(groupFile)
	require.NoError(t, err)
	encoder := yaml.NewEncoder(f)
	require.NoError(t, encoder.Encode(groupFileContent))
	require.NoError(t, f.Close())

	createAndEvalTestGroup := func(interval time.Duration, evalConcurrently bool) *rules.Group {
		st := teststorage.New(t)
		defer st.Close()

		ruleWaitTime := 1 * time.Millisecond
		opts := &rules.ManagerOptions{
			Appendable: st,
			// Make the rules take 1ms to evaluate.
			QueryFunc: func(_ context.Context, _ string, _ time.Time) (promql.Vector, error) {
				time.Sleep(ruleWaitTime)
				return promql.Vector{}, nil
			},
		}
		if evalConcurrently {
			opts.RuleConcurrencyController = &allowAllConcurrencyController{}
		}
		manager := rules.NewManager(opts)
		groups, errs := manager.LoadGroups(interval, labels.EmptyLabels(), "", nil, groupFile)
		require.Empty(t, errs)

		var g *rules.Group
		for _, group := range groups {
			g = group
		}

		rules.DefaultEvalIterationFunc(context.Background(), g, time.Now())

		// Sanity check that we're actually running the rules concurrently.
		// The group should take less time than the sum of all rules if we're running them concurrently, more otherwise.
		if evalConcurrently {
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

func (a *allowAllConcurrencyController) SplitGroupIntoBatches(_ context.Context, g *rules.Group) []rules.ConcurrentRules {
	batch := rules.ConcurrentRules{}
	for i := range g.Rules() {
		batch = append(batch, i)
	}
	return []rules.ConcurrentRules{batch}
}

func (a *allowAllConcurrencyController) Done(_ context.Context) {}
