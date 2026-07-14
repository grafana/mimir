// SPDX-License-Identifier: AGPL-3.0-only

package optimize_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func TestWalk(t *testing.T) {
	testCases := map[string]struct {
		expr           string
		paths          [][]string
		skipChildrenOf string
	}{
		"success no children": {
			expr:  "some_metric[5m]",
			paths: [][]string{},
		},
		"success with children": {
			expr: "sum(rate(some_metric[5m]))",
			paths: [][]string{
				{"sum"},
				{"sum", ""}, // Query planning inserts a DeduplicateAndMerge node around the rate function
				{"sum", "", "rate(...)"},
			},
		},
		"success with multiple children": {
			expr: "sum(rate(some_metric[5m]) + rate(other_metric[5m]))",
			paths: [][]string{
				{"sum"},
				{"sum", "LHS + RHS"},
				{"sum", "LHS + RHS", ""}, // Query planning inserts a DeduplicateAndMerge node around the rate function
				{"sum", "LHS + RHS", "", "rate(...)"},
				{"sum", "LHS + RHS"},
				{"sum", "LHS + RHS", ""}, // Query planning inserts a DeduplicateAndMerge node around the rate function
				{"sum", "LHS + RHS", "", "rate(...)"},
			},
		},
		"success with skipping of some branches": {
			expr:           `sum(metric_1 * metric_2) + avg(metric_3 * metric_4)`,
			skipChildrenOf: "avg",
			paths: [][]string{
				{"LHS + RHS"},
				{"LHS + RHS", "sum"},
				{"LHS + RHS", "sum", "LHS * RHS"},
				{"LHS + RHS", "sum", "LHS * RHS"},
				{"LHS + RHS"},
				{"LHS + RHS", "avg"},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			observer := streamingpromql.NoopPlanningObserver{}

			opts := streamingpromql.NewTestEngineOpts()
			planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, streamingpromql.DefaultLookbackDelta, false, observer)
			require.NoError(t, err)

			visitor := NewTestVisitor(t, testCase.skipChildrenOf)
			require.NoError(t, optimize.Walk(p.Root, visitor))
			require.Equal(t, testCase.paths, visitor.Paths)
			require.Equal(t, len(testCase.paths)+1, visitor.Visits)
		})
	}
}

func BenchmarkWalk(b *testing.B) {
	const query = "sum(rate(some_metric[5m]) + rate(other_metric[5m]))"

	ctx := context.Background()
	timeRange := types.NewInstantQueryTimeRange(time.Now())
	observer := streamingpromql.NoopPlanningObserver{}

	opts := streamingpromql.NewTestEngineOpts()
	planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
	require.NoError(b, err)

	p, err := planner.NewQueryPlan(ctx, query, timeRange, streamingpromql.DefaultLookbackDelta, false, observer)
	require.NoError(b, err)

	visitor := optimize.VisitorFunc(func(node planning.Node, path []planning.Node) (bool, error) {
		return true, nil // no-op
	})

	for b.Loop() {
		_ = optimize.Walk(p.Root, visitor)
	}
}

type TestVisitor struct {
	test *testing.T

	SkipChildrenOf string
	Paths          [][]string
	Visits         int
}

func NewTestVisitor(t *testing.T, skipChildrenOf string) *TestVisitor {
	return &TestVisitor{
		test:           t,
		Paths:          [][]string{},
		SkipChildrenOf: skipChildrenOf,
	}
}

func (v *TestVisitor) Visit(node planning.Node, path []planning.Node) (bool, error) {
	require.NotNil(v.test, node)
	v.Visits++

	if len(path) != 0 {
		thisPath := make([]string, 0, len(path))
		for _, n := range path {
			thisPath = append(thisPath, n.Describe())
		}

		v.Paths = append(v.Paths, thisPath)

		if v.SkipChildrenOf != "" && thisPath[len(thisPath)-1] == v.SkipChildrenOf {
			return false, nil
		}
	}

	return true, nil
}

func TestInspectSelectors(t *testing.T) {
	testCases := map[string]struct {
		expr                  string
		expectedInspectResult optimize.InspectSelectorsResult
	}{
		"raw vector selector": {
			expr: `some_metric`,
			expectedInspectResult: optimize.InspectSelectorsResult{
				HasSelectors:            true,
				IsRewrittenByMiddleware: false,
			},
		},
		"raw range selector": {
			expr: `some_metric[5m]`,
			expectedInspectResult: optimize.InspectSelectorsResult{
				HasSelectors:            true,
				IsRewrittenByMiddleware: false,
			},
		},
		"function call around selector": {
			expr: `rate(some_metric[5m])`,
			expectedInspectResult: optimize.InspectSelectorsResult{
				HasSelectors:            true,
				IsRewrittenByMiddleware: false,
			},
		},
		"middleware sharded query": {
			expr: `sum by (container) (__embedded_queries__{__queries__="something"})`,
			expectedInspectResult: optimize.InspectSelectorsResult{
				HasSelectors:            true,
				IsRewrittenByMiddleware: true,
			},
		},
		"middleware subquery spin off": {
			expr: `sum(sum_over_time(__subquery_spinoff__{__query__="sum(some_metric)",__range__="5h0m0s",__step__="30s"}[5h]))`,
			expectedInspectResult: optimize.InspectSelectorsResult{
				HasSelectors:            true,
				IsRewrittenByMiddleware: true,
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			timeRange := types.NewInstantQueryTimeRange(time.Now())
			observer := streamingpromql.NoopPlanningObserver{}

			opts := streamingpromql.NewTestEngineOpts()
			planner, err := streamingpromql.NewQueryPlannerWithoutOptimizationPasses(opts, streamingpromql.NewMaximumSupportedVersionQueryPlanVersionProvider())
			require.NoError(t, err)

			p, err := planner.NewQueryPlan(ctx, testCase.expr, timeRange, streamingpromql.DefaultLookbackDelta, false, observer)
			require.NoError(t, err)

			res := optimize.InspectSelectors(p.Root)
			require.Equal(t, testCase.expectedInspectResult, res)
		})
	}
}
