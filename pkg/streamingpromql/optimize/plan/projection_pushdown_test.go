// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// TestProjectionPushdown verifies that the projection pushdown optimization
// correctly analyzes queries and sets projection labels on the query plan.
func TestProjectionPushdown(t *testing.T) {
	logger := log.NewNopLogger()

	testCases := []struct {
		name           string
		query          string
		expectedLabels []string
	}{
		{
			name:           "simple vector selector",
			query:          `http_requests_total{status="200"}`,
			expectedLabels: []string{"__name__", "status", "s_series_hash"},
		},
		{
			name:           "aggregation with grouping",
			query:          `sum by (service) (http_requests_total{status="200"})`,
			expectedLabels: []string{"__name__", "status", "service", "s_series_hash"},
		},
		{
			name:           "binary expression",
			query:          `http_requests_total{status="200"} / http_requests_total{status="404", method="GET"}`,
			expectedLabels: []string{"__name__", "status", "method", "s_series_hash"},
		},
		{
			name:           "function call should not optimize",
			query:          `rate(http_requests_total{status="200"}[5m])`,
			expectedLabels: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reg := prometheus.NewRegistry()
			opts := streamingpromql.EngineOpts{
				CommonOpts: promql.EngineOpts{
					Reg: reg,
				},
				Logger:                   logger,
				EnableProjectionPushdown: true,
			}

			planner, err := streamingpromql.NewQueryPlanner(opts)
			require.NoError(t, err, "should create query planner")

			timeRange := types.NewInstantQueryTimeRange(time.Now())

			plan, err := planner.NewQueryPlan(context.Background(), tc.query, timeRange, streamingpromql.NoopPlanningObserver{})
			require.NoError(t, err, "should create query plan for %s", tc.query)

			if tc.expectedLabels != nil {
				require.NotEmpty(t, plan.ProjectionLabels, "ProjectionLabels should be set for query: %s", tc.query)
				require.True(t, plan.ProjectionInclude, "ProjectionInclude should be true for query: %s", tc.query)
				require.ElementsMatch(t, tc.expectedLabels, plan.ProjectionLabels, "Expected labels should match for query: %s", tc.query)
			} else {
				require.Empty(t, plan.ProjectionLabels, "ProjectionLabels should be empty for query: %s", tc.query)
				require.False(t, plan.ProjectionInclude, "ProjectionInclude should be false for query: %s", tc.query)
			}
		})
	}
}

func TestProjectionPushdownOptimizationPass_AnalyzeRequiredLabels(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	pass := NewProjectionPushdownOptimizationPass(reg, log.NewNopLogger())

	testCases := []struct {
		name           string
		node           planning.Node
		expectedLabels []string
	}{
		{
			name: "VectorSelector with matchers",
			node: &core.VectorSelector{
				VectorSelectorDetails: &core.VectorSelectorDetails{
					Matchers: []*core.LabelMatcher{
						{Name: "__name__", Value: "http_requests_total"},
						{Name: "status", Value: "200"},
					},
				},
			},
			expectedLabels: []string{"__name__", "status"},
		},
		{
			name: "MatrixSelector with matchers",
			node: &core.MatrixSelector{
				MatrixSelectorDetails: &core.MatrixSelectorDetails{
					Matchers: []*core.LabelMatcher{
						{Name: "__name__", Value: "http_requests_total"},
						{Name: "status", Value: "200"},
					},
				},
			},
			expectedLabels: []string{"__name__", "status"},
		},
		{
			name: "AggregateExpression with grouping",
			node: &core.AggregateExpression{
				Inner: &core.VectorSelector{
					VectorSelectorDetails: &core.VectorSelectorDetails{
						Matchers: []*core.LabelMatcher{
							{Name: "__name__", Value: "http_requests_total"},
							{Name: "status", Value: "200"},
						},
					},
				},
				AggregateExpressionDetails: &core.AggregateExpressionDetails{
					Grouping: []string{"service"},
				},
			},
			expectedLabels: []string{"__name__", "status", "service"},
		},
		{
			name: "FunctionCall returns nil",
			node: &core.FunctionCall{
				Args: []planning.Node{
					&core.VectorSelector{
						VectorSelectorDetails: &core.VectorSelectorDetails{
							Matchers: []*core.LabelMatcher{
								{Name: "__name__", Value: "http_requests_total"},
							},
						},
					},
				},
			},
			expectedLabels: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			labels := pass.analyzeRequiredLabels(tc.node)
			if tc.expectedLabels == nil {
				require.Nil(t, labels, "Expected nil labels for %s", tc.name)
			} else {
				require.ElementsMatch(t, tc.expectedLabels, labels, "Required labels should match expected for %s", tc.name)
			}
		})
	}
}

func TestProjectionPushdownOptimizationPass_CountSelectors(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	pass := NewProjectionPushdownOptimizationPass(reg, log.NewNopLogger())

	vs1 := &core.VectorSelector{
		VectorSelectorDetails: &core.VectorSelectorDetails{
			Matchers: []*core.LabelMatcher{
				{Name: "__name__", Value: "http_requests_total"},
			},
		},
	}

	vs2 := &core.VectorSelector{
		VectorSelectorDetails: &core.VectorSelectorDetails{
			Matchers: []*core.LabelMatcher{
				{Name: "__name__", Value: "http_errors_total"},
			},
		},
	}

	ms := &core.MatrixSelector{
		MatrixSelectorDetails: &core.MatrixSelectorDetails{
			Matchers: []*core.LabelMatcher{
				{Name: "__name__", Value: "cpu_usage"},
			},
		},
	}

	agg := &core.AggregateExpression{
		Inner: vs1,
		AggregateExpressionDetails: &core.AggregateExpressionDetails{
			Grouping: []string{"service"},
		},
	}

	bin := &core.BinaryExpression{
		LHS: agg,
		RHS: vs2,
	}

	testCases := []struct {
		name          string
		node          planning.Node
		expectedCount int
	}{
		{
			name:          "single vector selector",
			node:          vs1,
			expectedCount: 1,
		},
		{
			name:          "single matrix selector",
			node:          ms,
			expectedCount: 1,
		},
		{
			name:          "aggregation with inner selector",
			node:          agg,
			expectedCount: 1,
		},
		{
			name:          "binary expression with two selectors",
			node:          bin,
			expectedCount: 2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			count := pass.countSelectors(tc.node)
			require.Equal(t, tc.expectedCount, count, "Should count %d selectors for %s", tc.expectedCount, tc.name)
		})
	}
}

func TestProjectionPushdownOptimizationPass_Apply(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	pass := NewProjectionPushdownOptimizationPass(reg, log.NewNopLogger())

	vs := &core.VectorSelector{
		VectorSelectorDetails: &core.VectorSelectorDetails{
			Matchers: []*core.LabelMatcher{
				{Name: "__name__", Value: "http_requests_total"},
				{Name: "status", Value: "200"},
			},
		},
	}

	agg := &core.AggregateExpression{
		Inner: vs,
		AggregateExpressionDetails: &core.AggregateExpressionDetails{
			Grouping: []string{"service"},
		},
	}

	plan := &planning.QueryPlan{
		Root:      agg,
		TimeRange: types.NewInstantQueryTimeRange(time.Now()),
	}

	ctx := context.Background()
	updatedPlan, err := pass.Apply(ctx, plan)
	require.NoError(t, err, "Apply should succeed")
	require.Equal(t, plan, updatedPlan, "Plan should be the same object")

	expectedLabels := []string{"__name__", "status", "service", "s_series_hash"}
	require.ElementsMatch(t, expectedLabels, plan.ProjectionLabels, "Plan.ProjectionLabels should include all required labels plus series hash")
	require.True(t, plan.ProjectionInclude, "Plan.ProjectionInclude should be true")
}
