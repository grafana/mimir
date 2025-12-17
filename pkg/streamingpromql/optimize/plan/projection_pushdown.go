// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"slices"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const (
	ProjectionSeriesHash = "__series_hash__"

	SkipReasonNoAggregations  = "no-aggregations"
	SkipReasonBinaryOperation = "binary-operations"
	SkipReasonDeduplicate     = "deduplicate-and-merge"
	SkipReasonAmbiguousLabels = "ambiguous-required-labels"
)

var _ optimize.QueryPlanOptimizationPass = &ProjectionPushdownOptimizationPass{}

type ProjectionPushdownOptimizationPass struct {
	examined prometheus.Counter
	modified prometheus.Counter
	skipped  *prometheus.CounterVec
	logger   log.Logger
}

func NewProjectionPushdownOptimizationPass(reg prometheus.Registerer, logger log.Logger) *ProjectionPushdownOptimizationPass {
	return &ProjectionPushdownOptimizationPass{
		examined: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_projection_pushdown_examined_total",
			Help: "Total number of queries that the optimization pass examined to see if projections could be used.",
		}),
		modified: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_projection_pushdown_modified_total",
			Help: "Total number of queries where projections could be used.",
		}),
		skipped: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_projection_pushdown_skipped_total",
			Help: "Total number of queries where projections could not be used.",
		}, []string{"reason"}),
		logger: logger,
	}
}

func (p *ProjectionPushdownOptimizationPass) Name() string {
	return "Push down label projections"
}

func (p *ProjectionPushdownOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, v planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	spanlog := spanlogger.FromContext(ctx, p.logger)
	p.examined.Inc()

	requiredLabels := make(map[string]struct{})
	hasAggregation := false
	hasBinaryExpr := false
	hasDeduplicateAndMerge := false
	selectorCount := 0

	_ = optimize.Walk(plan.Root, optimize.VisitorFunc(func(node planning.Node, path []planning.Node) error {
		switch e := node.(type) {
		case *core.AggregateExpression:
			if len(e.Grouping) > 0 && !e.Without {
				for _, l := range e.Grouping {
					requiredLabels[l] = struct{}{}
				}
			}

			hasAggregation = true
		case *core.BinaryExpression:
			hasBinaryExpr = true
		case *core.DeduplicateAndMerge:
			hasDeduplicateAndMerge = true
		case *core.MatrixSelector, *core.VectorSelector:
			selectorCount++
		}
		return nil
	}))

	if !hasAggregation {
		spanlog.DebugLog("msg", "projection pushdown skipped - plan does not contain aggregations")
		p.skipped.WithLabelValues(SkipReasonNoAggregations).Inc()
		return plan, nil
	}

	if hasBinaryExpr {
		spanlog.DebugLog("msg", "projection pushdown skipped - plan contains binary operations")
		p.skipped.WithLabelValues(SkipReasonBinaryOperation).Inc()
		return plan, nil
	}

	if hasDeduplicateAndMerge {
		spanlog.DebugLog("msg", "projection pushdown skipped - plan contains DeduplicateAndMerge nodes")
		p.skipped.WithLabelValues(SkipReasonDeduplicate).Inc()
		return plan, nil
	}

	if len(requiredLabels) == 0 {
		spanlog.DebugLog("msg", "projection pushdown skipped - could not determine required labels")
		p.skipped.WithLabelValues(SkipReasonAmbiguousLabels).Inc()
		return plan, nil
	}

	// Unique identifier for each series is always necessary if we are using projections
	// so that we can deduplicate them in the absence of their full label set.
	requiredLabels[ProjectionSeriesHash] = struct{}{}

	// Convert required labels to a slice and sort it so that we end up with a consistent
	// query plan for the same set of required labels.
	sortedLabels := make([]string, 0, len(requiredLabels))
	for l := range requiredLabels {
		sortedLabels = append(sortedLabels, l)
	}
	slices.Sort(sortedLabels)

	_ = optimize.Walk(plan.Root, optimize.VisitorFunc(func(node planning.Node, path []planning.Node) error {
		switch e := node.(type) {
		case *core.VectorSelector:
			e.ProjectionInclude = true
			e.ProjectionLabels = slices.Clone(sortedLabels)
		case *core.MatrixSelector:
			e.ProjectionInclude = true
			e.ProjectionLabels = slices.Clone(sortedLabels)
		}
		return nil
	}))

	p.modified.Inc()
	spanlog.DebugLog(
		"msg", "projection pushdown complete",
		"selectors_affected", selectorCount,
		"required_labels", sortedLabels,
		"label_count", len(sortedLabels),
	)

	return plan, nil
}
