package plan

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

const ProjectionSeriesHash = "__series_hash__"

var _ optimize.QueryPlanOptimizationPass = &ProjectionPushdownOptimizationPass{}

type ProjectionPushdownOptimizationPass struct {
	logger log.Logger
}

func NewProjectionPushdownOptimizationPass(reg prometheus.Registerer, logger log.Logger) *ProjectionPushdownOptimizationPass {
	return &ProjectionPushdownOptimizationPass{logger: logger}
}

func (p *ProjectionPushdownOptimizationPass) Name() string {
	return "Push down label projections"
}

func (p *ProjectionPushdownOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, v planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	spanlog := spanlogger.FromContext(ctx, p.logger)

	var requiredLabels map[string]struct{}
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
		return plan, nil
	}

	if hasBinaryExpr {
		spanlog.DebugLog("msg", "projection pushdown skipped - plan contains binary operations")
		return plan, nil
	}

	if hasDeduplicateAndMerge {
		spanlog.DebugLog("msg", "projection pushdown skipped - plan contains DeduplicateAndMerge nodes")
		return plan, nil
	}

	if len(requiredLabels) == 0 {
		spanlog.DebugLog("msg", "projection pushdown skipped - could not determine required labels")
		return plan, nil
	}

	// Unique identifier for each series is always necessary if we are using projections
	// so that we can deduplicate them in the absence of their full label set.
	requiredLabels[ProjectionSeriesHash] = struct{}{}

	//plan.ProjectionLabels = requiredLabels
	//plan.ProjectionInclude = true

	//selectorCount := p.countSelectors(plan.Root)
	//p.projectionsSet.Add(float64(selectorCount))

	spanlog.DebugLog(
		"msg", "projection pushdown complete",
		"selectors_affected", selectorCount,
		"required_labels", requiredLabels,
		"label_count", len(requiredLabels),
	)

	return plan, nil
}
