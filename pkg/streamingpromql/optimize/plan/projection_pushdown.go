// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// ProjectionPushdownOptimizationPass is an optimization pass that analyzes
// the query tree to determine which labels are required and sets up
// projection hints for VectorSelector and MatrixSelector nodes.
//
// For example, in the query sum by (service) (http_request_total{status="200"}),
// only the labels __name__, service, and status need to be read from storage.
type ProjectionPushdownOptimizationPass struct {
	projectionsSet prometheus.Counter
	logger         log.Logger
}

func NewProjectionPushdownOptimizationPass(reg prometheus.Registerer, logger log.Logger) *ProjectionPushdownOptimizationPass {
	return &ProjectionPushdownOptimizationPass{
		projectionsSet: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_projection_pushdown_hints_set_total",
			Help: "Number of projection hints set by the projection pushdown optimization pass.",
		}),
		logger: logger,
	}
}

func (p *ProjectionPushdownOptimizationPass) Name() string {
	return "Push down column projections"
}

func (p *ProjectionPushdownOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	requiredLabels := p.analyzeRequiredLabels(plan.Root)

	if requiredLabels == nil {
		level.Debug(p.logger).Log("msg", "projection pushdown skipped - uncertain label requirements")
		return plan, nil
	}

	// TODO(jesusvazquez) In theory we might not need the name label if the query didnt ask for it. But for now, to be safe, we always include it.
	if !slices.Contains(requiredLabels, "__name__") {
		requiredLabels = append(requiredLabels, "__name__")
	}
	// TODO(jesusvazquez) the hash is always set because im assuming the query can be split into multiple timerange queries and we need the series hash to do the final deduplication/merge. I need to validate if this is the case everytime but for now lets just propagate it.
	if !slices.Contains(requiredLabels, "s_series_hash") {
		requiredLabels = append(requiredLabels, "s_series_hash")
	}

	plan.ProjectionLabels = requiredLabels
	plan.ProjectionInclude = true

	selectorCount := p.countSelectors(plan.Root)
	p.projectionsSet.Add(float64(selectorCount))

	level.Debug(p.logger).Log(
		"msg", "projection pushdown complete",
		"selectors_affected", selectorCount,
		"required_labels", fmt.Sprintf("%v", requiredLabels),
		"label_count", len(requiredLabels),
	)

	return plan, nil
}

// analyzeRequiredLabels recursively analyzes the query tree to determine
// which labels are required for query execution
func (p *ProjectionPushdownOptimizationPass) analyzeRequiredLabels(node planning.Node) []string {
	switch n := node.(type) {
	case *core.VectorSelector:
		var labels []string
		for _, matcher := range n.Matchers {
			if !slices.Contains(labels, matcher.Name) {
				labels = append(labels, matcher.Name)
			}
		}
		return labels

	case *core.MatrixSelector:
		var labels []string
		for _, matcher := range n.Matchers {
			if !slices.Contains(labels, matcher.Name) {
				labels = append(labels, matcher.Name)
			}
		}
		return labels

	case *core.AggregateExpression:
		childLabels := p.analyzeRequiredLabels(n.Inner)
		if childLabels == nil {
			return nil
		}
		groupingLabels := n.Grouping

		combinedLabels := make([]string, 0, len(childLabels)+len(groupingLabels))
		combinedLabels = append(combinedLabels, childLabels...)

		for _, groupLabel := range groupingLabels {
			if !slices.Contains(combinedLabels, groupLabel) {
				combinedLabels = append(combinedLabels, groupLabel)
			}
		}

		return combinedLabels

	case *core.BinaryExpression:
		leftLabels := p.analyzeRequiredLabels(n.LHS)
		rightLabels := p.analyzeRequiredLabels(n.RHS)

		if leftLabels == nil || rightLabels == nil {
			return nil
		}

		combinedLabels := make([]string, 0, len(leftLabels)+len(rightLabels))
		combinedLabels = append(combinedLabels, leftLabels...)

		for _, label := range rightLabels {
			if !slices.Contains(combinedLabels, label) {
				combinedLabels = append(combinedLabels, label)
			}
		}

		if n.VectorMatching != nil {
			for _, label := range n.VectorMatching.MatchingLabels {
				if !slices.Contains(combinedLabels, label) {
					combinedLabels = append(combinedLabels, label)
				}
			}
			for _, label := range n.VectorMatching.Include {
				if !slices.Contains(combinedLabels, label) {
					combinedLabels = append(combinedLabels, label)
				}
			}
		}

		return combinedLabels

	case *core.FunctionCall:
		return nil

	case *core.UnaryExpression:
		return p.analyzeRequiredLabels(n.Inner)

	case *core.Subquery:
		return p.analyzeRequiredLabels(n.Inner)

	case *core.DeduplicateAndMerge:
		return p.analyzeRequiredLabels(n.Inner)

	default:
		return nil
	}
}

// countSelectors recursively counts VectorSelector and MatrixSelector nodes in the query tree
func (p *ProjectionPushdownOptimizationPass) countSelectors(node planning.Node) int {
	count := 0

	switch node.(type) {
	case *core.VectorSelector, *core.MatrixSelector:
		count++
	}

	// Recursively process children
	for _, child := range node.Children() {
		count += p.countSelectors(child)
	}

	return count
}

var _ optimize.QueryPlanOptimizationPass = &ProjectionPushdownOptimizationPass{}
