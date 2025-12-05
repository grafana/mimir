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
type ProjectionPushdownOptimizationPass struct {
	projectionsSet prometheus.Counter
	logger         log.Logger
}

// It's based on the https://github.com/grafana/mimir/commit/72ed730e029e2ff40e619a4f60174c1ef36a80bf
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

func (p *ProjectionPushdownOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, _ planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// Must be an aggregation.
	// TODO: could aggregation exist somewhere other than the root node? probably yes.
	if !p.isAggregation(plan.Root) {
		level.Debug(p.logger).Log("msg", "projection pushdown skipped - root is not an aggregation")
		return plan, nil
	}
	// Must not contain binary operations
	// TODO: double-check that. Theory is that we need all labels from the binary operation sides to return error messages.
	if p.hasBinaryOperation(plan.Root) {
		level.Debug(p.logger).Log("msg", "projection pushdown skipped - plan contains binary operations")
		return plan, nil
	}
	// Must not contain DeduplicateAndMerge nodes, since they require all the labels to be present.
	if !plan.NoDeduplicateAndMergeNodes {
		level.Debug(p.logger).Log("msg", "projection pushdown skipped - plan contains DeduplicateAndMerge nodes")
		return plan, nil
	}

	requiredLabels := p.analyzeRequiredLabels(plan.Root)

	if requiredLabels == nil {
		level.Debug(p.logger).Log("msg", "projection pushdown skipped - uncertain label requirements")
		return plan, nil
	}

	if !slices.Contains(requiredLabels, "__series_hash__") {
		requiredLabels = append(requiredLabels, "__series_hash__")
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
// TODO: it's a very rough sketch. Currently it return grouping labels from the top-level aggregation node.
// It probably should iterave over the inner nodes recursively, since aggregation might be nested.

func (p *ProjectionPushdownOptimizationPass) analyzeRequiredLabels(node planning.Node) []string {
	switch n := node.(type) {
	case *core.AggregateExpression:
		return n.Grouping
	default:
		return nil // nil means all labels are required
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

// isAggregation checks if the root node is an aggregation
func (p *ProjectionPushdownOptimizationPass) isAggregation(node planning.Node) bool {
	_, ok := node.(*core.AggregateExpression)
	return ok
}

// hasBinaryOperation recursively checks if the query tree contains any binary operations
func (p *ProjectionPushdownOptimizationPass) hasBinaryOperation(node planning.Node) bool {
	if _, ok := node.(*core.BinaryExpression); ok {
		return true
	}

	// Recursively check children
	for _, child := range node.Children() {
		if p.hasBinaryOperation(child) {
			return true
		}
	}

	return false
}

var _ optimize.QueryPlanOptimizationPass = &ProjectionPushdownOptimizationPass{}
