// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package plan

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

type SkipHistogramDecodingOptimizationPass struct{}

func NewSkipHistogramDecodingOptimizationPass() *SkipHistogramDecodingOptimizationPass {
	return &SkipHistogramDecodingOptimizationPass{}
}

func (s *SkipHistogramDecodingOptimizationPass) Name() string {
	return "Skip decoding histogram buckets"
}

func (s *SkipHistogramDecodingOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan) (*planning.QueryPlan, error) {
	duplicatedSelectors := map[planning.Node]bool{}
	s.applyToNode(plan.Root, false, false, duplicatedSelectors)

	return plan, nil
}

func (s *SkipHistogramDecodingOptimizationPass) applyToNode(node planning.Node, skipHistogramBuckets bool, childOfDuplicateNode bool, duplicatedSelectors duplicatedSelectorTracker) {
	if vs, ok := node.(*core.VectorSelector); ok {
		if childOfDuplicateNode {
			skipHistogramBuckets = duplicatedSelectors.canSkipHistogramBuckets(node, skipHistogramBuckets)
		}

		vs.SkipHistogramBuckets = skipHistogramBuckets

		return
	}

	if ms, ok := node.(*core.MatrixSelector); ok {
		if childOfDuplicateNode {
			skipHistogramBuckets = duplicatedSelectors.canSkipHistogramBuckets(node, skipHistogramBuckets)
		}

		ms.SkipHistogramBuckets = skipHistogramBuckets
		return
	}

	if dup, ok := node.(*commonsubexpressionelimination.Duplicate); ok {
		s.applyToNode(dup.Inner, skipHistogramBuckets, true, duplicatedSelectors)
		return
	}

	if f, ok := node.(*core.FunctionCall); ok {
		switch f.FunctionName {
		case "histogram_count", "histogram_sum", "histogram_avg":
			skipHistogramBuckets = true
		case "histogram_fraction", "histogram_quantile":
			skipHistogramBuckets = false
		default:
			// Nothing to do.
		}
	}

	for _, child := range node.Children() {
		s.applyToNode(child, skipHistogramBuckets, childOfDuplicateNode, duplicatedSelectors)
	}
}

type duplicatedSelectorTracker map[planning.Node]bool

func (t duplicatedSelectorTracker) canSkipHistogramBuckets(node planning.Node, pathSeenAllowsSkippingHistogramBuckets bool) bool {
	otherInstancesCanSkipBuckets, seenOtherInstancesAlready := t[node]
	skipHistogramBuckets := pathSeenAllowsSkippingHistogramBuckets && (otherInstancesCanSkipBuckets || !seenOtherInstancesAlready)
	t[node] = skipHistogramBuckets
	return skipHistogramBuckets
}
