// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package plan

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting"
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

func (s *SkipHistogramDecodingOptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	s.applyToNode(plan.Root, false)

	return plan, nil
}

func (s *SkipHistogramDecodingOptimizationPass) applyToNode(node planning.Node, skipHistogramBuckets bool) {
	if vs, ok := node.(*core.VectorSelector); ok {
		vs.SkipHistogramBuckets = skipHistogramBuckets
		return
	}

	if ms, ok := node.(*core.MatrixSelector); ok {
		ms.SkipHistogramBuckets = skipHistogramBuckets
		return
	}

	// If we see a subquery, don't skip buckets. We need the buckets for correct counter reset detection.
	if _, ok := node.(*core.Subquery); ok {
		skipHistogramBuckets = false
	}

	// Don't skip buckets for range vector splitting either, buckets needed for counter reset detection at the boundary
	// between splits.
	if _, ok := node.(*rangevectorsplitting.SplitFunctionCall); ok {
		skipHistogramBuckets = false
	}

	if f, ok := node.(*core.FunctionCall); ok {
		switch f.Function {
		case functions.FUNCTION_HISTOGRAM_COUNT, functions.FUNCTION_HISTOGRAM_SUM, functions.FUNCTION_HISTOGRAM_AVG:
			skipHistogramBuckets = true
		case functions.FUNCTION_HISTOGRAM_FRACTION, functions.FUNCTION_HISTOGRAM_QUANTILE, functions.FUNCTION_HISTOGRAM_QUANTILES, functions.FUNCTION_HISTOGRAM_STDDEV, functions.FUNCTION_HISTOGRAM_STDVAR:
			skipHistogramBuckets = false
		case functions.FUNCTION_CHANGES:
			// Detecting a change compares whole histograms, including their buckets.
			skipHistogramBuckets = false
		default:
			// Nothing to do.
		}
	}

	// count_values uses the string representation of the whole histogram, including its buckets, as the value label.
	if a, ok := node.(*core.AggregateExpression); ok && a.Op == core.AGGREGATION_COUNT_VALUES {
		skipHistogramBuckets = false
	}

	if b, ok := node.(*core.BinaryExpression); ok {
		switch b.Op {
		case core.BINARY_TRIM_UPPER, core.BINARY_TRIM_LOWER:
			// Trim operators compute new count and sum from the bucket data, so we need to decode buckets.
			skipHistogramBuckets = false
		case core.BINARY_EQLC, core.BINARY_NEQ:
			// Comparing histograms for (in)equality takes their buckets into account.
			skipHistogramBuckets = false
		}
	}

	for child := range planning.ChildrenIter(node) {
		s.applyToNode(child, skipHistogramBuckets)
	}
}
