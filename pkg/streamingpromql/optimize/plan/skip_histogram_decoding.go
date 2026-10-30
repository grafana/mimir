// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/promql/engine.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors

package plan

import (
	"context"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
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

func (s *SkipHistogramDecodingOptimizationPass) Slug() string {
	return "skip-decoding-histogram-buckets"
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

	skipHistogramBuckets = canSkipHistogramBucketsBelow(node, skipHistogramBuckets)

	for child := range planning.ChildrenIter(node) {
		s.applyToNode(child, skipHistogramBuckets)
	}
}

// canSkipHistogramBucketsBelow reports whether the selectors below node only need histogram statistics.
// skipHistogramBuckets is whether node itself only needs histogram statistics.
//
// Only nodes known not to look at buckets may appear between a selector and a histogram statistics function.
// Any other node needs the buckets by default, so that a new function or operator misses out on this
// optimization rather than silently returning wrong results.
func canSkipHistogramBucketsBelow(node planning.Node, skipHistogramBuckets bool) bool {
	switch n := node.(type) {
	case *core.FunctionCall:
		switch n.Function {
		case functions.FUNCTION_HISTOGRAM_COUNT, functions.FUNCTION_HISTOGRAM_SUM, functions.FUNCTION_HISTOGRAM_AVG:
			// These functions only need the count and sum of a histogram, and return floats.
			// Only floats flow above them, so it does not matter what is above them.
			return true
		}

		return skipHistogramBuckets && isHistogramStatsSafeFunction(n.Function)

	case *core.AggregateExpression:
		switch n.Op {
		case core.AGGREGATION_SUM, core.AGGREGATION_AVG, core.AGGREGATION_COUNT, core.AGGREGATION_GROUP, core.AGGREGATION_LIMITK, core.AGGREGATION_LIMIT_RATIO:
			return skipHistogramBuckets
		default:
			// Notably, count_values uses the string representation of the whole histogram, including its buckets,
			// and the others ignore histograms.
			return false
		}

	case *core.BinaryExpression:
		switch n.Op {
		case core.BINARY_MUL, core.BINARY_DIV, core.BINARY_LAND, core.BINARY_LOR, core.BINARY_LUNLESS:
			return skipHistogramBuckets
		case core.BINARY_ADD, core.BINARY_SUB:
			// Prometheus' engine needs the buckets here, because it drops the result when the counter reset hints
			// conflict, and the hints depend on whether the buckets are decoded. Our binary operations ignore
			// conflicting counter reset hints, so they do not need the buckets.
			return skipHistogramBuckets
		default:
			// Notably, == and != compare the buckets, trimming changes them, and the others drop histograms.
			return false
		}

	case *core.DeduplicateAndMerge, *core.DropName, *core.EvaluationRoot, *core.StepInvariantExpression, *core.UnaryExpression:
		return skipHistogramBuckets

	default:
		// Notably, subqueries and split function calls need the buckets for counter reset detection,
		// including at the boundary between splits.
		return false
	}
}

// isHistogramStatsSafeFunction reports whether f may appear between a selector and a histogram statistics function
// without requiring histogram buckets.
// These functions either pass histograms through, combine them using only their count and sum, or only look at
// timestamps and labels. Functions that ignore histograms are not listed, as below a histogram statistics function
// they always return an empty result.
func isHistogramStatsSafeFunction(f functions.Function) bool {
	switch f {
	case functions.FUNCTION_RATE,
		functions.FUNCTION_INCREASE,
		functions.FUNCTION_IRATE,
		functions.FUNCTION_DELTA,
		functions.FUNCTION_IDELTA:
		// Counter reset detection, where needed, happens in HistogramStatsIterator, which still sees the buckets.
		return true
	case functions.FUNCTION_SUM_OVER_TIME,
		functions.FUNCTION_AVG_OVER_TIME,
		functions.FUNCTION_FIRST_OVER_TIME,
		functions.FUNCTION_LAST_OVER_TIME,
		functions.FUNCTION_COUNT_OVER_TIME,
		functions.FUNCTION_PRESENT_OVER_TIME,
		functions.FUNCTION_ABSENT_OVER_TIME,
		functions.FUNCTION_TS_OF_FIRST_OVER_TIME,
		functions.FUNCTION_TS_OF_LAST_OVER_TIME:
		// These functions combine histograms using only their count and sum, pass them through, or only look at timestamps.
		return true
	case functions.FUNCTION_LABEL_REPLACE,
		functions.FUNCTION_LABEL_JOIN,
		functions.FUNCTION_SORT_BY_LABEL,
		functions.FUNCTION_SORT_BY_LABEL_DESC:
		// These functions only change labels or order, and pass the sample values through unchanged.
		return true
	case functions.FUNCTION_INFO:
		// info only adds labels from the info series, and passes the sample values through unchanged.
		return true
	case functions.FUNCTION_SHARDING_CONCAT,
		functions.FUNCTION_SHARDING_AVG:
		// Query sharding inserts these functions, which pass their input through unchanged.
		return true
	default:
		return false
	}
}
