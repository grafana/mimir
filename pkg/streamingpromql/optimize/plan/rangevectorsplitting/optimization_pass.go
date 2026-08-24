// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type OptimizationPass struct {
	splitInterval time.Duration

	splitNodesIntroduced   prometheus.Counter
	functionNodesInspected prometheus.Counter
	functionNodesUnsplit   *prometheus.CounterVec

	logger log.Logger
}

func NewOptimizationPass(splitInterval time.Duration, reg prometheus.Registerer, logger log.Logger) *OptimizationPass {
	return &OptimizationPass{
		splitInterval: splitInterval,
		splitNodesIntroduced: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_range_vector_splitting_nodes_introduced_total",
			Help: "Total number of SplitFunctionCall nodes introduced by the range vector splitting optimization pass.",
		}),
		functionNodesInspected: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_range_vector_splitting_function_nodes_inspected_total",
			Help: "Total number of function nodes inspected by range vector splitting to decide whether to split.",
		}),
		functionNodesUnsplit: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_range_vector_splitting_function_nodes_unsplit_total",
			Help: "Total number of function nodes inspected by range vector splitting but not split.",
		}, []string{"reason"}),
		logger: logger,
	}
}

func (o *OptimizationPass) Name() string {
	return "Range vector splitting"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	if maximumSupportedQueryPlanVersion < planning.QueryPlanV18 {
		return plan, nil
	}

	var err error
	plan.Root, err = o.wrapSplitRangeVectorFunctions(ctx, plan.Root, plan.Parameters.TimeRange)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (o *OptimizationPass) wrapSplitRangeVectorFunctions(ctx context.Context, n planning.Node, timeRange types.QueryTimeRange) (planning.Node, error) {
	logger := spanlogger.FromContext(ctx, o.logger)

	// Skip processing children of subqueries - range vectors inside subqueries
	// create a range query context which isn't supported yet.
	if _, isSubquery := n.(*core.Subquery); isSubquery {
		return n, nil
	}

	if functionCall, isFunctionCall := n.(*core.FunctionCall); isFunctionCall {
		o.functionNodesInspected.Inc()
		wrappedNode, notAppliedReason, err := o.trySplitFunction(functionCall, timeRange)
		if err != nil {
			o.functionNodesUnsplit.WithLabelValues("error").Inc()
			return nil, err
		}
		if notAppliedReason != "" {
			level.Debug(logger).Log("msg", "range vector splitting not applied to function", "function", functionCall.GetFunction().PromQLName(), "reason", notAppliedReason)
			o.functionNodesUnsplit.WithLabelValues(notAppliedReason).Inc()
		} else {
			level.Debug(logger).Log("msg", "range vector splitting applied to function", "function", functionCall.GetFunction().PromQLName())
			o.splitNodesIntroduced.Inc()
			return wrappedNode, nil
		}
	}

	for i := range n.ChildCount() {
		child := n.Child(i)
		newChild, err := o.wrapSplitRangeVectorFunctions(ctx, child, timeRange)
		if err != nil {
			return nil, err
		}
		if newChild != child {
			if err := n.ReplaceChild(i, newChild); err != nil {
				return nil, err
			}
		}
	}

	return n, nil
}

// trySplitFunction decides whether a function call is eligible to be wrapped with query splitting for intermediate
// result caching, based on structural properties known at planning time (the query is instant, the function is
// supported, the inner node is splittable, and the range is longer than the split interval).
//
// The concrete split ranges - including out-of-order-window-based cacheability - are NOT computed here. They are
// computed at materialize time (see Materializer.computeRanges), because they depend on the querier's current time and
// the tenant's out-of-order window, as well as the exact time range being evaluated (which can vary if splitting
// and caching applies).
func (o *OptimizationPass) trySplitFunction(functionCall *core.FunctionCall, timeRange types.QueryTimeRange) (planning.Node, string, error) {
	// For now, only support instant queries (range queries are more complex)
	if !timeRange.IsInstant {
		return nil, "range_query", nil
	}

	if _, ok := SplitFunctionRegistry[functionCall.GetFunction()]; !ok {
		return nil, "unsupported_function", nil
	}

	if functionCall.ChildCount() != 1 {
		return nil, "", fmt.Errorf("function child count is not 1")
	}

	inner, ok := functionCall.Child(0).(planning.SplitNode)
	if !ok || !inner.IsSplittable() {
		return nil, "unsupported_inner_node", nil
	}

	// Skip splitting for the fake selectors that subquery spinoff generates for now. These selectors will ignore the
	// sub time ranges from splitting and instead always query for the entire original range, so each split would end up
	// fetching more data than needed.
	if ms, ok := inner.(*core.MatrixSelector); ok && optimize.IsSpunOff(ms) {
		return nil, "subquery_spinoff", nil
	}

	if !inner.GetRangeParams().IsSet {
		// Should always be set if it's a splittable node
		return nil, "", fmt.Errorf("time range params not specified")
	}

	if inner.GetRangeParams().Range.Milliseconds() <= o.splitInterval.Milliseconds() {
		return nil, "too_short_interval", nil
	}

	n := &SplitFunctionCall{
		SplitFunctionCallDetails: &SplitFunctionCallDetails{},
		Inner:                    functionCall,
	}
	return n, "", nil
}
