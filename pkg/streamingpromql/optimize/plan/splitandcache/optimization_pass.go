// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/commonsubexpressionelimination"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/grafana/mimir/pkg/util/spanlogger"
)

type OptimizationPass struct {
	splitEnabled  bool
	splitInterval time.Duration
	cacheEnabled  bool

	limits              LimitsProvider
	cacheAttemptCounter prometheus.Counter
	cacheSkippedCounter *prometheus.CounterVec
	timeNow             func() time.Time
	logger              log.Logger
}

func NewOptimizationPass(splitEnabled bool, splitInterval time.Duration, cacheEnabled bool, limits LimitsProvider, reg prometheus.Registerer, logger log.Logger) *OptimizationPass {
	return &OptimizationPass{
		splitEnabled:        splitEnabled,
		splitInterval:       splitInterval,
		cacheEnabled:        cacheEnabled,
		limits:              limits,
		cacheAttemptCounter: NewQueryResultCacheAttemptedCounter(reg),
		cacheSkippedCounter: NewQueryResultCacheSkippedCounter(reg),
		timeNow:             time.Now,
		logger:              logger,
	}
}

// OverrideTimeNow overrides the time.Now function used by this optimization pass.
// This should only be used for testing purposes.
func (o *OptimizationPass) OverrideTimeNow(f func() time.Time) {
	o.timeNow = f
}

func (o *OptimizationPass) Name() string {
	return "Range query splitting and caching"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, _ planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	// We do not need to check the maximum supported query plan version here: the nodes injected by this optimization pass
	// only run in query-frontends (and therefore would only run inside this process).

	spanLogger := spanlogger.FromContext(ctx, o.logger)
	now := o.timeNow()
	maxFreshness, err := o.limits.GetMaxCacheFreshness(ctx)
	if err != nil {
		return nil, err
	}
	freshnessThreshold := now.Add(-maxFreshness)

	// When a query has been rewritten to spin off subqueries, each __vector_evaluation_root__ and __scalar_evaluation_root__ subtree is a
	// separate query, and the spun-off subqueries are range queries. Inject splitting and caching nodes
	// beneath each EvaluationRoot that evaluates a range query, rather than at the root of the plan
	// (which is the overall instant query, and so is left unchanged just like an instant query without
	// any spun-off subqueries).
	if containsEvaluationRoot(plan.Root) {
		if err := o.applyToEvaluationRoots(ctx, plan.Root, plan.Parameters.TimeRange, freshnessThreshold, spanLogger); err != nil {
			return nil, err
		}

		return plan, nil
	}

	plan.Root, err = o.applySplittingAndCaching(ctx, plan.Root, plan.Parameters.TimeRange, freshnessThreshold, spanLogger)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func containsEvaluationRoot(node planning.Node) bool {
	if _, ok := node.(*core.EvaluationRoot); ok {
		return true
	}

	for child := range planning.ChildrenIter(node) {
		if containsEvaluationRoot(child) {
			return true
		}
	}

	return false
}

// applyToEvaluationRoots descends the plan, tracking the time range at which each node is evaluated,
// and injects splitting and caching nodes beneath each EvaluationRoot that evaluates a range query
// producing an instant vector.
func (o *OptimizationPass) applyToEvaluationRoots(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange, freshnessThreshold time.Time, spanLogger *spanlogger.SpanLogger) error {
	if evaluationRoot, ok := node.(*core.EvaluationRoot); ok {
		var err error
		evaluationRoot.Inner, err = o.applySplittingAndCaching(ctx, evaluationRoot.Inner, timeRange, freshnessThreshold, spanLogger)
		if err != nil {
			return err
		}

		// EvaluationRoots are never nested, so there's no need to descend further.
		return nil
	}

	childrenTimeRange := node.ChildrenTimeRange(timeRange)
	for child := range planning.ChildrenIter(node) {
		if err := o.applyToEvaluationRoots(ctx, child, childrenTimeRange, freshnessThreshold, spanLogger); err != nil {
			return err
		}
	}

	return nil
}

func (o *OptimizationPass) applySplittingAndCaching(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange, freshnessThreshold time.Time, spanLogger *spanlogger.SpanLogger) (planning.Node, error) {
	if timeRange.IsInstant {
		return node, nil
	}

	if isSplittingOrCachingNode(node) {
		// This node has already had splitting or caching applied to it (eg. a duplicate subexpression).
		return node, nil
	}

	if resultType, err := node.ResultType(); err != nil {
		return nil, err
	} else if resultType != parser.ValueTypeVector {
		return node, nil
	}

	node, err := o.applyCaching(ctx, node, timeRange, freshnessThreshold, spanLogger)
	if err != nil {
		return nil, err
	}

	node, err = o.applySplitting(node, spanLogger)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func isSplittingOrCachingNode(node planning.Node) bool {
	switch node.(type) {
	case *Cache, *TimeRangeSplit:
		return true
	default:
		return false
	}
}

func (o *OptimizationPass) applyCaching(ctx context.Context, node planning.Node, timeRange types.QueryTimeRange, freshnessThreshold time.Time, spanLogger *spanlogger.SpanLogger) (planning.Node, error) {
	if !o.cacheEnabled {
		spanLogger.DebugLog("msg", "range query caching disabled by config")
		return node, nil
	}

	if requestoptions.OptionsFromContext(ctx).CacheDisabled {
		spanLogger.DebugLog("msg", "range query caching disabled by request options")
		return node, nil
	}

	o.cacheAttemptCounter.Inc()

	if !isStepAligned(timeRange) {
		if allowed, err := o.limits.AllowCachingUnalignedQueries(ctx); err != nil {
			spanLogger.DebugLog("msg", "retrieving 'allow caching unaligned queries' tenant limit failed", "err", err)
			return nil, err
		} else if !allowed {
			spanLogger.DebugLog("msg", "range query not cacheable: it is not step-aligned and caching unaligned queries is disabled")
			o.cacheSkippedCounter.WithLabelValues(NotCachableReasonUnalignedTimeRange).Inc()
			return node, nil
		}
	}

	if timeRange.StartT > timestamp.FromTime(freshnessThreshold) {
		// The query starts after the freshness threshold, so the entire query is not cacheable.
		// If the query straddles the freshness threshold, the cache operator will only cache the
		// portion of the query that is before the freshness threshold.
		spanLogger.DebugLog("msg", "range query not cacheable: it is entirely within the freshness window")
		o.cacheSkippedCounter.WithLabelValues(NotCachableReasonTooNew).Inc()
		return node, nil
	}

	if !o.modifiersAllowCaching(node, timeRange, freshnessThreshold) {
		spanLogger.DebugLog("msg", "range query not cacheable: it contains modifiers that prevent caching")
		o.cacheSkippedCounter.WithLabelValues(NotCachableReasonModifiersNotCachable).Inc()
		return node, nil
	}

	return &Cache{
		CacheDetails: &CacheDetails{
			SplitInterval: o.splitInterval,
		},
		Inner: node,
	}, nil
}

func (o *OptimizationPass) modifiersAllowCaching(node planning.Node, timeRange types.QueryTimeRange, freshnessThreshold time.Time) bool {
	switch node := node.(type) {
	case *core.VectorSelector:
		return node.Offset >= 0 && timestampAllowsCaching(node.Timestamp, timeRange, freshnessThreshold)

	case *core.MatrixSelector:
		return node.Offset >= 0 && timestampAllowsCaching(node.Timestamp, timeRange, freshnessThreshold)

	case *core.Subquery:
		return node.Offset >= 0 && timestampAllowsCaching(node.Timestamp, timeRange, freshnessThreshold) && o.modifiersAllowCaching(node.Inner, timeRange, freshnessThreshold)

	default:
		for n := range planning.ChildrenIter(node) {
			if !o.modifiersAllowCaching(n, timeRange, freshnessThreshold) {
				return false
			}
		}

		return true
	}
}

func timestampAllowsCaching(ts *time.Time, timeRange types.QueryTimeRange, freshnessThreshold time.Time) bool {
	if ts == nil {
		return true
	}

	return timestamp.FromTime(*ts) <= timeRange.EndT && (*ts).Before(freshnessThreshold)
}

func isStepAligned(timeRange types.QueryTimeRange) bool {
	// Note that this deliberately differs from the middleware implementation:
	// the middleware implementation also checks that the end timestamp is step aligned, but this is not necessary.
	// All step timestamps are calculated relative to the start timestamp, so it is sufficient to check that the start timestamp is step aligned.
	return timeRange.StartT%timeRange.IntervalMilliseconds == 0
}

func (o *OptimizationPass) applySplitting(node planning.Node, spanLogger *spanlogger.SpanLogger) (planning.Node, error) {
	if !o.splitEnabled {
		spanLogger.DebugLog("msg", "range query splitting disabled by config")
		return node, nil
	}

	if err := o.injectDuplicateNodesForStepInvariantExpressions(node); err != nil {
		return nil, err
	}

	// If splitting is enabled, we always add a TimeRangeSplit node, regardless of the time range.
	// This keeps the split time range calculation logic in one place, and simplifies the logic here.
	// The materializer for TimeRangeSplit will omit the splitting operator if there is only one range.
	return &TimeRangeSplit{
		TimeRangeSplitDetails: &TimeRangeSplitDetails{
			SplitInterval: o.splitInterval,
		},
		Inner: node,
	}, nil
}

// We want to only evaluate step-invariant expressions once per query request, rather than once per split.
// Each child of a step-invariant expression will be evaluated at T=0, and the StepInvariantExpression node itself will
// be evaluated over the split time range. So we need to duplicate the step-invariant expression for each split,
// so the result can be used for each split time range.
func (o *OptimizationPass) injectDuplicateNodesForStepInvariantExpressions(node planning.Node) error {
	if node, ok := node.(*core.StepInvariantExpression); ok {
		if _, isDuplicate := node.Inner.(*commonsubexpressionelimination.Duplicate); !isDuplicate {
			node.Inner = &commonsubexpressionelimination.Duplicate{
				DuplicateDetails: &commonsubexpressionelimination.DuplicateDetails{},
				Inner:            node.Inner,
			}
		}
	}

	for n := range planning.ChildrenIter(node) {
		if err := o.injectDuplicateNodesForStepInvariantExpressions(n); err != nil {
			return err
		}
	}

	return nil
}
