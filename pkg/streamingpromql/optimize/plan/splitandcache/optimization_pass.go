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

	if plan.Parameters.TimeRange.IsInstant {
		// Nothing to do.
		return plan, nil
	}

	if resultType, err := plan.Root.ResultType(); err != nil {
		return nil, err
	} else if resultType != parser.ValueTypeVector {
		return plan, nil
	}

	spanLogger := spanlogger.FromContext(ctx, o.logger)

	now := o.timeNow()
	maxFreshness, err := o.limits.GetMaxCacheFreshness(ctx)
	if err != nil {
		return nil, err
	}
	freshnessThreshold := now.Add(-maxFreshness)

	root := plan.Root
	root, err = o.applyCaching(ctx, root, plan.Parameters.TimeRange, freshnessThreshold, spanLogger)
	if err != nil {
		return nil, err
	}

	root, err = o.applySplitting(root, spanLogger)
	if err != nil {
		return nil, err
	}

	plan.Root = root
	return plan, nil
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

	if timeRange.StartT > timestamp.FromTime(freshnessThreshold) {
		// The query starts after the freshness threshold, so the entire query is not cacheable.
		// If the query straddles the freshness threshold, the cache operator will only cache the
		// portion of the query that is before the freshness threshold.
		spanLogger.DebugLog("msg", "range query not cacheable: it is entirely within the freshness window")
		o.cacheSkippedCounter.WithLabelValues(NotCachableReasonTooNew).Inc()
		return node, nil
	}

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

	if !o.modifiersAllowCaching(node, timeRange, freshnessThreshold) {
		spanLogger.DebugLog("msg", "range query not cacheable: it contains modifers that prevent caching")
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
