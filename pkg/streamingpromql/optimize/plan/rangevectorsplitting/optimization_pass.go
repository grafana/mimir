// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/user"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type limitsProvider interface {
	OutOfOrderTimeWindow(userID string) time.Duration
}

// errNotApplied is a sentinel error returned by trySplitFunction when splitting cannot be applied
// (but not due to an actual error condition).
type errNotApplied struct {
	reason string
}

func (e *errNotApplied) Error() string {
	return e.reason
}

// OptimizationPass identifies range vector function calls that can benefit from splitting
// their computation into fixed-interval blocks for intermediate result caching.
type OptimizationPass struct {
	splitInterval time.Duration
	limits        limitsProvider

	splitNodesIntroduced   prometheus.Counter
	functionNodesInspected prometheus.Counter
	functionNodesUnsplit   *prometheus.CounterVec

	logger log.Logger

	timeNow func() time.Time
}

func NewOptimizationPass(splitInterval time.Duration, limits limitsProvider, timeNowFn func() time.Time, reg prometheus.Registerer, logger log.Logger) *OptimizationPass {
	return &OptimizationPass{
		splitInterval: splitInterval,
		limits:        limits,
		timeNow:       timeNowFn,
		splitNodesIntroduced: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_range_vector_splitting_nodes_introduced_total",
			Help: "Total number of range vector splitting nodes introduced by the range vector splitting optimization pass.",
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

// TestOnlySetTimeNow sets the time function. For tests only.
func (o *OptimizationPass) TestOnlySetTimeNow(timeNow func() time.Time) {
	o.timeNow = timeNow
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	if maximumSupportedQueryPlanVersion < planning.QueryPlanV5 {
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
	tenantID, err := user.ExtractOrgID(ctx)
	if err != nil {
		level.Warn(o.logger).Log("msg", "failed to extract tenant ID for range vector splitting, skipping optimization", "err", err)
		return n, nil
	}

	// Skip processing children of subqueries - range vectors inside subqueries
	// create a range query context which isn't supported yet.
	if _, isSubquery := n.(*core.Subquery); isSubquery {
		return n, nil
	}

	if functionCall, isFunctionCall := n.(*core.FunctionCall); isFunctionCall {
		o.functionNodesInspected.Inc()
		wrappedNode, err := o.trySplitFunction(functionCall, timeRange, tenantID)
		if err != nil {
			var notAppliedErr *errNotApplied
			if errors.As(err, &notAppliedErr) {
				level.Debug(o.logger).Log("msg", "range vector splitting not applied to function", "function", functionCall.GetFunction().PromQLName(), "reason", notAppliedErr.reason)
				o.functionNodesUnsplit.WithLabelValues(notAppliedErr.reason).Inc()
			} else {
				o.functionNodesUnsplit.WithLabelValues("error").Inc()
				return nil, err
			}
		} else {
			level.Debug(o.logger).Log("msg", "range vector splitting applied to function", "function", functionCall.GetFunction().PromQLName())
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

// trySplitFunction attempts to wrap a function call with query splitting for intermediate result caching.
// When creating the split ranges to query/cache by, the start and end timestamps are adjusted by the offset and @
// modifiers. The adjustment means that queries which load the same data will use the same cache entries, even if the
// actual query itself is different. E.g. sum_over_time(foo[2h]) at 4h and sum_over_time(foo[2h] offset 1h) at 5h load
// the same data and have the same result.
// For range vector selectors, this also means we can align split ranges to block boundaries(ish - the split interval is
// currently hardcoded, while blocks vary in size). This reduces unnecessary block reads.
// TODO: consider if the modifier adjustments are worth it when the supported nodes/functions are expanded.
//   - For subqueries the resulting time range might not be indicative of the actual queried timerange depending on the
//     inner nodes for the subquery, so the split ranges might not align with the stored blocks after the adjustments.
//   - For functions that require timestamps (e.g. ts_of_min_over_time), we will need to shift the result timestamps to
//     accommodate for the adjustment done for modifiers.
//   - We probably shouldn't cache intermediate results queries for @ modifiers at all. Caching is good for cases like
//     rules where the data being queried is partially the same as for previous executions. In the @ modifier case, the
//     exact same result will be returned time after time (disregarding OOO or querying in the future), so we should just
//     cache the entire result instead. We might still want to take advantage of the splitting part even if we don't want
//     to cache the intermediate results though - if we could parallelise query splitting it can still be beneficial for
//     queries with @ modifiers in terms of response time.
func (o *OptimizationPass) trySplitFunction(functionCall *core.FunctionCall, timeRange types.QueryTimeRange, tenantID string) (planning.Node, error) {
	// For now, only support instant queries (range queries are more complex)
	if !timeRange.IsInstant {
		return nil, &errNotApplied{reason: "range_query"}
	}

	f, ok := functions.RegisteredFunctions[functionCall.GetFunction()]
	if !ok {
		return nil, &errNotApplied{reason: "function_not_found"}
	}
	if f.RangeVectorSplitting == nil {
		return nil, &errNotApplied{reason: "unsupported_function"}
	}

	inner, ok := functionCall.Child(f.RangeVectorSplitting.RangeVectorChildIndex).(planning.SplitNode)
	if !ok || !inner.IsSplittable() {
		return nil, &errNotApplied{reason: "unsupported_inner_node"}
	}

	if !inner.GetRangeParams().IsSet {
		// Should always be set if it's a splittable node
		return nil, errors.New("time range params not specified")
	}

	if inner.GetRangeParams().Range.Milliseconds() <= o.splitInterval.Milliseconds() {
		return nil, &errNotApplied{reason: "too_short_interval"}
	}

	timeParams := inner.GetRangeParams()
	startTs, endTs := calculateInnerTimeRange(timeRange.StartT, timeParams)

	alignedStart := computeBlockAlignedStart(startTs, o.splitInterval)

	hasCompleteBlock := alignedStart+o.splitInterval.Milliseconds() <= endTs
	if !hasCompleteBlock {
		return nil, &errNotApplied{reason: "no_complete_cache_block"}
	}

	var oooThreshold int64
	oooWindow := o.limits.OutOfOrderTimeWindow(tenantID)
	if oooWindow > 0 {
		oooThreshold = o.timeNow().Add(-oooWindow).UnixMilli()
	}

	splitRanges := computeSplitRanges(startTs, endTs, o.splitInterval, oooThreshold)

	hasCacheable := false
	for _, r := range splitRanges {
		if r.Cacheable {
			hasCacheable = true
			break
		}
	}
	if !hasCacheable {
		return nil, &errNotApplied{reason: "no_cacheable_blocks_after_ooo_filter"}
	}

	n := &SplitFunctionCall{
		SplitFunctionCallDetails: &SplitFunctionCallDetails{
			SplitRanges:       splitRanges,
			InnerNodeCacheKey: inner.RangeVectorSplittingCacheKey(),
		},
	}
	if err := n.SetChildren([]planning.Node{functionCall}); err != nil {
		return nil, err
	}

	return n, nil
}

func calculateInnerTimeRange(evalTime int64, timeParams planning.RangeParams) (startTs, endTs int64) {
	endTs = evalTime
	if timeParams.Timestamp != nil {
		endTs = timeParams.Timestamp.UnixMilli()
	}

	endTs = endTs - timeParams.Offset.Milliseconds()
	startTs = endTs - timeParams.Range.Milliseconds()

	return startTs, endTs
}

// computeSplitRanges divides [startTs, endTs] into split ranges using PromQL semantics.
//
// Split ranges use left-open, right-closed intervals: (Start, End].
// When converted to storage queries, these become closed intervals [Start+1, End] on both sides,
// since the storage API expects closed intervals [mint, maxt].
//
// To align with TSDB block boundaries (which use [MinTime, MaxTime) semantics), we shift
// aligned boundaries by -1ms. This ensures that a split (Start, End] corresponds exactly
// to block [Start+1ms, End+1ms). For example:
//   - Split (7:59:59.999, 9:59:59.999] gets samples where 8:00:00.000 <= t <= 9:59:59.999
//   - This would map exactly to a two hour block:
//   - Block [8:00:00.000, 10:00:00.000) contains samples where 8:00:00.000 <= t < 10:00:00.000
func computeSplitRanges(startTs, endTs int64, splitInterval time.Duration, oooThreshold int64) []SplitRange {
	splitIntervalMs := splitInterval.Milliseconds()
	alignedStart := computeBlockAlignedStart(startTs, splitInterval)

	var ranges []SplitRange

	if alignedStart >= endTs {
		return []SplitRange{{Start: startTs, End: endTs, Cacheable: false}}
	}

	if startTs < alignedStart {
		// Check if range would be in ooo window
		if oooThreshold > 0 && alignedStart > oooThreshold {
			return []SplitRange{{Start: startTs, End: endTs, Cacheable: false}}
		}
		ranges = append(ranges, SplitRange{
			Start:     startTs,
			End:       alignedStart,
			Cacheable: false,
		})
	}

	var splitStart int64
	for splitStart = alignedStart; splitStart+splitIntervalMs <= endTs; splitStart += splitIntervalMs {
		splitEnd := splitStart + splitIntervalMs

		// Check if range would be in ooo window
		if oooThreshold > 0 && splitEnd > oooThreshold {
			ranges = append(ranges, SplitRange{
				Start:     splitStart,
				End:       endTs,
				Cacheable: false,
			})
			return ranges
		}

		ranges = append(ranges, SplitRange{
			Start:     splitStart,
			End:       splitEnd,
			Cacheable: true,
		})
	}

	// Add tail range if needed
	if splitStart < endTs {
		ranges = append(ranges, SplitRange{
			Start:     splitStart,
			End:       endTs,
			Cacheable: false,
		})
	}

	return ranges
}

func computeBlockAlignedStart(startTs int64, splitInterval time.Duration) int64 {
	splitIntervalMs := splitInterval.Milliseconds()
	// -1 to adjust for block boundaries. Query splitting time ranges are left open, the same as for PromQL. However,
	// block boundaries are left closed, in the sense that a 2h block will store samples from e.g. 8h to 10-1ms.
	alignedStart := (startTs/splitIntervalMs)*splitIntervalMs - 1
	if alignedStart < startTs {
		alignedStart += splitIntervalMs
	}
	return alignedStart
}
