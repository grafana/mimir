// SPDX-License-Identifier: AGPL-3.0-only

package querysplitting

import (
	"context"
	"errors"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

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
// TODO: does this affect other optimisation passes? e.g. query sharding
type OptimizationPass struct {
	splitInterval time.Duration
	logger        log.Logger
}

func NewOptimizationPass(splitInterval time.Duration, logger log.Logger) *OptimizationPass {
	return &OptimizationPass{
		splitInterval: splitInterval,
		logger:        logger,
	}
}

func (o *OptimizationPass) Name() string {
	return "Query splitting"
}

func (o *OptimizationPass) Apply(ctx context.Context, plan *planning.QueryPlan, maximumSupportedQueryPlanVersion planning.QueryPlanVersion) (*planning.QueryPlan, error) {
	var err error
	plan.Root, err = o.wrapSplittableRangeVectorFunctions(plan.Root, plan.TimeRange)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

func (o *OptimizationPass) wrapSplittableRangeVectorFunctions(n planning.Node, timeRange types.QueryTimeRange) (planning.Node, error) {
	if functionCall, isFunctionCall := n.(*core.FunctionCall); isFunctionCall {
		wrappedNode, err := o.trySplitFunction(functionCall, timeRange)
		if err != nil {
			var notAppliedErr *errNotApplied
			if errors.As(err, &notAppliedErr) {
				level.Debug(o.logger).Log("msg", "query splitting not applied to function", "function", functionCall.GetFunction().PromQLName(), "reason", notAppliedErr.reason)
			} else {
				return nil, err
			}
		} else {
			level.Debug(o.logger).Log("msg", "query splitting applied to function", "function", functionCall.GetFunction().PromQLName())
			return wrappedNode, nil
		}
	}

	for i := range n.ChildCount() {
		child := n.Child(i)
		newChild, err := o.wrapSplittableRangeVectorFunctions(child, timeRange)
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
//  - For subqueries the resulting time range might not be indicative of the actual queried timerange depending on the
//    inner nodes for the subquery, so the split ranges might not align with the stored blocks after the adjustments.
//  - Similar for smoothed and anchored modifiers - these will load more data than for the specified range so the
//    adjustments won't work as well (as-is).
//  - For functions that require timestamps (e.g. ts_of_min_over_time), we will need to shift the result timestamps to
//    accommodate for the adjustment done for modifiers.
//  - We probably shouldn't cache intermediate results queries for @ modifiers at all. Caching is good for cases like
//    rules where the data being queried is partially the same as for previous executions. In the @ modifier case, the
//    exact same result will be returned time after time (disregarding OOO or querying in the future), so we should just
//    cache the entire result instead. We might still want to take advantage of the splitting part even if we don't want
//    to cache the intermediate results though - if we could parallelise query splitting it can still be beneficial for
//    queries with @ modifiers in terms of response time.
func (o *OptimizationPass) trySplitFunction(functionCall *core.FunctionCall, timeRange types.QueryTimeRange) (planning.Node, error) {
	// For now, only support instant queries (range queries are more complex)
	if !timeRange.IsInstant {
		return nil, &errNotApplied{reason: "not an instant query"}
	}

	// TODO: add more functions
	switch functionCall.GetFunction() {
	case functions.FUNCTION_SUM_OVER_TIME, functions.FUNCTION_COUNT_OVER_TIME:
	default:
		return nil, &errNotApplied{reason: "function not supported for splitting"}
	}

	// TODO: not all splittable functions will have the first child as the range vector operator
	if functionCall.ChildCount() == 0 {
		return nil, &errNotApplied{reason: "function has no children"}
	}

	inner, ok := functionCall.Child(0).(planning.SplittableNode)
	if !ok {
		return nil, &errNotApplied{reason: "failed to cast first child to SplittableNode"}
	}

	if inner.GetTimeRangeParams().Range.Milliseconds() <= o.splitInterval.Milliseconds() {
		return nil, &errNotApplied{reason: "range is not larger than split interval"}
	}

	timeParams := inner.GetTimeRangeParams()
	startTs, endTs := calculateInnerTimeRange(timeRange.StartT, timeParams)

	alignedStart := computeBlockAlignedStart(startTs, o.splitInterval)

	hasCompleteBlock := alignedStart+o.splitInterval.Milliseconds() <= endTs
	if !hasCompleteBlock {
		return nil, &errNotApplied{reason: "no complete blocks to cache"}
	}

	splitRanges := computeSplitRanges(startTs, endTs, o.splitInterval)

	n := &SplittableFunctionCall{
		SplittableFunctionCallDetails: &SplittableFunctionCallDetails{
			SplitRanges:       splitRanges,
			InnerNodeCacheKey: inner.QuerySplittingCacheKey(),
		},
	}
	if err := n.SetChildren([]planning.Node{functionCall}); err != nil {
		return nil, err
	}

	return n, nil
}

func calculateInnerTimeRange(evalTime int64, timeParams types.TimeRangeParams) (startTs, endTs int64) {
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
func computeSplitRanges(startTs, endTs int64, splitInterval time.Duration) []SplitRange {
	splitIntervalMs := splitInterval.Milliseconds()
	alignedStart := computeBlockAlignedStart(startTs, splitInterval)

	var ranges []SplitRange

	// Add head range if start is before first aligned boundary
	if startTs < alignedStart {
		ranges = append(ranges, SplitRange{
			Start:     startTs,
			End:       alignedStart,
			Cacheable: false,
		})
	}

	for splitStart := alignedStart; splitStart+splitIntervalMs <= endTs; splitStart += splitIntervalMs {
		ranges = append(ranges, SplitRange{
			Start:     splitStart,
			End:       splitStart + splitIntervalMs,
			Cacheable: true,
		})
	}

	// Add tail range if there's a partial block at the end
	lastAlignedEnd := alignedStart + ((endTs-alignedStart)/splitIntervalMs)*splitIntervalMs
	if lastAlignedEnd < endTs {
		ranges = append(ranges, SplitRange{
			Start:     lastAlignedEnd,
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
