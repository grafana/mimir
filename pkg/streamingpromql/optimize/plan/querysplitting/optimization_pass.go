// SPDX-License-Identifier: AGPL-3.0-only

package querysplitting

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

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
		shouldSplit, reason := o.shouldSplitFunction(functionCall, timeRange)

		if shouldSplit {
			wrappedNode, err := o.wrapInSplitNode(functionCall)
			if err != nil {
				return nil, err
			}
			matrixSelector := functionCall.Child(0).(*core.MatrixSelector)
			o.logger.Log("msg", "query splitting applied to function", "function", functionCall.GetFunction().PromQLName(), "range_ms", matrixSelector.Range.Milliseconds(), "split_interval_ms", o.splitInterval.Milliseconds())
			return wrappedNode, nil
		}

		if reason != "" {
			o.logger.Log("msg", "query splitting not applied to function", "function", functionCall.GetFunction().PromQLName(), "reason", reason)
		}
	}

	for i := range n.ChildCount() {
		child := n.Child(i)
		newChild, err := o.wrapSplittableRangeVectorFunctions(child, timeRange)
		if err != nil {
			return nil, err
		}
		if newChild != child {
			n.ReplaceChild(i, newChild)
		}
	}

	return n, nil
}

func (o *OptimizationPass) shouldSplitFunction(functionCall *core.FunctionCall, timeRange types.QueryTimeRange) (bool, string) {
	// For now, only support instant queries (range queries are more complex)
	if !timeRange.IsInstant {
		return false, "not an instant query"
	}

	// TODO: add more functions
	switch functionCall.GetFunction() {
	case functions.FUNCTION_SUM_OVER_TIME:
	default:
		return false, "function not supported for splitting"
	}

	// TODO: not all splittable functions will have the first child as the range vector operator
	if functionCall.ChildCount() == 0 {
		return false, "function has no children"
	}

	matrixSelector, ok := functionCall.Child(0).(*core.MatrixSelector)
	if !ok {
		return false, "failed to cast first child to matrix selector"
	}

	// Check if the range is large enough to benefit from splitting
	// The range must be larger than the split interval for caching to be useful
	splitIntervalMs := o.splitInterval.Milliseconds()
	rangeMs := matrixSelector.Range.Milliseconds()
	if rangeMs <= splitIntervalMs {
		return false, "range is not larger than split interval"
	}

	// Additional check: verify there would be at least one complete block to cache
	// Calculate split boundaries using query time (matching what the operator does in createSplits)
	// TODO: Should instead account for timestamp and offset to align with how samples are divided into blocks in storage.
	startTs := timeRange.StartT - rangeMs
	endTs := timeRange.StartT

	alignedStart := (startTs / splitIntervalMs) * splitIntervalMs
	if alignedStart < startTs {
		alignedStart += splitIntervalMs
	}

	if alignedStart+splitIntervalMs > endTs {
		return false, "no complete blocks to cache"
	}

	return true, ""
}

func (o *OptimizationPass) wrapInSplitNode(functionCall *core.FunctionCall) (planning.Node, error) {
	// Split duration is passed to the operator - splits are computed at runtime
	splitDurationMs := o.splitInterval.Milliseconds()

	n := &SplittableFunctionCall{
		SplittableFunctionCallDetails: &SplittableFunctionCallDetails{
			SplitDurationMs: splitDurationMs,
		},
	}
	if err := n.SetChildren([]planning.Node{functionCall}); err != nil {
		return nil, err
	}

	return n, nil
}
