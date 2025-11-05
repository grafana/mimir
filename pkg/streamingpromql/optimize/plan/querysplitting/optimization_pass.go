// SPDX-License-Identifier: AGPL-3.0-only

package querysplitting

import (
	"context"
	"time"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

// OptimizationPass identifies range vector function calls that can benefit from splitting
// their computation into fixed-interval blocks for intermediate result caching.
type OptimizationPass struct {
	splitInterval time.Duration
}

func NewOptimizationPass(splitInterval time.Duration) *OptimizationPass {
	return &OptimizationPass{
		splitInterval: splitInterval,
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
		if o.shouldSplitFunction(functionCall, timeRange) {
			wrappedNode, err := o.wrapInSplitNode(functionCall, timeRange)
			if err != nil {
				return nil, err
			}
			return wrappedNode, nil
		}
	}

	children := n.Children()
	modified := false

	for i, child := range children {
		newChild, err := o.wrapSplittableRangeVectorFunctions(child, timeRange)
		if err != nil {
			return nil, err
		}

		if newChild != child {
			children[i] = newChild
			modified = true
		}
	}

	if modified {
		if err := n.SetChildren(children); err != nil {
			return nil, err
		}
	}

	return n, nil
}

func (o *OptimizationPass) shouldSplitFunction(functionCall *core.FunctionCall, timeRange types.QueryTimeRange) bool {
	// For now, only support instant queries (range queries are more complex)
	if !timeRange.IsInstant {
		return false
	}

	if _, exists := functions.RegisteredFunctions[functionCall.Function]; !exists {
		return false
	}

	// TODO: add more functions
	switch functionCall.Function {
	case functions.FUNCTION_SUM_OVER_TIME:
	default:
		return false
	}

	// TODO: not all splittable functions will have the first child as the range vector operator
	children := functionCall.Children()
	if len(children) == 0 {
		return false
	}

	if children[0].NodeType() != planning.NODE_TYPE_MATRIX_SELECTOR {
		return false
	}

	matrixSelector, ok := children[0].(*core.MatrixSelector)
	if !ok {
		return false
	}

	// Check if the range is large enough to benefit from splitting
	// The range must be larger than the split interval for caching to be useful
	splitIntervalMs := o.splitInterval.Milliseconds()
	rangeMs := matrixSelector.Range.Milliseconds()
	if rangeMs <= splitIntervalMs {
		return false
	}

	// Additional check: verify there would be at least one complete block to cache
	// Calculate the actual data range that will be queried
	firstStepT := timeRange.StartT
	firstRangeEnd := firstStepT
	if matrixSelector.Timestamp != nil {
		firstRangeEnd = matrixSelector.Timestamp.UnixMilli()
	}
	firstRangeEnd = firstRangeEnd - matrixSelector.Offset.Milliseconds()
	earliestDataT := firstRangeEnd - rangeMs

	lastStepT := timeRange.EndT
	lastRangeEnd := lastStepT
	if matrixSelector.Timestamp != nil {
		lastRangeEnd = matrixSelector.Timestamp.UnixMilli()
	}
	lastRangeEnd = lastRangeEnd - matrixSelector.Offset.Milliseconds()
	latestDataT := lastRangeEnd

	firstBlockStart := ((earliestDataT / splitIntervalMs) + 1) * splitIntervalMs
	lastBlockEnd := (latestDataT / splitIntervalMs) * splitIntervalMs

	// If no complete blocks would fit, don't use the split operator
	if firstBlockStart >= lastBlockEnd {
		return false
	}

	return true
}

func (o *OptimizationPass) wrapInSplitNode(functionCall *core.FunctionCall, timeRange types.QueryTimeRange) (planning.Node, error) {
	// Calculate cache blocks during planning
	blocks := o.calculateCacheBlocks(functionCall, timeRange)

	// Calculate cache key from the inner node (matrix selector)
	innerNode := functionCall.Children()[0]
	cacheKey := planning.CacheKey(innerNode)

	n := &SplitRangeVector{
		SplitRangeVectorDetails: &SplitRangeVectorDetails{
			CacheBlocks: blocks,
			CacheKey:    cacheKey,
		},
	}
	if err := n.SetChildren([]planning.Node{functionCall}); err != nil {
		return nil, err
	}

	return n, nil
}

func (o *OptimizationPass) calculateCacheBlocks(functionCall *core.FunctionCall, timeRange types.QueryTimeRange) []CacheBlock {
	matrixSelector := functionCall.Children()[0].(*core.MatrixSelector)

	// TODO: Split interval could vary based on data age to align with storage block boundaries:
	// - Recent data (last 2-24h): Use 2h blocks to align with ingester blocks
	// - Older data (>24h): Use 24h blocks to align with final compacted blocks

	// Calculate the actual data range that will be queried
	// For instant queries, firstStepT == lastStepT, so earliestDataT == latestDataT - range
	firstStepT := timeRange.StartT
	firstRangeEnd := firstStepT
	if matrixSelector.Timestamp != nil {
		firstRangeEnd = matrixSelector.Timestamp.UnixMilli()
	}
	firstRangeEnd = firstRangeEnd - matrixSelector.Offset.Milliseconds()
	earliestDataT := firstRangeEnd - matrixSelector.Range.Milliseconds()

	lastStepT := timeRange.EndT
	lastRangeEnd := lastStepT
	if matrixSelector.Timestamp != nil {
		lastRangeEnd = matrixSelector.Timestamp.UnixMilli()
	}
	lastRangeEnd = lastRangeEnd - matrixSelector.Offset.Milliseconds()
	latestDataT := lastRangeEnd

	splitIntervalMs := o.splitInterval.Milliseconds()

	// Find block boundaries
	firstBlockStart := ((earliestDataT / splitIntervalMs) + 1) * splitIntervalMs
	lastBlockEnd := (latestDataT / splitIntervalMs) * splitIntervalMs

	// Calculate number of complete blocks
	numBlocks := int((lastBlockEnd - firstBlockStart) / splitIntervalMs)
	if numBlocks <= 0 {
		return nil
	}

	blocks := make([]CacheBlock, numBlocks)
	blockStart := firstBlockStart

	for i := 0; i < numBlocks; i++ {
		blocks[i] = CacheBlock{
			StartTimestampMs: blockStart,
			DurationMs:       splitIntervalMs,
		}
		blockStart += splitIntervalMs
	}

	return blocks
}
