// SPDX-License-Identifier: AGPL-3.0-only

package querysplitting

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &SplitRangeVector{SplitRangeVectorDetails: &SplitRangeVectorDetails{}}
	})
}

// SplitRangeVector wraps a range vector function call to split its computation into
// fixed-interval blocks for intermediate result caching.
type SplitRangeVector struct {
	*SplitRangeVectorDetails
	Inner planning.Node
}

func (s *SplitRangeVector) Details() proto.Message {
	return s.SplitRangeVectorDetails
}

func (s *SplitRangeVector) NodeType() planning.NodeType {
	return planning.NODE_TYPE_SPLIT_RANGE_VECTOR
}

func (s *SplitRangeVector) Children() []planning.Node {
	return []planning.Node{s.Inner}
}

func (s *SplitRangeVector) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type SplitRangeVector supports 1 child, but got %d", len(children))
	}

	s.Inner = children[0]
	return nil
}

func (s *SplitRangeVector) EquivalentTo(other planning.Node) bool {
	otherSplit, ok := other.(*SplitRangeVector)
	return ok && s.Inner.EquivalentTo(otherSplit.Inner)
}

func (s *SplitRangeVector) Describe() string {
	return "split into cacheable blocks"
}

func (s *SplitRangeVector) ChildrenLabels() []string {
	return []string{""}
}

func (s *SplitRangeVector) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (s *SplitRangeVector) ResultType() (parser.ValueType, error) {
	return s.Inner.ResultType()
}

func (s *SplitRangeVector) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return s.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (s *SplitRangeVector) ExpressionPosition() posrange.PositionRange {
	return s.Inner.ExpressionPosition()
}

func (s *SplitRangeVector) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return s.Inner.MinimumRequiredPlanVersion()
}

func MaterializeSplitRangeVector(s *SplitRangeVector, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	// Get the inner operator (which should be a FunctionOverRangeVector)
	innerOp, err := materializer.ConvertNodeToOperator(s.Inner, timeRange)
	if err != nil {
		return nil, err
	}

	// The inner MUST be a FunctionOverRangeVector - the optimization pass should only wrap FunctionCall
	// nodes that produce FunctionOverRangeVector operators. If not, it's a programming error.
	baseFuncRange, ok := innerOp.(*functions.FunctionOverRangeVector)
	if !ok {
		return nil, fmt.Errorf("SplitRangeVector node should only wrap operators that produce FunctionOverRangeVector, got %T", innerOp)
	}

	// Get the cache
	var irCache *cache.IntermediateResultTenantCache
	if params.IntermediateResultCache != nil {
		if tenantCache, ok := params.IntermediateResultCache.(*cache.IntermediateResultTenantCache); ok {
			irCache = tenantCache
		}
	}

	// Convert proto CacheBlocks to IntermediateResultBlocks
	intermediateBlocks := make([]functions.IntermediateResultBlock, len(s.CacheBlocks))
	for i, block := range s.CacheBlocks {
		intermediateBlocks[i] = functions.IntermediateResultBlock{
			StartTimestampMs: block.StartTimestampMs,
			DurationMs:       block.DurationMs,
		}
	}

	// Create the split version with pre-calculated blocks and cache key
	splitOp := functions.NewFunctionOverRangeVectorSplitWithBlocks(
		baseFuncRange.Inner,
		baseFuncRange.ScalarArgs,
		baseFuncRange.MemoryConsumptionTracker,
		baseFuncRange.Func,
		baseFuncRange.Annotations,
		baseFuncRange.ExpressionPosition(),
		timeRange,
		params.EnableDelayedNameRemoval,
		s.CacheKey,
		irCache,
		intermediateBlocks,
	)

	return planning.NewSingleUseOperatorFactory(splitOp), nil
}
