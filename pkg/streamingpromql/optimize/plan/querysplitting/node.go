// SPDX-License-Identifier: AGPL-3.0-only

package querysplitting

import (
	"errors"
	"fmt"
	"github.com/grafana/mimir/pkg/streamingpromql/cache"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &SplittableFunctionCall{SplittableFunctionCallDetails: &SplittableFunctionCallDetails{}}
	})
}

// SplittableFunctionCall wraps a range vector function call to split its computation into
// fixed-interval blocks for intermediate result caching.
type SplittableFunctionCall struct {
	*SplittableFunctionCallDetails
	Inner planning.Node
}

func (s *SplittableFunctionCall) Details() proto.Message {
	return s.SplittableFunctionCallDetails
}

func (s *SplittableFunctionCall) NodeType() planning.NodeType {
	return planning.NODE_TYPE_SPLIT_RANGE_VECTOR
}

func (s *SplittableFunctionCall) Children() []planning.Node {
	return []planning.Node{s.Inner}
}

func (s *SplittableFunctionCall) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type SplittableFunctionCall supports 1 child, but got %d", len(children))
	}

	s.Inner = children[0]
	return nil
}

func (s *SplittableFunctionCall) EquivalentTo(other planning.Node) bool {
	otherSplit, ok := other.(*SplittableFunctionCall)
	return ok && s.Inner.EquivalentTo(otherSplit.Inner)
}

func (s *SplittableFunctionCall) Describe() string {
	return "split into cacheable blocks"
}

func (s *SplittableFunctionCall) ChildrenLabels() []string {
	return []string{""}
}

func (s *SplittableFunctionCall) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (s *SplittableFunctionCall) ResultType() (parser.ValueType, error) {
	return s.Inner.ResultType()
}

func (s *SplittableFunctionCall) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return s.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (s *SplittableFunctionCall) ExpressionPosition() posrange.PositionRange {
	return s.Inner.ExpressionPosition()
}

func (s *SplittableFunctionCall) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return s.Inner.MinimumRequiredPlanVersion()
}

type Materializer struct {
	cache cache.IntermediateResultsCache
}

var _ planning.NodeMaterializer = &Materializer{}

func NewMaterializer(cache cache.IntermediateResultsCache) *Materializer {
	return &Materializer{
		cache: cache,
	}
}

func (m Materializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	s, ok := n.(*SplittableFunctionCall)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to materializer: expected SplittableFunctionCall, got %T", n)
	}
	innerFunctionCall, ok := s.Inner.(*core.FunctionCall)
	if !ok {
		return nil, fmt.Errorf("SplittableFunctionCall node should only wrap FunctionCall nodes, got %T", s.Inner)
	}

	var funcDef functions.FunctionOverRangeVectorDefinition
	switch innerFunctionCall.Function {
	case functions.FUNCTION_SUM_OVER_TIME:
		funcDef = functions.SumOverTime
	default:
		return nil, fmt.Errorf("function %v is not yet supported for split range vector optimization", innerFunctionCall.Function)
	}

	splitDuration := time.Duration(s.SplittableFunctionCallDetails.SplitDurationMs) * time.Millisecond

	matrixSelector, ok := s.Inner.Children()[0].(*core.MatrixSelector)
	if !ok {
		return nil, errors.New("inner.children[0] is expected to be a matrix selector")
	}

	splitOp, err := functions.NewFunctionOverRangeVectorSplit(
		matrixSelector,
		materializer,
		timeRange,
		splitDuration,
		m.cache,
		funcDef,
		innerFunctionCall.ExpressionPosition(),
		params.Annotations,
		params.MemoryConsumptionTracker,
		params.EnableDelayedNameRemoval,
	)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(splitOp), nil
}
