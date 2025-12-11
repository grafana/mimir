// SPDX-License-Identifier: AGPL-3.0-only

package querysplitting

import (
	"fmt"
	"slices"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/querysplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &SplittableFunctionCall{SplittableFunctionCallDetails: &SplittableFunctionCallDetails{}}
	})
}

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

func (s *SplittableFunctionCall) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type SplittableFunctionCall supports 1 child, but got %d", len(children))
	}

	s.Inner = children[0]
	return nil
}

func (s *SplittableFunctionCall) Child(idx int) planning.Node {
	if idx > 0 {
		panic(fmt.Sprintf("SplittableFunctionCall node has 1 child, but attempted to get child at index %d", idx))
	}
	return s.Inner
}

func (s *SplittableFunctionCall) ChildCount() int {
	return 1
}

func (s *SplittableFunctionCall) ReplaceChild(idx int, child planning.Node) error {
	if idx > 0 {
		fmt.Errorf("SplittableFunctionCall node has 1 child, but attempted to replace child at index %d", idx)
	}
	s.Inner = child
	return nil
}

func (s *SplittableFunctionCall) MergeHints(other planning.Node) error {
	// Nothing to do.
	return nil
}

func (s *SplittableFunctionCall) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherSplit, ok := other.(*SplittableFunctionCall)
	if !ok {
		return false
	}

	return slices.EqualFunc(s.SplitRanges, otherSplit.SplitRanges, func(a, b SplitRange) bool {
		return a.Start == b.Start && a.End == b.End && a.Cacheable == b.Cacheable
	})
}

func (s *SplittableFunctionCall) Describe() string {
	if len(s.SplitRanges) == 0 {
		return "splits=0"
	}

	// Format: splits=4 [(3600000,7199999], (7199999,14399999]*, (14399999,21599999]*, (21599999,21600000]]
	// where * indicates cacheable ranges
	// Timestamps are in milliseconds since epoch
	var result string
	result = fmt.Sprintf("splits=%d [", len(s.SplitRanges))

	for i, sr := range s.SplitRanges {
		if i > 0 {
			result += ", "
		}

		result += fmt.Sprintf("(%d,%d]", sr.Start, sr.End)

		if sr.Cacheable {
			result += "*"
		}
	}

	result += "]"
	return result
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

func (s *SplittableFunctionCall) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return s.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (s *SplittableFunctionCall) ExpressionPosition() (posrange.PositionRange, error) {
	return s.Inner.ExpressionPosition()
}

func (s *SplittableFunctionCall) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return s.Inner.MinimumRequiredPlanVersion()
}

type Materializer struct {
	cache *cache.Cache
}

var _ planning.NodeMaterializer = &Materializer{}

func NewMaterializer(cache *cache.Cache) *Materializer {
	return &Materializer{
		cache: cache,
	}
}

func (m Materializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, _ types.TimeRangeParams) (planning.OperatorFactory, error) {
	s, ok := n.(*SplittableFunctionCall)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to materializer: expected SplittableFunctionCall, got %T", n)
	}
	innerFunctionCall, ok := s.Inner.(*core.FunctionCall)
	if !ok {
		return nil, fmt.Errorf("SplittableFunctionCall node should only wrap FunctionCall nodes, got %T", s.Inner)
	}

	f, exists := functions.RegisteredFunctions[innerFunctionCall.Function]
	if !exists {
		// TODO: log function string
		return nil, fmt.Errorf("function '%v' not found in functions list", innerFunctionCall.Function)
	}
	if f.SplittableOperatorFactory == nil {
		return nil, fmt.Errorf("function %v does not support query splitting", innerFunctionCall.Function)
	}

	ranges := make([]functions.Range, len(s.SplittableFunctionCallDetails.SplitRanges))
	for i, sr := range s.SplittableFunctionCallDetails.SplitRanges {
		ranges[i] = functions.Range{
			Start:     sr.Start,
			End:       sr.End,
			Cacheable: sr.Cacheable,
		}
	}

	expressionPos, err := innerFunctionCall.ExpressionPosition()
	if err != nil {
		return nil, err
	}

	splitOp, err := f.SplittableOperatorFactory(
		s.Inner.Child(0),
		materializer,
		timeRange,
		ranges,
		s.SplittableFunctionCallDetails.InnerNodeCacheKey,
		m.cache,
		expressionPos,
		params.Annotations,
		params.MemoryConsumptionTracker,
		params.QueryParameters.EnableDelayedNameRemoval,
		params.Logger,
	)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(splitOp), nil
}
