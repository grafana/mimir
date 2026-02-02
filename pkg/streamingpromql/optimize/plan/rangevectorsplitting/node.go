// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"fmt"
	"slices"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &SplitFunctionCall{SplitFunctionCallDetails: &SplitFunctionCallDetails{}}
	})
}

type SplitFunctionCall struct {
	*SplitFunctionCallDetails
	Inner planning.Node
}

func (s *SplitFunctionCall) Details() proto.Message {
	return s.SplitFunctionCallDetails
}

func (s *SplitFunctionCall) NodeType() planning.NodeType {
	return planning.NODE_TYPE_SPLIT_FUNCTION_OVER_RANGE_VECTOR
}

func (s *SplitFunctionCall) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type SplitFunctionCall supports 1 child, but got %d", len(children))
	}

	s.Inner = children[0]
	return nil
}

func (s *SplitFunctionCall) Child(idx int) planning.Node {
	if idx > 0 {
		panic(fmt.Sprintf("SplitFunctionCall node has 1 child, but attempted to get child at index %d", idx))
	}
	return s.Inner
}

func (s *SplitFunctionCall) ChildCount() int {
	return 1
}

func (s *SplitFunctionCall) ReplaceChild(idx int, child planning.Node) error {
	if idx > 0 {
		fmt.Errorf("SplitFunctionCall node has 1 child, but attempted to replace child at index %d", idx)
	}
	s.Inner = child
	return nil
}

func (s *SplitFunctionCall) MergeHints(other planning.Node) error {
	// Nothing to do.
	return nil
}

func (s *SplitFunctionCall) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherSplit, ok := other.(*SplitFunctionCall)
	if !ok {
		return false
	}

	return slices.EqualFunc(s.SplitRanges, otherSplit.SplitRanges, func(a, b SplitRange) bool {
		return a.Start == b.Start && a.End == b.End && a.Cacheable == b.Cacheable
	})
}

func (s *SplitFunctionCall) Describe() string {
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

func (s *SplitFunctionCall) ChildrenLabels() []string {
	return []string{""}
}

func (s *SplitFunctionCall) ChildrenTimeRange(parentTimeRange types.QueryTimeRange) types.QueryTimeRange {
	return parentTimeRange
}

func (s *SplitFunctionCall) ResultType() (parser.ValueType, error) {
	return s.Inner.ResultType()
}

func (s *SplitFunctionCall) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return s.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (s *SplitFunctionCall) ExpressionPosition() (posrange.PositionRange, error) {
	return s.Inner.ExpressionPosition()
}

func (s *SplitFunctionCall) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	// Query splitting with intermediate result caching requires QueryPlanV6
	return planning.QueryPlanV6
}

type Materializer struct {
	cache *cache.CacheFactory
}

var _ planning.NodeMaterializer = &Materializer{}

func NewMaterializer(cache *cache.CacheFactory) *Materializer {
	return &Materializer{
		cache: cache,
	}
}

func (m Materializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, _ planning.RangeParams) (planning.OperatorFactory, error) {
	s, ok := n.(*SplitFunctionCall)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to materializer: expected SplitFunctionCall, got %T", n)
	}
	innerFunctionCall, ok := s.Inner.(*core.FunctionCall)
	if !ok {
		return nil, fmt.Errorf("SplitFunctionCall node should only wrap FunctionCall nodes, got %T", s.Inner)
	}

	f, exists := functions.RegisteredFunctions[innerFunctionCall.Function]
	if !exists {
		return nil, fmt.Errorf("function '%v' not found in functions list", innerFunctionCall.Function.PromQLName())
	}
	if f.RangeVectorSplitting == nil {
		return nil, fmt.Errorf("function %v does not support range vector splitting", innerFunctionCall.Function.PromQLName())
	}

	ranges := make([]functions.Range, len(s.SplitFunctionCallDetails.SplitRanges))
	for i, sr := range s.SplitFunctionCallDetails.SplitRanges {
		ranges[i] = functions.Range{
			Start:     sr.Start,
			End:       sr.End,
			Cacheable: sr.Cacheable,
		}
	}

	if s.Inner.ChildCount() != 1 {
		return nil, fmt.Errorf("expected exactly 1 child for range vector splitting function %s, got %d", innerFunctionCall.Function.PromQLName(), s.Inner.ChildCount())
	}

	expressionPos, err := innerFunctionCall.ExpressionPosition()
	if err != nil {
		return nil, err
	}

	splitOp, err := f.RangeVectorSplitting(
		s.Inner.Child(0),
		materializer,
		timeRange,
		ranges,
		s.SplitFunctionCallDetails.InnerNodeCacheKey,
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
