// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

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

//node:generate
type SplitFunctionCall struct {
	*SplitFunctionCallDetails
	Inner *core.FunctionCall `node:"child"`
}

func (s *SplitFunctionCall) Details() proto.Message {
	return s.SplitFunctionCallDetails
}

func (s *SplitFunctionCall) NodeType() planning.NodeType {
	return planning.NODE_TYPE_SPLIT_FUNCTION_OVER_RANGE_VECTOR
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
	var b strings.Builder
	fmt.Fprintf(&b, "splits=%d [", len(s.SplitRanges))

	for i, sr := range s.SplitRanges {
		if i > 0 {
			b.WriteString(", ")
		}

		fmt.Fprintf(&b, "(%d,%d]", sr.Start, sr.End)

		if sr.Cacheable {
			b.WriteByte('*')
		}
	}

	b.WriteByte(']')
	return b.String()
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

func (s *SplitFunctionCall) MinimumRequiredPlanVersion(types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV13, nil
}

type Materializer struct {
	enabled bool
	cache   *cache.CacheFactory
	logger  log.Logger
}

var _ planning.NodeMaterializer = &Materializer{}

func NewMaterializer(enabled bool, cache *cache.CacheFactory, logger log.Logger) *Materializer {
	return &Materializer{
		enabled: enabled,
		cache:   cache,
		logger:  logger,
	}
}

func (m Materializer) Materialize(n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideRangeParams planning.RangeParams) (planning.OperatorFactory, error) {
	if overrideRangeParams.IsSet {
		return nil, errors.New("overrideRangeParams not supported for rangevectorsplitting.Materialize")
	}
	s, ok := n.(*SplitFunctionCall)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to materializer: expected SplitFunctionCall, got %T", n)
	}

	if !m.enabled {
		level.Warn(m.logger).Log("msg", "split function node is present but range vector splitting is disabled, falling back to unsplit execution; this can happen if splitting is enabled on the query-frontend but not yet on the querier")
		return materializer.FactoryForNode(s.Inner, timeRange)
	}

	splitFactory, exists := SplitFunctionRegistry[s.Inner.Function]
	if !exists {
		return nil, fmt.Errorf("function %v does not support range vector splitting", s.Inner.Function.PromQLName())
	}

	ranges := make([]Range, len(s.SplitRanges))
	for i, sr := range s.SplitRanges {
		ranges[i] = Range(sr)
	}

	if s.Inner.ChildCount() != 1 {
		return nil, fmt.Errorf("expected exactly 1 child for range vector splitting function %s, got %d", s.Inner.Function.PromQLName(), s.Inner.ChildCount())
	}

	innerNode := s.Inner.Child(0)
	splitNode, ok := innerNode.(planning.SplitNode)
	if !ok {
		return nil, fmt.Errorf("inner node of split function call does not implement SplitNode: %T", innerNode)
	}

	expressionPos, err := s.Inner.ExpressionPosition()
	if err != nil {
		return nil, err
	}

	innerCacheKey, err := SplittingCacheKey(splitNode, params.QueryParameters)
	if err != nil {
		return nil, fmt.Errorf("computing splitting cache key: %w", err)
	}

	splitOp, err := splitFactory(
		innerNode,
		materializer,
		timeRange,
		ranges,
		innerCacheKey,
		m.cache,
		expressionPos,
		params.MemoryConsumptionTracker,
		params.QueryParameters.EnableDelayedNameRemoval,
		params.Logger,
	)
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(splitOp), nil
}

func SplittingCacheKey(node planning.Node, params *planning.QueryParameters) ([]byte, error) {
	// Make a copy of params so we can clear a couple of unnecessary fields before encoding.
	cacheKeyParams := *params
	// Clear query time range as queries at different times can share cache entries if the queries overlap.
	cacheKeyParams.TimeRange = types.QueryTimeRange{}
	cacheKeyParams.OriginalExpression = ""

	plan := &planning.QueryPlan{Root: node, Parameters: &cacheKeyParams}
	encoded, _, err := plan.ToEncodedPlan(false, true)
	if err != nil {
		return nil, fmt.Errorf("encoding %T for splitting cache key: %w", node, err)
	}

	b, err := proto.Marshal(encoded)
	if err != nil {
		return nil, fmt.Errorf("marshalling encoded plan for splitting cache key: %w", err)
	}
	return b, nil
}
