// SPDX-License-Identifier: AGPL-3.0-only

package rangevectorsplitting

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/plan/rangevectorsplitting/cache"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
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

func (s *SplitFunctionCall) Describe() string {
	return ""
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
	return planning.QueryPlanV18, nil
}

// limitsProvider provides the tenant limits needed to compute split ranges at materialize time.
type limitsProvider interface {
	GetMaxOutOfOrderTimeWindow(ctx context.Context) (time.Duration, error)
}

type Materializer struct {
	enabled       bool
	splitInterval time.Duration
	limits        limitsProvider
	timeNow       func() time.Time
	cache         *cache.CacheFactory
	logger        log.Logger

	nodesSplit   prometheus.Counter
	nodesUnsplit *prometheus.CounterVec
}

var _ planning.NodeMaterializer = &Materializer{}

func NewMaterializer(enabled bool, splitInterval time.Duration, limits limitsProvider, timeNow func() time.Time, cache *cache.CacheFactory, reg prometheus.Registerer, logger log.Logger) *Materializer {
	if timeNow == nil {
		timeNow = time.Now
	}

	return &Materializer{
		enabled:       enabled,
		splitInterval: splitInterval,
		limits:        limits,
		timeNow:       timeNow,
		cache:         cache,
		logger:        logger,
		nodesSplit: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_range_vector_splitting_nodes_materialized_split_total",
			Help: "Total number of range vector splitting nodes materialized as split operators.",
		}),
		nodesUnsplit: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_range_vector_splitting_nodes_materialized_unsplit_total",
			Help: "Total number of range vector splitting nodes that fell back to unsplit execution at materialize time.",
		}, []string{"reason"}),
	}
}

func (m Materializer) Materialize(ctx context.Context, n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideRangeParams planning.RangeParams) (planning.OperatorFactory, error) {
	if overrideRangeParams.IsSet {
		return nil, fmt.Errorf("overrideRangeParams not supported for %T", n)
	}
	s, ok := n.(*SplitFunctionCall)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to materializer: expected SplitFunctionCall, got %T", n)
	}

	if !m.enabled {
		level.Warn(m.logger).Log("msg", "split function node is present but range vector splitting is disabled, falling back to unsplit execution; this can happen if splitting is enabled on the query-frontend but not yet on the querier")
		m.nodesUnsplit.WithLabelValues("disabled").Inc()
		return materializer.FactoryForNode(ctx, s.Inner, timeRange)
	}

	splitFactory, exists := SplitFunctionRegistry[s.Inner.Function]
	if !exists {
		return nil, fmt.Errorf("function %v does not support range vector splitting", s.Inner.Function.PromQLName())
	}

	if s.Inner.ChildCount() != 1 {
		return nil, fmt.Errorf("expected exactly 1 child for range vector splitting function %s, got %d", s.Inner.Function.PromQLName(), s.Inner.ChildCount())
	}

	innerNode := s.Inner.Child(0)
	splitNode, ok := innerNode.(planning.SplitNode)
	if !ok {
		return nil, fmt.Errorf("inner node of split function call does not implement SplitNode: %T", innerNode)
	}

	// The split ranges are computed here, at materialize time, rather than at planning time, because they depend on
	// the querier's current time and the tenant's out-of-order window. If the ranges turn out not to be worth
	// splitting (e.g. there's no complete cacheable block, or every block falls within the out-of-order window), fall
	// back to unsplit execution.
	ranges, notApplied, err := m.computeRanges(ctx, splitNode, timeRange)
	if err != nil {
		return nil, err
	}
	if notApplied != "" {
		level.Debug(m.logger).Log("msg", "range vector splitting not applied at materialize time, falling back to unsplit execution", "function", s.Inner.Function.PromQLName(), "reason", notApplied)
		m.nodesUnsplit.WithLabelValues(notApplied).Inc()
		return materializer.FactoryForNode(ctx, s.Inner, timeRange)
	}
	m.nodesSplit.Inc()

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

// computeRanges computes the split ranges for the inner node at the given query time range.
//
// It returns the ranges to split into, or a non-empty notApplied reason if splitting is not worthwhile (in which case
// ranges is nil and the caller should fall back to unsplit execution). The notApplied reasons mirror those recorded by
// the optimization pass at planning time, but for the checks that depend on runtime state (the current time and the
// tenant's out-of-order window).
func (m Materializer) computeRanges(ctx context.Context, inner planning.SplitNode, timeRange types.QueryTimeRange) (ranges []Range, notApplied string, err error) {
	timeParams := inner.GetRangeParams()
	if !timeParams.IsSet {
		// Should always be set if it's a splittable node.
		return nil, "", fmt.Errorf("time range params not specified")
	}

	startTs, endTs := calculateInnerTimeRange(timeRange.StartT, timeParams)

	alignedStart := computeBlockAlignedStart(startTs, m.splitInterval)
	hasCompleteBlock := alignedStart+m.splitInterval.Milliseconds() <= endTs
	if !hasCompleteBlock {
		return nil, "no_complete_cache_block", nil
	}

	var oooThreshold int64
	oooWindow, err := m.limits.GetMaxOutOfOrderTimeWindow(ctx)
	if err != nil {
		return nil, "", err
	}
	if oooWindow > 0 {
		oooThreshold = m.timeNow().Add(-oooWindow).UnixMilli()
	}

	ranges = computeSplitRanges(startTs, endTs, m.splitInterval, oooThreshold)

	hasCacheable := false
	for _, r := range ranges {
		if r.Cacheable {
			hasCacheable = true
			break
		}
	}
	if !hasCacheable {
		return nil, "no_cacheable_blocks_after_ooo_filter", nil
	}

	if requestoptions.OptionsFromContext(ctx).CacheDisabled {
		for i := range ranges {
			ranges[i].Cacheable = false
		}
	}

	return ranges, "", nil
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
