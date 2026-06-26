// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/dskit/cache"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/streamingpromql/caching"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

func init() {
	planning.RegisterNodeFactory(func() planning.Node {
		return &TimeRangeSplit{TimeRangeSplitDetails: &TimeRangeSplitDetails{}}
	})
	planning.RegisterNodeFactory(func() planning.Node {
		return &Cache{CacheDetails: &CacheDetails{}}
	})
}

//node:generate
type TimeRangeSplit struct {
	*TimeRangeSplitDetails
	Inner planning.Node `node:"child"`
}

func (s *TimeRangeSplit) Details() proto.Message {
	return s.TimeRangeSplitDetails
}

func (s *TimeRangeSplit) NodeType() planning.NodeType {
	return planning.NODE_TYPE_TIME_RANGE_SPLIT
}

func (s *TimeRangeSplit) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherConsumer, ok := other.(*TimeRangeSplit)

	return ok && s.SplitInterval == otherConsumer.SplitInterval
}

func (s *TimeRangeSplit) MergeHints(other planning.Node) error {
	return nil
}

func (s *TimeRangeSplit) Describe() string {
	return fmt.Sprintf("interval %s", s.SplitInterval.String())
}

func (s *TimeRangeSplit) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (s *TimeRangeSplit) ResultType() (parser.ValueType, error) {
	return s.Inner.ResultType()
}

func (s *TimeRangeSplit) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return s.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (s *TimeRangeSplit) ExpressionPosition() (posrange.PositionRange, error) {
	return s.Inner.ExpressionPosition()
}

func (s *TimeRangeSplit) MinimumRequiredPlanVersion(timeRange types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV14, nil
}

type TimeRangeSplitMaterializer struct {
	splittingEnabled    bool
	splitQueriesCounter prometheus.Counter
}

func NewTimeRangeSplitMaterializer(splittingEnabled bool, reg prometheus.Registerer) *TimeRangeSplitMaterializer {
	m := &TimeRangeSplitMaterializer{
		splittingEnabled: splittingEnabled,
	}

	if splittingEnabled {
		// Only register the metric if splitting is enabled, to avoid conflicting with the query-frontend middleware doing the same thing.
		m.splitQueriesCounter = NewSplitQueriesCounter(reg)
	}

	return m
}

// The logic below is based on the equivalent middleware logic in splitQueryByInterval.
func (m *TimeRangeSplitMaterializer) Materialize(ctx context.Context, n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideRangeParams planning.RangeParams) (planning.OperatorFactory, error) {
	if overrideRangeParams.IsSet {
		return nil, fmt.Errorf("overrideRangeParams is not supported for TimeRangeSplitMaterializer")
	}

	if !m.splittingEnabled {
		return nil, errors.New("attempted to materialize a TimeRangeSplit node, but splitting is disabled")
	}

	node, ok := n.(*TimeRangeSplit)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to TimeRangeSplitMaterializer: got %T", n)
	}

	ranges := make([]*splitRange, 0, (timeRange.EndT-timeRange.StartT)/timeRange.IntervalMilliseconds+1) // Over-allocate in case the time range straddles an interval boundary.

	for start := timeRange.StartT; start <= timeRange.EndT; {
		end := min(nextIntervalBoundary(start, timeRange.IntervalMilliseconds, node.SplitInterval), timeRange.EndT)

		// If step isn't too big, and adding another step saves us one extra request,
		// then extend the current request to cover the extra step too.
		if end+timeRange.IntervalMilliseconds == timeRange.EndT && timeRange.IntervalMilliseconds <= 5*time.Minute.Milliseconds() {
			end = timeRange.EndT
		}

		innerTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(start), timestamp.Time(end), time.Duration(timeRange.IntervalMilliseconds)*time.Millisecond)
		inner, err := materializer.ConvertNodeToInstantVectorOperator(ctx, node.Inner, innerTimeRange)
		if err != nil {
			return nil, err
		}

		ranges = append(ranges, newSplitRange(inner, params.MemoryConsumptionTracker))
		start = end + timeRange.IntervalMilliseconds
	}

	queryStats := stats.FromContext(ctx)
	queryStats.AddSplitQueries(uint32(len(ranges)))
	m.splitQueriesCounter.Add(float64(len(ranges)))

	if len(ranges) == 1 {
		// If we have just one range, return the inner operator without wrapping it.
		return planning.NewSingleUseOperatorFactory(ranges[0].operator), nil
	}

	operator := newTimeRangeSplitOperator(ranges, params.MemoryConsumptionTracker, timeRange)
	return planning.NewSingleUseOperatorFactory(operator), nil
}

// This is based on the equivalent method in the middleware logic, nextIntervalBoundary.
func nextIntervalBoundary(t, step int64, interval time.Duration) int64 {
	intervalMillis := interval.Milliseconds()
	startOfNextInterval := ((t / intervalMillis) + 1) * intervalMillis
	// ensure that target is a multiple of steps away from the start time
	target := startOfNextInterval - ((startOfNextInterval - t) % step)
	if target == startOfNextInterval {
		target -= step
	}
	return target
}

//node:generate
type Cache struct {
	*CacheDetails
	Inner planning.Node `node:"child"`
}

func (c *Cache) Details() proto.Message {
	return c.CacheDetails
}

func (c *Cache) NodeType() planning.NodeType {
	return planning.NODE_TYPE_CACHE
}

func (c *Cache) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherCache, ok := other.(*Cache)

	return ok && c.SplitInterval == otherCache.SplitInterval
}

func (c *Cache) MergeHints(other planning.Node) error {
	return nil
}

func (c *Cache) Describe() string {
	return fmt.Sprintf("split interval %s", c.SplitInterval.String())

}

func (c *Cache) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (c *Cache) ResultType() (parser.ValueType, error) {
	return c.Inner.ResultType()
}

func (c *Cache) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	return c.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (c *Cache) ExpressionPosition() (posrange.PositionRange, error) {
	return c.Inner.ExpressionPosition()
}

func (c *Cache) MinimumRequiredPlanVersion(_ types.QueryTimeRange) (planning.QueryPlanVersion, error) {
	return planning.QueryPlanV16, nil
}

type CacheMaterializer struct {
	cache          caching.Backend
	limitsProvider LimitsProvider
	metrics        *ResultsCacheMetrics
}

func NewCacheMaterializer(baseCache cache.Cache, cachePrefixGenerator caching.PrefixGenerator, limitsProvider LimitsProvider, reg prometheus.Registerer) *CacheMaterializer {
	m := &CacheMaterializer{
		limitsProvider: limitsProvider,
	}

	if baseCache != nil {
		m.cache = caching.NewPrefixingCache(
			caching.NewAdaptor(baseCache),
			caching.VersioningAndItemTypePrefixGenerator(cachePrefixGenerator, cacheVersion, "MQEQR"),
		)

		// Only register the metrics if caching is enabled, to avoid conflicting with the query-frontend middleware doing the same thing.
		m.metrics = NewResultsCacheMetrics("query_range", reg)
	}

	return m
}

func (m *CacheMaterializer) Materialize(ctx context.Context, n planning.Node, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters, overrideRangeParams planning.RangeParams) (planning.OperatorFactory, error) {
	if overrideRangeParams.IsSet {
		return nil, fmt.Errorf("overrideRangeParams is not supported for CacheMaterializer")
	}

	if m.cache == nil {
		return nil, fmt.Errorf("attempted to materialize a node of type %T, but no cache is configured", n)
	}

	node, ok := n.(*Cache)
	if !ok {
		return nil, fmt.Errorf("unexpected type passed to CacheMaterializer: got %T", n)
	}

	expressionPosition, err := node.ExpressionPosition()
	if err != nil {
		return nil, err
	}

	operator := newCacheOperator(
		m.cache,
		materializer,
		node.Inner,
		timeRange,
		params.MemoryConsumptionTracker,
		expressionPosition,
		params.QueryParameters,
		m.limitsProvider,
		params.Logger,
		node.SplitInterval,
		m.metrics,
	)

	return planning.NewSingleUseOperatorFactory(operator), nil
}
