// SPDX-License-Identifier: AGPL-3.0-only

package splitandcache

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

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

// The logic below is based on the equivalent middleware logic in splitQueryByInterval.
func MaterializeSplit(node *TimeRangeSplit, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	ranges := make([]*splitRange, 0, (timeRange.EndT-timeRange.StartT)/timeRange.IntervalMilliseconds+1) // Over-allocate in case the time range straddles an interval boundary.

	for start := timeRange.StartT; start <= timeRange.EndT; {
		end := min(nextIntervalBoundary(start, timeRange.IntervalMilliseconds, node.SplitInterval), timeRange.EndT)

		// If step isn't too big, and adding another step saves us one extra request,
		// then extend the current request to cover the extra step too.
		if end+timeRange.IntervalMilliseconds == timeRange.EndT && timeRange.IntervalMilliseconds <= 5*time.Minute.Milliseconds() {
			end = timeRange.EndT
		}

		innerTimeRange := types.NewRangeQueryTimeRange(timestamp.Time(start), timestamp.Time(end), time.Duration(timeRange.IntervalMilliseconds)*time.Millisecond)
		inner, err := materializer.ConvertNodeToInstantVectorOperator(node.Inner, innerTimeRange)
		if err != nil {
			return nil, err
		}

		ranges = append(ranges, newSplitRange(inner))
		start = end + timeRange.IntervalMilliseconds
	}

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

type Cache struct {
	*CacheDetails
	Inner planning.Node
}

func (c *Cache) Details() proto.Message {
	return c.CacheDetails
}

func (c *Cache) NodeType() planning.NodeType {
	return planning.NODE_TYPE_CACHE
}

func (c *Cache) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type Cache supports 1 child, but attempted to get child at index %d", idx))
	}

	return c.Inner
}

func (c *Cache) ChildCount() int {
	return 1
}

func (c *Cache) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Cache requires 1 child, but got %d", len(children))
	}

	c.Inner = children[0]
	return nil
}

func (c *Cache) ReplaceChild(idx int, child planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type Cache supports 1 child, but attempted to replace child at index %d", idx)
	}

	c.Inner = child
	return nil
}

func (c *Cache) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	_, ok := other.(*Cache)

	return ok
}

func (c *Cache) MergeHints(other planning.Node) error {
	return nil
}

func (c *Cache) Describe() string {
	return ""
}

func (c *Cache) ChildrenLabels() []string {
	return []string{""}
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
