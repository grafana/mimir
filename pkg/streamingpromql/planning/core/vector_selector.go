// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type VectorSelector struct {
	*VectorSelectorDetails
}

func (v *VectorSelector) Describe() string {
	d := describeSelector(v.Matchers, v.Timestamp, v.Offset, nil, v.SkipHistogramBuckets, false, v.Smoothed, false, v.ProjectionLabels, v.ProjectionInclude)

	if v.ReturnSampleTimestamps {
		return d + ", return sample timestamps"
	}

	return d
}

func (v *VectorSelector) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (v *VectorSelector) Details() proto.Message {
	return v.VectorSelectorDetails
}

func (v *VectorSelector) NodeType() planning.NodeType {
	return planning.NODE_TYPE_VECTOR_SELECTOR
}

func (v *VectorSelector) Child(idx int) planning.Node {
	panic(fmt.Sprintf("node of type VectorSelector has no children, but attempted to get child at index %d", idx))
}

func (v *VectorSelector) ChildCount() int {
	return 0
}

func (v *VectorSelector) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type VectorSelector expects 0 children, but got %d", len(children))
	}

	return nil
}

func (v *VectorSelector) ReplaceChild(idx int, node planning.Node) error {
	return fmt.Errorf("node of type VectorSelector supports no children, but attempted to replace child at index %d", idx)
}

func (v *VectorSelector) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherVectorSelector, ok := other.(*VectorSelector)

	return ok &&
		slices.EqualFunc(v.Matchers, otherVectorSelector.Matchers, matchersEqual) &&
		((v.Timestamp == nil && otherVectorSelector.Timestamp == nil) || (v.Timestamp != nil && otherVectorSelector.Timestamp != nil && v.Timestamp.Equal(*otherVectorSelector.Timestamp))) &&
		v.Offset == otherVectorSelector.Offset &&
		v.ReturnSampleTimestamps == otherVectorSelector.ReturnSampleTimestamps &&
		v.Smoothed == otherVectorSelector.Smoothed
}

func (v *VectorSelector) MergeHints(other planning.Node) error {
	otherVectorSelector, ok := other.(*VectorSelector)
	if !ok {
		return fmt.Errorf("cannot merge hints from %T into %T", other, v)
	}

	v.SkipHistogramBuckets = v.SkipHistogramBuckets && otherVectorSelector.SkipHistogramBuckets
	v.ProjectionLabels = MergeLabelNames(v.ProjectionLabels, otherVectorSelector.ProjectionLabels)

	return nil
}

func (v *VectorSelector) ChildrenLabels() []string {
	return nil
}

func MaterializeVectorSelector(v *VectorSelector, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	selector := &selectors.Selector{
		Queryable:                params.Queryable,
		TimeRange:                timeRange,
		Timestamp:                TimestampFromTime(v.Timestamp),
		Offset:                   v.Offset.Milliseconds(),
		LookbackDelta:            params.LookbackDelta,
		Matchers:                 LabelMatchersToOperatorType(v.Matchers),
		EagerLoad:                params.EagerLoadSelectors,
		SkipHistogramBuckets:     v.SkipHistogramBuckets,
		ExpressionPosition:       v.GetExpressionPosition().ToPrometheusType(),
		MemoryConsumptionTracker: params.MemoryConsumptionTracker,
		Smoothed:                 v.Smoothed,
		ProjectionLabels:         v.ProjectionLabels,
	}

	return planning.NewSingleUseOperatorFactory(selectors.NewInstantVectorSelector(selector, params.MemoryConsumptionTracker, params.QueryStats, v.ReturnSampleTimestamps)), nil
}

func (v *VectorSelector) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeVector, nil
}

func (v *VectorSelector) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	minT, maxT := selectors.ComputeQueriedTimeRange(queryTimeRange, TimestampFromTime(v.Timestamp), 0, v.Offset.Milliseconds(), lookbackDelta, false, v.Smoothed)
	return planning.NewQueriedTimeRange(timestamp.Time(minT), timestamp.Time(maxT)), nil
}

func (v *VectorSelector) ExpressionPosition() (posrange.PositionRange, error) {
	return v.GetExpressionPosition().ToPrometheusType(), nil
}

func (v *VectorSelector) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	if v.Smoothed {
		return planning.QueryPlanV4
	}
	return planning.QueryPlanVersionZero
}
