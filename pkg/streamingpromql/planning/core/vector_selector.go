// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"

	"github.com/gogo/protobuf/proto"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type VectorSelector struct {
	*VectorSelectorDetails
}

func (v *VectorSelector) Describe() string {
	return describeSelector(v.Matchers, v.Timestamp, v.Offset, nil)
}

func (v *VectorSelector) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (v *VectorSelector) Details() proto.Message {
	return v.VectorSelectorDetails
}

func (v *VectorSelector) Children() []planning.Node {
	return nil
}

func (v *VectorSelector) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type VectorSelector expects 0 children, but got %d", len(children))
	}

	return nil
}

func (v *VectorSelector) EquivalentTo(other planning.Node) bool {
	otherVectorSelector, ok := other.(*VectorSelector)

	return ok &&
		slices.EqualFunc(v.Matchers, otherVectorSelector.Matchers, matchersEqual) &&
		((v.Timestamp == nil && otherVectorSelector.Timestamp == nil) || (v.Timestamp != nil && otherVectorSelector.Timestamp != nil && v.Timestamp.Equal(*otherVectorSelector.Timestamp))) &&
		v.Offset == otherVectorSelector.Offset
}

func (v *VectorSelector) ChildrenLabels() []string {
	return nil
}

func (v *VectorSelector) OperatorFactory(_ []types.Operator, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	matchers, err := LabelMatchersToPrometheusType(v.Matchers)
	if err != nil {
		return nil, err
	}

	o := &selectors.InstantVectorSelector{
		MemoryConsumptionTracker: params.MemoryConsumptionTracker,
		Selector: &selectors.Selector{
			Queryable:          params.Queryable,
			TimeRange:          timeRange,
			Timestamp:          TimestampFromTime(v.Timestamp),
			Offset:             v.Offset.Milliseconds(),
			LookbackDelta:      params.LookbackDelta,
			Matchers:           matchers,
			ExpressionPosition: v.ExpressionPosition.ToPrometheusType(),
		},
		Stats: params.Stats,
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}
