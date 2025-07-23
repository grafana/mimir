// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/selectors"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type VectorSelector struct {
	*VectorSelectorDetails
}

func (v *VectorSelector) Describe() string {
	d := describeSelector(v.Matchers, v.Timestamp, v.Offset, nil, v.SkipHistogramBuckets)

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
		v.Offset == otherVectorSelector.Offset &&
		v.ReturnSampleTimestamps == otherVectorSelector.ReturnSampleTimestamps &&
		v.SkipHistogramBuckets == otherVectorSelector.SkipHistogramBuckets
}

func (v *VectorSelector) ChildrenLabels() []string {
	return nil
}

func (v *VectorSelector) OperatorFactory(_ []types.Operator, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	matchers, err := LabelMatchersToPrometheusType(v.Matchers)
	if err != nil {
		return nil, err
	}
	selector := selectors.NewSelector(params.Queryable, timeRange, TimestampFromTime(v.Timestamp), v.Offset.Milliseconds(), matchers, params.EagerLoadSelectors, v.SkipHistogramBuckets, params.LookbackDelta, v.ExpressionPosition.ToPrometheusType(), params.MemoryConsumptionTracker)
	if v.SampleCountMultiplicator != 0 {
		selector.MultiplexStats(v.SampleCountMultiplicator)
	}
	return planning.NewSingleUseOperatorFactory(selectors.NewInstantVectorSelector(selector, params.MemoryConsumptionTracker, v.ReturnSampleTimestamps)), nil
}

func (v *VectorSelector) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeVector, nil
}
