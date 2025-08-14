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

type MatrixSelector struct {
	*MatrixSelectorDetails
}

func (m *MatrixSelector) Describe() string {
	return describeSelector(m.Matchers, m.Timestamp, m.Offset, &m.Range, m.SkipHistogramBuckets)
}

func (m *MatrixSelector) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (m *MatrixSelector) Details() proto.Message {
	return m.MatrixSelectorDetails
}

func (m *MatrixSelector) NodeType() planning.NodeType {
	return planning.NODE_TYPE_MATRIX_SELECTOR
}

func (m *MatrixSelector) Children() []planning.Node {
	return nil
}

func (m *MatrixSelector) SetChildren(children []planning.Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type MatrixSelector expects 0 children, but got %d", len(children))
	}

	return nil
}

func (m *MatrixSelector) EquivalentTo(other planning.Node) bool {
	otherMatrixSelector, ok := other.(*MatrixSelector)

	return ok &&
		slices.EqualFunc(m.Matchers, otherMatrixSelector.Matchers, matchersEqual) &&
		((m.Timestamp == nil && otherMatrixSelector.Timestamp == nil) || (m.Timestamp != nil && otherMatrixSelector.Timestamp != nil && m.Timestamp.Equal(*otherMatrixSelector.Timestamp))) &&
		m.Offset == otherMatrixSelector.Offset &&
		m.Range == otherMatrixSelector.Range &&
		m.SkipHistogramBuckets == otherMatrixSelector.SkipHistogramBuckets
}

func (m *MatrixSelector) ChildrenLabels() []string {
	return nil
}

func MaterializeMatrixSelector(m *MatrixSelector, _ *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	matchers, err := LabelMatchersToPrometheusType(m.Matchers)
	if err != nil {
		return nil, err
	}

	selector := &selectors.Selector{
		Queryable:                params.Queryable,
		TimeRange:                timeRange,
		Timestamp:                TimestampFromTime(m.Timestamp),
		Offset:                   m.Offset.Milliseconds(),
		Range:                    m.Range,
		Matchers:                 matchers,
		EagerLoad:                params.EagerLoadSelectors,
		SkipHistogramBuckets:     m.SkipHistogramBuckets,
		ExpressionPosition:       m.ExpressionPosition.ToPrometheusType(),
		MemoryConsumptionTracker: params.MemoryConsumptionTracker,
	}

	o := selectors.NewRangeVectorSelector(selector, params.MemoryConsumptionTracker)

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (m *MatrixSelector) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeMatrix, nil
}
