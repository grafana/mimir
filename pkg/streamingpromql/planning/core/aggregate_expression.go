// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations/topkbottomk"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type AggregateExpression struct {
	*AggregateExpressionDetails
	Inner planning.Node
	Param planning.Node
}

func (a *AggregateExpression) Describe() string {
	builder := &strings.Builder{}
	builder.WriteString(a.Op.Describe())

	if a.Without || len(a.Grouping) > 0 {
		if a.Without {
			builder.WriteString(" without (")
		} else {
			builder.WriteString(" by (")
		}

		for i, l := range a.Grouping {
			if i > 0 {
				builder.WriteString(", ")
			}

			builder.WriteString(l)
		}

		builder.WriteString(")")
	}

	return builder.String()
}

func (a *AggregateExpression) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (a *AggregateExpression) Details() proto.Message {
	return a.AggregateExpressionDetails
}

func (a *AggregateExpression) NodeType() planning.NodeType {
	return planning.NODE_TYPE_AGGREGATE_EXPRESSION
}

func (a *AggregateExpression) Children() []planning.Node {
	if a.Param == nil {
		return []planning.Node{a.Inner}
	}

	return []planning.Node{a.Inner, a.Param}
}

func (a *AggregateExpression) SetChildren(children []planning.Node) error {
	switch len(children) {
	case 1:
		a.Inner, a.Param = children[0], nil
	case 2:
		a.Inner, a.Param = children[0], children[1]
	default:
		return fmt.Errorf("node of type AggregateExpression expects 1 or 2 children, but got %d", len(children))
	}

	return nil
}

func (a *AggregateExpression) EquivalentTo(other planning.Node) bool {
	otherAggregateExpression, ok := other.(*AggregateExpression)

	return ok &&
		a.Op == otherAggregateExpression.Op &&
		a.Inner.EquivalentTo(otherAggregateExpression.Inner) &&
		((a.Param == nil && otherAggregateExpression.Param == nil) ||
			(a.Param != nil && otherAggregateExpression.Param != nil && a.Param.EquivalentTo(otherAggregateExpression.Param))) &&
		slices.Equal(a.Grouping, otherAggregateExpression.Grouping) &&
		a.Without == otherAggregateExpression.Without
}

func (a *AggregateExpression) ChildrenLabels() []string {
	if a.Param == nil {
		return []string{""}
	}

	return []string{"expression", "parameter"}
}

func (a *AggregateExpression) OperatorFactory(children []types.Operator, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	if len(children) < 1 {
		return nil, fmt.Errorf("expected at least 1 child for AggregateExpression, got %v", len(children))
	}

	inner, ok := children[0].(types.InstantVectorOperator)
	if !ok {
		return nil, fmt.Errorf("expected InstantVectorOperator as expression child of AggregateExpression, got %T", children[0])
	}

	var o types.InstantVectorOperator

	switch a.Op {
	case AGGREGATION_TOPK, AGGREGATION_BOTTOMK:
		if len(children) != 2 {
			return nil, fmt.Errorf("expected exactly 2 children for AggregateExpression with operation %s, got %v", a.Op.String(), len(children))
		}

		param, ok := children[1].(types.ScalarOperator)
		if !ok {
			return nil, fmt.Errorf("expected ScalarOperator as parameter child of AggregateExpression with operation %s, got %T", a.Op.String(), children[0])
		}

		o = topkbottomk.New(inner, param, timeRange, a.Grouping, a.Without, a.Op == AGGREGATION_TOPK, params.MemoryConsumptionTracker, params.Annotations, a.ExpressionPosition.ToPrometheusType())

	case AGGREGATION_QUANTILE:
		if len(children) != 2 {
			return nil, fmt.Errorf("expected exactly 2 children for AggregateExpression with operation %s, got %v", a.Op.String(), len(children))
		}

		param, ok := children[1].(types.ScalarOperator)
		if !ok {
			return nil, fmt.Errorf("expected ScalarOperator as parameter child of AggregateExpression with operation %s, got %T", a.Op.String(), children[0])
		}

		var err error
		o, err = aggregations.NewQuantileAggregation(inner, param, timeRange, a.Grouping, a.Without, params.MemoryConsumptionTracker, params.Annotations, a.ExpressionPosition.ToPrometheusType())
		if err != nil {
			return nil, err
		}

	case AGGREGATION_COUNT_VALUES:
		if len(children) != 2 {
			return nil, fmt.Errorf("expected exactly 2 children for AggregateExpression with operation %s, got %v", a.Op.String(), len(children))
		}

		param, ok := children[1].(types.StringOperator)
		if !ok {
			return nil, fmt.Errorf("expected StringOperator as parameter child of AggregateExpression with operation %s, got %T", a.Op.String(), children[0])
		}

		o = aggregations.NewCountValues(
			inner,
			param,
			timeRange,
			a.Grouping,
			a.Without,
			params.MemoryConsumptionTracker,
			a.ExpressionPosition.ToPrometheusType(),
			params.NameValidationScheme,
		)

	default:
		if len(children) != 1 {
			return nil, fmt.Errorf("expected exactly 1 child for AggregateExpression with operation %s, got %v", a.Op.String(), len(children))
		}

		itemType, ok := a.Op.ToItemType()
		if !ok {
			return nil, fmt.Errorf("unknown aggregation operation %s", a.Op.String())
		}

		var err error
		o, err = aggregations.NewAggregation(inner, timeRange, a.Grouping, a.Without, itemType, params.MemoryConsumptionTracker, params.Annotations, a.ExpressionPosition.ToPrometheusType())
		if err != nil {
			return nil, err
		}
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (a *AggregateExpression) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeVector, nil
}
