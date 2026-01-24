// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/aggregations/limitklimitratio"
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
	return a.AggregateExpressionDetails.Describe()
}

func (a *AggregateExpressionDetails) Describe() string {
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

func (a *AggregateExpression) Child(idx int) planning.Node {
	switch idx {
	case 0:
		return a.Inner
	case 1:
		if a.Param == nil {
			panic("cannot get AggregateExpression child at index 1 if there is no parameter")
		}
		return a.Param
	default:
		panic(fmt.Sprintf("node of type AggregateExpression supports at most 2 children, but attempted to get child at index %d", idx))
	}
}

func (a *AggregateExpression) ChildCount() int {
	if a.Param == nil {
		return 1
	}

	return 2
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

func (a *AggregateExpression) ReplaceChild(idx int, node planning.Node) error {
	switch idx {
	case 0:
		a.Inner = node
		return nil
	case 1:
		a.Param = node
		return nil
	default:
		return fmt.Errorf("node of type AggregateExpression expects 1 or 2 children, but attempted to replace child at index %d", idx)
	}
}

func (a *AggregateExpression) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherAggregateExpression, ok := other.(*AggregateExpression)

	return ok && a.EquivalentTo(otherAggregateExpression.AggregateExpressionDetails)
}

func (a *AggregateExpressionDetails) EquivalentTo(other *AggregateExpressionDetails) bool {
	return a.Op == other.Op &&
		slices.Equal(a.Grouping, other.Grouping) &&
		a.Without == other.Without
}

func (a *AggregateExpression) MergeHints(other planning.Node) error {
	otherAggregateExpression, ok := other.(*AggregateExpression)
	if !ok {
		return fmt.Errorf("cannot merge hints from %T into %T", other, a)
	}

	return a.AggregateExpressionDetails.MergeHints(otherAggregateExpression.AggregateExpressionDetails)
}

func (a *AggregateExpressionDetails) MergeHints(other *AggregateExpressionDetails) error {
	// Nothing to do.
	return nil
}

func (a *AggregateExpression) ChildrenLabels() []string {
	if a.Param == nil {
		return []string{""}
	}

	return []string{"expression", "parameter"}
}

func MaterializeAggregateExpression(a *AggregateExpression, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToInstantVectorOperator(a.Inner, timeRange)
	if err != nil {
		return nil, fmt.Errorf("could not create inner operator for AggregateExpression: %w", err)
	}

	var o types.InstantVectorOperator

	switch a.Op {
	case AGGREGATION_TOPK, AGGREGATION_BOTTOMK:
		param, err := materializer.ConvertNodeToScalarOperator(a.Param, timeRange)
		if err != nil {
			return nil, fmt.Errorf("could not create parameter operator for AggregateExpression %s: %w", a.Op.String(), err)
		}

		o = topkbottomk.New(inner, param, timeRange, a.Grouping, a.Without, a.Op == AGGREGATION_TOPK, params.MemoryConsumptionTracker, params.Annotations, a.GetExpressionPosition().ToPrometheusType())

	case AGGREGATION_LIMITK:
		param, err := materializer.ConvertNodeToScalarOperator(a.Param, timeRange)
		if err != nil {
			return nil, fmt.Errorf("could not create parameter operator for AggregateExpression %s: %w", a.Op.String(), err)
		}

		o = limitklimitratio.NewLimitK(inner, param, timeRange, a.Grouping, a.Without, params.MemoryConsumptionTracker, params.Annotations, a.GetExpressionPosition().ToPrometheusType())

	case AGGREGATION_LIMIT_RATIO:
		param, err := materializer.ConvertNodeToScalarOperator(a.Param, timeRange)
		if err != nil {
			return nil, fmt.Errorf("could not create parameter operator for AggregateExpression %s: %w", a.Op.String(), err)
		}

		o = limitklimitratio.NewLimitRatio(inner, param, timeRange, a.Grouping, a.Without, params.MemoryConsumptionTracker, params.Annotations, a.GetExpressionPosition().ToPrometheusType())

	case AGGREGATION_QUANTILE:
		param, err := materializer.ConvertNodeToScalarOperator(a.Param, timeRange)
		if err != nil {
			return nil, fmt.Errorf("could not create parameter operator for AggregateExpression %s: %w", a.Op.String(), err)
		}

		o, err = aggregations.NewQuantileAggregation(inner, param, timeRange, a.Grouping, a.Without, params.MemoryConsumptionTracker, params.Annotations, a.GetExpressionPosition().ToPrometheusType())
		if err != nil {
			return nil, err
		}

	case AGGREGATION_COUNT_VALUES:
		param, err := materializer.ConvertNodeToStringOperator(a.Param, timeRange)
		if err != nil {
			return nil, fmt.Errorf("could not create parameter operator for AggregateExpression %s: %w", a.Op.String(), err)
		}

		o = aggregations.NewCountValues(inner, param, timeRange, a.Grouping, a.Without, params.MemoryConsumptionTracker, a.GetExpressionPosition().ToPrometheusType())

	default:
		itemType, ok := a.Op.ToItemType()
		if !ok {
			return nil, fmt.Errorf("unknown aggregation operation %s", a.Op.String())
		}

		var err error
		o, err = aggregations.NewAggregation(inner, timeRange, a.Grouping, a.Without, itemType, params.MemoryConsumptionTracker, params.Annotations, a.GetExpressionPosition().ToPrometheusType())
		if err != nil {
			return nil, err
		}
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (a *AggregateExpression) ResultType() (parser.ValueType, error) {
	return parser.ValueTypeVector, nil
}

func (a *AggregateExpression) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) (planning.QueriedTimeRange, error) {
	innerRange, err := a.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
	if err != nil {
		return planning.NoDataQueried(), err
	}
	if a.Param == nil {
		return innerRange, nil
	}

	paramRange, err := a.Param.QueriedTimeRange(queryTimeRange, lookbackDelta)
	if err != nil {
		return planning.NoDataQueried(), err
	}

	return innerRange.Union(paramRange), nil
}

func (a *AggregateExpression) ExpressionPosition() (posrange.PositionRange, error) {
	return a.GetExpressionPosition().ToPrometheusType(), nil
}

func (a *AggregateExpression) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	switch a.Op {
	case AGGREGATION_LIMITK, AGGREGATION_LIMIT_RATIO:
		return planning.QueryPlanV2
	}
	return planning.QueryPlanVersionZero
}
