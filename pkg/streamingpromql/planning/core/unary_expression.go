// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/functions"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/scalars"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type UnaryExpression struct {
	*UnaryExpressionDetails
	Inner planning.Node
}

func (u *UnaryExpression) Describe() string {
	return u.Op.Describe()
}

func (u *UnaryExpression) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (u *UnaryExpression) Details() proto.Message {
	return u.UnaryExpressionDetails
}

func (u *UnaryExpression) NodeType() planning.NodeType {
	return planning.NODE_TYPE_UNARY_EXPRESSION
}

func (u *UnaryExpression) Children() []planning.Node {
	return []planning.Node{u.Inner}
}

func (u *UnaryExpression) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type UnaryExpression expects 1 child, but got %d", len(children))
	}

	u.Inner = children[0]

	return nil
}

func (u *UnaryExpression) EquivalentTo(other planning.Node) bool {
	otherUnaryExpression, ok := other.(*UnaryExpression)

	return ok &&
		u.Op == otherUnaryExpression.Op &&
		u.Inner.EquivalentTo(otherUnaryExpression.Inner)
}

func (u *UnaryExpression) ChildrenLabels() []string {
	return []string{""}
}

func MaterializeUnaryExpression(u *UnaryExpression, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	inner, err := materializer.ConvertNodeToOperator(u.Inner, timeRange)
	if err != nil {
		return nil, fmt.Errorf("could not create inner operator for UnaryExpression: %w", err)
	}

	if u.Op != UNARY_SUB {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("unary expression with '%s'", u.Op))
	}

	switch inner := inner.(type) {
	case types.InstantVectorOperator:
		o := functions.UnaryNegationOfInstantVectorOperatorFactory(inner, params.MemoryConsumptionTracker, u.ExpressionPosition(), timeRange, params.EnableDelayedNameRemoval)
		return planning.NewSingleUseOperatorFactory(o), nil
	case types.ScalarOperator:
		o := scalars.NewUnaryNegationOfScalar(inner, u.ExpressionPosition())
		return planning.NewSingleUseOperatorFactory(o), nil
	default:
		return nil, fmt.Errorf("expected InstantVectorOperator or ScalarOperator as child of UnaryExpression, got %T", inner)
	}
}

func (u *UnaryExpression) ResultType() (parser.ValueType, error) {
	return u.Inner.ResultType()
}

func (u *UnaryExpression) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	return u.Inner.QueriedTimeRange(queryTimeRange, lookbackDelta)
}

func (u *UnaryExpression) ExpressionPosition() posrange.PositionRange {
	return u.GetExpressionPosition().ToPrometheusType()
}
