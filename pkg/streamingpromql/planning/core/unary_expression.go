// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"

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

func (u *UnaryExpression) OperatorFactory(children []types.Operator, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	if len(children) != 1 {
		return nil, fmt.Errorf("expected exactly 1 child for UnaryExpression, got %v", len(children))
	}

	if u.Op != UNARY_SUB {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("unary expression with '%s'", u.Op))
	}

	switch child := children[0].(type) {
	case types.InstantVectorOperator:
		o := functions.UnaryNegationOfInstantVectorOperatorFactory(child, params.MemoryConsumptionTracker, u.ExpressionPosition.ToPrometheusType(), timeRange)
		return planning.NewSingleUseOperatorFactory(o), nil
	case types.ScalarOperator:
		o := scalars.NewUnaryNegationOfScalar(child, u.ExpressionPosition.ToPrometheusType())
		return planning.NewSingleUseOperatorFactory(o), nil
	default:
		return nil, fmt.Errorf("expected InstantVectorOperator or ScalarOperator as child of UnaryExpression, got %T", children[0])
	}
}

func (u *UnaryExpression) ResultType() (parser.ValueType, error) {
	return u.Inner.ResultType()
}
