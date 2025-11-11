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

func (u *UnaryExpression) Child(idx int) planning.Node {
	if idx != 0 {
		panic(fmt.Sprintf("node of type UnaryExpression supports 1 child, but attempted to get child at index %d", idx))
	}

	return u.Inner
}

func (u *UnaryExpression) ChildCount() int {
	return 1
}

func (u *UnaryExpression) SetChildren(children []planning.Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type UnaryExpression expects 1 child, but got %d", len(children))
	}

	u.Inner = children[0]

	return nil
}

func (u *UnaryExpression) ReplaceChild(idx int, node planning.Node) error {
	if idx != 0 {
		return fmt.Errorf("node of type UnaryExpression supports 1 child, but attempted to replace child at index %d", idx)
	}

	u.Inner = node
	return nil
}

func (u *UnaryExpression) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherUnaryExpression, ok := other.(*UnaryExpression)

	return ok &&
		u.Op == otherUnaryExpression.Op
}

func (u *UnaryExpression) MergeHints(_ planning.Node) error {
	// Nothing to do.
	return nil
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
		o := functions.UnaryNegationOfInstantVectorOperatorFactory(inner, params, u.ExpressionPosition(), timeRange)
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

func (u *UnaryExpression) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanVersionZero
}
