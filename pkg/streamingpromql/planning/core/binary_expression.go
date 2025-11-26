// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/binops"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var errCannotMergeBinaryExpressionHints = errors.New("cannot merge hints for binary expressions with different included labels")

type BinaryExpression struct {
	*BinaryExpressionDetails
	LHS planning.Node
	RHS planning.Node
}

func (b *BinaryExpression) Describe() string {
	builder := &strings.Builder{}

	builder.WriteString("LHS ")
	builder.WriteString(b.Op.Describe())

	if b.ReturnBool {
		builder.WriteString(" bool")
	}

	if b.VectorMatching != nil {
		if b.VectorMatching.On || len(b.VectorMatching.MatchingLabels) > 0 {
			if b.VectorMatching.On {
				builder.WriteString(" on (")
			} else {
				builder.WriteString(" ignoring (")
			}

			for i, l := range b.VectorMatching.MatchingLabels {
				if i > 0 {
					builder.WriteString(", ")
				}

				builder.WriteString(l)
			}

			builder.WriteRune(')')
		}

		if b.VectorMatching.Card == parser.CardOneToMany || b.VectorMatching.Card == parser.CardManyToOne {
			if b.VectorMatching.Card == parser.CardOneToMany {
				builder.WriteString(" group_right (")
			} else {
				builder.WriteString(" group_left (")
			}

			for i, l := range b.VectorMatching.Include {
				if i > 0 {
					builder.WriteString(", ")
				}

				builder.WriteString(l)
			}

			builder.WriteRune(')')
		}
	}

	builder.WriteString(" RHS")

	if b.Hints != nil {
		builder.WriteString(", hints (")
		for i, l := range b.Hints.Include {
			if i > 0 {
				builder.WriteString(", ")
			}

			builder.WriteString(l)
		}

		builder.WriteByte(')')
	}

	return builder.String()
}

func (b *BinaryExpression) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (b *BinaryExpression) Details() proto.Message {
	return b.BinaryExpressionDetails
}

func (b *BinaryExpression) NodeType() planning.NodeType {
	return planning.NODE_TYPE_BINARY_EXPRESSION
}

func (b *BinaryExpression) Child(idx int) planning.Node {
	switch idx {
	case 0:
		return b.LHS
	case 1:
		return b.RHS
	default:
		panic(fmt.Sprintf("node of type BinaryExpression supports 2 children, but attempted to get child at index %d", idx))
	}
}

func (b *BinaryExpression) ChildCount() int {
	return 2
}

func (b *BinaryExpression) SetChildren(children []planning.Node) error {
	if len(children) != 2 {
		return fmt.Errorf("node of type BinaryExpression expects 2 children, but got %d", len(children))
	}

	b.LHS, b.RHS = children[0], children[1]

	return nil
}

func (b *BinaryExpression) ReplaceChild(idx int, node planning.Node) error {
	switch idx {
	case 0:
		b.LHS = node
		return nil
	case 1:
		b.RHS = node
		return nil
	default:
		return fmt.Errorf("node of type BinaryExpression expects 1 or 2 children, but attempted to replace child at index %d", idx)
	}
}

func (b *BinaryExpression) EquivalentToIgnoringHintsAndChildren(other planning.Node) bool {
	otherBinaryExpression, ok := other.(*BinaryExpression)

	return ok &&
		b.Op == otherBinaryExpression.Op &&
		b.VectorMatching.Equals(otherBinaryExpression.VectorMatching) &&
		b.ReturnBool == otherBinaryExpression.ReturnBool
}

func (b *BinaryExpression) MergeHints(other planning.Node) error {
	otherBinaryExpression, ok := other.(*BinaryExpression)
	if !ok {
		return fmt.Errorf("cannot merge hints from %T into %T", other, b)
	}

	var thisLabels []string
	var otherLabels []string

	if b.Hints != nil {
		thisLabels = b.Hints.Include
	}

	if otherBinaryExpression.Hints != nil {
		otherLabels = otherBinaryExpression.Hints.Include
	}

	if slices.Equal(thisLabels, otherLabels) {
		return nil
	}

	return errCannotMergeBinaryExpressionHints
}

func (b *BinaryExpression) ChildrenLabels() []string {
	return []string{"LHS", "RHS"}
}

func MaterializeBinaryExpression(b *BinaryExpression, materializer *planning.Materializer, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	op, ok := b.Op.ToItemType()
	if !ok {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("'%v' binary expression", b.Op.String()))
	}

	lhsVector, lhsScalar, err := b.getChildOperator(b.LHS, timeRange, materializer, "left")
	if err != nil {
		return nil, err
	}

	rhsVector, rhsScalar, err := b.getChildOperator(b.RHS, timeRange, materializer, "right")
	if err != nil {
		return nil, err
	}

	if lhsScalar != nil && rhsScalar != nil {
		o, err := binops.NewScalarScalarBinaryOperation(lhsScalar, rhsScalar, op, params.MemoryConsumptionTracker, params.Annotations, b.ExpressionPosition())
		if err != nil {
			return nil, err
		}

		return planning.NewSingleUseOperatorFactory(o), nil
	}

	if lhsVector != nil && rhsVector != nil {
		o, err := b.createVectorVectorOperator(lhsVector, rhsVector, op, timeRange, params)
		if err != nil {
			return nil, err
		}

		return planning.NewSingleUseOperatorFactory(o), nil
	}

	// Only possibility left is some kind of vector / scalar combination.
	var scalar types.ScalarOperator
	var vector types.InstantVectorOperator
	scalarIsLeftSide := lhsScalar != nil

	if scalarIsLeftSide {
		scalar = lhsScalar
		vector = rhsVector
	} else {
		scalar = rhsScalar
		vector = lhsVector
	}

	o, err := binops.NewVectorScalarBinaryOperation(scalar, vector, scalarIsLeftSide, op, b.ReturnBool, timeRange, params, b.ExpressionPosition())
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(o), nil
}

func (b *BinaryExpression) getChildOperator(node planning.Node, timeRange types.QueryTimeRange, materializer *planning.Materializer, side string) (types.InstantVectorOperator, types.ScalarOperator, error) {
	o, err := materializer.ConvertNodeToOperator(node, timeRange)
	if err != nil {
		return nil, nil, err
	}

	switch o := o.(type) {
	case types.InstantVectorOperator:
		return o, nil, nil
	case types.ScalarOperator:
		return nil, o, nil
	default:
		return nil, nil, fmt.Errorf("expected either InstantVectorOperator or ScalarOperator on %s-hand side of BinaryExpression, got %T", side, o)
	}
}

func (b *BinaryExpression) createVectorVectorOperator(lhs, rhs types.InstantVectorOperator, op parser.ItemType, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (types.InstantVectorOperator, error) {
	switch op {
	case parser.LAND, parser.LUNLESS:
		return binops.NewAndUnlessBinaryOperation(lhs, rhs, *b.VectorMatching.ToPrometheusType(), params.MemoryConsumptionTracker, op == parser.LUNLESS, timeRange, b.ExpressionPosition()), nil
	case parser.LOR:
		return binops.NewOrBinaryOperation(lhs, rhs, *b.VectorMatching.ToPrometheusType(), params.MemoryConsumptionTracker, timeRange, b.ExpressionPosition()), nil
	default:
		switch b.VectorMatching.Card {
		case parser.CardOneToMany, parser.CardManyToOne:
			return binops.NewGroupedVectorVectorBinaryOperation(lhs, rhs, *b.VectorMatching.ToPrometheusType(), op, b.ReturnBool, params.MemoryConsumptionTracker, params.Annotations, b.ExpressionPosition(), timeRange)
		case parser.CardOneToOne:
			return binops.NewOneToOneVectorVectorBinaryOperation(lhs, rhs, *b.VectorMatching.ToPrometheusType(), op, b.ReturnBool, params.MemoryConsumptionTracker, params.Annotations, b.ExpressionPosition(), timeRange, b.Hints.ToOperatorType(), params.Logger)
		default:
			return nil, compat.NewNotSupportedError(fmt.Sprintf("binary expression with %v matching for '%v'", b.VectorMatching.Card, b.Op.String()))
		}
	}
}

func (b *BinaryExpression) ResultType() (parser.ValueType, error) {
	lhs, err := b.LHS.ResultType()
	if err != nil {
		return parser.ValueTypeNone, err
	}

	rhs, err := b.RHS.ResultType()
	if err != nil {
		return parser.ValueTypeNone, err
	}

	if lhs == parser.ValueTypeScalar && rhs == parser.ValueTypeScalar {
		return parser.ValueTypeScalar, nil
	}

	if lhs != parser.ValueTypeVector && lhs != parser.ValueTypeScalar {
		return parser.ValueTypeNone, fmt.Errorf("left side of binary expression must be scalar or vector, got %v", lhs)
	}

	if rhs != parser.ValueTypeVector && rhs != parser.ValueTypeScalar {
		return parser.ValueTypeNone, fmt.Errorf("right side of binary expression must be scalar or vector, got %v", rhs)
	}

	return parser.ValueTypeVector, nil
}

func (b *BinaryExpression) QueriedTimeRange(queryTimeRange types.QueryTimeRange, lookbackDelta time.Duration) planning.QueriedTimeRange {
	lhs := b.LHS.QueriedTimeRange(queryTimeRange, lookbackDelta)
	rhs := b.RHS.QueriedTimeRange(queryTimeRange, lookbackDelta)
	return lhs.Union(rhs)
}

func (b *BinaryExpression) ExpressionPosition() posrange.PositionRange {
	return b.GetExpressionPosition().ToPrometheusType()
}

func (b *BinaryExpression) MinimumRequiredPlanVersion() planning.QueryPlanVersion {
	return planning.QueryPlanVersionZero
}

func (v *VectorMatching) Equals(other *VectorMatching) bool {
	if v == nil && other == nil {
		// Both are nil.
		return true
	}

	return v != nil && other != nil &&
		v.On == other.On &&
		v.Card == other.Card &&
		slices.Equal(v.MatchingLabels, other.MatchingLabels) &&
		slices.Equal(v.Include, other.Include)
}
