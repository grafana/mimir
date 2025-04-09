// SPDX-License-Identifier: AGPL-3.0-only

package core

import (
	"fmt"
	"slices"
	"strings"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/streamingpromql/compat"
	"github.com/grafana/mimir/pkg/streamingpromql/operators"
	"github.com/grafana/mimir/pkg/streamingpromql/operators/binops"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

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

	return builder.String()
}

func (b *BinaryExpression) ChildrenTimeRange(timeRange types.QueryTimeRange) types.QueryTimeRange {
	return timeRange
}

func (b *BinaryExpression) Details() proto.Message {
	return b.BinaryExpressionDetails
}

func (b *BinaryExpression) Children() []planning.Node {
	return []planning.Node{b.LHS, b.RHS}
}

func (b *BinaryExpression) SetChildren(children []planning.Node) error {
	if len(children) != 2 {
		return fmt.Errorf("node of type BinaryExpression expects 2 children, but got %d", len(children))
	}

	b.LHS, b.RHS = children[0], children[1]

	return nil
}

func (b *BinaryExpression) EquivalentTo(other planning.Node) bool {
	otherBinaryExpression, ok := other.(*BinaryExpression)

	return ok &&
		b.Op == otherBinaryExpression.Op &&
		b.LHS.EquivalentTo(otherBinaryExpression.LHS) &&
		b.RHS.EquivalentTo(otherBinaryExpression.RHS) &&
		b.VectorMatching.Equals(otherBinaryExpression.VectorMatching) &&
		b.ReturnBool == otherBinaryExpression.ReturnBool
}

func (b *BinaryExpression) ChildrenLabels() []string {
	return []string{"LHS", "RHS"}
}

func (b *BinaryExpression) OperatorFactory(children []types.Operator, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (planning.OperatorFactory, error) {
	op, ok := b.Op.ToItemType()
	if !ok {
		return nil, compat.NewNotSupportedError(fmt.Sprintf("'%v' binary expression", b.Op.String()))
	}

	if len(children) != 2 {
		return nil, fmt.Errorf("expected exactly 2 children for BinaryExpression, got %v", len(children))
	}

	lhsVector, lhsScalar := b.getChildOperator(children[0])
	rhsVector, rhsScalar := b.getChildOperator(children[1])

	if lhsVector == nil && lhsScalar == nil {
		return nil, fmt.Errorf("expected either InstantVectorOperator or ScalarOperator on left-hand side of BinaryExpression, got %T", children[0])
	}

	if rhsVector == nil && rhsScalar == nil {
		return nil, fmt.Errorf("expected either InstantVectorOperator or ScalarOperator on right-hand side of BinaryExpression, got %T", children[1])
	}

	if lhsScalar != nil && rhsScalar != nil {
		o, err := binops.NewScalarScalarBinaryOperation(lhsScalar, rhsScalar, op, params.MemoryConsumptionTracker, b.ExpressionPosition.ToPrometheusType())
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

	o, err := binops.NewVectorScalarBinaryOperation(scalar, vector, scalarIsLeftSide, op, b.ReturnBool, timeRange, params.MemoryConsumptionTracker, params.Annotations, b.ExpressionPosition.ToPrometheusType())
	if err != nil {
		return nil, err
	}

	return planning.NewSingleUseOperatorFactory(operators.NewDeduplicateAndMerge(o, params.MemoryConsumptionTracker)), nil
}

func (b *BinaryExpression) getChildOperator(child types.Operator) (types.InstantVectorOperator, types.ScalarOperator) {
	switch child := child.(type) {
	case types.InstantVectorOperator:
		return child, nil
	case types.ScalarOperator:
		return nil, child
	default:
		return nil, nil
	}
}

func (b *BinaryExpression) createVectorVectorOperator(lhs, rhs types.InstantVectorOperator, op parser.ItemType, timeRange types.QueryTimeRange, params *planning.OperatorParameters) (types.InstantVectorOperator, error) {
	switch op {
	case parser.LAND, parser.LUNLESS:
		return binops.NewAndUnlessBinaryOperation(lhs, rhs, *b.VectorMatching.ToPrometheusType(), params.MemoryConsumptionTracker, op == parser.LUNLESS, timeRange, b.ExpressionPosition.ToPrometheusType()), nil
	case parser.LOR:
		return binops.NewOrBinaryOperation(lhs, rhs, *b.VectorMatching.ToPrometheusType(), params.MemoryConsumptionTracker, timeRange, b.ExpressionPosition.ToPrometheusType()), nil
	default:
		switch b.VectorMatching.Card {
		case parser.CardOneToMany, parser.CardManyToOne:
			return binops.NewGroupedVectorVectorBinaryOperation(lhs, rhs, *b.VectorMatching.ToPrometheusType(), op, b.ReturnBool, params.MemoryConsumptionTracker, params.Annotations, b.ExpressionPosition.ToPrometheusType(), timeRange)
		case parser.CardOneToOne:
			return binops.NewOneToOneVectorVectorBinaryOperation(lhs, rhs, *b.VectorMatching.ToPrometheusType(), op, b.ReturnBool, params.MemoryConsumptionTracker, params.Annotations, b.ExpressionPosition.ToPrometheusType(), timeRange)
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
