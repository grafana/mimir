// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"slices"

	"github.com/gogo/protobuf/proto"
)

// TODO: actually generate some / all of this automatically
// TODO: tests for equality edge cases

func (a *AggregateExpression) Details() proto.Message {
	return a.AggregateExpressionDetails
}

func (a *AggregateExpression) Children() []Node {
	if a.Param == nil {
		return []Node{a.Inner}
	}

	return []Node{a.Inner, a.Param}
}

func (a *AggregateExpression) SetChildren(children []Node) error {
	switch len(children) {
	case 1:
		a.Inner, a.Param = children[0], nil
	case 2:
		a.Inner, a.Param = children[0], children[1]
	default:
		return fmt.Errorf("node of type AggregateExpression supports 1 or 2 children, but got %d", len(children))
	}

	return nil
}

func (a *AggregateExpression) Equals(other Node) bool {
	otherAggregateExpression, ok := other.(*AggregateExpression)

	return ok &&
		a.Op == otherAggregateExpression.Op &&
		a.Inner.Equals(otherAggregateExpression.Inner) &&
		((a.Param == nil && otherAggregateExpression.Param == nil) ||
			(a.Param != nil && otherAggregateExpression.Param != nil && a.Param.Equals(otherAggregateExpression.Param))) &&
		slices.Equal(a.Grouping, otherAggregateExpression.Grouping) &&
		a.Without == otherAggregateExpression.Without
}
func (a *AggregateExpression) ChildrenLabels() []string {
	if a.Param == nil {
		return []string{""}
	}

	return []string{"expression", "parameter"}
}

func (b *BinaryExpression) Details() proto.Message {
	return b.BinaryExpressionDetails
}

func (b *BinaryExpression) Children() []Node {
	return []Node{b.LHS, b.RHS}
}

func (b *BinaryExpression) SetChildren(children []Node) error {
	if len(children) != 2 {
		return fmt.Errorf("node of type BinaryExpression supports 2 children, but got %d", len(children))
	}

	b.LHS, b.RHS = children[0], children[1]

	return nil
}

func (b *BinaryExpression) Equals(other Node) bool {
	otherBinaryExpression, ok := other.(*BinaryExpression)

	return ok &&
		b.Op == otherBinaryExpression.Op &&
		b.LHS.Equals(otherBinaryExpression.LHS) &&
		b.RHS.Equals(otherBinaryExpression.RHS) &&
		b.VectorMatching.Equals(otherBinaryExpression.VectorMatching) &&
		b.ReturnBool == otherBinaryExpression.ReturnBool
}

func (b *BinaryExpression) ChildrenLabels() []string {
	return []string{"LHS", "RHS"}
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

func (f *FunctionCall) Details() proto.Message {
	return f.FunctionCallDetails
}

func (f *FunctionCall) Children() []Node {
	return f.Args
}

func (f *FunctionCall) SetChildren(children []Node) error {
	f.Args = children
	return nil
}

func (f *FunctionCall) Equals(other Node) bool {
	otherFunctionCall, ok := other.(*FunctionCall)

	return ok &&
		slices.EqualFunc(f.Args, otherFunctionCall.Args, func(a, b Node) bool {
			return a.Equals(b)
		})
}

func (f *FunctionCall) ChildrenLabels() []string {
	if len(f.Args) == 0 {
		return nil
	}

	if len(f.Args) == 1 {
		return []string{""}
	}

	l := make([]string, len(f.Args))

	for i := range l {
		l[i] = fmt.Sprintf("param %v", i)
	}

	return l
}

func (n *NumberLiteral) Details() proto.Message {
	return n.NumberLiteralDetails
}

func (n *NumberLiteral) Children() []Node {
	return nil
}

func (n *NumberLiteral) SetChildren(children []Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type NumberLiteral supports 0 children, but got %d", len(children))
	}

	return nil
}

func (n *NumberLiteral) Equals(other Node) bool {
	otherLiteral, ok := other.(*NumberLiteral)

	return ok && n.Value == otherLiteral.Value
}

func (n *NumberLiteral) ChildrenLabels() []string {
	return nil
}

func (n *StringLiteral) Details() proto.Message {
	return n.StringLiteralDetails
}

func (n *StringLiteral) Children() []Node {
	return nil
}

func (n *StringLiteral) SetChildren(children []Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type StringLiteral supports 0 children, but got %d", len(children))
	}

	return nil
}

func (n *StringLiteral) Equals(other Node) bool {
	otherLiteral, ok := other.(*StringLiteral)

	return ok && n.Value == otherLiteral.Value
}

func (n *StringLiteral) ChildrenLabels() []string {
	return nil
}

func (u *UnaryExpression) Details() proto.Message {
	return u.UnaryExpressionDetails
}

func (u *UnaryExpression) Children() []Node {
	return []Node{u.Inner}
}

func (u *UnaryExpression) SetChildren(children []Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type UnaryExpression supports 1 child, but got %d", len(children))
	}

	u.Inner = children[0]

	return nil
}

func (u *UnaryExpression) Equals(other Node) bool {
	otherUnaryExpression, ok := other.(*UnaryExpression)

	return ok &&
		u.Op == otherUnaryExpression.Op &&
		u.Inner.Equals(otherUnaryExpression.Inner)
}

func (u *UnaryExpression) ChildrenLabels() []string {
	return []string{""}
}

func (v *VectorSelector) Details() proto.Message {
	return v.VectorSelectorDetails
}

func (v *VectorSelector) Children() []Node {
	return nil
}

func (v *VectorSelector) SetChildren(children []Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type VectorSelector supports 0 children, but got %d", len(children))
	}

	return nil
}

func (v *VectorSelector) Equals(other Node) bool {
	otherVectorSelector, ok := other.(*VectorSelector)

	return ok &&
		slices.EqualFunc(v.Matchers, otherVectorSelector.Matchers, matchersEqual) &&
		((v.Timestamp == nil && otherVectorSelector.Timestamp == nil) || (v.Timestamp != nil && otherVectorSelector.Timestamp != nil && v.Timestamp.Timestamp == otherVectorSelector.Timestamp.Timestamp)) &&
		v.Offset == otherVectorSelector.Offset
}

func (v *VectorSelector) ChildrenLabels() []string {
	return nil
}

func (m *MatrixSelector) Details() proto.Message {
	return m.MatrixSelectorDetails
}

func (m *MatrixSelector) Children() []Node {
	return nil
}

func (m *MatrixSelector) SetChildren(children []Node) error {
	if len(children) != 0 {
		return fmt.Errorf("node of type MatrixSelector supports 0 children, but got %d", len(children))
	}

	return nil
}

func (m *MatrixSelector) Equals(other Node) bool {
	otherMatrixSelector, ok := other.(*MatrixSelector)

	return ok &&
		slices.EqualFunc(m.Matchers, otherMatrixSelector.Matchers, matchersEqual) &&
		((m.Timestamp == nil && otherMatrixSelector.Timestamp == nil) || (m.Timestamp != nil && otherMatrixSelector.Timestamp != nil && m.Timestamp.Timestamp == otherMatrixSelector.Timestamp.Timestamp)) &&
		m.Offset == otherMatrixSelector.Offset &&
		m.Range == otherMatrixSelector.Range
}

func (m *MatrixSelector) ChildrenLabels() []string {
	return nil
}

func matchersEqual(a, b *LabelMatcher) bool {
	return a.Type == b.Type &&
		a.Name == b.Name &&
		a.Value == b.Value
}

func (s *Subquery) Details() proto.Message {
	return s.SubqueryDetails
}

func (s *Subquery) Children() []Node {
	return []Node{s.Inner}
}

func (s *Subquery) SetChildren(children []Node) error {
	if len(children) != 1 {
		return fmt.Errorf("node of type Subquery supports 1 child, but got %d", len(children))
	}

	s.Inner = children[0]

	return nil
}

func (s *Subquery) Equals(other Node) bool {
	otherSubquery, ok := other.(*Subquery)

	return ok &&
		((s.Timestamp == nil && otherSubquery.Timestamp == nil) || (s.Timestamp != nil && otherSubquery.Timestamp != nil && s.Timestamp.Timestamp == otherSubquery.Timestamp.Timestamp)) &&
		s.Offset == otherSubquery.Offset &&
		s.Range == otherSubquery.Range &&
		s.Step == otherSubquery.Step
}

func (s *Subquery) ChildrenLabels() []string {
	return []string{""}
}
