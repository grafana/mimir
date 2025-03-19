// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
)

// TODO: actually generate some / all of this automatically
// TODO: tests for equality edge cases
// TODO: tests for descriptions

func (a *AggregateExpression) Type() string {
	return "AggregateExpression"
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

func (a *AggregateExpression) Describe() string {
	builder := &strings.Builder{}
	builder.WriteString(a.Op.String())

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

func (a *AggregateExpression) ChildrenLabels() []string {
	if a.Param == nil {
		return []string{""}
	}

	return []string{"expression", "parameter"}
}

func (b *BinaryExpression) Type() string {
	return "BinaryExpression"
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

func (b *BinaryExpression) Describe() string {
	builder := &strings.Builder{}

	builder.WriteString("LHS ")
	builder.WriteString(b.Op.String())

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

func (b *BinaryExpression) ChildrenLabels() []string {
	return []string{"LHS", "RHS"}
}

func (f *FunctionCall) Type() string {
	return "FunctionCall"
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

func (f *FunctionCall) Describe() string {
	return fmt.Sprintf("%v(...)", f.FunctionName)
}

func (f *FunctionCall) ChildrenLabels() []string {
	if len(f.Args) == 1 {
		return []string{""}
	}

	l := make([]string, len(f.Args))

	for i := range l {
		l[i] = fmt.Sprintf("param %v", i)
	}

	return l
}

func (n *NumberLiteral) Type() string {
	return "NumberLiteral"
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

func (n *NumberLiteral) Describe() string {
	return strconv.FormatFloat(n.Value, 'g', -1, 64)
}

func (n *NumberLiteral) ChildrenLabels() []string {
	return nil
}

func (n *StringLiteral) Type() string {
	return "StringLiteral"
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

func (n *StringLiteral) Describe() string {
	return strconv.QuoteToASCII(n.Value)
}

func (n *StringLiteral) ChildrenLabels() []string {
	return nil
}

func (u *UnaryExpression) Type() string {
	return "UnaryExpression"
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

func (u *UnaryExpression) Describe() string {
	return u.Op.String()
}

func (u *UnaryExpression) ChildrenLabels() []string {
	return []string{""}
}

func (v *VectorSelector) Type() string {
	return "VectorSelector"
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
		((v.Timestamp == nil && otherVectorSelector.Timestamp == nil) || (v.Timestamp != nil && otherVectorSelector.Timestamp != nil && *v.Timestamp == *otherVectorSelector.Timestamp)) &&
		v.Offset == otherVectorSelector.Offset
}

func (v *VectorSelector) Describe() string {
	return describeSelector(v.Matchers, v.Timestamp, v.Offset, nil)
}

func describeSelector(matchers []*labels.Matcher, ts *int64, offset time.Duration, rng *time.Duration) string {
	builder := &strings.Builder{}
	builder.WriteRune('{')
	for i, m := range matchers {
		if i > 0 {
			builder.WriteString(", ")
		}

		builder.WriteString(m.String())
	}
	builder.WriteRune('}')

	if rng != nil {
		builder.WriteRune('[')
		builder.WriteString(rng.String())
		builder.WriteRune(']')
	}

	if ts != nil {
		builder.WriteString(" @ ")
		builder.WriteString(strconv.FormatInt(*ts, 10))
		builder.WriteString(" (")
		builder.WriteString(timestamp.Time(*ts).Format(time.RFC3339))
		builder.WriteRune(')')
	}

	if offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(offset.String())
	}

	return builder.String()
}

func (v *VectorSelector) ChildrenLabels() []string {
	return nil
}

func (m *MatrixSelector) Type() string {
	return "MatrixSelector"
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
		((m.Timestamp == nil && otherMatrixSelector.Timestamp == nil) || (m.Timestamp != nil && otherMatrixSelector.Timestamp != nil && *m.Timestamp == *otherMatrixSelector.Timestamp)) &&
		m.Offset == otherMatrixSelector.Offset &&
		m.Range == otherMatrixSelector.Range
}

func (m *MatrixSelector) Describe() string {
	return describeSelector(m.Matchers, m.Timestamp, m.Offset, &m.Range)
}

func (m *MatrixSelector) ChildrenLabels() []string {
	return nil
}

func matchersEqual(a, b *labels.Matcher) bool {
	return a.Type == b.Type &&
		a.Name == b.Name &&
		a.Value == b.Value
}

func (s *Subquery) Type() string {
	return "Subquery"
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
		((s.Timestamp == nil && otherSubquery.Timestamp == nil) || (s.Timestamp != nil && otherSubquery.Timestamp != nil && *s.Timestamp == *otherSubquery.Timestamp)) &&
		s.Offset == otherSubquery.Offset &&
		s.Range == otherSubquery.Range &&
		s.Step == otherSubquery.Step
}

func (s *Subquery) Describe() string {
	builder := &strings.Builder{}

	builder.WriteRune('[')
	builder.WriteString(s.Range.String())
	builder.WriteRune(':')
	builder.WriteString(s.Step.String())
	builder.WriteRune(']')

	if s.Timestamp != nil {
		builder.WriteString(" @ ")
		builder.WriteString(strconv.FormatInt(*s.Timestamp, 10))
		builder.WriteString(" (")
		builder.WriteString(timestamp.Time(*s.Timestamp).Format(time.RFC3339))
		builder.WriteRune(')')
	}

	if s.Offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(s.Offset.String())
	}

	return builder.String()
}

func (s *Subquery) ChildrenLabels() []string {
	return []string{""}
}
