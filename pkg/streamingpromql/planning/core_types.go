// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/promql/parser"
)

func init() {
	RegisterNodeFactory(func() Node { return &AggregateExpression{AggregateExpressionDetails: &AggregateExpressionDetails{}} })
	RegisterNodeFactory(func() Node { return &BinaryExpression{BinaryExpressionDetails: &BinaryExpressionDetails{}} })
	RegisterNodeFactory(func() Node { return &FunctionCall{FunctionCallDetails: &FunctionCallDetails{}} })
	RegisterNodeFactory(func() Node { return &NumberLiteral{NumberLiteralDetails: &NumberLiteralDetails{}} })
	RegisterNodeFactory(func() Node { return &StringLiteral{StringLiteralDetails: &StringLiteralDetails{}} })
	RegisterNodeFactory(func() Node { return &UnaryExpression{UnaryExpressionDetails: &UnaryExpressionDetails{}} })
	RegisterNodeFactory(func() Node { return &VectorSelector{VectorSelectorDetails: &VectorSelectorDetails{}} })
	RegisterNodeFactory(func() Node { return &MatrixSelector{MatrixSelectorDetails: &MatrixSelectorDetails{}} })
	RegisterNodeFactory(func() Node { return &Subquery{SubqueryDetails: &SubqueryDetails{}} })
}

// Why do we have this slightly odd structure with some fields declared below and some in the Protobuf message types?
// When marshalling a query plan to Protobuf (or any other format), we need to handle references to nodes separately,
// as multiple parent nodes can refer to the same child node.
// If we declared these fields as ordinary Protobuf message fields, then these references would be lost, and each parent
// would end up with a unique copy of the child node on unmarshalling.
// So the rule is:
// - if it's a reference to a Node instance, it needs to be declared here
// - if it's anything else, it can go in the Protobuf message definition

// TODO: tests for descriptions

type AggregateExpression struct {
	*AggregateExpressionDetails
	Inner Node
	Param Node
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

type BinaryExpression struct {
	*BinaryExpressionDetails
	LHS Node
	RHS Node
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

type FunctionCall struct {
	*FunctionCallDetails
	Args []Node `json:"-"`
}

func (f *FunctionCall) Describe() string {
	return fmt.Sprintf("%v(...)", f.FunctionName)
}

type NumberLiteral struct {
	*NumberLiteralDetails
}

func (n *NumberLiteral) Describe() string {
	return strconv.FormatFloat(n.Value, 'g', -1, 64)
}

type StringLiteral struct {
	*StringLiteralDetails
}

func (n *StringLiteral) Describe() string {
	return strconv.QuoteToASCII(n.Value)
}

type UnaryExpression struct {
	*UnaryExpressionDetails
	Inner Node
}

func (u *UnaryExpression) Describe() string {
	return u.Op.Describe()
}

type VectorSelector struct {
	*VectorSelectorDetails
}

func (v *VectorSelector) Describe() string {
	return describeSelector(v.Matchers, v.Timestamp, v.Offset, nil)
}

type MatrixSelector struct {
	*MatrixSelectorDetails
}

func (m *MatrixSelector) Describe() string {
	return describeSelector(m.Matchers, m.Timestamp, m.Offset, &m.Range)
}

type Subquery struct {
	*SubqueryDetails
	Inner Node `json:"-"`
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
		builder.WriteString(strconv.FormatInt(s.Timestamp.Timestamp, 10))
		builder.WriteString(" (")
		builder.WriteString(timestamp.Time(s.Timestamp.Timestamp).Format(time.RFC3339))
		builder.WriteRune(')')
	}

	if s.Offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(s.Offset.String())
	}

	return builder.String()
}

func describeSelector(matchers []*LabelMatcher, ts *Timestamp, offset time.Duration, rng *time.Duration) string {
	builder := &strings.Builder{}
	builder.WriteRune('{')
	for i, m := range matchers {
		if i > 0 {
			builder.WriteString(", ")
		}

		// Convert to the Prometheus type so we can use its String().
		promMatcher := labels.Matcher{Type: m.Type, Name: m.Name, Value: m.Value}
		builder.WriteString(promMatcher.String())
	}
	builder.WriteRune('}')

	if rng != nil {
		builder.WriteRune('[')
		builder.WriteString(rng.String())
		builder.WriteRune(']')
	}

	if ts != nil {
		builder.WriteString(" @ ")
		builder.WriteString(strconv.FormatInt(ts.Timestamp, 10))
		builder.WriteString(" (")
		builder.WriteString(timestamp.Time(ts.Timestamp).Format(time.RFC3339))
		builder.WriteRune(')')
	}

	if offset != 0 {
		builder.WriteString(" offset ")
		builder.WriteString(offset.String())
	}

	return builder.String()
}
