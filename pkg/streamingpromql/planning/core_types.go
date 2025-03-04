// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"slices"
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

type AggregateExpression struct {
	Op                 parser.ItemType        `json:"op"`
	Inner              Node                   `json:"-"`
	Param              Node                   `json:"-"`
	Grouping           []string               `json:"grouping,omitempty"`
	Without            bool                   `json:"without,omitempty"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type BinaryExpression struct {
	Op             parser.ItemType `json:"op"`
	LHS            Node            `json:"-"`
	RHS            Node            `json:"-"`
	VectorMatching *VectorMatching `json:"vectorMatching,omitempty"`
	ReturnBool     bool            `json:"returnBool,omitempty"`
}

// VectorMatching is the same as parser.VectorMatching, but with JSON tags so we can skip serializing fields with their default values.
type VectorMatching struct {
	Card           parser.VectorMatchCardinality `json:"card,omitempty"`
	MatchingLabels []string                      `json:"matchingLabels,omitempty"`
	On             bool                          `json:"on,omitempty"`
	Include        []string                      `json:"include,omitempty"`
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

func (v *VectorMatching) AsParserType() *parser.VectorMatching {
	return (*parser.VectorMatching)(v)
}

func VectorMatchingFromParserType(v *parser.VectorMatching) *VectorMatching {
	return (*VectorMatching)(v)
}

type FunctionCall struct {
	FunctionName       string                 `json:"functionName"`
	Args               []Node                 `json:"-"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type NumberLiteral struct {
	Value              float64                `json:"value"` // TODO: encode as string to preserve precision?
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type StringLiteral struct {
	Value              string                 `json:"value"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type UnaryExpression struct {
	Op                 parser.ItemType        `json:"op,omitempty"`
	Inner              Node                   `json:"-"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type VectorSelector struct {
	Matchers           []*labels.Matcher      `json:"matchers,omitempty"`
	Timestamp          *int64                 `json:"timestamp,omitempty"`
	Offset             time.Duration          `json:"offset,omitempty"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type MatrixSelector struct {
	Matchers           []*labels.Matcher      `json:"matchers"`
	Timestamp          *int64                 `json:"timestamp,omitempty"`
	Offset             time.Duration          `json:"offset,omitempty"`
	Range              time.Duration          `json:"range,omitempty"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type Subquery struct {
	Inner              Node                   `json:"-"`
	Timestamp          *int64                 `json:"timestamp,omitempty"`
	Offset             time.Duration          `json:"offset,omitempty"`
	Range              time.Duration          `json:"range,omitempty"`
	Step               time.Duration          `json:"step,omitempty"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}
