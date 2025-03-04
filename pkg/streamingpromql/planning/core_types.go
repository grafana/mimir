// SPDX-License-Identifier: AGPL-3.0-only

package planning

import (
	"time"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

type AggregateExpression struct {
	Op                 parser.ItemType `json:"op"`
	Inner              Node
	Param              Node
	Grouping           []string               `json:"grouping,omitempty"`
	Without            bool                   `json:"without,omitempty"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type BinaryExpression struct {
	Op             parser.ItemType `json:"op"`
	LHS            Node
	RHS            Node
	VectorMatching *parser.VectorMatching `json:"vectorMatching,omitempty"` // TODO: own type?
	ReturnBool     bool                   `json:"returnBool,omitempty"`
}

type FunctionCall struct {
	FunctionName       string `json:"functionName"`
	Args               []Node
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
	Op                 parser.ItemType `json:"op,omitempty"`
	Inner              Node
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type VectorSelector struct {
	Matchers           []*labels.Matcher      `json:"matchers,omitempty"`  // TODO: will need custom serializer to ensure regexp is correctly populated
	Timestamp          *int64                 `json:"timestamp,omitempty"` // TODO: check effect of omitempty if value is 0
	Offset             time.Duration          `json:"offset,omitempty"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type MatrixSelector struct {
	Matchers           []*labels.Matcher      `json:"matchers"`
	Timestamp          *int64                 `json:"timestamp,omitempty"` // TODO: check effect of omitempty if value is 0
	Offset             time.Duration          `json:"offset,omitempty"`
	Range              time.Duration          `json:"range,omitempty"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}

type Subquery struct {
	Inner              Node
	Timestamp          *int64                 `json:"timestamp,omitempty"` // TODO: check effect of omitempty if value is 0
	Offset             time.Duration          `json:"offset,omitempty"`
	Range              time.Duration          `json:"range,omitempty"`
	Step               time.Duration          `json:"step,omitempty"`
	ExpressionPosition posrange.PositionRange `json:"expressionPosition"`
}
