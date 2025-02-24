// SPDX-License-Identifier: AGPL-3.0-only

package operators

import (
	"github.com/prometheus/prometheus/promql/parser/posrange"

	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type StringLiteral struct {
	Value string

	expressionPosition posrange.PositionRange
}

var _ types.StringOperator = &StringLiteral{}

func NewStringLiteral(
	value string,
	expressionPosition posrange.PositionRange,
) *StringLiteral {
	return &StringLiteral{
		Value:              value,
		expressionPosition: expressionPosition,
	}
}

func (s *StringLiteral) GetValue() string {
	return s.Value
}

func (s *StringLiteral) ExpressionPosition() posrange.PositionRange {
	return s.expressionPosition
}

func (s *StringLiteral) Close() {
	// Nothing to do.
}
