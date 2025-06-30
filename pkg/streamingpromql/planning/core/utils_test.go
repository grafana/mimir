// SPDX-License-Identifier: AGPL-3.0-only

package core

func numberLiteralOf(value float64) *NumberLiteral {
	return &NumberLiteral{
		NumberLiteralDetails: &NumberLiteralDetails{
			Value:              value,
			ExpressionPosition: PositionRange{Start: 1, End: 2},
		},
	}
}
