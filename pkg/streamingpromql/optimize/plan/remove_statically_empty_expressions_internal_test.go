// SPDX-License-Identifier: AGPL-3.0-only

package plan

import (
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

func TestIsEitherBinaryExpressionSideEmptyWithFillUsesSyntacticSides(t *testing.T) {
	testCases := map[string]struct {
		card          parser.VectorMatchCardinality
		emptyLHS      bool
		fillValues    core.VectorMatchFillValues
		expectedEmpty bool
	}{
		"one-to-one LhsSet covers the syntactic LHS": {
			card:       parser.CardOneToOne,
			emptyLHS:   true,
			fillValues: core.VectorMatchFillValues{LhsSet: true},
		},
		"one-to-one RhsSet does not cover the syntactic LHS": {
			card:          parser.CardOneToOne,
			emptyLHS:      true,
			fillValues:    core.VectorMatchFillValues{RhsSet: true},
			expectedEmpty: true,
		},
		"group_left LhsSet covers the syntactic LHS": {
			card:       parser.CardManyToOne,
			emptyLHS:   true,
			fillValues: core.VectorMatchFillValues{LhsSet: true},
		},
		"group_left RhsSet covers the syntactic RHS": {
			card:       parser.CardManyToOne,
			fillValues: core.VectorMatchFillValues{RhsSet: true},
		},
		"group_right RhsSet covers the syntactic LHS": {
			card:       parser.CardOneToMany,
			emptyLHS:   true,
			fillValues: core.VectorMatchFillValues{RhsSet: true},
		},
		"group_right LhsSet does not cover the syntactic LHS": {
			card:          parser.CardOneToMany,
			emptyLHS:      true,
			fillValues:    core.VectorMatchFillValues{LhsSet: true},
			expectedEmpty: true,
		},
		"group_right LhsSet covers the syntactic RHS": {
			card:       parser.CardOneToMany,
			fillValues: core.VectorMatchFillValues{LhsSet: true},
		},
		"group_right RhsSet does not cover the syntactic RHS": {
			card:          parser.CardOneToMany,
			fillValues:    core.VectorMatchFillValues{RhsSet: true},
			expectedEmpty: true,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			empty := planning.Node(&core.NoOp{NoOpDetails: &core.NoOpDetails{}})
			nonEmpty := planning.Node(&core.VectorSelector{VectorSelectorDetails: &core.VectorSelectorDetails{}})
			lhs, rhs := nonEmpty, empty
			if testCase.emptyLHS {
				lhs, rhs = empty, nonEmpty
			}

			node := &core.BinaryExpression{
				LHS: lhs,
				RHS: rhs,
				BinaryExpressionDetails: &core.BinaryExpressionDetails{
					Op: core.BINARY_ADD,
					VectorMatching: &core.VectorMatching{
						Card:       testCase.card,
						FillValues: testCase.fillValues,
					},
				},
			}

			actual, err := isEitherBinaryExpressionSideEmptyWithFill(node, &planning.QueryParameters{})
			require.NoError(t, err)
			require.Equal(t, testCase.expectedEmpty, actual)
		})
	}
}
