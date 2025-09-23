// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/subtree_folder_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"
)

func TestEvalPredicate(t *testing.T) {
	for testName, tc := range map[string]struct {
		input       string
		fn          predicate
		expectedRes bool
		expectedErr bool
	}{
		"should return error if the predicate returns error": {
			input: "selector1{} or selector2{}",
			fn: func(parser.Node) (bool, error) {
				return false, errors.New("some err")
			},
			expectedRes: false,
			expectedErr: true,
		},
		"should return false if the predicate returns false for all nodes in the subtree": {
			input: "selector1{} or selector2{}",
			fn: func(parser.Node) (bool, error) {
				return false, nil
			},
			expectedRes: false,
			expectedErr: false,
		},
		"should return true if the predicate returns true for at least 1 node in the subtree": {
			input: "selector1{} or selector2{}",
			fn: func(node parser.Node) (bool, error) {
				// Return true only for 1 node in the subtree.
				if node.String() == "selector1" {
					return true, nil
				}
				return false, nil
			},
			expectedRes: true,
			expectedErr: false,
		},
		"hasEmbeddedQueries()": {
			input:       `sum without(__query_shard__) (__embedded_queries__{__queries__="tstquery"}) or sum(selector)`,
			fn:          EmbeddedQueriesSquasher.ContainsSquashedExpression,
			expectedRes: true,
			expectedErr: false,
		},
	} {
		t.Run(testName, func(t *testing.T) {
			expr, err := parser.ParseExpr(tc.input)
			require.Nil(t, err)

			res, err := anyNode(expr.(parser.Node), tc.fn)
			if tc.expectedErr {
				require.Error(t, err)
			} else {
				require.Nil(t, err)
			}

			require.Equal(t, tc.expectedRes, res)
		})
	}
}
