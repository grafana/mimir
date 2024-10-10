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
			fn:          hasEmbeddedQueries,
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

func TestSubtreeFolder(t *testing.T) {
	for testName, tc := range map[string]struct {
		input    string
		expected string
	}{
		"embed an entire histogram": {
			input:    "histogram_quantile(0.5, rate(alertmanager_http_request_duration_seconds_bucket[1m]))",
			expected: `__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"histogram_quantile(0.5, rate(alertmanager_http_request_duration_seconds_bucket[1m]))\"}]}"}`,
		},
		"embed a binary expression across two functions": {
			input:    `rate(http_requests_total{cluster="eu-west2"}[5m]) or rate(http_requests_total{cluster="us-central1"}[5m])`,
			expected: `__embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"rate(http_requests_total{cluster=\\\"eu-west2\\\"}[5m]) or rate(http_requests_total{cluster=\\\"us-central1\\\"}[5m])\"}]}"}`,
		},
		"embed one out of two legs of the query (right leg has already been embedded)": {
			input: `sum(histogram_quantile(0.5, rate(selector[1m]))) +
				sum without(__query_shard__) (__embedded_queries__{__queries__="tstquery"})`,
			expected: `
			  __embedded_queries__{__queries__="{\"Concat\":[{\"Expr\":\"sum(histogram_quantile(0.5, rate(selector[1m])))\"}]}"} + sum without (__query_shard__) (__embedded_queries__{__queries__="tstquery"})`,
		},
		"should not embed scalars": {
			input:    `histogram_quantile(0.5, __embedded_queries__{__queries__="tstquery"})`,
			expected: `histogram_quantile(0.5, __embedded_queries__{__queries__="tstquery"})`,
		},
	} {
		t.Run(testName, func(t *testing.T) {
			mapper := newSubtreeFolder()

			expr, err := parser.ParseExpr(tc.input)
			require.Nil(t, err)
			res, err := mapper.Map(expr)
			require.Nil(t, err)

			expected, err := parser.ParseExpr(tc.expected)
			require.Nil(t, err)

			require.Equal(t, expected.String(), res.String())
		})
	}
}
