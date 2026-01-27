// SPDX-License-Identifier: AGPL-3.0-only

package ast_test

import (
	"context"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/streamingpromql/optimize/ast"
)

func TestReduceMatchers_Apply_Vectors(t *testing.T) {
	// NOTE: Expr.String() for vector or matrix expression orders label matchers so it's
	// important that tests put them in order to prevent tests failing for no reason.
	tests := []struct {
		name          string
		inputQuery    string
		expectedQuery string
	}{
		{
			name:          "only name matcher",
			inputQuery:    `test_series`,
			expectedQuery: `test_series`,
		},
		{
			name:          "deduplicate matchers",
			inputQuery:    `test_series{foo="bar",foo="bar",foo="bar",foo=~".*baz.*",foo=~".*baz.*"}`,
			expectedQuery: `test_series{foo="bar",foo=~".*baz.*"}`,
		},
		{
			name: "multiple unique equals matchers should only return equals matchers",
			// Even though the regex matcher matches neither equals matcher,
			// a query with multiple equals matchers for the same label name is already guaranteed to return an empty set
			inputQuery:    `test_series{foo="bar",foo=~".*bananas.*",foo="baz"}`,
			expectedQuery: `test_series{foo="bar",foo="baz"}`,
		},
		{
			name:          "should remove a regex matcher if it is a superset of an equals matcher",
			inputQuery:    `test_series{foo="bar",foo=~".*bar.*"}`,
			expectedQuery: `test_series{foo="bar"}`,
		},
		{
			name:          "should preserve a regex matcher if it is not a superset of an equals matcher",
			inputQuery:    `test_series{foo="bar",foo="bar",foo=~".*baz.*"}`,
			expectedQuery: `test_series{foo="bar",foo=~".*baz.*"}`,
		},
		{
			name:          "do not drop wildcard negative regex matcher",
			inputQuery:    `test_series{foo!~".*"}`,
			expectedQuery: `test_series{foo!~".*"}`,
		},
		{
			name:          "single non-wildcard matcher should not be dropped",
			inputQuery:    `test_series{foo!=""}`,
			expectedQuery: `test_series{foo!=""}`,
		},
		{
			name:          "drop all matchers that match supersets of an equals matcher",
			inputQuery:    `test_series{foo=~".*bar.*",foo="bar",foo!="",foo!~"",foo!="baz"}`,
			expectedQuery: `test_series{foo="bar"}`,
		},
		{
			name:          "keep one matcher of ones that reduce the set size equivalently",
			inputQuery:    `test_series{foo=~".*",foo!~"",foo!=""}`,
			expectedQuery: `test_series{foo!~""}`,
		},
		{
			name:          "keep at least one matcher for each label name",
			inputQuery:    `test_series{foo=~".*",baz!="",foo!~"",foo!=""}`,
			expectedQuery: `test_series{baz!="",foo!~""}`,
		},
		{
			name:          "keep matcher that excludes empty strings if no other matcher does so",
			inputQuery:    `test_series{foo!="",foo!="bar"}`,
			expectedQuery: `test_series{foo!="",foo!="bar"}`,
		},
		{
			name:          "not equals matcher should not be removed if it doesn't match equals matcher value",
			inputQuery:    `test_series{foo!="bar",foo="bar"}`,
			expectedQuery: `test_series{foo!="bar",foo="bar"}`,
		},
		{
			name:          "not equals matcher should be removed if it does match equals matcher value",
			inputQuery:    `test_series{foo!="bar",foo="baz"}`,
			expectedQuery: `test_series{foo="baz"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pass := ast.NewReduceMatchers(prometheus.NewPedanticRegistry(), log.NewNopLogger())
			outputExpr := runASTOptimizationPassWithoutMetrics(t, context.Background(), tt.inputQuery, pass)
			outputQuery := outputExpr.String()
			require.Equal(t, tt.expectedQuery, outputQuery)
		})
	}
}

func TestReduceMatchers_Apply_ComplexQueries(t *testing.T) {
	enableExperimentalFunctions := parser.EnableExperimentalFunctions
	t.Cleanup(func() {
		parser.EnableExperimentalFunctions = enableExperimentalFunctions
	})
	parser.EnableExperimentalFunctions = true

	tests := []struct {
		name          string
		inputQuery    string
		expectedQuery string
	}{
		{
			name:          "no matchers",
			inputQuery:    `vector(1)`,
			expectedQuery: `vector(1)`,
		},
		{
			name:          "deduplicate matchers for aggregation",
			inputQuery:    `sum(rate(test_series{foo="bar",foo="bar"}[5m]))`,
			expectedQuery: `sum(rate(test_series{foo="bar"}[5m]))`,
		},
		{
			name:          "deduplicate matchers for binary expression of aggregations",
			inputQuery:    `sum(rate(test_sum{foo="bar",foo="bar"}[5m])) / sum(rate(test_count{foo="bar",foo="bar"}[5m]))`,
			expectedQuery: `sum(rate(test_sum{foo="bar"}[5m])) / sum(rate(test_count{foo="bar"}[5m]))`,
		},
		{
			name:          "should remove a regex matcher if it is a superset of an equals matcher for subquery",
			inputQuery:    `max_over_time(rate(test_series{foo="bar",foo=~"bar|baz|bing"}[5m])[1d:5m])`,
			expectedQuery: `max_over_time(rate(test_series{foo="bar"}[5m])[1d:5m])`,
		},
		{
			name:          "do not reduce matchers for info function",
			inputQuery:    `info(test_series{foo="bar",foo="bar"}, {__name__="test_info",foo="bar",foo="bar"})`,
			expectedQuery: `info(test_series{foo="bar",foo="bar"}, {__name__="test_info",foo="bar",foo="bar"})`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pass := ast.NewReduceMatchers(prometheus.NewPedanticRegistry(), log.NewNopLogger())
			outputExpr := runASTOptimizationPassWithoutMetrics(t, context.Background(), tt.inputQuery, pass)
			outputQuery := outputExpr.String()
			require.Equal(t, tt.expectedQuery, outputQuery)
		})
	}
}
