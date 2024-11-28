// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/astmapper_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
	"github.com/stretchr/testify/require"
)

func TestCloneExpr(t *testing.T) {
	var testExpr = []struct {
		input    parser.Expr
		expected parser.Expr
	}{
		// simple unmodified case
		{
			&parser.BinaryExpr{
				Op:  parser.ADD,
				LHS: &parser.NumberLiteral{Val: 1},
				RHS: &parser.NumberLiteral{Val: 1},
			},
			&parser.BinaryExpr{
				Op:  parser.ADD,
				LHS: &parser.NumberLiteral{Val: 1, PosRange: posrange.PositionRange{Start: 0, End: 1}},
				RHS: &parser.NumberLiteral{Val: 1, PosRange: posrange.PositionRange{Start: 4, End: 5}},
			},
		},
		{
			&parser.AggregateExpr{
				Op:      parser.SUM,
				Without: true,
				Expr: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "some_metric"),
					},
				},
				Grouping: []string{"foo"},
			},
			&parser.AggregateExpr{
				Op:      parser.SUM,
				Without: true,
				Expr: &parser.VectorSelector{
					Name: "some_metric",
					LabelMatchers: []*labels.Matcher{
						mustLabelMatcher(labels.MatchEqual, string(model.MetricNameLabel), "some_metric"),
					},
					PosRange: posrange.PositionRange{
						Start: 19,
						End:   30,
					},
				},
				Grouping: []string{"foo"},
				PosRange: posrange.PositionRange{
					Start: 0,
					End:   31,
				},
			},
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			res, err := cloneExpr(c.input)
			require.NoError(t, err)
			require.Equal(t, c.expected, res)
		})
	}
}

func TestCloneExpr_String(t *testing.T) {
	var testExpr = []struct {
		input    string
		expected string
	}{
		{
			input:    `rate(http_requests_total{cluster="us-central1"}[1m])`,
			expected: `rate(http_requests_total{cluster="us-central1"}[1m])`,
		},
		{
			input: `sum(
sum(rate(http_requests_total{cluster="us-central1"}[1m]))
/
sum(rate(http_requests_total{cluster="ops-tools1"}[1m]))
)`,
			expected: `sum(sum(rate(http_requests_total{cluster="us-central1"}[1m])) / sum(rate(http_requests_total{cluster="ops-tools1"}[1m])))`,
		},
		{
			input:    `sum({__name__="",a="x"})`,
			expected: `sum({__name__="",a="x"})`,
		},
	}

	for i, c := range testExpr {
		t.Run(fmt.Sprintf("[%d]", i), func(t *testing.T) {
			expr, err := parser.ParseExpr(c.input)
			require.Nil(t, err)
			res, err := cloneExpr(expr)
			require.Nil(t, err)
			require.Equal(t, c.expected, res.String())
		})
	}
}

func mustLabelMatcher(mt labels.MatchType, name, val string) *labels.Matcher {
	m, err := labels.NewMatcher(mt, name, val)
	if err != nil {
		panic(err)
	}
	return m
}

func TestSharding_BinaryExpressionsDontTakeExponentialTime(t *testing.T) {
	const expressions = 30
	const timeout = 10 * time.Second

	query := `vector(1)`
	// On 11th Gen Intel(R) Core(TM) i7-11700K @ 3.60GHz:
	// This was taking 3s for 20 expressions, and doubled the time for each extra one.
	// So checking for 30 expressions would take an hour if processing time is exponential.
	for i := 2; i <= expressions; i++ {
		query += fmt.Sprintf("or vector(%d)", i)
	}
	expr, err := parser.ParseExpr(query)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	summer, err := NewQueryShardSummer(ctx, 2, VectorSquasher, log.NewNopLogger(), NewMapperStats(), true)
	require.NoError(t, err)
	mapper := NewSharding(summer)

	_, err = mapper.Map(expr)
	require.NoError(t, err)
}
