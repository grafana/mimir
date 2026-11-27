// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"errors"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/util/promqlext"
)

func TestSubsetShardingLabels(t *testing.T) {
	for _, tc := range []struct {
		query string
		want  []string
	}{
		{`histogram_quantile(0.9, sum by (le, z, a, z) (rate(metric[5m])))`, []string{"a", "z"}},
		{`histogram_quantile(0.9, sum by (le) (rate(metric[5m])))`, nil},
		{`histogram_quantile(0.9, sum by (job) (rate(metric[5m])))`, nil},
		{`histogram_quantile(0.9, sum without (instance) (rate(metric[5m])))`, nil},
		{`histogram_quantile(scalar(foo), sum by (le, job) (rate(metric[5m])))`, nil},
		{`histogram_quantile(0.9, avg by (le, job) (rate(metric[5m])))`, nil},
		{`histogram_quantile(0.9, sum by (le, job) (irate(metric[5m])))`, nil},
		{`histogram_quantile(0.9, sum by (le, job) (rate(metric{__query_shard__="1_of_2"}[5m])))`, nil},
		{`sum(histogram_quantile(0.9, sum by (le, job) (rate(metric[5m]))))`, nil},
		{`sum(foo)`, nil},
	} {
		t.Run(tc.query, func(t *testing.T) {
			expr, err := promqlext.NewPromQLParser().ParseExpr(tc.query)
			require.NoError(t, err)
			original := expr.String()
			require.Equal(t, tc.want, SubsetShardingLabels(expr))
			require.Equal(t, original, expr.String())
		})
	}
}

func TestShardByLabels(t *testing.T) {
	const input = `histogram_quantile(0.9, sum by (le, job) (rate(metric[5m])))`
	expr, err := promqlext.NewPromQLParser().ParseExpr(input)
	require.NoError(t, err)
	original := expr.String()
	var captured []EmbeddedQuery
	squasher := &subsetShardingTestSquasher{capture: func(queries []EmbeddedQuery) {
		captured = queries
	}}
	_, err = ShardByLabels(t.Context(), expr, SubsetShardingLabels(expr), 4, squasher)
	require.NoError(t, err)
	require.Len(t, captured, 4)
	for index, query := range captured {
		selectors := parser.ExtractSelectors(query.Expr)
		require.Len(t, selectors, 1)
		want := sharding.ShardSelector{ShardIndex: uint64(index), ShardCount: 4, ByLabels: []string{"job"}}.Matcher()
		require.Contains(t, selectors[0], want)
		require.Contains(t, query.Expr.String(), "histogram_quantile")
	}
	// Each shard must own its selector and matcher slice independently.
	selector := captured[0].Expr.(*parser.Call).Args[1].(*parser.AggregateExpr).Expr.(*parser.Call).Args[0].(*parser.MatrixSelector).VectorSelector.(*parser.VectorSelector)
	selector.LabelMatchers = nil
	require.Contains(t, captured[1].Expr.String(), `__query_shard__="2_of_4_by_job"`)
	require.Equal(t, original, expr.String())
}

func TestShardByLabels_MultipleSelectors(t *testing.T) {
	// This expression is not recognized by SubsetShardingLabels yet, but the
	// rewrite already supports partitioning all selectors with the same labels.
	expr, err := promqlext.NewPromQLParser().ParseExpr(`sum by (job) (foo) + sum by (job) (bar)`)
	require.NoError(t, err)
	original := expr.String()
	_, err = ShardByLabels(t.Context(), expr, []string{"job"}, 2, &subsetShardingTestSquasher{capture: func(queries []EmbeddedQuery) {
		require.Len(t, queries, 2)
		for index, query := range queries {
			selectors := parser.ExtractSelectors(query.Expr)
			require.Len(t, selectors, 2)
			want := sharding.ShardSelector{ShardIndex: uint64(index), ShardCount: 2, ByLabels: []string{"job"}}.Matcher()
			for _, matchers := range selectors {
				require.Contains(t, matchers, want)
			}
		}
	}})
	require.NoError(t, err)
	require.Equal(t, original, expr.String())
}

func TestShardByLabels_Error(t *testing.T) {
	expr, err := promqlext.NewPromQLParser().ParseExpr(`histogram_quantile(0.9, sum by (le, job) (rate(metric[5m])))`)
	require.NoError(t, err)
	original := expr.String()
	squashErr := errors.New("squash failed")
	_, err = ShardByLabels(t.Context(), expr, SubsetShardingLabels(expr), 4, &subsetShardingTestSquasher{err: squashErr})
	require.ErrorIs(t, err, squashErr)
	require.Equal(t, original, expr.String())

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = ShardByLabels(ctx, expr, SubsetShardingLabels(expr), 4, &subsetShardingTestSquasher{capture: func([]EmbeddedQuery) {
		t.Fatal("squasher called after cancellation")
	}})
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, original, expr.String())
}

type subsetShardingTestSquasher struct {
	capture func([]EmbeddedQuery)
	err     error
}

func (s *subsetShardingTestSquasher) Squash(queries ...EmbeddedQuery) (parser.Expr, error) {
	if s.capture != nil {
		s.capture(queries)
	}
	return nil, s.err
}

func (*subsetShardingTestSquasher) WrapAvgResult(parser.Expr) (parser.Expr, error) {
	panic("unexpected average rewrite")
}
