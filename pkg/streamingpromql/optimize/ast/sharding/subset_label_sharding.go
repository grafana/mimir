// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"errors"
	"slices"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	storagesharding "github.com/grafana/mimir/pkg/storage/sharding"
)

// planSubsetLabelSharding selects a supported shape before its shard budget is calculated.
func planSubsetLabelSharding(expr parser.Expr, squasher astmapper.Squasher) (querymiddleware.QueryShardingPlan, bool) {
	if _, byLabels, ok := classicHistogramQuantileSelector(expr); ok {
		return querymiddleware.QueryShardingPlan{
			ShardedSelectors: 1,
			Rewrite: func(ctx context.Context, shardCount int) (parser.Expr, int, error) {
				sharded, err := shardHistogramQuantileByLabels(ctx, expr, byLabels, shardCount, squasher)
				return sharded, shardCount, err
			},
		}, true
	}

	return querymiddleware.QueryShardingPlan{}, false
}

func classicHistogramQuantileSelector(expr parser.Expr) (*parser.VectorSelector, []string, bool) {
	call, ok := expr.(*parser.Call)
	if !ok || call.Func.Name != "histogram_quantile" || len(call.Args) != 2 {
		return nil, nil, false
	}
	if _, ok := call.Args[0].(*parser.NumberLiteral); !ok {
		return nil, nil, false
	}

	aggregation, ok := call.Args[1].(*parser.AggregateExpr)
	if !ok || aggregation.Op != parser.SUM || aggregation.Without {
		return nil, nil, false
	}

	byLabels := make([]string, 0, len(aggregation.Grouping))
	hasBucketLabel := false
	for _, name := range aggregation.Grouping {
		if name == model.BucketLabel {
			hasBucketLabel = true
		} else {
			byLabels = append(byLabels, name)
		}
	}
	if !hasBucketLabel || len(byLabels) == 0 {
		return nil, nil, false
	}
	slices.Sort(byLabels)
	byLabels = slices.Compact(byLabels)

	rate, ok := aggregation.Expr.(*parser.Call)
	if !ok || rate.Func.Name != "rate" || len(rate.Args) != 1 {
		return nil, nil, false
	}
	matrix, ok := rate.Args[0].(*parser.MatrixSelector)
	if !ok {
		return nil, nil, false
	}
	selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
	if !ok {
		return nil, nil, false
	}
	for _, matcher := range selector.LabelMatchers {
		if matcher.Name == storagesharding.ShardLabel {
			return nil, nil, false
		}
	}

	return selector, byLabels, true
}

func shardHistogramQuantileByLabels(ctx context.Context, expr parser.Expr, byLabels []string, shardCount int, squasher astmapper.Squasher) (parser.Expr, error) {
	args := make([]astmapper.EmbeddedQuery, 0, shardCount)
	for shardIndex := range shardCount {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cloned, err := astmapper.CloneExpr(expr)
		if err != nil {
			return nil, err
		}
		selector, _, ok := classicHistogramQuantileSelector(cloned)
		if !ok {
			return nil, errors.New("cloned histogram_quantile expression no longer matches subset sharding shape")
		}
		selector.LabelMatchers = append(selector.LabelMatchers, storagesharding.ShardSelector{
			ShardIndex: uint64(shardIndex),
			ShardCount: uint64(shardCount),
			ByLabels:   byLabels,
		}.Matcher())
		args = append(args, astmapper.NewEmbeddedQuery(cloned, nil))
	}

	return squasher.Squash(args...)
}
