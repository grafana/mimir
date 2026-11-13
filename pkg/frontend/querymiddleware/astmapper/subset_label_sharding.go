// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"slices"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

// SubsetShardingLabels returns the labels that can partition expr into independent
// queries whose results can be concatenated, or nil if the expression is unsupported.
// Every selector must use the same partition, and no operation in expr may require
// combining results from different partitions. Recognition does not modify expr.
func SubsetShardingLabels(expr parser.Expr) []string {
	return histogramQuantileShardingLabels(expr)
}

// histogramQuantileShardingLabels recognizes the initial subset-label sharding
// shape: histogram_quantile(number, sum by (le, labels...) (rate(selector[range]))).
func histogramQuantileShardingLabels(expr parser.Expr) []string {
	call, ok := expr.(*parser.Call)
	if !ok || call.Func.Name != "histogram_quantile" || len(call.Args) != 2 {
		return nil
	}
	if _, ok := call.Args[0].(*parser.NumberLiteral); !ok {
		return nil
	}

	aggregation, ok := call.Args[1].(*parser.AggregateExpr)
	if !ok || aggregation.Op != parser.SUM || aggregation.Without {
		return nil
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
		return nil
	}

	rate, ok := aggregation.Expr.(*parser.Call)
	if !ok || rate.Func.Name != "rate" || len(rate.Args) != 1 {
		return nil
	}
	matrix, ok := rate.Args[0].(*parser.MatrixSelector)
	if !ok {
		return nil
	}
	selector, ok := matrix.VectorSelector.(*parser.VectorSelector)
	if !ok {
		return nil
	}
	for _, matcher := range selector.LabelMatchers {
		if matcher.Name == sharding.ShardLabel {
			return nil
		}
	}

	// Keep every bucket of a histogram in one shard so quantiles can be computed
	// independently. The bucket label itself must not participate in the hash.
	slices.Sort(byLabels)
	return slices.Compact(byLabels)
}

// ShardByLabels clones expr for each shard, partitions all its selectors by byLabels,
// and concatenates the results using squasher. The caller must first establish that
// expr can be partitioned this way using SubsetShardingLabels.
// The input expression is not modified, including if an error is returned.
func ShardByLabels(ctx context.Context, expr parser.Expr, byLabels []string, shardCount int, squasher Squasher) (parser.Expr, error) {
	queries := make([]EmbeddedQuery, 0, shardCount)
	for shardIndex := range shardCount {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cloned, err := CloneExpr(expr)
		if err != nil {
			return nil, err
		}
		matcher := sharding.ShardSelector{
			ShardIndex: uint64(shardIndex),
			ShardCount: uint64(shardCount),
			ByLabels:   byLabels,
		}.Matcher()
		parser.Inspect(cloned, func(node parser.Node, _ []parser.Node) error {
			if selector, ok := node.(*parser.VectorSelector); ok {
				selector.LabelMatchers = append(selector.LabelMatchers, matcher)
			}
			return nil
		})
		queries = append(queries, NewEmbeddedQuery(cloned, nil))
	}

	return squasher.Squash(queries...)
}
