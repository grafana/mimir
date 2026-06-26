// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"errors"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

type OptimizationPass struct {
	sharder *querymiddleware.QuerySharder
}

func NewOptimizationPass(limits querymiddleware.ShardingLimits, maxSeriesPerShard uint64, reg prometheus.Registerer, logger log.Logger) optimize.ASTOptimizationPass {
	return &OptimizationPass{
		sharder: querymiddleware.NewQuerySharder(ConcatSquasher, limits, maxSeriesPerShard, reg, logger),
	}
}

func (o *OptimizationPass) Name() string {
	return "Sharding"
}

func (o *OptimizationPass) Apply(ctx context.Context, expr parser.Expr, _ types.QueryTimeRange) (parser.Expr, error) {
	if containsSpunOffSubquery(expr) {
		return expr, nil
	}

	options := requestoptions.OptionsFromContext(ctx)
	if options.ShardingDisabled {
		return expr, nil
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

	// When the query has been rewritten to spin off subqueries, each __evaluation_root__ marks a
	// separate query that must be sharded independently. Some of these subtrees may be shardable and
	// others not; that's fine, just like for top-level range queries today, where the shardable parts
	// are sharded and the rest is left unchanged.
	if roots := collectEvaluationRoots(expr); len(roots) > 0 {
		for _, root := range roots {
			shardedChild, err := o.shard(ctx, tenantIDs, root.Args[0], options)
			if err != nil {
				return nil, err
			}

			root.Args[0] = shardedChild
		}

		return expr, nil
	}

	return o.shard(ctx, tenantIDs, expr, options)
}

// shard shards expr, returning the sharded expression, or expr unchanged if it cannot be sharded.
func (o *OptimizationPass) shard(ctx context.Context, tenantIDs []string, expr parser.Expr, options requestoptions.Options) (parser.Expr, error) {
	requestedShardCount := int(options.TotalShards)
	totalQueries := int32(1)
	var seriesCount *querymiddleware.EstimatedSeriesCount

	if hints := querymiddleware.RequestHintsFromContext(ctx); hints != nil {
		seriesCount = hints.GetCardinalityEstimate()

		if hints.TotalQueries > 0 {
			totalQueries = hints.TotalQueries
		}
	}

	shardedExpr, err := o.sharder.Shard(ctx, tenantIDs, expr, requestedShardCount, seriesCount, totalQueries)
	if err != nil {
		return nil, err
	}

	if shardedExpr == nil {
		return expr, nil
	}

	return shardedExpr, nil
}

// collectEvaluationRoots returns the __evaluation_root__ marker function calls in expr.
//
// Markers are never nested inside one another (the subquery spin-off mapper does not recurse into a
// query once it has spun it off), so this does not descend into a marker once found.
func collectEvaluationRoots(expr parser.Expr) []*parser.Call {
	var roots []*parser.Call

	var visit func(node parser.Node)
	visit = func(node parser.Node) {
		if call, ok := node.(*parser.Call); ok && core.IsEvaluationRootFunctionCall(call) {
			roots = append(roots, call)
			return
		}

		for _, child := range parser.Children(node) {
			visit(child)
		}
	}

	visit(expr)

	return roots
}

var ConcatSquasher astmapper.Squasher = &concatSquasher{}

type concatSquasher struct{}

func (c *concatSquasher) Squash(exprs ...astmapper.EmbeddedQuery) (parser.Expr, error) {
	args := make([]parser.Expr, 0, len(exprs))

	for _, expr := range exprs {
		if len(expr.Params) > 0 {
			return nil, errors.New("concatSquasher does not support squashing embedded queries with params")
		}

		args = append(args, expr.Expr)
	}

	return &parser.Call{
		Func: ConcatFunction,
		Args: args,
	}, nil
}

func (c *concatSquasher) WrapAvgResult(expr parser.Expr) (parser.Expr, error) {
	return &parser.Call{
		Func: AvgFunction,
		Args: []parser.Expr{expr},
	}, nil
}

func containsSpunOffSubquery(expr parser.Expr) bool {
	return astmapper.AnyNode(expr, func(node parser.Node) bool {
		ms, isMatrixSelector := node.(*parser.MatrixSelector)
		if !isMatrixSelector {
			return false
		}

		vs, isVectorSelector := ms.VectorSelector.(*parser.VectorSelector)
		if !isVectorSelector {
			return false
		}

		for _, matcher := range vs.LabelMatchers {
			if matcher.Name == model.MetricNameLabel && matcher.Type == labels.MatchEqual && matcher.Value == astmapper.SubqueryMetricName {
				return true
			}
		}

		return false
	})
}
