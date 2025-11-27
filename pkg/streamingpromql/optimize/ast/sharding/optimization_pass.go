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

func (o *OptimizationPass) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	if containsSpunOffSubquery(expr) {
		return expr, nil
	}

	options := querymiddleware.RequestOptionsFromContext(ctx)
	if options.ShardingDisabled {
		return expr, nil
	}

	tenantIDs, err := tenant.TenantIDs(ctx)
	if err != nil {
		return nil, apierror.New(apierror.TypeBadData, err.Error())
	}

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
