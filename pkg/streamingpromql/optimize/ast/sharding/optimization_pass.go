// SPDX-License-Identifier: AGPL-3.0-only

package sharding

import (
	"context"
	"errors"
	"slices"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/tenant"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"go.opentelemetry.io/otel"

	apierror "github.com/grafana/mimir/pkg/api/error"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware"
	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	storagesharding "github.com/grafana/mimir/pkg/storage/sharding"
	"github.com/grafana/mimir/pkg/streamingpromql/optimize"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
	"github.com/grafana/mimir/pkg/streamingpromql/requestoptions"
	"github.com/grafana/mimir/pkg/streamingpromql/types"
)

var tracer = otel.Tracer("pkg/streamingpromql/optimize/ast/sharding")

const experimentalSubsetShardingHeader = "X-Mimir-Subset-Label-Sharding"

type OptimizationPass struct {
	sharder   *querymiddleware.QuerySharder
	estimator CardinalityEstimator
}

func NewOptimizationPass(limits querymiddleware.ShardingLimits, maxSeriesPerShard uint64, estimator CardinalityEstimator, reg prometheus.Registerer, logger log.Logger) optimize.ASTOptimizationPass {
	return &OptimizationPass{
		sharder:   querymiddleware.NewQuerySharder(ConcatSquasher, limits, maxSeriesPerShard, reg, logger),
		estimator: estimator,
	}
}

func (o *OptimizationPass) Name() string {
	return "Sharding"
}

func (o *OptimizationPass) Apply(ctx context.Context, expr parser.Expr, params *planning.QueryParameters) (parser.Expr, error) {
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

	// When the query has been rewritten to spin off subqueries, each __vector_evaluation_root__ or __scalar_evaluation_root__ marks a
	// separate query that must be sharded independently. Some of these subtrees may be shardable and
	// others not; that's fine, just like for top-level range queries today, where the shardable parts
	// are sharded and the rest is left unchanged.
	if roots := collectEvaluationRoots(expr, params.TimeRange); len(roots) > 0 {
		for _, root := range roots {
			shardedChild, err := o.shard(ctx, tenantIDs, params.OriginalExpression, root.call.Args[0], root.timeRange, params.LookbackDelta, options)
			if err != nil {
				return nil, err
			}

			root.call.Args[0] = shardedChild
		}

		return expr, nil
	}

	return o.shard(ctx, tenantIDs, params.OriginalExpression, expr, params.TimeRange, params.LookbackDelta, options)
}

// shard shards expr, returning the sharded expression, or expr unchanged if it cannot be sharded.
func (o *OptimizationPass) shard(ctx context.Context, tenantIDs []string, originalExpression string, expr parser.Expr, timeRange types.QueryTimeRange, lookbackDelta time.Duration, options requestoptions.Options) (parser.Expr, error) {
	requestedShardCount := int(options.TotalShards)
	totalQueries := int32(1)
	var seriesCount *querymiddleware.EstimatedSeriesCount

	if o.estimator != nil {
		var err error
		seriesCount, err = o.estimator.EstimateSeriesCount(ctx, originalExpression, expr, timeRange, lookbackDelta)

		if err != nil {
			return nil, err
		}
	}

	if hints := querymiddleware.RequestHintsFromContext(ctx); hints != nil {
		if hints.TotalQueries > 0 {
			// If splitting and caching inside MQE is enabled, this will be 1, and that is OK:
			// the impact of this is that the -query-frontend.query-sharding-max-sharded-queries limit will apply per time-split
			// interval and evaluation root, rather than to the entire time range (or entire spun-off subquery).
			//
			// (In the middleware implementation, the limit considers all shards across all time-split intervals, except if
			// subquery spin-off applies, in which case it applies per evaluation root.)
			totalQueries = hints.TotalQueries
		}
	}

	if strings.EqualFold(options.PropagatedHeaders.Get(experimentalSubsetShardingHeader), "true") {
		if _, byLabels, ok := classicHistogramQuantileSelector(expr); ok {
			shardCount, err := o.sharder.ShardCount(ctx, tenantIDs, expr, requestedShardCount, seriesCount, totalQueries)
			if err != nil {
				return nil, err
			}
			if shardCount <= 1 {
				return expr, nil
			}
			sharded, err := shardHistogramQuantileByLabels(expr, byLabels, shardCount)
			if err != nil {
				return nil, err
			}
			o.sharder.RecordSharding(ctx, shardCount)
			return sharded, nil
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

func shardHistogramQuantileByLabels(expr parser.Expr, byLabels []string, shardCount int) (parser.Expr, error) {
	args := make([]parser.Expr, 0, shardCount)
	for shardIndex := range shardCount {
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
		args = append(args, cloned)
	}

	return &parser.Call{Func: ConcatFunction, Args: args}, nil
}

// collectEvaluationRoots returns the __vector_evaluation_root__ and __scalar_evaluation_root__ marker function calls in expr.
//
// Markers are never nested inside one another (the subquery spin-off mapper does not recurse into a
// query once it has spun it off), so this does not descend into a marker once found.
func collectEvaluationRoots(expr parser.Expr, rootTimeRange types.QueryTimeRange) []evaluationRoot {
	var roots []evaluationRoot

	var visit func(node parser.Node, timeRange types.QueryTimeRange)
	visit = func(node parser.Node, timeRange types.QueryTimeRange) {
		if call, ok := node.(*parser.Call); ok && core.IsEvaluationRootFunctionCall(call) {
			roots = append(roots, evaluationRoot{call: call, timeRange: timeRange})
			return
		}

		childrenTimeRange := timeRange
		if subquery, isSubquery := node.(*parser.SubqueryExpr); isSubquery {
			childrenTimeRange = core.SubqueryChildrenTimeRange(timeRange, subquery.Range, subquery.Step, subquery.OriginalOffset, core.TimeFromTimestamp(subquery.Timestamp))
		}

		for _, child := range parser.Children(node) {
			visit(child, childrenTimeRange)
		}
	}

	visit(expr, rootTimeRange)

	return roots
}

type evaluationRoot struct {
	call      *parser.Call
	timeRange types.QueryTimeRange
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
