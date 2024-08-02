// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

// NewSharding creates a new query sharding mapper.
func NewSharding(ctx context.Context, shards int, logger log.Logger, stats *MapperStats) (ASTMapper, error) {
	shardSummer, err := newShardSummer(ctx, shards, vectorSquasher, logger, stats, nil)
	if err != nil {
		return nil, err
	}
	subtreeFolder := newSubtreeFolder()
	return NewMultiMapper(
		shardSummer,
		subtreeFolder,
	), nil
}

type squasher = func(...parser.Expr) (parser.Expr, error)

type shardSummer struct {
	ctx context.Context

	shards       int
	currentShard *int
	squash       squasher
	logger       log.Logger
	stats        *MapperStats

	splitLabel string
	clusters   []string

	canShardAllVectorSelectorsCache map[string]bool
}

// newShardSummer instantiates an ASTMapper which will fan out sum queries by shard. If clusters is defined,
// the mapper will be used to rewrite the queries for cross-cluster query federation. Otherwise, it will
// rewrite the queries for query sharding.
func newShardSummer(ctx context.Context, shards int, squasher squasher, logger log.Logger, stats *MapperStats, clusters []string) (ASTMapper, error) {
	if squasher == nil {
		return nil, errors.Errorf("squasher required and not passed")
	}

	var splitLabel string
	if len(clusters) > 0 {
		shards = len(clusters)
		splitLabel = sharding.ClusterLabel
	} else {
		splitLabel = sharding.ShardLabel
	}

	return NewASTExprMapper(&shardSummer{
		ctx: ctx,

		shards:       shards,
		squash:       squasher,
		currentShard: nil,
		logger:       logger,
		stats:        stats,

		splitLabel: splitLabel,
		clusters:   clusters,

		canShardAllVectorSelectorsCache: make(map[string]bool),
	}), nil
}

// Clone returns a clone of shardSummer with stats and current shard index reset to default.
func (summer *shardSummer) Clone() *shardSummer {
	s := *summer
	s.stats = NewMapperStats()
	s.currentShard = nil
	return &s
}

// CopyWithCurShard clones a shardSummer with a new current shard.
func (summer *shardSummer) CopyWithCurShard(curshard int) *shardSummer {
	s := *summer
	s.currentShard = &curshard
	return &s
}

// MapExpr processes the input expr and checks if it can be sharded. If so, it returns
// a new expr which is expected to provide the same output when executed but splitting
// the execution into multiple shards.
func (summer *shardSummer) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := summer.ctx.Err(); err != nil {
		return nil, false, err
	}

	switch e := expr.(type) {
	case *parser.AggregateExpr:
		if summer.currentShard != nil {
			return e, false, nil
		}
		if CanParallelize(e, summer.logger) {
			return summer.shardAggregate(e)
		}
		return e, false, nil

	case *parser.VectorSelector:
		if summer.currentShard != nil {
			mapped, err := summer.shardVectorSelector(e)
			return mapped, true, err
		}
		return e, true, nil

	case *parser.Call:
		// only shard the most outer function call.
		if summer.currentShard == nil {
			// Only shards Subqueries, they are parallelizable if they are parallelizable themselves
			// and they don't contain aggregations over series in children exprs.
			if isSubqueryCall(e) {
				if containsAggregateExpr(e) {
					return e, true, nil
				}
				if !CanParallelize(e, summer.logger) {
					return e, true, nil
				}
				return summer.shardAndSquashFuncCall(e)
			}
			return e, false, nil
		}
		return e, false, nil

	case *parser.BinaryExpr:
		if summer.currentShard != nil {
			return e, false, nil
		}

		// If we can parallelize the whole binary operation then just do it.
		if CanParallelize(e, summer.logger) {
			return summer.shardBinOp(e)
		}

		// We can't parallelize the whole binary operation but we could still parallelize
		// at least one of the two legs. However, if we parallelize only one of the two legs
		// then fetching results from the other (non parallelized) leg could be very expensive
		// because it could result in very high cardinality results to fetch from querier and
		// process in the query-frontend. Since we can't estimate the cardinality, we prefer
		// to be pessimistic and not parallelize at all the two legs unless we're able to
		// parallelize all vector selectors in the legs.
		canShardAllVectorSelectors := func(expr parser.Expr) (can bool, err error) {
			query := expr.String()
			// We need to cache the results of this function to avoid processing it again and again
			// in queries like `a or b or c or d`, which would lead to exponential processing time.
			if can, ok := summer.canShardAllVectorSelectorsCache[query]; ok {
				return can, nil
			}
			defer func() {
				if err == nil {
					summer.canShardAllVectorSelectorsCache[query] = can
				}
			}()

			// Clone the expression cause the mapper can modify it in-place.
			clonedExpr, err := parser.ParseExpr(query)
			if err != nil {
				return false, err
			}

			c := summer.Clone()
			c.shards = 1
			m := NewASTExprMapper(c)

			mappedExpr, err := m.Map(clonedExpr)
			if err != nil {
				return false, err
			}

			// The mapped expression could have been rewritten and the number of vector selectors
			// be changed compared to the original expression, so we should compare with that.
			return c.stats.GetShardedQueries() == countVectorSelectors(mappedExpr), nil
		}

		canLHS, err := canShardAllVectorSelectors(e.LHS)
		if err != nil {
			return e, true, err
		}
		if !canLHS {
			return e, true, nil
		}
		canRHS, err := canShardAllVectorSelectors(e.RHS)
		return e, !canRHS, err

	case *parser.SubqueryExpr:
		// If the mapped expr is part of the sharded query, then it means we already checked whether it was
		// safe to shard so we must keep it as is.
		if summer.currentShard != nil {
			return e, false, nil
		}

		// If the mapper hits a subquery expression, it means it's a subquery whose sharding is currently
		// not supported, so we terminate the mapping here. If the subquery was parallelizable we didn't reach
		// this point, because the subquery was part of a parent shardable expr.
		return e, true, nil

	default:
		return e, false, nil
	}
}

// shardAndSquashFuncCall shards the given function call by cloning it and adding the shard label to the most outer matrix/vector selector.
func (summer *shardSummer) shardAndSquashFuncCall(expr *parser.Call) (mapped parser.Expr, finished bool, err error) {
	/*
		parallelizing a func call (and subqueries) is representable naively as

		concat(
			min_over_time(
							rate(metric_counter{__query_shard__="0_of_2"}[1m])
						[5m:1m]
					),
			min_over_time(
							rate(metric_counter{__query_shard__="1_of_2"}[1m])
						[5m:1m]
					),
		)

		To find the most outer matrix/vector selector, we need to traverse the AST by using each function arguments.
	*/

	children := make([]parser.Expr, 0, summer.shards)

	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		cloned, err := cloneExpr(expr)
		if err != nil {
			return nil, true, err
		}
		var (
			clonedCall = cloned.(*parser.Call)
			subSummer  = NewASTExprMapper(summer.CopyWithCurShard(i))
		)

		for i, arg := range clonedCall.Args {
			switch typedArg := arg.(type) {
			case *parser.SubqueryExpr:
				subExprMapped, err := subSummer.Map(typedArg.Expr)
				if err != nil {
					return nil, true, err
				}
				typedArg.Expr = subExprMapped
				clonedCall.Args[i] = arg
			default:
				mapped, err := subSummer.Map(typedArg)
				if err != nil {
					return nil, true, err
				}
				clonedCall.Args[i] = mapped
			}
		}

		children = append(children, clonedCall)
	}

	// Update stats.
	summer.stats.AddShardedQueries(summer.shards)
	squashed, err := summer.squash(children...)
	if err != nil {
		return nil, true, err
	}
	return squashed, true, nil
}

// shardAggregate attempts to shard the given aggregation expression.
func (summer *shardSummer) shardAggregate(expr *parser.AggregateExpr) (mapped parser.Expr, finished bool, err error) {
	switch expr.Op {
	case parser.GROUP:
		mapped, err = summer.shardGroup(expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.SUM:
		mapped, err = summer.shardSum(expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.COUNT:
		mapped, err = summer.shardCount(expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.MAX, parser.MIN:
		mapped, err = summer.shardMinMax(expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.AVG:
		mapped, err = summer.shardAvg(expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	}

	// If the aggregation operation is not shardable, we have to return the input
	// expr as is.
	return expr, false, nil
}

// shardGroup attempts to shard the given GROUP aggregation expression.
func (summer *shardSummer) shardGroup(expr *parser.AggregateExpr) (result *parser.AggregateExpr, err error) {
	/*
		parallelizing a group using without(foo) is representable naively as
		group without(foo) (
		  group without(foo) (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
		  group without(foo) (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
		)

		parallelizing a group using by(foo) is representable as
		group by(foo) (
		  group by(foo) (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
		  group by(foo) (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
		)

		parallelizing a non-parameterized group is representable as
		group (
		  group (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
		  group (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
		)
	*/

	// Create a GROUP sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(expr, parser.GROUP)
	if err != nil {
		return nil, err
	}

	// Create the parent expression. We need to preserve the grouping as it was in the original one.
	return &parser.AggregateExpr{
		Op:       parser.GROUP,
		Expr:     sharded,
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
	}, nil
}

// shardSum attempts to shard the given SUM aggregation expression.
func (summer *shardSummer) shardSum(expr *parser.AggregateExpr) (result *parser.AggregateExpr, err error) {
	/*
		parallelizing a sum using without(foo) is representable naively as
		sum without(foo) (
		  sum without(foo) (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
		  sum without(foo) (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
		)

		parallelizing a sum using by(foo) is representable as
		sum by(foo) (
		  sum by(foo) (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
		  sum by(foo) (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
		)

		parallelizing a non-parameterized sum is representable as
		sum(
		  sum (rate(bar1{__query_shard__="0_of_2",baz="blip"}[1m])) or
		  sum (rate(bar1{__query_shard__="1_of_2",baz="blip"}[1m]))
		)
	*/

	// Create a SUM sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(expr, parser.SUM)
	if err != nil {
		return nil, err
	}

	// Create the parent expression. We need to preserve the grouping as it was in the original one.
	return &parser.AggregateExpr{
		Op:       parser.SUM,
		Expr:     sharded,
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
	}, nil
}

// shardCount attempts to shard the given COUNT aggregation expression.
func (summer *shardSummer) shardCount(expr *parser.AggregateExpr) (result *parser.AggregateExpr, err error) {
	// The COUNT aggregation can be parallelized as the SUM of per-shard COUNT.
	// Create a COUNT sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(expr, parser.COUNT)
	if err != nil {
		return nil, err
	}

	return &parser.AggregateExpr{
		Op:       parser.SUM,
		Expr:     sharded,
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
	}, nil
}

// shardMinMax attempts to shard the given MIN/MAX aggregation expression.
func (summer *shardSummer) shardMinMax(expr *parser.AggregateExpr) (result parser.Expr, err error) {
	// We expect the given aggregation is either a MIN or MAX.
	if expr.Op != parser.MIN && expr.Op != parser.MAX {
		return nil, errors.Errorf("expected MIN or MAX aggregation while got %s", expr.Op.String())
	}

	// The MIN/MAX aggregation can be parallelized as the MIN/MAX of per-shard MIN/MAX.
	// Create a MIN/MAX sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(expr, expr.Op)
	if err != nil {
		return nil, err
	}

	return &parser.AggregateExpr{
		Op:       expr.Op,
		Expr:     sharded,
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
	}, nil
}

// shardAvg attempts to shard the given AVG aggregation expression.
func (summer *shardSummer) shardAvg(expr *parser.AggregateExpr) (result parser.Expr, err error) {
	// The AVG aggregation can be parallelized as per-shard SUM() divided by per-shard COUNT().
	sumExpr, err := summer.shardSum(expr)
	if err != nil {
		return nil, err
	}

	countExpr, err := summer.shardCount(expr)
	if err != nil {
		return nil, err
	}

	return &parser.ParenExpr{
		Expr: &parser.BinaryExpr{
			Op:  parser.DIV,
			LHS: sumExpr,
			RHS: countExpr,
		},
	}, nil
}

// shardAndSquashAggregateExpr returns a squashed CONCAT expression including N embedded
// queries, where N is the number of shards and each sub-query queries a different shard
// with the given "op" aggregation operation.
func (summer *shardSummer) shardAndSquashAggregateExpr(expr *parser.AggregateExpr, op parser.ItemType) (parser.Expr, error) {
	children := make([]parser.Expr, 0, summer.shards)

	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		sharded, err := cloneAndMap(NewASTExprMapper(summer.CopyWithCurShard(i)), expr.Expr)
		if err != nil {
			return nil, err
		}

		// Create the child expression, which runs the given aggregation operation
		// on a single shard. We need to preserve the grouping as it was
		// in the original one.
		children = append(children, &parser.AggregateExpr{
			Op:       op,
			Expr:     sharded,
			Grouping: expr.Grouping,
			Without:  expr.Without,
		})
	}

	// Update stats.
	summer.stats.AddShardedQueries(summer.shards)

	return summer.squash(children...)
}

// shardBinOp attempts to shard the given binary operation expression.
func (summer *shardSummer) shardBinOp(expr *parser.BinaryExpr) (mapped parser.Expr, finished bool, err error) {
	switch expr.Op {
	case parser.GTR,
		parser.GTE,
		parser.LSS,
		parser.LTE:
		mapped, err = summer.shardAndSquashBinOp(expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	default:
		return expr, false, nil
	}
}

// shardAndSquashBinOp returns a squashed CONCAT expression including N embedded
// queries, where N is the number of shards and each sub-query queries a different shard
// with the same binary operation.
func (summer *shardSummer) shardAndSquashBinOp(expr *parser.BinaryExpr) (parser.Expr, error) {
	if expr.VectorMatching != nil {
		// We shouldn't ever reach this point with a vector matching binary expression,
		// but it's better to check twice than completely mess it up with the results.
		return nil, fmt.Errorf("tried to shard a bin op with vector matching: %s", expr)
	}

	children := make([]parser.Expr, 0, summer.shards)
	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		shardedLHS, err := cloneAndMap(NewASTExprMapper(summer.CopyWithCurShard(i)), expr.LHS)
		if err != nil {
			return nil, err
		}
		shardedRHS, err := cloneAndMap(NewASTExprMapper(summer.CopyWithCurShard(i)), expr.RHS)
		if err != nil {
			return nil, err
		}

		children = append(children, &parser.BinaryExpr{
			LHS:        shardedLHS,
			Op:         expr.Op,
			RHS:        shardedRHS,
			ReturnBool: expr.ReturnBool,
		})
	}

	// Update stats.
	summer.stats.AddShardedQueries(summer.shards)

	return summer.squash(children...)
}

func (summer *shardSummer) shardVectorSelector(selector *parser.VectorSelector) (parser.Expr, error) {
	var splitLabelValue string
	if len(summer.clusters) > 0 {
		splitLabelValue = summer.clusters[*summer.currentShard]
	} else {
		splitLabelValue = sharding.ShardSelector{ShardIndex: uint64(*summer.currentShard), ShardCount: uint64(summer.shards)}.LabelValue()
	}
	shardMatcher, err := labels.NewMatcher(labels.MatchEqual, summer.splitLabel, splitLabelValue)
	if err != nil {
		return nil, err
	}
	return &parser.VectorSelector{
		Name:                 selector.Name,
		Offset:               selector.Offset,
		OriginalOffset:       selector.OriginalOffset,
		Timestamp:            copyTimestamp(selector.Timestamp),
		SkipHistogramBuckets: selector.SkipHistogramBuckets,
		StartOrEnd:           selector.StartOrEnd,
		LabelMatchers: append(
			[]*labels.Matcher{shardMatcher},
			selector.LabelMatchers...,
		),
	}, nil
}

// isSubqueryCall returns true if the given function call expression is a subquery,
// or a subquery wrapped by parenthesis.
func isSubqueryCall(n *parser.Call) bool {
	for _, arg := range n.Args {
		if ok := isSubqueryCallVisitFn(arg); ok {
			return true
		}
	}

	return false
}

func isSubqueryCallVisitFn(expr parser.Expr) bool {
	switch e := expr.(type) {
	case *parser.ParenExpr:
		return isSubqueryCallVisitFn(e.Expr)
	case *parser.SubqueryExpr:
		return true
	default:
		return false
	}
}

func copyTimestamp(original *int64) *int64 {
	if original == nil {
		return nil
	}
	ts := *original
	return &ts
}
