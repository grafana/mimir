// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/querier/querysharding"
)

// NewSharding creates a new query sharding mapper.
func NewSharding(shards int, shardedQueries prometheus.Counter) (ASTMapper, error) {
	shardSummer, err := newShardSummer(shards, vectorSquasher, shardedQueries)
	if err != nil {
		return nil, err
	}
	subtreeFolder := newSubtreeFolder()
	return NewMultiMapper(
		shardSummer,
		subtreeFolder,
	), nil
}

type squasher = func(...parser.Node) (parser.Expr, error)

type shardSummer struct {
	shards       int
	currentShard *int
	squash       squasher

	// Metrics.
	shardedQueries prometheus.Counter
}

// newShardSummer instantiates an ASTMapper which will fan out sum queries by shard
func newShardSummer(shards int, squasher squasher, shardedQueries prometheus.Counter) (ASTMapper, error) {
	if squasher == nil {
		return nil, errors.Errorf("squasher required and not passed")
	}

	return NewASTNodeMapper(&shardSummer{
		shards:         shards,
		squash:         squasher,
		currentShard:   nil,
		shardedQueries: shardedQueries,
	}), nil
}

// CopyWithCurShard clones a shardSummer with a new current shard.
func (summer *shardSummer) CopyWithCurShard(curshard int) *shardSummer {
	s := *summer
	s.currentShard = &curshard
	return &s
}

// MapNode processes the input node and checks if it can be sharded. If so, it returns
// a new node which is expected to provide the same output when executed but splitting
// the execution into multiple shards.
func (summer *shardSummer) MapNode(node parser.Node) (mapped parser.Node, finished, sharded bool, err error) {
	switch n := node.(type) {
	case *parser.AggregateExpr:
		if CanParallelize(n) {
			return summer.shardAggregate(n)
		}
		return n, false, false, nil

	case *parser.VectorSelector:
		if summer.currentShard != nil {
			mapped, err := shardVectorSelector(*summer.currentShard, summer.shards, n)
			return mapped, true, true, err
		}
		return n, true, false, nil

	case *parser.MatrixSelector:
		if summer.currentShard != nil {
			mapped, err := shardMatrixSelector(*summer.currentShard, summer.shards, n)
			return mapped, true, true, err
		}
		return n, true, false, nil

	default:
		return n, false, false, nil
	}
}

// shardAggregate attempts to shard the given aggregation expression.
func (summer *shardSummer) shardAggregate(expr *parser.AggregateExpr) (mapped parser.Node, finished, sharded bool, err error) {
	switch expr.Op {
	case parser.SUM:
		mapped, err = summer.shardSum(expr)
		if err != nil {
			return nil, false, false, err
		}
		return mapped, true, true, nil
	case parser.COUNT:
		mapped, err = summer.shardCount(expr)
		if err != nil {
			return nil, false, false, err
		}
		return mapped, true, true, nil
	case parser.MAX, parser.MIN:
		mapped, err = summer.shardMinMax(expr)
		if err != nil {
			return nil, false, false, err
		}
		return mapped, true, true, nil
	case parser.AVG:
		mapped, err = summer.shardAvg(expr)
		if err != nil {
			return nil, false, false, err
		}
		return mapped, true, true, nil
	}

	// If the aggregation operation is not shardable, we have to return the input
	// node as is.
	return expr, false, false, nil
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
func (summer *shardSummer) shardMinMax(expr *parser.AggregateExpr) (result parser.Node, err error) {
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
func (summer *shardSummer) shardAvg(expr *parser.AggregateExpr) (result parser.Node, err error) {
	// The AVG aggregation can be parallelized as per-shard SUM() divided by per-shard COUNT().
	sumExpr, err := summer.shardSum(expr)
	if err != nil {
		return nil, err
	}

	countExpr, err := summer.shardCount(expr)
	if err != nil {
		return nil, err
	}

	return &parser.BinaryExpr{
		Op:  parser.DIV,
		LHS: sumExpr,
		RHS: countExpr,
	}, nil
}

// shardAndSquashAggregateExpr returns a squashed CONCAT expression including N embedded
// queries, where N is the number of shards and each sub-query queries a different shard
// with the given "op" aggregation operation.
//
// This function also keeps track of the number of sharded queries embeedded in the returned
// expression, which is used to update metrics.
func (summer *shardSummer) shardAndSquashAggregateExpr(expr *parser.AggregateExpr, op parser.ItemType) (parser.Expr, error) {
	children := make([]parser.Node, 0, summer.shards)

	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		cloned, err := CloneNode(expr.Expr)
		if err != nil {
			return nil, err
		}

		subSummer := NewASTNodeMapper(summer.CopyWithCurShard(i))
		sharded, _, err := subSummer.Map(cloned)
		if err != nil {
			return nil, err
		}

		// Create the child expression, which runs the given aggregation operation
		// on a single shard. We need to preserve the grouping as it was
		// in the original one.
		children = append(children, &parser.AggregateExpr{
			Op:       op,
			Expr:     sharded.(parser.Expr),
			Grouping: expr.Grouping,
			Without:  expr.Without,
		})
	}

	// Update metrics.
	if summer.shardedQueries != nil {
		summer.shardedQueries.Add(float64(summer.shards))
	}

	return summer.squash(children...)
}

func shardVectorSelector(curshard, shards int, selector *parser.VectorSelector) (parser.Node, error) {
	shardMatcher, err := labels.NewMatcher(labels.MatchEqual, querysharding.ShardLabel, fmt.Sprintf(querysharding.ShardLabelFmt, curshard, shards))
	if err != nil {
		return nil, err
	}

	return &parser.VectorSelector{
		Name:   selector.Name,
		Offset: selector.Offset,
		LabelMatchers: append(
			[]*labels.Matcher{shardMatcher},
			selector.LabelMatchers...,
		),
	}, nil
}

func shardMatrixSelector(curshard, shards int, selector *parser.MatrixSelector) (parser.Node, error) {
	shardMatcher, err := labels.NewMatcher(labels.MatchEqual, querysharding.ShardLabel, fmt.Sprintf(querysharding.ShardLabelFmt, curshard, shards))
	if err != nil {
		return nil, err
	}

	if vs, ok := selector.VectorSelector.(*parser.VectorSelector); ok {
		return &parser.MatrixSelector{
			VectorSelector: &parser.VectorSelector{
				Name:   vs.Name,
				Offset: vs.Offset,
				LabelMatchers: append(
					[]*labels.Matcher{shardMatcher},
					vs.LabelMatchers...,
				),
				PosRange: vs.PosRange,
			},
			Range:  selector.Range,
			EndPos: selector.EndPos,
		}, nil
	}

	return nil, fmt.Errorf("invalid selector type: %T", selector.VectorSelector)
}
