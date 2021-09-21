// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/querier/querysharding"
)

// NewSharding creates a new query sharding mapper.
func NewSharding(shards int) (ASTMapper, error) {
	shardSummer, err := newShardSummer(shards, vectorSquasher)
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
}

// newShardSummer instantiates an ASTMapper which will fan out sum queries by shard
func newShardSummer(shards int, squasher squasher) (ASTMapper, error) {
	if squasher == nil {
		return nil, errors.Errorf("squasher required and not passed")
	}

	return NewASTNodeMapper(&shardSummer{
		shards:       shards,
		squash:       squasher,
		currentShard: nil,
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
func (summer *shardSummer) MapNode(node parser.Node, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	switch n := node.(type) {
	case *parser.AggregateExpr:
		if summer.currentShard != nil {
			return n, false, nil
		}
		if CanParallelize(n) {
			return summer.shardAggregate(n, stats)
		}
		return n, false, nil

	case *parser.VectorSelector:
		if summer.currentShard != nil {
			mapped, err := shardVectorSelector(*summer.currentShard, summer.shards, n)
			return mapped, true, err
		}
		return n, true, nil

	case *parser.MatrixSelector:
		if summer.currentShard != nil {
			mapped, err := shardMatrixSelector(*summer.currentShard, summer.shards, n)
			return mapped, true, err
		}
		return n, true, nil
	case *parser.Call:
		// only shard the most outer function call.
		if summer.currentShard == nil {
			// Only shards Subqueries, they are parallelizable if they are parallelizable themselves
			// and they don't contain aggregations over series in children nodes.
			if isSubquery(n) {
				if containsAggregateExpr(n) {
					return n, true, nil
				}
				if !CanParallelize(n) {
					return n, true, nil
				}
				return summer.shardAndSquashFuncCall(n, stats)
			}
			return n, false, nil
		}
		return n, false, nil
	default:
		return n, false, nil
	}
}

// shardAndSquashFuncCall shards the given function call by cloning it and adding the shard label to the most outer matrix/vector selector.
func (summer *shardSummer) shardAndSquashFuncCall(node *parser.Call, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
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

	children := make([]parser.Node, 0, summer.shards)

	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		cloned, err := CloneNode(node)
		if err != nil {
			return nil, true, err
		}
		var (
			mapped     parser.Node
			clonedCall = cloned.(*parser.Call)
			subSummer  = NewASTNodeMapper(summer.CopyWithCurShard(i))
		)

		for i, arg := range clonedCall.Args {
			switch typedArg := arg.(type) {
			case *parser.SubqueryExpr:
				subExprMapped, err := subSummer.Map(typedArg.Expr, nil)
				if err != nil {
					return nil, true, err
				}
				typedArg.Expr = subExprMapped.(parser.Expr)
				mapped = arg
			default:
				mapped, err = subSummer.Map(typedArg, stats)
				if err != nil {
					return nil, true, err
				}
			}
			clonedCall.Args[i] = mapped.(parser.Expr)
		}

		children = append(children, clonedCall)
	}

	// Update stats.
	stats.AddShardedQueries(summer.shards)
	squashed, err := summer.squash(children...)
	if err != nil {
		return nil, true, err
	}
	return squashed, true, nil
}

// shardAggregate attempts to shard the given aggregation expression.
func (summer *shardSummer) shardAggregate(expr *parser.AggregateExpr, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	switch expr.Op {
	case parser.SUM:
		mapped, err = summer.shardSum(expr, stats)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.COUNT:
		mapped, err = summer.shardCount(expr, stats)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.MAX, parser.MIN:
		mapped, err = summer.shardMinMax(expr, stats)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.AVG:
		mapped, err = summer.shardAvg(expr, stats)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	}

	// If the aggregation operation is not shardable, we have to return the input
	// node as is.
	return expr, false, nil
}

// shardSum attempts to shard the given SUM aggregation expression.
func (summer *shardSummer) shardSum(expr *parser.AggregateExpr, stats *MapperStats) (result *parser.AggregateExpr, err error) {
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
	sharded, err := summer.shardAndSquashAggregateExpr(expr, parser.SUM, stats)
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
func (summer *shardSummer) shardCount(expr *parser.AggregateExpr, stats *MapperStats) (result *parser.AggregateExpr, err error) {
	// The COUNT aggregation can be parallelized as the SUM of per-shard COUNT.
	// Create a COUNT sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(expr, parser.COUNT, stats)
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
func (summer *shardSummer) shardMinMax(expr *parser.AggregateExpr, stats *MapperStats) (result parser.Node, err error) {
	// We expect the given aggregation is either a MIN or MAX.
	if expr.Op != parser.MIN && expr.Op != parser.MAX {
		return nil, errors.Errorf("expected MIN or MAX aggregation while got %s", expr.Op.String())
	}

	// The MIN/MAX aggregation can be parallelized as the MIN/MAX of per-shard MIN/MAX.
	// Create a MIN/MAX sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(expr, expr.Op, stats)
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
func (summer *shardSummer) shardAvg(expr *parser.AggregateExpr, stats *MapperStats) (result parser.Node, err error) {
	// The AVG aggregation can be parallelized as per-shard SUM() divided by per-shard COUNT().
	sumExpr, err := summer.shardSum(expr, stats)
	if err != nil {
		return nil, err
	}

	countExpr, err := summer.shardCount(expr, stats)
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
func (summer *shardSummer) shardAndSquashAggregateExpr(expr *parser.AggregateExpr, op parser.ItemType, stats *MapperStats) (parser.Expr, error) {
	children := make([]parser.Node, 0, summer.shards)

	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		cloned, err := CloneNode(expr.Expr)
		if err != nil {
			return nil, err
		}

		subSummer := NewASTNodeMapper(summer.CopyWithCurShard(i))
		sharded, err := subSummer.Map(cloned, stats)
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

	// Update stats.
	stats.AddShardedQueries(summer.shards)

	return summer.squash(children...)
}

func shardVectorSelector(curshard, shards int, selector *parser.VectorSelector) (parser.Node, error) {
	shardMatcher, err := querysharding.ShardSelector{ShardIndex: uint64(curshard), ShardCount: uint64(shards)}.LabelMatcher()
	if err != nil {
		return nil, err
	}
	return &parser.VectorSelector{
		Name:           selector.Name,
		Offset:         selector.Offset,
		OriginalOffset: selector.OriginalOffset,
		Timestamp:      copyTimestamp(selector.Timestamp),
		StartOrEnd:     selector.StartOrEnd,
		LabelMatchers: append(
			[]*labels.Matcher{shardMatcher},
			selector.LabelMatchers...,
		),
	}, nil
}

func shardMatrixSelector(curshard, shards int, selector *parser.MatrixSelector) (parser.Node, error) {
	shardMatcher, err := querysharding.ShardSelector{ShardIndex: uint64(curshard), ShardCount: uint64(shards)}.LabelMatcher()
	if err != nil {
		return nil, err
	}

	if vs, ok := selector.VectorSelector.(*parser.VectorSelector); ok {
		return &parser.MatrixSelector{
			VectorSelector: &parser.VectorSelector{
				Name:           vs.Name,
				OriginalOffset: vs.OriginalOffset,
				Offset:         vs.Offset,
				Timestamp:      copyTimestamp(vs.Timestamp),
				StartOrEnd:     vs.StartOrEnd,
				LabelMatchers: append(
					[]*labels.Matcher{shardMatcher},
					vs.LabelMatchers...,
				),
			},
			Range: selector.Range,
		}, nil
	}

	return nil, fmt.Errorf("invalid selector type: %T", selector.VectorSelector)
}

// isSubquery returns true if the given function call expression is a subquery.
func isSubquery(n *parser.Call) bool {
	if len(n.Args) == 0 {
		return false
	}
	_, ok := n.Args[0].(*parser.SubqueryExpr)
	return ok
}

func copyTimestamp(original *int64) *int64 {
	if original == nil {
		return nil
	}
	ts := *original
	return &ts
}
