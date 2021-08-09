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

type squasher = func(...parser.Node) (parser.Expr, error)

type shardSummer struct {
	shards       int
	currentShard *int
	squash       squasher

	// Metrics.
	shardedQueries prometheus.Counter
}

// NewShardSummer instantiates an ASTMapper which will fan out sum queries by shard
func NewShardSummer(shards int, squasher squasher, shardedQueries prometheus.Counter) (ASTMapper, error) {
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

// shardSummer expands a query AST by sharding and re-summing when possible
func (summer *shardSummer) MapNode(node parser.Node) (parser.Node, bool, error) {

	switch n := node.(type) {
	case *parser.AggregateExpr:
		if CanParallelize(n) && n.Op == parser.SUM {
			result, err := summer.shardSum(n)
			return result, true, err
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

	default:
		return n, false, nil
	}
}

// shardSum contains the logic for how we split/stitch legs of a parallelized sum query
func (summer *shardSummer) shardSum(expr *parser.AggregateExpr) (parser.Node, error) {

	parent, subSums, err := summer.splitSum(expr)
	if err != nil {
		return nil, err
	}

	combinedSums, err := summer.squash(subSums...)

	if err != nil {
		return nil, err
	}

	parent.Expr = combinedSums
	return parent, nil
}

// splitSum forms the parent and child legs of a parallel query
func (summer *shardSummer) splitSum(
	expr *parser.AggregateExpr,
) (
	parent *parser.AggregateExpr,
	children []parser.Node,
	err error,
) {
	parent = &parser.AggregateExpr{
		Op:    expr.Op,
		Param: expr.Param,
	}
	var mkChild func(sharded *parser.AggregateExpr) parser.Expr

	if expr.Without {
		/*
			parallelizing a sum using without(foo) is representable naively as
			sum without(foo) (
			  sum without(__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum without(__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)
			or (more optimized):
			sum without(__cortex_shard__) (
			  sum without(foo) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum without(foo) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)

		*/
		parent.Grouping = []string{querysharding.ShardLabel}
		parent.Without = true
		mkChild = func(sharded *parser.AggregateExpr) parser.Expr {
			sharded.Grouping = expr.Grouping
			sharded.Without = true
			return sharded
		}
	} else if len(expr.Grouping) > 0 {
		/*
			parallelizing a sum using by(foo) is representable as
			sum by(foo) (
			  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum by(foo, __cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)
		*/
		parent.Grouping = expr.Grouping
		mkChild = func(sharded *parser.AggregateExpr) parser.Expr {
			groups := make([]string, 0, len(expr.Grouping)+1)
			groups = append(groups, expr.Grouping...)
			groups = append(groups, querysharding.ShardLabel)
			sharded.Grouping = groups
			return sharded
		}
	} else {
		/*
			parallelizing a non-parameterized sum is representable as
			sum(
			  sum without(__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum without(__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)
			or (more optimized):
			sum without(__cortex_shard__) (
			  sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="0_of_2",baz="blip"}[1m])) or
			  sum by(__cortex_shard__) (rate(bar1{__cortex_shard__="1_of_2",baz="blip"}[1m]))
			)
		*/
		parent.Grouping = []string{querysharding.ShardLabel}
		parent.Without = true
		mkChild = func(sharded *parser.AggregateExpr) parser.Expr {
			sharded.Grouping = []string{querysharding.ShardLabel}
			return sharded
		}
	}

	// iterate across shardFactor to create children
	for i := 0; i < summer.shards; i++ {
		cloned, err := CloneNode(expr.Expr)
		if err != nil {
			return parent, children, err
		}

		subSummer := NewASTNodeMapper(summer.CopyWithCurShard(i))
		sharded, err := subSummer.Map(cloned)
		if err != nil {
			return parent, children, err
		}

		subSum := mkChild(&parser.AggregateExpr{
			Op:   expr.Op,
			Expr: sharded.(parser.Expr),
		})

		children = append(children,
			subSum,
		)
	}

	summer.recordShards(float64(summer.shards))

	return parent, children, nil
}

// ShardSummer is explicitly passed a prometheus.Counter during construction
// in order to prevent duplicate metric registerings (ShardSummers are created per request).
//recordShards prevents calling nil interfaces (commonly used in tests).
func (summer *shardSummer) recordShards(n float64) {
	if summer.shardedQueries != nil {
		summer.shardedQueries.Add(float64(summer.shards))
	}
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
