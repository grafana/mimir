package astmapper

import (
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
)

type rangeMapper struct {
	splitByInterval time.Duration
	currentSplit    *int
	downstreamExpr  *parser.Expr
	// TODO: add metrics
}

// NewRangeMapper creates a new query range mapper.
func NewRangeMapper(interval time.Duration, logger log.Logger) (ASTMapper, error) {
	// TODO: required ASTMapper? shardSummer and/or subtreeFolder?
	rangeMapper := NewASTNodeMapper(&rangeMapper{splitByInterval: interval, currentSplit: nil})
	return rangeMapper, nil
}

// CopyWithCurrentSplit clones a rangeMapper with a new current split.
func (r *rangeMapper) CopyWithCurrentSplit(currentSplit int) *rangeMapper {
	rangeMapper := *r
	rangeMapper.currentSplit = &currentSplit
	return &rangeMapper
}

func (r *rangeMapper) CopyWithDownstream(downstreamExpr parser.Expr) *rangeMapper {
	rangeMapper := *r
	rangeMapper.downstreamExpr = &downstreamExpr
	return &rangeMapper
}

func (r *rangeMapper) MapNode(node parser.Node, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	// TODO: check if it is splittable with supported expression types

	switch n := node.(type) {
	// Vector aggregator: sum
	case *parser.AggregateExpr:
		if r.currentSplit != nil {
			return n, false, nil
		}
		return r.mapVectorAggregator(n)
	case *parser.Call:
		// TODO: is Call always range vector aggregator? e.g., count_over_time
		if r.currentSplit == nil {
			return r.mapRangeAggregator(n)
		}
		return n, false, nil
	case *parser.MatrixSelector:
		return n, false, nil
	// TODO: add other expression types
	default:
		return n, false, nil
	}
}

// getRangeInterval returns the interval in the range vector node
// Example: expression `count_over_time({app="foo"}[10m])` returns 10m
func getRangeInterval(node parser.Node) time.Duration {
	switch n := node.(type) {
	// Vector aggregator: sum
	case *parser.AggregateExpr:
		return getRangeInterval(n.Expr)
	case *parser.Call:
		// TODO: Is node always a range vector aggregator?
		if len(n.Args) < 1 {
			return 0
		}
		ms, ok := n.Args[0].(*parser.MatrixSelector)
		if !ok {
			return 0
		}
		return ms.Range
	}
	return 0
}

func (r *rangeMapper) getRangeAggregator(expr parser.Node) (*parser.Call, error) {
	switch n := expr.(type) {
	case *parser.Call:
		return n, nil
	case *parser.AggregateExpr:
		return r.getRangeAggregator(n.Expr)
	}
	return nil, fmt.Errorf("no range aggregator expression found")
}

func (r *rangeMapper) splitNode(expr parser.Expr, currentSplit int) (parser.Node, error) {
	node, err := cloneNode(expr)
	if err != nil {
		return nil, err
	}

	rangeAggregator, err := r.getRangeAggregator(node)
	if err != nil {
		return nil, err
	}

	// TODO: better way than all these type casts
	ms, ok := rangeAggregator.Args[0].(*parser.MatrixSelector)
	if !ok {
		return expr, fmt.Errorf("unable to get matrix selector from call")
	}
	vs, ok := ms.VectorSelector.(*parser.VectorSelector)
	if !ok {
		return expr, fmt.Errorf("unable to get vector selector from matrix selector")
	}

	// Change expression range interval and offset
	vs.OriginalOffset += time.Duration(currentSplit) * r.splitByInterval

	// TODO: add remainder offset
	ms.Range = r.splitByInterval

	return node, nil
}

func (r *rangeMapper) splitAndSquashNode(expr parser.Expr, rangeInterval time.Duration) (parser.Node, error) {
	// TODO: Make this dynamic based on configuration values
	splitCount := int(math.Ceil(float64(rangeInterval) / float64(r.splitByInterval)))

	if splitCount <= 0 {
		return expr, nil
	}

	children := make([]parser.Node, 0, splitCount)

	if r.downstreamExpr != nil {
		expr = *r.downstreamExpr
	}

	// Create partial query for each split
	for split := 0; split < splitCount; split++ {
		splitNode, err := r.splitNode(expr, split)
		if err != nil {
			return expr, err
		}

		// Prepend to children vector
		children = append([]parser.Node{splitNode}, children...)
	}

	return vectorSquasher(children...)
}

func (r *rangeMapper) mapVectorAggregator(expr *parser.AggregateExpr) (mapped parser.Node, finished bool, err error) {
	mapped, _, err = NewASTNodeMapper(r.CopyWithDownstream(expr)).MapNode(expr.Expr, nil)
	if err != nil {
		return nil, false, err
	}

	// Create the parent expression. We need to preserve the grouping as it was in the original one.
	return &parser.AggregateExpr{
		Op:       expr.Op,
		Expr:     mapped.(parser.Expr),
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
	}, true, nil
}

func (r *rangeMapper) splitAndSquashExpr(node parser.Expr, rangeInterval time.Duration, op parser.ItemType) (parser.Node, error) {
	mapped, err := r.splitAndSquashNode(node, rangeInterval)
	if err != nil {
		return nil, err
	}
	vs, ok := mapped.(*parser.VectorSelector)
	if !ok {
		return nil, err
	}
	return &parser.AggregateExpr{
		Op:       op,
		Expr:     vs,
		Param:    nil,
		Grouping: nil,
		Without:  true,
	}, nil
}

func (r *rangeMapper) mapRangeAggregator(node *parser.Call) (mapped parser.Node, finished bool, err error) {
	rangeInterval := getRangeInterval(node)

	// in case the interval is smaller than the configured split interval,
	// don't split it.
	if rangeInterval <= r.splitByInterval {
		return node, true, nil
	}

	switch node.Func.Name {
	// TODO: is there a better constant value to use here?
	case "count_over_time", "sum_over_time":
		mapped, err := r.splitAndSquashExpr(node, rangeInterval, parser.SUM)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, err
	case "max_over_time":
		mapped, err := r.splitAndSquashExpr(node, rangeInterval, parser.MAX)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, err
	case "min_over_time":
		mapped, err := r.splitAndSquashExpr(node, rangeInterval, parser.MIN)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, err
	default:
		return node, true, err
	}
}
