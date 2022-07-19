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

func (r *rangeMapper) MapNode(node parser.Node, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	// TODO: check if it is splittable with supported expression types

	switch n := node.(type) {
	// Vector aggregator: sum
	case *parser.AggregateExpr:
		if r.currentSplit != nil {
			return n, false, nil
		}
		return r.splitVectorAggregator(n)
	case *parser.Call:
		// TODO: is Call always range vector aggregator? e.g., count_over_time
		if r.currentSplit == nil {
			return r.splitRangeAggregator(n)
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

func getRangeAggregatorExpr(expr parser.Node) *parser.Call {
	switch e := expr.(type) {
	case *parser.Call:
		return e
	case *parser.AggregateExpr:
		return getRangeAggregatorExpr(e.Expr)
	}
	return nil
}

//  TODO: this should be merged with the splitAndSquashRangeVectorAggregatorNode
func (r *rangeMapper) splitAndSquashVectorAggregatorNode(expr parser.Expr) (parser.Node, error) {
	rangeInterval := getRangeInterval(expr)

	// in case the interval is smaller than the configured split interval,
	// don't split it.
	if rangeInterval <= r.splitByInterval {
		return expr, nil
	}

	// TODO: Make this dynamic based on configuration values
	splitCount := int(math.Ceil(float64(rangeInterval) / float64(r.splitByInterval)))

	if splitCount <= 0 {
		return expr, nil
	}

	children := make([]parser.Node, 0, splitCount)

	var rangeAggregatorFunc string

	// Create partial query for each split
	for split := 0; split < splitCount; split++ {
		splitNode, err := cloneAndMap(NewASTNodeMapper(r.CopyWithCurrentSplit(split)), expr, nil)
		if err != nil {
			return expr, err
		}

		rangeAggregatorExpr := getRangeAggregatorExpr(splitNode)
		if rangeAggregatorExpr == nil {
			return expr, fmt.Errorf("unable to get range aggregator expression")
		}

		// TODO: better way than all these type casts
		ms, ok := rangeAggregatorExpr.Args[0].(*parser.MatrixSelector)
		if !ok {
			return expr, fmt.Errorf("unable to convert children split node to matrix selector")
		}
		vs, ok := ms.VectorSelector.(*parser.VectorSelector)
		if !ok {
			return expr, fmt.Errorf("unable to convert children split node to vector selector")
		}

		// Change expression range interval and offset
		vs.OriginalOffset += time.Duration(split) * r.splitByInterval

		// Add remainder offset
		if split == splitCount-1 {
			ms.Range = ms.Range - vs.OriginalOffset
		} else {
			ms.Range = r.splitByInterval
		}

		rangeAggregatorFunc = rangeAggregatorExpr.Func.Name

		// Prepend to children vector
		children = append([]parser.Node{splitNode}, children...)
	}

	expr, err := vectorSquasher(children...)
	if err != nil {
		return nil, err
	}

	// TODO: this should be mapped recursively!
	switch rangeAggregatorFunc {
	case "count_over_time", "sum_over_time":
		return &parser.AggregateExpr{
			Op:       parser.SUM,
			Expr:     expr,
			Param:    nil,
			Grouping: nil,
			Without:  true,
		}, nil
	case "max_over_time":
		return &parser.AggregateExpr{
			Op:       parser.MAX,
			Expr:     expr,
			Param:    nil,
			Grouping: nil,
			Without:  true,
		}, nil
	case "min_over_time":
		return &parser.AggregateExpr{
			Op:       parser.MIN,
			Expr:     expr,
			Param:    nil,
			Grouping: nil,
			Without:  true,
		}, nil
	}

	return vectorSquasher(children...)
}

//  TODO: this should be merged with the splitAndSquashVectorAggregatorNode
func (r *rangeMapper) splitAndSquashRangeVectorAggregatorNode(expr parser.Expr) (parser.Node, error) {
	rangeInterval := getRangeInterval(expr)

	// in case the interval is smaller than the configured split interval,
	// don't split it.
	if rangeInterval <= r.splitByInterval {
		return expr, nil
	}

	// TODO: Make this dynamic based on configuration values
	splitCount := int(math.Ceil(float64(rangeInterval) / float64(r.splitByInterval)))

	if splitCount <= 0 {
		return expr, nil
	}

	children := make([]parser.Node, 0, splitCount)

	// Create partial query for each split
	for split := 0; split < splitCount; split++ {
		splitNode, err := cloneAndMap(NewASTNodeMapper(r.CopyWithCurrentSplit(split)), expr, nil)
		if err != nil {
			return expr, err
		}

		rangeAggregatorExpr := getRangeAggregatorExpr(splitNode)
		if rangeAggregatorExpr == nil {
			return expr, fmt.Errorf("unable to get range aggregator expression")
		}

		// TODO: better way than all these type casts
		ms, ok := rangeAggregatorExpr.Args[0].(*parser.MatrixSelector)
		if !ok {
			return expr, fmt.Errorf("unable to convert children split node to matrix selector")
		}
		vs, ok := ms.VectorSelector.(*parser.VectorSelector)
		if !ok {
			return expr, fmt.Errorf("unable to convert children split node to vector selector")
		}

		// Change expression range interval and offset
		vs.OriginalOffset += time.Duration(split) * r.splitByInterval

		// Add remainder offset
		if split == splitCount-1 {
			ms.Range = ms.Range - vs.OriginalOffset
		} else {
			ms.Range = r.splitByInterval
		}

		// Prepend to children vector
		children = append([]parser.Node{splitNode}, children...)
	}

	return vectorSquasher(children...)
}

func (r *rangeMapper) splitVectorExpr(expr *parser.AggregateExpr, op parser.ItemType) (result *parser.AggregateExpr, err error) {
	mapped, err := r.splitAndSquashVectorAggregatorNode(expr)
	if err != nil {
		return nil, err
	}

	// Create the parent expression. We need to preserve the grouping as it was in the original one.
	return &parser.AggregateExpr{
		Op:       op,
		Expr:     mapped.(parser.Expr),
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
	}, nil
}

func (r *rangeMapper) splitVectorAggregator(expr *parser.AggregateExpr) (mapped parser.Node, finished bool, err error) {
	mapped, err = r.splitVectorExpr(expr, expr.Op)
	if err != nil {
		return nil, false, err
	}
	return mapped, true, nil
}

func (r *rangeMapper) splitRangeExpr(node *parser.Call, op parser.ItemType) (parser.Node, error) {
	mappedNode, err := r.splitAndSquashRangeVectorAggregatorNode(node)
	if err != nil {
		return nil, err
	}
	vs, ok := mappedNode.(*parser.VectorSelector)
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

func (r *rangeMapper) splitRangeAggregator(node *parser.Call) (mapped parser.Node, finished bool, err error) {
	switch node.Func.Name {
	// TODO: is there a better constant value to use here?
	case "count_over_time", "sum_over_time":
		mapped, err := r.splitRangeExpr(node, parser.SUM)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, err
	case "max_over_time":
		mapped, err := r.splitRangeExpr(node, parser.MAX)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, err
	case "min_over_time":
		mapped, err := r.splitRangeExpr(node, parser.MIN)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, err
	default:
		return node, true, err
	}
}
