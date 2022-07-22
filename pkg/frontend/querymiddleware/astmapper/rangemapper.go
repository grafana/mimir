package astmapper

import (
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/promql/parser"
)

type rangeMapper struct {
	splitByInterval        time.Duration
	embeddedAggregatorExpr *parser.AggregateExpr
	// TODO: add metrics
}

// Supported vector aggregators

// Note: count and topk are supported, but not splittable, i.e., the inner expressions can still be splittable
var splittableVectorAggregators = map[parser.ItemType]struct{}{
	parser.AVG:   {},
	parser.COUNT: {},
	parser.MAX:   {},
	parser.MIN:   {},
	parser.SUM:   {},
	parser.TOPK:  {},
}

// Supported range vector aggregators

type RangeVectorName string

// TODO: are there better constant values to use here?
const countOverTime = RangeVectorName("count_over_time")
const increase = RangeVectorName("increase")
const maxOverTime = RangeVectorName("max_over_time")
const minOverTime = RangeVectorName("min_over_time")
const rate = RangeVectorName("rate")
const sumOverTime = RangeVectorName("sum_over_time")

var splittableRangeVectorAggregators = map[RangeVectorName]struct{}{
	countOverTime: {},
	maxOverTime:   {},
	minOverTime:   {},
	rate:          {},
	sumOverTime:   {},
}

// NewRangeMapper creates a new query range mapper.
func NewRangeMapper(interval time.Duration, logger log.Logger) (ASTMapper, error) {
	rangeMapper := NewASTNodeMapper(&rangeMapper{splitByInterval: interval})
	return rangeMapper, nil
}

// MapNode returns node mapped as embedded queries
// isMapped is false in case the mapping was a noOp
func (r *rangeMapper) MapNode(node parser.Node, stats *MapperStats) (mapped parser.Node, isMapped bool, err error) {
	if !isSplittable(node) {
		return node, false, nil
	}

	// Immediately clone the node to avoid mutating the original
	node, err = cloneNode(node)
	if err != nil {
		return nil, false, err
	}

	switch n := node.(type) {
	case *parser.AggregateExpr:
		return r.mapAggregatorExpr(n, stats)
	case *parser.BinaryExpr:
		return r.mapBinaryExpr(n, stats)
	case *parser.Call:
		return r.mapCall(n, stats)
	case *parser.ParenExpr:
		return r.MapNode(n.Expr, stats)
	// TODO: add other expression types? EvalStmt, Expressions, StepInvariantExpr, TestStmt, UnaryExpr?
	default:
		return n, false, nil
	}
}

// copyWithEmbeddedExpr clones a rangeMapper with a new embedded expression.
func (r *rangeMapper) copyWithEmbeddedExpr(embeddedExpr *parser.AggregateExpr) *rangeMapper {
	rangeMapper := *r
	rangeMapper.embeddedAggregatorExpr = embeddedExpr
	return &rangeMapper
}

// isSplittable returns whether it is possible to optimize the given
// sample expression.
// A vector aggregation is splittable, if the aggregation operation is
// supported and the inner expression is also splittable.
// A range aggregation is splittable, if the aggregation operation is
// supported.
// A binary expression is splittable, if at least one operand is splittable.
func isSplittable(node parser.Node) bool {
	switch n := node.(type) {
	case *parser.AggregateExpr:
		_, ok := splittableVectorAggregators[n.Op]
		return ok && isSplittable(n.Expr)
	case *parser.BinaryExpr:
		return isSplittable(n.LHS) || isSplittable(n.RHS)
	case *parser.Call:
		_, ok := splittableRangeVectorAggregators[RangeVectorName(n.Func.Name)]
		return ok
	case *parser.ParenExpr:
		return isSplittable(n.Expr)
	}
	return false
}

// mapAggregatorExpr maps vector aggregator expression expr
func (r *rangeMapper) mapAggregatorExpr(expr *parser.AggregateExpr, stats *MapperStats) (mapped parser.Node, isMapped bool, err error) {
	// In case the range interval is smaller than the configured split interval,
	// don't split it
	rangeInterval := getRangeInterval(expr)
	if rangeInterval <= r.splitByInterval {
		return expr, false, nil
	}

	var mappedNode parser.Node

	// If the embeddedAggregatorExpr is not set, update it
	// Note: vector aggregators count and topk are supported but not splittable, so cannot be sent downstream
	if r.embeddedAggregatorExpr == nil && expr.Op != parser.COUNT && expr.Op != parser.TOPK {
		mappedNode, isMapped, err = NewASTNodeMapper(r.copyWithEmbeddedExpr(expr)).MapNode(expr.Expr, nil)
	} else {
		mappedNode, isMapped, err = r.MapNode(expr.Expr, nil)
	}
	if err != nil {
		return nil, false, err
	}

	mappedExpr, ok := mappedNode.(parser.Expr)
	if !ok {
		return nil, false, fmt.Errorf("unable to map expr '%s'", expr)
	}

	// Create the parent expression while preserving the grouping from the original one
	return &parser.AggregateExpr{
		Op:       expr.Op,
		Expr:     mappedExpr,
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
	}, true, nil
}

// mapBinaryExpr maps binary expression expr
func (r *rangeMapper) mapBinaryExpr(expr *parser.BinaryExpr, stats *MapperStats) (mapped parser.Node, isMapped bool, err error) {
	// Noop if both LHS and RHS are literal numbers
	_, literalLhs := expr.LHS.(*parser.NumberLiteral)
	_, literalRhs := expr.RHS.(*parser.NumberLiteral)
	if literalLhs && literalRhs {
		return expr, false, nil
	}

	lhsMapped, isLhsMapped, err := r.MapNode(expr.LHS, stats)
	if err != nil {
		return nil, false, err
	}
	lhsMappedExpr, ok := lhsMapped.(parser.Expr)
	if !ok {
		return nil, false, fmt.Errorf("unable to map expr '%s'", lhsMapped)
	}
	rhsMapped, isRhsMapped, err := r.MapNode(expr.RHS, stats)
	if err != nil {
		return nil, false, err
	}
	rhsMappedExpr, ok := rhsMapped.(parser.Expr)
	if !ok {
		return nil, false, fmt.Errorf("unable to map expr '%s'", rhsMapped)
	}
	isMapped = isLhsMapped || isRhsMapped
	if isMapped {
		expr.LHS = lhsMappedExpr
		expr.RHS = rhsMappedExpr
	}

	return expr, isMapped, nil
}

// mapCall maps range vector aggregator expression expr
func (r *rangeMapper) mapCall(expr *parser.Call, stats *MapperStats) (mapped parser.Node, isMapped bool, err error) {
	// In case the range interval is smaller than the configured split interval,
	// don't split it
	rangeInterval := getRangeInterval(expr)
	if rangeInterval <= r.splitByInterval {
		return expr, false, nil
	}

	switch RangeVectorName(expr.Func.Name) {
	case countOverTime:
		return r.mapCallByRangeInterval(expr, rangeInterval, parser.SUM)
	case maxOverTime:
		return r.mapCallByRangeInterval(expr, rangeInterval, parser.MAX)
	case minOverTime:
		return r.mapCallByRangeInterval(expr, rangeInterval, parser.MIN)
	case rate:
		return r.mapCallRate(expr, rangeInterval)
	case sumOverTime:
		return r.mapCallByRangeInterval(expr, rangeInterval, parser.SUM)
	default:
		return expr, false, nil
	}
}

// mapCallRate maps a rate function to increase / rangeInterval
func (r *rangeMapper) mapCallRate(expr *parser.Call, rangeInterval time.Duration) (mapped parser.Node, isMapped bool, err error) {
	expr.Func = parser.Functions[string(increase)]

	// TODO: if wrapped by a vector aggregator it needs to update the inner range vector aggregator to use increase instead of rate

	mappedNode, isMapped, err := r.mapCallByRangeInterval(expr, rangeInterval, parser.SUM)
	if err != nil {
		return nil, false, err
	}
	if !isMapped {
		return mapped, false, nil
	}

	mappedExpr, ok := mappedNode.(parser.Expr)
	if !ok {
		return nil, false, fmt.Errorf("unable to map expr '%s'", expr)
	}

	return &parser.BinaryExpr{
		Op:  parser.DIV,
		LHS: mappedExpr,
		RHS: &parser.NumberLiteral{Val: rangeInterval.Seconds()},
	}, true, nil
}

func (r *rangeMapper) mapCallByRangeInterval(expr *parser.Call, rangeInterval time.Duration, op parser.ItemType) (mapped parser.Node, isMapped bool, err error) {
	// Default grouping is 'without' for concatenating the embedded queries
	var grouping []string
	groupingWithout := true
	if r.embeddedAggregatorExpr != nil {
		grouping = append(grouping, r.embeddedAggregatorExpr.Grouping...)
		groupingWithout = r.embeddedAggregatorExpr.Without
	}

	embeddedExpr, isMapped, err := r.splitAndSquashCall(expr, rangeInterval)
	if err != nil {
		return nil, false, err
	}

	return &parser.AggregateExpr{
		Op:       op,
		Expr:     embeddedExpr,
		Param:    nil,
		Grouping: grouping,
		Without:  groupingWithout,
		PosRange: parser.PositionRange{},
	}, true, nil
}

// expr is the range vector aggregator expression
// If the outer expression is a vector aggregator, r.embeddedAggregatorExpr will contain the expression
// In this case, the vector aggregator should be downstream to the embedded queries in order to limit
// the label cardinality of the parallel queries
func (r *rangeMapper) splitAndSquashCall(expr *parser.Call, rangeInterval time.Duration) (mapped parser.Expr, isMapped bool, err error) {
	// TODO: Make this dynamic based on configuration values
	splitCount := int(math.Ceil(float64(rangeInterval) / float64(r.splitByInterval)))
	if splitCount <= 0 {
		return expr, false, nil
	}

	var embeddedQuery parser.Expr = expr

	if r.embeddedAggregatorExpr != nil {
		embeddedQuery = r.embeddedAggregatorExpr
	}

	// Create a partial query for each split
	embeddedQueries := make([]parser.Node, 0, splitCount)
	for split := 0; split < splitCount; split++ {
		splitNode, err := createSplitNode(embeddedQuery, r.splitByInterval, time.Duration(split)*r.splitByInterval)
		if err != nil {
			return nil, false, err
		}

		// Prepend to embedded queries
		embeddedQueries = append([]parser.Node{splitNode}, embeddedQueries...)
	}

	squashExpr, err := vectorSquasher(embeddedQueries...)
	if err != nil {
		return nil, false, err
	}

	return squashExpr, true, nil
}

// getRangeInterval returns the interval in the range vector node
// Returns 0 if no range interval is found
// Example: expression `count_over_time({app="foo"}[10m])` returns 10m
func getRangeInterval(node parser.Node) time.Duration {
	switch n := node.(type) {
	case *parser.AggregateExpr:
		return getRangeInterval(n.Expr)
	case *parser.Call:
		argRangeInterval := time.Duration(0)
		// Iterate over Call's arguments until a MatrixSelector is found
		for _, arg := range n.Args {
			if argRangeInterval = getRangeInterval(arg); argRangeInterval != 0 {
				break
			}
		}
		return argRangeInterval
	case *parser.MatrixSelector:
		return n.Range
	default:
		return 0
	}
}

// expr can be a parser.Call or a parser.AggregateExpr
func createSplitNode(expr parser.Expr, rangeInterval time.Duration, offset time.Duration) (parser.Node, error) {
	splitNode, err := cloneNode(expr)
	if err != nil {
		return nil, err
	}
	updateRangeInterval(splitNode, rangeInterval)
	updateOffset(splitNode, offset)

	return splitNode, nil
}

func updateRangeInterval(expr parser.Node, rangeInterval time.Duration) {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		updateRangeInterval(e.Expr, rangeInterval)
	case *parser.Call:
		// Iterate over Call's arguments until a MatrixSelector is found
		for _, arg := range e.Args {
			updateRangeInterval(arg, rangeInterval)
		}
		return
	case *parser.MatrixSelector:
		e.Range = rangeInterval
		return
	default:
		return
	}
}

func updateOffset(expr parser.Node, offset time.Duration) {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		updateOffset(e.Expr, offset)
		return
	case *parser.Call:
		// Iterate over Call's arguments until a VectorSelector is found
		for _, arg := range e.Args {
			updateOffset(arg, offset)
		}
		return
	case *parser.MatrixSelector:
		updateOffset(e.VectorSelector, offset)
		return
	case *parser.VectorSelector:
		e.OriginalOffset += offset
		return
	default:
		return
	}
}
