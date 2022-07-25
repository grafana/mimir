// SPDX-License-Identifier: AGPL-3.0-only

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

// Note: avg, count and topk are supported, but not splittable, i.e., cannot be sent downstream,
// but the inner expressions can still be splittable
var supportedVectorAggregators = map[parser.ItemType]struct{}{
	parser.AVG:   {},
	parser.COUNT: {},
	parser.MAX:   {},
	parser.MIN:   {},
	parser.SUM:   {},
	parser.TOPK:  {},
}

var splittableVectorAggregators = map[parser.ItemType]struct{}{
	parser.MAX: {},
	parser.MIN: {},
	parser.SUM: {},
}

// Supported range vector aggregators

type RangeVectorName string

// TODO: are there better constant values to use here?
const avgOverTime = RangeVectorName("avg_over_time")
const countOverTime = RangeVectorName("count_over_time")
const increase = RangeVectorName("increase")
const maxOverTime = RangeVectorName("max_over_time")
const minOverTime = RangeVectorName("min_over_time")
const rate = RangeVectorName("rate")
const sumOverTime = RangeVectorName("sum_over_time")

var splittableRangeVectorAggregators = map[RangeVectorName]struct{}{
	avgOverTime:   {},
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
func (r *rangeMapper) MapNode(node parser.Node, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	if !isSplittable(node) {
		// If no node in the tree is splittable, finish the AST traversal
		return node, true, nil
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
		return r.mapParenExpr(n, stats)
	// TODO: add other expression types? EvalStmt, Expressions, StepInvariantExpr, TestStmt, UnaryExpr?
	default:
		return n, false, nil
	}
}

// copyWithEmbeddedExpr clones a rangeMapper with a new embedded expression.
// This expression is the one that will be used in all the embedded queries in the split and squash operation
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
		_, ok := supportedVectorAggregators[n.Op]
		return ok && isSplittable(n.Expr)
	case *parser.BinaryExpr:
		return isSplittable(n.LHS) || isSplittable(n.RHS)
	case *parser.Call:
		_, ok := splittableRangeVectorAggregators[RangeVectorName(n.Func.Name)]
		if ok {
			return true
		}
		var isArgSplittable bool
		// It is considered splittable if at least a Call argument is splittable
		for _, arg := range n.Args {
			isArgSplittable = isSplittable(arg)
			if isArgSplittable {
				break
			}
		}
		return isArgSplittable
	case *parser.ParenExpr:
		return isSplittable(n.Expr)
	}
	return false
}

func isVectorAggregatorSplittable(expr *parser.AggregateExpr) bool {
	_, ok := splittableVectorAggregators[expr.Op]
	return ok
}

// mapAggregatorExpr maps vector aggregator expression expr
func (r *rangeMapper) mapAggregatorExpr(expr *parser.AggregateExpr, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	// In case the range interval is smaller than the configured split interval,
	// don't split it
	rangeInterval := getRangeInterval(expr)
	if rangeInterval <= r.splitByInterval {
		return expr, false, nil
	}

	var mappedNode parser.Node

	// If the embeddedAggregatorExpr is not set, update it
	// Note: vector aggregators avg, count and topk are supported but not splittable, so cannot be sent downstream
	if r.embeddedAggregatorExpr == nil && isVectorAggregatorSplittable(expr) {
		mappedNode, finished, err = NewASTNodeMapper(r.copyWithEmbeddedExpr(expr)).MapNode(expr.Expr, nil)
	} else {
		mappedNode, finished, err = r.MapNode(expr.Expr, nil)
	}
	if err != nil {
		return nil, false, err
	}
	if !finished {
		return expr, false, nil
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
func (r *rangeMapper) mapBinaryExpr(expr *parser.BinaryExpr, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	// Noop if both LHS and RHS are literal numbers
	_, literalLHS := expr.LHS.(*parser.NumberLiteral)
	_, literalRHS := expr.RHS.(*parser.NumberLiteral)
	if literalLHS && literalRHS {
		return expr, false, nil
	}

	lhsMapped, lhsFinished, err := r.MapNode(expr.LHS, stats)
	if err != nil {
		return nil, false, err
	}
	lhsMappedExpr, ok := lhsMapped.(parser.Expr)
	if !ok {
		return nil, false, fmt.Errorf("unable to map expr '%s'", expr.LHS)
	}
	rhsMapped, rhsFinished, err := r.MapNode(expr.RHS, stats)
	if err != nil {
		return nil, false, err
	}
	rhsMappedExpr, ok := rhsMapped.(parser.Expr)
	if !ok {
		return nil, false, fmt.Errorf("unable to map expr '%s'", expr.RHS)
	}
	finished = lhsFinished || rhsFinished
	// Wrap binary operands in parentheses expression
	if finished {
		expr.LHS = &parser.ParenExpr{
			Expr: lhsMappedExpr,
		}
		expr.RHS = &parser.ParenExpr{
			Expr: rhsMappedExpr,
		}
	}

	return expr, finished, nil
}

// mapParenExpr maps parenthesis expression expr
func (r *rangeMapper) mapParenExpr(expr *parser.ParenExpr, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	parenNode, finished, err := r.MapNode(expr.Expr, stats)
	if err != nil {
		return nil, false, err
	}
	if !finished {
		return expr, false, nil
	}
	parenExpr, ok := parenNode.(parser.Expr)
	if !ok {
		return nil, false, fmt.Errorf("unable to map expr '%s'", expr)
	}

	return &parser.ParenExpr{
		Expr:     parenExpr,
		PosRange: parser.PositionRange{},
	}, true, nil
}

// mapCall maps range vector aggregator expression expr
func (r *rangeMapper) mapCall(expr *parser.Call, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	// In case the range interval is smaller than the configured split interval,
	// don't split it
	rangeInterval := getRangeInterval(expr)
	if rangeInterval <= r.splitByInterval {
		return expr, false, nil
	}

	switch RangeVectorName(expr.Func.Name) {
	case avgOverTime:
		return r.mapCallAvgOverTime(expr, stats)
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

// mapCallAvgOverTime maps an avg_over_time function to expression sum_over_time / count_over_time
func (r *rangeMapper) mapCallAvgOverTime(expr *parser.Call, stats *MapperStats) (mapped parser.Node, finished bool, err error) {
	avgOverTimeExpr := &parser.BinaryExpr{
		Op: parser.DIV,
		LHS: &parser.Call{
			Func:     parser.Functions[string(sumOverTime)],
			Args:     expr.Args,
			PosRange: expr.PosRange,
		},
		RHS: &parser.Call{
			Func:     parser.Functions[string(countOverTime)],
			Args:     expr.Args,
			PosRange: expr.PosRange,
		},
	}

	// If avg_over_time is wrapped by a vector aggregator,
	// the embedded query cannot be sent downstream
	if r.embeddedAggregatorExpr != nil {
		r.embeddedAggregatorExpr = nil
	}

	return r.MapNode(avgOverTimeExpr, stats)
}

// mapCallRate maps a rate function to expression increase / rangeInterval
func (r *rangeMapper) mapCallRate(expr *parser.Call, rangeInterval time.Duration) (mapped parser.Node, finished bool, err error) {
	increaseExpr := &parser.Call{
		Func:     parser.Functions[string(increase)],
		Args:     expr.Args,
		PosRange: expr.PosRange,
	}

	// If rate is wrapped by a vector aggregator,
	// the embedded query also needs to be updated to use increase
	if r.embeddedAggregatorExpr != nil {
		updatedExpr := updateEmbeddedExpr(r.embeddedAggregatorExpr, increaseExpr)
		if updatedExpr == nil {
			r.embeddedAggregatorExpr = nil
		}
	}

	mappedNode, finished, err := r.mapCallByRangeInterval(increaseExpr, rangeInterval, parser.SUM)
	if err != nil {
		return nil, false, err
	}
	if !finished {
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

func (r *rangeMapper) mapCallByRangeInterval(expr *parser.Call, rangeInterval time.Duration, op parser.ItemType) (mapped parser.Node, finished bool, err error) {
	// Default grouping is 'without' for concatenating the embedded queries
	var grouping []string
	groupingWithout := true
	if r.embeddedAggregatorExpr != nil {
		grouping = append(grouping, r.embeddedAggregatorExpr.Grouping...)
		groupingWithout = r.embeddedAggregatorExpr.Without
	}

	embeddedExpr, finished, err := r.splitAndSquashCall(expr, rangeInterval)
	if err != nil {
		return nil, false, err
	}
	if !finished {
		return expr, false, nil
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
func (r *rangeMapper) splitAndSquashCall(expr *parser.Call, rangeInterval time.Duration) (mapped parser.Expr, finished bool, err error) {
	// TODO: Make this dynamic based on configuration values
	splitCount := int(math.Ceil(float64(rangeInterval) / float64(r.splitByInterval)))
	if splitCount <= 0 {
		return expr, false, nil
	}

	var embeddedQuery parser.Expr = expr

	if r.embeddedAggregatorExpr != nil {
		embeddedQuery = r.embeddedAggregatorExpr
	}

	originalOffset := getOffset(expr)

	// Create a partial query for each split
	embeddedQueries := make([]parser.Node, 0, splitCount)
	for split := 0; split < splitCount; split++ {
		splitOffset := time.Duration(split) * r.splitByInterval
		// The range interval of the last embedded query can be smaller than r.splitByInterval
		splitRangeInterval := r.splitByInterval
		if splitOffset+splitRangeInterval > rangeInterval {
			splitRangeInterval = rangeInterval - splitOffset
		}
		// The offset of the embedded queries is always the original offset + a multiple of r.splitByInterval
		splitOffset += originalOffset
		splitNode, err := createSplitNode(embeddedQuery, splitRangeInterval, splitOffset)
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

// getRangeInterval returns the range interval in the range vector node
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

// getOffset returns the offset interval in the range vector node
// Returns 0 if no offset interval is found
// Example: expression `count_over_time({app="foo"}[10m]) offset 1m` returns 1m
func getOffset(node parser.Node) time.Duration {
	switch n := node.(type) {
	case *parser.AggregateExpr:
		return getOffset(n.Expr)
	case *parser.Call:
		argRangeInterval := time.Duration(0)
		// Iterate over Call's arguments until a MatrixSelector is found
		for _, arg := range n.Args {
			if argRangeInterval = getOffset(arg); argRangeInterval != 0 {
				break
			}
		}
		return argRangeInterval
	case *parser.MatrixSelector:
		return getOffset(n.VectorSelector)
	case *parser.VectorSelector:
		return n.OriginalOffset
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
	rangeIntervalUpdated := updateRangeInterval(splitNode, rangeInterval)
	if !rangeIntervalUpdated {
		return nil, fmt.Errorf("unable to update range interval on node: %v", splitNode)
	}
	offsetUpdated := updateOffset(splitNode, offset)
	if !offsetUpdated {
		return nil, fmt.Errorf("unable to update offset operator on node: %v", splitNode)
	}

	return splitNode, nil
}

// updateEmbeddedExpr returns the updated node if inner call expression was updated successfully,
// otherwise returns nil
func updateEmbeddedExpr(node parser.Node, call *parser.Call) parser.Node {
	switch n := node.(type) {
	case *parser.AggregateExpr:
		embeddedNode := updateEmbeddedExpr(n.Expr, call)
		embeddedExpr, ok := embeddedNode.(parser.Expr)
		if !ok {
			return nil
		}
		n.Expr = embeddedExpr
		return n
	case *parser.Call:
		return call
	case *parser.ParenExpr:
		return updateEmbeddedExpr(n.Expr, call)
	default:
		return nil
	}
}

// updateRangeInterval returns true if range interval was updated successfully,
// false otherwise
func updateRangeInterval(expr parser.Node, rangeInterval time.Duration) bool {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		return updateRangeInterval(e.Expr, rangeInterval)
	case *parser.Call:
		var updated bool
		// Iterate over Call's arguments until a MatrixSelector is found
		for _, arg := range e.Args {
			updated = updateRangeInterval(arg, rangeInterval)
			if updated {
				break
			}
		}
		return updated
	case *parser.MatrixSelector:
		e.Range = rangeInterval
		return true
	case *parser.ParenExpr:
		return updateRangeInterval(e.Expr, rangeInterval)
	default:
		return false
	}
}

// updateOffset returns true if offset operator was updated successfully,
// false otherwise
func updateOffset(expr parser.Node, offset time.Duration) bool {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		return updateOffset(e.Expr, offset)
	case *parser.Call:
		var updated bool
		// Iterate over Call's arguments until a VectorSelector is found
		for _, arg := range e.Args {
			updated = updateOffset(arg, offset)
			if updated {
				break
			}
		}
		return updated
	case *parser.MatrixSelector:
		return updateOffset(e.VectorSelector, offset)
	case *parser.ParenExpr:
		return updateOffset(e.Expr, offset)
	case *parser.VectorSelector:
		e.OriginalOffset = offset
		return true
	default:
		return false
	}
}
