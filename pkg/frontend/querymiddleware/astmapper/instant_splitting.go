// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"
)

type instantSplitter struct {
	interval time.Duration
	// In case of outer vector aggregator expressions, this contains the expression that will be used on the
	// downstream queries, i.e. the query that will be executed in parallel in each partial query.
	// This is an optimization to send outer vector aggregator expressions to reduce the label sets returned
	// by queriers, and therefore minimize the merging of results in the query-frontend.
	outerAggregationExpr *parser.AggregateExpr
	logger               log.Logger
}

// Supported vector aggregators

// Note: avg, count and topk are supported, but not splittable, i.e., cannot be sent downstream,
// but the inner expressions can still be splittable
var supportedVectorAggregators = map[parser.ItemType]bool{
	parser.AVG:   true,
	parser.COUNT: true,
	parser.MAX:   true,
	parser.MIN:   true,
	parser.SUM:   true,
	parser.TOPK:  true,
}

var splittableVectorAggregators = map[parser.ItemType]bool{
	parser.MAX: true,
	parser.MIN: true,
	parser.SUM: true,
}

// Supported range vector aggregators

const (
	avgOverTime   = "avg_over_time"
	countOverTime = "count_over_time"
	increase      = "increase"
	maxOverTime   = "max_over_time"
	minOverTime   = "min_over_time"
	rate          = "rate"
	sumOverTime   = "sum_over_time"
)

var splittableRangeVectorAggregators = map[string]bool{
	avgOverTime:   true,
	countOverTime: true,
	maxOverTime:   true,
	minOverTime:   true,
	rate:          true,
	sumOverTime:   true,
}

// NewInstantQuerySplitter creates a new query range mapper.
func NewInstantQuerySplitter(interval time.Duration, logger log.Logger) ASTMapper {
	instantQueryMapper := NewASTExprMapper(
		&instantSplitter{
			interval: interval,
			logger:   logger,
		},
	)

	return NewMultiMapper(
		instantQueryMapper,
		newSubtreeFolder(),
	)
}

// MapExpr returns expr mapped as embedded queries
func (i *instantSplitter) MapExpr(expr parser.Expr, stats *MapperStats) (mapped parser.Expr, finished bool, err error) {
	if !isSplittable(expr) {
		level.Debug(i.logger).Log("msg", "expr is not supported for split by interval", "expr", expr)
		// If no node in the tree is splittable, finish the AST traversal
		return expr, true, nil
	}

	// Immediately clone the expr to avoid mutating the original
	expr, err = cloneExpr(expr)
	if err != nil {
		return nil, false, err
	}

	switch e := expr.(type) {
	case *parser.AggregateExpr:
		return i.mapAggregatorExpr(e, stats)
	case *parser.BinaryExpr:
		return i.mapBinaryExpr(e, stats)
	case *parser.Call:
		return i.mapCall(e, stats)
	case *parser.ParenExpr:
		return i.mapParenExpr(e, stats)
	default:
		return e, false, nil
	}
}

// copyWithEmbeddedExpr clones a instantSplitter with a new embedded expression.
// This expression is the one that will be used in all the embedded queries in the split and squash operation
func (i *instantSplitter) copyWithEmbeddedExpr(embeddedExpr *parser.AggregateExpr) *instantSplitter {
	instantSplitter := *i
	instantSplitter.outerAggregationExpr = embeddedExpr
	return &instantSplitter
}

// isSplittable returns whether it is possible to optimize the given sample expression.
func isSplittable(expr parser.Expr) bool {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		// A vector aggregation is splittable, if the aggregation operation is supported and the inner expression is also splittable.
		return supportedVectorAggregators[e.Op] && isSplittable(e.Expr)
	case *parser.BinaryExpr:
		// A binary expression is splittable, if at least one operand is splittable.
		return isSplittable(e.LHS) || isSplittable(e.RHS)
	case *parser.Call:
		// A range aggregation is splittable, if the aggregation operation is supported.
		if splittableRangeVectorAggregators[e.Func.Name] {
			return true
		}
		// It is considered splittable if at least a Call argument is splittable
		for _, arg := range e.Args {
			if isSplittable(arg) {
				return true
			}
		}
		return false
	case *parser.ParenExpr:
		return isSplittable(e.Expr)
	}
	return false
}

// mapAggregatorExpr maps vector aggregator expression expr
func (i *instantSplitter) mapAggregatorExpr(expr *parser.AggregateExpr, stats *MapperStats) (mapped parser.Expr, finished bool, err error) {
	var mappedExpr parser.Expr

	// If the outerAggregationExpr is not set, update it.
	// Note: vector aggregators avg, count and topk are supported but not splittable, so cannot be sent downstream.
	if i.outerAggregationExpr == nil && splittableVectorAggregators[expr.Op] {
		mappedExpr, finished, err = NewASTExprMapper(i.copyWithEmbeddedExpr(expr)).MapExpr(expr.Expr, stats)
	} else {
		mappedExpr, finished, err = i.MapExpr(expr.Expr, stats)
	}
	if err != nil {
		return nil, false, err
	}
	if !finished {
		return expr, false, nil
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
func (i *instantSplitter) mapBinaryExpr(expr *parser.BinaryExpr, stats *MapperStats) (mapped parser.Expr, finished bool, err error) {
	// Binary expressions cannot be sent downstream, only their respective operands.
	// Therefore, the embedded aggregator expression needs to be reset.
	i.outerAggregationExpr = nil

	// Noop if both LHS and RHS are literal numbers
	_, literalLHS := expr.LHS.(*parser.NumberLiteral)
	_, literalRHS := expr.RHS.(*parser.NumberLiteral)
	if literalLHS && literalRHS {
		return expr, false, fmt.Errorf("both operands of binary expression are number literals '%s'", expr)
	}

	lhsMapped, lhsFinished, err := i.MapExpr(expr.LHS, stats)
	if err != nil {
		return nil, false, err
	}
	rhsMapped, rhsFinished, err := i.MapExpr(expr.RHS, stats)
	if err != nil {
		return nil, false, err
	}
	// if query was split and at least one operand successfully finished, the binary operations is mapped.
	// The binary operands need to be wrapped in a parentheses' expression to ensure operator precedence.
	if stats.GetShardedQueries() > 0 && (lhsFinished || rhsFinished) {
		expr.LHS = &parser.ParenExpr{
			Expr: lhsMapped,
		}
		expr.RHS = &parser.ParenExpr{
			Expr: rhsMapped,
		}
		return expr, true, nil
	}
	return expr, false, nil
}

// mapParenExpr maps parenthesis expression expr
func (i *instantSplitter) mapParenExpr(expr *parser.ParenExpr, stats *MapperStats) (mapped parser.Expr, finished bool, err error) {
	parenExpr, finished, err := i.MapExpr(expr.Expr, stats)
	if err != nil {
		return nil, false, err
	}
	if !finished {
		return expr, false, nil
	}

	return &parser.ParenExpr{
		Expr:     parenExpr,
		PosRange: parser.PositionRange{},
	}, true, nil
}

// mapCall maps a function call if it's a range vector aggregator.
func (i *instantSplitter) mapCall(expr *parser.Call, stats *MapperStats) (mapped parser.Expr, finished bool, err error) {
	switch expr.Func.Name {
	case avgOverTime:
		return i.mapCallAvgOverTime(expr, stats)
	case countOverTime:
		return i.mapCallVectorAggregation(expr, stats, parser.SUM)
	case maxOverTime:
		return i.mapCallVectorAggregation(expr, stats, parser.MAX)
	case minOverTime:
		return i.mapCallVectorAggregation(expr, stats, parser.MIN)
	case rate:
		return i.mapCallRate(expr, stats)
	case sumOverTime:
		return i.mapCallVectorAggregation(expr, stats, parser.SUM)
	default:
		// Continue the mapping on child expressions.
		return expr, false, nil
	}
}

// mapCallAvgOverTime maps an avg_over_time function to expression sum_over_time / count_over_time
func (i *instantSplitter) mapCallAvgOverTime(expr *parser.Call, stats *MapperStats) (mapped parser.Expr, finished bool, err error) {
	avgOverTimeExpr := &parser.BinaryExpr{
		Op: parser.DIV,
		LHS: &parser.Call{
			Func:     parser.Functions[sumOverTime],
			Args:     expr.Args,
			PosRange: expr.PosRange,
		},
		RHS: &parser.Call{
			Func:     parser.Functions[countOverTime],
			Args:     expr.Args,
			PosRange: expr.PosRange,
		},
	}

	// If avg_over_time is wrapped by a vector aggregator,
	// the embedded query cannot be sent downstream
	if i.outerAggregationExpr != nil {
		i.outerAggregationExpr = nil
	}

	return i.MapExpr(avgOverTimeExpr, stats)
}

// mapCallRate maps a rate function to expression increase / rangeInterval
func (i *instantSplitter) mapCallRate(expr *parser.Call, stats *MapperStats) (mapped parser.Expr, finished bool, err error) {
	// In case the range interval is smaller than the configured split interval,
	// don't split it and don't map further nodes (finished=true).
	rangeInterval, canSplit := i.assertSplittableRangeInterval(expr)
	if !canSplit {
		return expr, true, nil
	}

	increaseExpr := &parser.Call{
		Func:     parser.Functions[increase],
		Args:     expr.Args,
		PosRange: expr.PosRange,
	}

	// If rate is wrapped by a vector aggregator,
	// the embedded query also needs to be updated to use increase
	if i.outerAggregationExpr != nil {
		updatedExpr := updateEmbeddedExpr(i.outerAggregationExpr, increaseExpr)
		if updatedExpr == nil {
			i.outerAggregationExpr = nil
		}
	}

	mappedExpr, finished, err := i.mapCallByRangeInterval(increaseExpr, stats, rangeInterval, parser.SUM)
	if err != nil {
		return nil, false, err
	}
	if !finished {
		return mapped, false, nil
	}

	return &parser.BinaryExpr{
		Op:  parser.DIV,
		LHS: mappedExpr,
		RHS: &parser.NumberLiteral{Val: rangeInterval.Seconds()},
	}, true, nil
}

func (i *instantSplitter) mapCallVectorAggregation(expr *parser.Call, stats *MapperStats, op parser.ItemType) (mapped parser.Expr, finished bool, err error) {
	// In case the range interval is smaller than the configured split interval,
	// don't split it and don't map further nodes (finished=true).
	rangeInterval, canSplit := i.assertSplittableRangeInterval(expr)
	if !canSplit {
		return expr, true, nil
	}

	return i.mapCallByRangeInterval(expr, stats, rangeInterval, op)
}

func (i *instantSplitter) mapCallByRangeInterval(expr *parser.Call, stats *MapperStats, rangeInterval time.Duration, op parser.ItemType) (mapped parser.Expr, finished bool, err error) {
	// Default grouping is 'without' for concatenating the embedded queries
	var grouping []string
	groupingWithout := true
	if i.outerAggregationExpr != nil {
		grouping = append(grouping, i.outerAggregationExpr.Grouping...)
		groupingWithout = i.outerAggregationExpr.Without
	}

	embeddedExpr, finished, err := i.splitAndSquashCall(expr, stats, rangeInterval)
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
// If the outer expression is a vector aggregator, r.outerAggregationExpr will contain the expression
// In this case, the vector aggregator should be downstream to the embedded queries in order to limit
// the label cardinality of the parallel queries
func (i *instantSplitter) splitAndSquashCall(expr *parser.Call, stats *MapperStats, rangeInterval time.Duration) (mapped parser.Expr, finished bool, err error) {
	splitCount := int(math.Ceil(float64(rangeInterval) / float64(i.interval)))
	if splitCount <= 1 {
		return expr, false, nil
	}

	var embeddedQuery parser.Expr = expr

	if i.outerAggregationExpr != nil {
		embeddedQuery = i.outerAggregationExpr
	}

	originalOffset := getOffset(expr)

	// Create a partial query for each split
	embeddedQueries := make([]parser.Expr, 0, splitCount)
	for split := 0; split < splitCount; split++ {
		splitOffset := time.Duration(split) * i.interval
		// The range interval of the last embedded query can be smaller than i.interval
		splitRangeInterval := i.interval
		if splitOffset+splitRangeInterval > rangeInterval {
			splitRangeInterval = rangeInterval - splitOffset
		}
		// The offset of the embedded queries is always the original offset + a multiple of i.interval
		splitOffset += originalOffset
		splitExpr, err := createSplitExpr(embeddedQuery, splitRangeInterval, splitOffset)
		if err != nil {
			return nil, false, err
		}

		// Prepend to embedded queries
		embeddedQueries = append([]parser.Expr{splitExpr}, embeddedQueries...)
	}

	squashExpr, err := vectorSquasher(embeddedQueries...)
	if err != nil {
		return nil, false, err
	}

	// Update stats
	stats.AddShardedQueries(splitCount)

	return squashExpr, true, nil
}

// assertSplittableRangeInterval returns the range interval specified in the input expr and whether it is greater than
// the configured split interval.
func (i *instantSplitter) assertSplittableRangeInterval(expr parser.Expr) (rangeInterval time.Duration, canSplit bool) {
	rangeInterval = getRangeInterval(expr)
	if rangeInterval > i.interval {
		return rangeInterval, true
	}

	level.Debug(i.logger).Log("msg", "unable to split expression because range interval is smaller than configured split interval", "expr", expr, "range_interval", rangeInterval, "split_interval", i.interval)
	return time.Duration(0), false
}

// getRangeInterval returns the range interval in the range vector expression
// Returns 0 if no range interval is found
// Example: expression `count_over_time({app="foo"}[10m])` returns 10m
func getRangeInterval(expr parser.Expr) time.Duration {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		return getRangeInterval(e.Expr)
	case *parser.Call:
		// Iterate over Call's arguments until a MatrixSelector and a valid range interval are found
		for _, arg := range e.Args {
			if argRangeInterval := getRangeInterval(arg); argRangeInterval != 0 {
				return argRangeInterval
			}
		}
		return time.Duration(0)
	case *parser.MatrixSelector:
		return e.Range
	default:
		// parser.SubqueryExpr and parser.BinaryExpr should return 0
		return 0
	}
}

// getOffset returns the offset interval in the range vector expression
// Returns 0 if no offset interval is found
// Examples:
//   * `count_over_time({app="foo"}[10m])` returns 0
//   * `count_over_time({app="foo"}[10m]) offset 1m` returns 1m
func getOffset(expr parser.Expr) time.Duration {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		return getOffset(e.Expr)
	case *parser.Call:
		// Iterate over Call's arguments until a VectorSelector and a valid offset are found
		for _, arg := range e.Args {
			if argRangeInterval := getOffset(arg); argRangeInterval > 0 {
				return argRangeInterval
			}
		}
		return time.Duration(0)
	case *parser.MatrixSelector:
		return getOffset(e.VectorSelector)
	case *parser.VectorSelector:
		return e.OriginalOffset
	default:
		return 0
	}
}

// expr can be a parser.Call or a parser.AggregateExpr
func createSplitExpr(expr parser.Expr, rangeInterval time.Duration, offset time.Duration) (parser.Expr, error) {
	splitExpr, err := cloneExpr(expr)
	if err != nil {
		return nil, err
	}
	rangeIntervalUpdated := updateRangeInterval(splitExpr, rangeInterval)
	if !rangeIntervalUpdated {
		return nil, fmt.Errorf("unable to update range interval on expression: %v", splitExpr)
	}
	offsetUpdated := updateOffset(splitExpr, offset)
	if !offsetUpdated {
		return nil, fmt.Errorf("unable to update offset operator on expression: %v", splitExpr)
	}

	return splitExpr, nil
}

// updateEmbeddedExpr returns the updated expression if inner call expression was updated successfully,
// otherwise returns nil
func updateEmbeddedExpr(expr parser.Expr, call *parser.Call) parser.Expr {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		e.Expr = updateEmbeddedExpr(e.Expr, call)
		return e
	case *parser.Call:
		return call
	case *parser.ParenExpr:
		return updateEmbeddedExpr(e.Expr, call)
	default:
		return nil
	}
}

// updateRangeInterval returns true if range interval was updated successfully,
// false otherwise
func updateRangeInterval(expr parser.Expr, rangeInterval time.Duration) bool {
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
func updateOffset(expr parser.Expr, offset time.Duration) bool {
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
