// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/prometheus/promql/parser"
	"github.com/prometheus/prometheus/promql/parser/posrange"
)

type instantSplitter struct {
	ctx context.Context

	interval time.Duration
	// In case of outer vector aggregator expressions, this contains the expression that will be used on the
	// downstream queries, i.e. the query that will be executed in parallel in each partial query.
	// This is an optimization to send outer vector aggregator expressions to reduce the label sets returned
	// by queriers, and therefore minimize the merging of results in the query-frontend.
	outerAggregationExpr *parser.AggregateExpr
	logger               log.Logger
	stats                *InstantSplitterStats
}

// Supported vector aggregators

var splittableVectorAggregators = map[parser.ItemType]bool{
	parser.MAX: true,
	parser.MIN: true,
	parser.SUM: true,
}

// Supported range vector aggregators

const (
	avgOverTime     = "avg_over_time"
	countOverTime   = "count_over_time"
	increase        = "increase"
	maxOverTime     = "max_over_time"
	minOverTime     = "min_over_time"
	presentOverTime = "present_over_time"
	rate            = "rate"
	sumOverTime     = "sum_over_time"
)

// cannotDoubleCountBoundaries is the list of functions that cannot double count the boundaries points when being split by range.
// Consider a timeline with samples at 1s,2s,3s,4s,5s...
// Prometheus ranges are closed on both ends, so range [2s,4s] includes samples at 2s and 4s.
// If we split that into [2s, 3s] and [3s, 4s], then we will include the sample at 3s in both ranges.
// This cannot be done for some sample-aggregating functions like `count_over_time`: we would count 4 samples (2s,3s,3s,4s) after splitting.
// However, for other functions, like `rate`, it is important to have both ends includes, as the rate is (start-end)/range.
var cannotDoubleCountBoundaries = map[string]bool{
	countOverTime: true,
	sumOverTime:   true,
}

// NewInstantQuerySplitter creates a new query range mapper.
func NewInstantQuerySplitter(ctx context.Context, interval time.Duration, logger log.Logger, stats *InstantSplitterStats) ASTMapper {
	instantQueryMapper := NewASTExprMapper(
		&instantSplitter{
			ctx:      ctx,
			interval: interval,
			logger:   logger,
			stats:    stats,
		},
	)

	return NewMultiMapper(
		instantQueryMapper,
		newSubtreeFolder(),
	)
}

// MapExpr returns expr mapped as embedded queries
func (i *instantSplitter) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := i.ctx.Err(); err != nil {
		return nil, false, err
	}

	// Immediately clone the expr to avoid mutating the original
	expr, err = cloneExpr(expr)
	if err != nil {
		return nil, false, err
	}

	switch e := expr.(type) {
	case *parser.AggregateExpr:
		return i.mapAggregatorExpr(e)
	case *parser.BinaryExpr:
		return i.mapBinaryExpr(e)
	case *parser.Call:
		if isSubqueryCall(e) {
			// Subqueries are currently not supported by splitting, so we stop the mapping here.
			i.stats.SetSkippedReason(SkippedReasonSubquery)
			return e, true, nil
		}

		return i.mapCall(e)
	case *parser.ParenExpr:
		return i.mapParenExpr(e)
	case *parser.SubqueryExpr:
		// Subqueries are currently not supported by splitting, so we stop the mapping here.
		i.stats.SetSkippedReason(SkippedReasonSubquery)
		return e, true, nil
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

// mapAggregatorExpr maps vector aggregator expression expr
func (i *instantSplitter) mapAggregatorExpr(expr *parser.AggregateExpr) (mapped parser.Expr, finished bool, err error) {
	var mappedExpr parser.Expr

	// If the outerAggregationExpr is not set, update it.
	// Note: vector aggregators avg, count and topk are supported but not splittable, so cannot be sent downstream.
	if i.outerAggregationExpr == nil && splittableVectorAggregators[expr.Op] {
		mappedExpr, finished, err = NewASTExprMapper(i.copyWithEmbeddedExpr(expr)).MapExpr(expr.Expr)
	} else {
		mappedExpr, finished, err = i.MapExpr(expr.Expr)
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
func (i *instantSplitter) mapBinaryExpr(expr *parser.BinaryExpr) (mapped parser.Expr, finished bool, err error) {
	// Binary expressions cannot be sent downstream, only their respective operands.
	// Therefore, the embedded aggregator expression needs to be reset.
	i.outerAggregationExpr = nil

	// Noop if both LHS and RHS are constant scalars.
	isLHSConst := isConstantScalar(expr.LHS)
	isRHSConst := isConstantScalar(expr.RHS)
	if isLHSConst && isRHSConst {
		return expr, true, nil
	}

	lhsMapped, lhsFinished, err := i.MapExpr(expr.LHS)
	if err != nil {
		return nil, false, err
	}
	rhsMapped, rhsFinished, err := i.MapExpr(expr.RHS)
	if err != nil {
		return nil, false, err
	}
	// if query was split and at least one operand successfully finished, the binary operations is mapped.
	// The binary operands need to be wrapped in a parentheses' expression to ensure operator precedence.
	if i.stats.GetSplitQueries() > 0 && (lhsFinished || rhsFinished) {
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
func (i *instantSplitter) mapParenExpr(expr *parser.ParenExpr) (mapped parser.Expr, finished bool, err error) {
	parenExpr, finished, err := i.MapExpr(expr.Expr)
	if err != nil {
		return nil, false, err
	}
	if !finished {
		return expr, false, nil
	}

	return &parser.ParenExpr{
		Expr:     parenExpr,
		PosRange: posrange.PositionRange{},
	}, true, nil
}

// mapCall maps a function call if it's a range vector aggregator.
func (i *instantSplitter) mapCall(expr *parser.Call) (mapped parser.Expr, finished bool, err error) {
	switch expr.Func.Name {
	case avgOverTime:
		return i.mapCallAvgOverTime(expr)
	case countOverTime:
		return i.mapCallVectorAggregation(expr, parser.SUM)
	case increase:
		return i.mapCallVectorAggregation(expr, parser.SUM)
	case maxOverTime:
		return i.mapCallVectorAggregation(expr, parser.MAX)
	case minOverTime:
		return i.mapCallVectorAggregation(expr, parser.MIN)
	case presentOverTime:
		// present_over_time returns the value 1 for any series in the specified interval,
		// therefore, using aggregator MAX enforces that all 1 values are returned.
		return i.mapCallVectorAggregation(expr, parser.MAX)
	case rate:
		return i.mapCallRate(expr)
	case sumOverTime:
		return i.mapCallVectorAggregation(expr, parser.SUM)
	default:
		// Continue the mapping on child expressions.
		return expr, false, nil
	}
}

// mapCallAvgOverTime maps an avg_over_time function to expression sum_over_time / count_over_time
func (i *instantSplitter) mapCallAvgOverTime(expr *parser.Call) (mapped parser.Expr, finished bool, err error) {
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

	return i.MapExpr(avgOverTimeExpr)
}

// mapCallRate maps a rate function to expression increase / rangeInterval
func (i *instantSplitter) mapCallRate(expr *parser.Call) (mapped parser.Expr, finished bool, err error) {
	// In case the range interval is smaller than the configured split interval,
	// don't split it and don't map further nodes (finished=true).
	rangeInterval, canSplit, err := i.assertSplittableRangeInterval(expr)
	if err != nil {
		return nil, false, err
	}
	if !canSplit {
		i.stats.SetSkippedReason(SkippedReasonSmallInterval)
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

	mappedExpr, finished, err := i.mapCallByRangeInterval(increaseExpr, rangeInterval, parser.SUM)
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

func (i *instantSplitter) mapCallVectorAggregation(expr *parser.Call, op parser.ItemType) (mapped parser.Expr, finished bool, err error) {
	// In case the range interval is smaller than the configured split interval,
	// don't split it and don't map further nodes (finished=true).
	rangeInterval, canSplit, err := i.assertSplittableRangeInterval(expr)
	if err != nil {
		return nil, false, err
	}
	if !canSplit {
		i.stats.SetSkippedReason(SkippedReasonSmallInterval)
		return expr, true, nil
	}

	return i.mapCallByRangeInterval(expr, rangeInterval, op)
}

func (i *instantSplitter) mapCallByRangeInterval(expr *parser.Call, rangeInterval time.Duration, op parser.ItemType) (mapped parser.Expr, finished bool, err error) {
	// Default grouping is 'without' for concatenating the embedded queries
	var grouping []string
	groupingWithout := true
	if i.outerAggregationExpr != nil {
		grouping = append(grouping, i.outerAggregationExpr.Grouping...)
		groupingWithout = i.outerAggregationExpr.Without
	}

	embeddedExpr, finished, err := i.splitAndSquashCall(expr, rangeInterval)
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
		PosRange: posrange.PositionRange{},
	}, true, nil
}

// expr is the range vector aggregator expression
// If the outer expression is a vector aggregator, r.outerAggregationExpr will contain the expression
// In this case, the vector aggregator should be downstream to the embedded queries in order to limit
// the label cardinality of the parallel queries
func (i *instantSplitter) splitAndSquashCall(expr *parser.Call, rangeInterval time.Duration) (mapped parser.Expr, finished bool, err error) {
	splitCount := int(math.Ceil(float64(rangeInterval) / float64(i.interval)))
	if splitCount <= 1 {
		return expr, false, nil
	}

	var embeddedQuery parser.Expr = expr

	if i.outerAggregationExpr != nil {
		embeddedQuery = i.outerAggregationExpr
	}

	originalOffset, err := i.assertOffset(expr)
	if err != nil {
		return nil, false, err
	}

	// Create a partial query for each split
	embeddedQueries := make([]EmbeddedQuery, 0, splitCount)
	for split := 0; split < splitCount; split++ {
		splitOffset := time.Duration(split) * i.interval
		// The range interval of the last embedded query can be smaller than i.interval
		splitRangeInterval := i.interval
		if lastSplit := split == splitCount-1; cannotDoubleCountBoundaries[expr.Func.Name] && !lastSplit {
			splitRangeInterval -= time.Millisecond
		}
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
		embeddedQueries = append([]EmbeddedQuery{NewEmbeddedQuery(splitExpr.String(), nil)}, embeddedQueries...)
	}

	squashExpr, err := VectorSquasher(embeddedQueries...)
	if err != nil {
		return nil, false, err
	}

	// Update stats
	i.stats.AddSplitQueries(splitCount)

	return squashExpr, true, nil
}

// assertSplittableRangeInterval returns the range interval specified in the input expr and whether it is greater than
// the configured split interval.
func (i *instantSplitter) assertSplittableRangeInterval(expr parser.Expr) (rangeInterval time.Duration, canSplit bool, err error) {
	rangeIntervals := getRangeIntervals(expr)
	if len(rangeIntervals) == 0 {
		return time.Duration(0), false, nil
	}
	if len(rangeIntervals) > 1 {
		return time.Duration(0), false, fmt.Errorf("found %d range intervals while expecting at most 1", len(rangeIntervals))
	}

	rangeInterval = rangeIntervals[0]
	if rangeInterval > i.interval {
		return rangeInterval, true, nil
	}

	level.Debug(i.logger).Log("msg", "unable to split expression because range interval is smaller than configured split interval", "expr", expr, "range_interval", rangeInterval, "split_interval", i.interval)
	return time.Duration(0), false, nil
}

// getRangeIntervals recursively visit the input expr and returns a slice containing all range intervals found.
func getRangeIntervals(expr parser.Expr) []time.Duration {
	// Due to how this function is used, we expect to always find at most 1 range interval
	// so we preallocate it accordingly.
	ranges := make([]time.Duration, 0, 1)

	// Ignore the error since we never return it.
	visitNode(expr, func(entry parser.Node) {
		switch e := entry.(type) {
		case *parser.MatrixSelector:
			ranges = append(ranges, e.Range)
		case *parser.SubqueryExpr:
			ranges = append(ranges, e.Range)
		}
	})

	return ranges
}

// assertOffset returns the offset specified in the input expr
// Note that the returned offset can be zero or negative
func (i *instantSplitter) assertOffset(expr parser.Expr) (offset time.Duration, err error) {
	offsets := getOffsets(expr)
	if len(offsets) == 0 {
		return time.Duration(0), nil
	}
	if len(offsets) > 1 {
		return time.Duration(0), fmt.Errorf("found %d offsets while expecting at most 1", len(offsets))
	}

	return offsets[0], nil
}

// getOffsets recursively visit the input expr and returns a slice containing all offsets found.
func getOffsets(expr parser.Expr) []time.Duration {
	// Due to how this function is used, we expect to always find at most 1 offset
	// so we preallocate it accordingly.
	offsets := make([]time.Duration, 0, 1)

	// Ignore the error since we never return it.
	visitNode(expr, func(entry parser.Node) {
		switch e := entry.(type) {
		case *parser.VectorSelector:
			offsets = append(offsets, e.OriginalOffset)
		case *parser.SubqueryExpr:
			offsets = append(offsets, e.OriginalOffset)
		}
	})

	return offsets
}

// expr can be a parser.Call or a parser.AggregateExpr
func createSplitExpr(expr parser.Expr, rangeInterval time.Duration, offset time.Duration) (parser.Expr, error) {
	splitExpr, err := cloneExpr(expr)
	if err != nil {
		return nil, err
	}
	if err = updateRangeInterval(splitExpr, rangeInterval); err != nil {
		return nil, err
	}
	if err = updateOffset(splitExpr, offset); err != nil {
		return nil, err
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

// updateRangeInterval modifies the input expr in-place and updates the range interval on the matrix selector.
// Returns an error if 0 or 2+ matrix selectors are found.
func updateRangeInterval(expr parser.Expr, rangeInterval time.Duration) error {
	if rangeInterval <= 0 {
		return fmt.Errorf("unable to update range interval on expression, because a negative interval %d was provided: %v", rangeInterval, expr)
	}

	updates := 0

	// Ignore the error since we never return it.
	visitNode(expr, func(entry parser.Node) {
		if matrix, ok := entry.(*parser.MatrixSelector); ok {
			matrix.Range = rangeInterval
			updates++
		}
	})

	if updates == 0 {
		return fmt.Errorf("unable to update range interval on expression, because no matrix selector has been found: %v", expr)
	}
	if updates > 1 {
		return fmt.Errorf("unable to update range interval on expression, because multiple matrix selectors have been found: %v", expr)
	}
	return nil
}

// updateOffset modifies the input expr in-place and updates the offset modifier on the vector selector.
// Returns an error if 0 or 2+ vector selectors are found.
func updateOffset(expr parser.Expr, offset time.Duration) error {
	updates := 0

	// Ignore the error since we never return it.
	visitNode(expr, func(entry parser.Node) {
		if vector, ok := entry.(*parser.VectorSelector); ok {
			vector.OriginalOffset = offset
			updates++
		}
	})

	if updates == 0 {
		return fmt.Errorf("unable to update offset modifier on expression, because no vector selector has been found: %v", expr)
	}
	if updates > 1 {
		return fmt.Errorf("unable to update offset modifier on expression, because multiple vector selectors have been found: %v", expr)
	}
	return nil
}
