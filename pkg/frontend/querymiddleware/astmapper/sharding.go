// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/astmapper/shard_summer.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package astmapper

import (
	"context"
	"fmt"
	"slices"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/storage/sharding"
)

type Squasher interface {
	Squash(...EmbeddedQuery) (parser.Expr, error)
}

type ShardLabeller interface {
	GetLabelMatcher(shard int) (*labels.Matcher, error)
	GetParams(shard int) map[string]string
}

// queryShardLabeller implements ShardLabeller for query sharding.
type queryShardLabeller struct {
	shards int
}

func newQueryShardLabeller(shards int) ShardLabeller {
	return &queryShardLabeller{shards: shards}
}

func (lbl *queryShardLabeller) GetLabelMatcher(shard int) (*labels.Matcher, error) {
	return labels.NewMatcher(labels.MatchEqual, sharding.ShardLabel, sharding.ShardSelector{ShardIndex: uint64(shard), ShardCount: uint64(lbl.shards)}.LabelValue())
}

func (lbl *queryShardLabeller) GetParams(_ int) map[string]string {
	return nil
}

// NewQueryShardSummer instantiates an ASTMapper which will fan out sum queries by shard.
func NewQueryShardSummer(shards int, squasher Squasher, logger log.Logger, stats *MapperStats) ASTMapper {
	return NewShardSummerWithLabeller(shards, squasher, logger, stats, newQueryShardLabeller(shards))
}

func NewShardSummerWithLabeller(shards int, squasher Squasher, logger log.Logger, stats *MapperStats, labeller ShardLabeller) ASTMapper {
	summer := newShardSummer(shards, squasher, logger, stats, labeller)
	return NewASTExprMapper(summer)
}

type shardSummer struct {
	shards       int
	currentShard *int
	squasher     Squasher
	logger       log.Logger
	stats        *MapperStats

	shardLabeller ShardLabeller

	analyzer *ShardingAnalyzer
}

func newShardSummer(shards int, squasher Squasher, logger log.Logger, stats *MapperStats, shardLabeller ShardLabeller) *shardSummer {
	return &shardSummer{
		shards:       shards,
		squasher:     squasher,
		currentShard: nil,
		logger:       logger,
		stats:        stats,

		shardLabeller: shardLabeller,

		// FIXME: if we already have an analyzer we've used for the expression we're going to shard, we could reuse it here
		// and make use of its cached results.
		// Most expressions won't trigger the need for analysis during sharding, and passing the analyzer around adds some
		// extra complexity, so we haven't bothered to do this yet.
		analyzer: NewShardingAnalyzer(logger),
	}
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
func (summer *shardSummer) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	switch e := expr.(type) {
	case *parser.AggregateExpr:
		if summer.currentShard != nil {
			return e, false, nil
		}
		if CanParallelize(e, summer.logger) {
			return summer.shardAggregate(ctx, e)
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
				return summer.shardAndSquashFuncCall(ctx, e)
			}

			analysis, err := summer.analyzer.Analyze(expr)
			if err != nil {
				return nil, true, err
			}

			return e, !analysis.WillShardAllSelectors, nil
		}

		return e, false, nil

	case *parser.BinaryExpr:
		if summer.currentShard != nil {
			return e, false, nil
		}

		// If we can parallelize the whole binary operation then just do it.
		if CanParallelize(e, summer.logger) {
			return summer.shardBinOp(ctx, e)
		}

		// We can't parallelize the whole binary operation but we could still parallelize
		// at least one of the two legs. However, if we parallelize only one of the two legs
		// then fetching results from the other (non parallelized) leg could be very expensive
		// because it could result in very high cardinality results to fetch from querier and
		// process in the query-frontend. Since we can't estimate the cardinality, we prefer
		// to be pessimistic and not parallelize at all the two legs unless we're able to
		// parallelize all vector selectors in the legs.
		analysis, err := summer.analyzer.Analyze(e)
		return e, !analysis.WillShardAllSelectors, err

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

type ShardingAnalyzer struct {
	logger        log.Logger
	analysisCache map[parser.Expr]ShardingAnalysisResult
}

type ShardingAnalysisResult struct {
	// WillShardAllSelectors is true if all selectors in the expression will be sharded,
	// or if there are no selectors in the expression.
	WillShardAllSelectors bool

	// The number of sharded selectors in the final expression.
	// - If the original query was sum(foo), this would be 1.
	// - If the original query was sum(foo) + bar, this would be 0, as the bar selector won't be sharded and so the sum is not sharded.
	// - If the original query was sum(foo) + sum(bar), this would be 2.
	// - If the original query was avg(foo), this would be 2, as avg(foo) is rewritten as sum(foo) / count(foo).
	//
	// If WillShardAllSelectors is false, then this will be 0.
	ShardedSelectors int
}

func UnshardedExpression() ShardingAnalysisResult {
	return ShardingAnalysisResult{
		WillShardAllSelectors: false,
		ShardedSelectors:      0,
	}
}

func ShardedExpression(shardedSelectors int) ShardingAnalysisResult {
	return ShardingAnalysisResult{
		WillShardAllSelectors: true,
		ShardedSelectors:      shardedSelectors,
	}
}

func NewShardingAnalyzer(logger log.Logger) *ShardingAnalyzer {
	return &ShardingAnalyzer{
		logger:        logger,
		analysisCache: make(map[parser.Expr]ShardingAnalysisResult),
	}
}

// Analyze analyzes the given expression and returns true if all selectors in expr will be sharded,
// and the number of shardable queries in the sharded expression.
//
// This differs from CanParallelize, which returns true if it is valid to shard the expr.
//
// For example, it is valid to shard a vector selector (so CanParallelize returns true), but we
// don't bother doing this because it has no benefit (so Analyze returns false).
//
// However, if the expression was a sum aggregation over a vector selector, for example, then
// both methods would return true.
func (analyzer *ShardingAnalyzer) Analyze(expr parser.Expr) (result ShardingAnalysisResult, err error) {
	// We need to cache the results of this function to avoid processing it again and again
	// in queries like `a or b or c or d`, which would lead to exponential processing time.
	if cached, ok := analyzer.analysisCache[expr]; ok {
		return cached, nil
	}
	defer func() {
		if err == nil {
			analyzer.analysisCache[expr] = result
		}
	}()

	switch expr := expr.(type) {
	case *parser.VectorSelector, *parser.MatrixSelector:
		return UnshardedExpression(), nil
	case *parser.ParenExpr:
		return analyzer.Analyze(expr.Expr)
	case *parser.StepInvariantExpr:
		return analyzer.Analyze(expr.Expr)
	case *parser.AggregateExpr:
		if CanParallelize(expr, analyzer.logger) {
			selectorCount := 1

			if expr.Op == parser.AVG {
				// We'll rewrite avg(foo) as sum(foo) / count(foo), so we need to account for both selectors.
				selectorCount = 2
			}

			return ShardedExpression(selectorCount), nil
		}

		// If we can't shard this expression, we might still be able to shard the inner expression.
		// eg. in avg(count(test)), we can't shard the avg(), but we can shard the count().
		exprResult, err := analyzer.Analyze(expr.Expr)
		if err != nil {
			return UnshardedExpression(), err
		}
		if !exprResult.WillShardAllSelectors {
			return UnshardedExpression(), nil
		}

		if expr.Param != nil {
			paramResult, err := analyzer.Analyze(expr.Param)
			if err != nil {
				return UnshardedExpression(), err
			}
			if !paramResult.WillShardAllSelectors {
				return UnshardedExpression(), nil
			}

			exprResult.ShardedSelectors += paramResult.ShardedSelectors
		}

		return exprResult, nil

	case *parser.BinaryExpr:
		if isShardableBinOp(expr) && CanParallelize(expr, analyzer.logger) {
			return ShardedExpression(1), nil
		}

		lhs, err := analyzer.Analyze(expr.LHS)
		if err != nil {
			return UnshardedExpression(), err
		}
		if !lhs.WillShardAllSelectors {
			return UnshardedExpression(), nil
		}

		rhs, err := analyzer.Analyze(expr.RHS)
		if err != nil {
			return UnshardedExpression(), err
		}

		if !rhs.WillShardAllSelectors {
			return UnshardedExpression(), nil
		}

		return ShardedExpression(lhs.ShardedSelectors + rhs.ShardedSelectors), nil

	case *parser.Call:
		if isSubqueryCall(expr) {
			if containsAggregateExpr(expr) {
				return UnshardedExpression(), nil
			}

			if !CanParallelize(expr, analyzer.logger) {
				return UnshardedExpression(), nil
			}

			return ShardedExpression(1), nil
		}

		for _, arg := range argsWithDefaults(expr) {
			if result, err := analyzer.Analyze(arg); err != nil {
				return UnshardedExpression(), err
			} else if !result.WillShardAllSelectors {
				return UnshardedExpression(), nil
			}
		}

		return ShardedExpression(countSelectors(expr)), nil

	case *parser.NumberLiteral, *parser.StringLiteral:
		return ShardedExpression(0), nil

	case *parser.UnaryExpr:
		return analyzer.Analyze(expr.Expr)

	case *parser.SubqueryExpr:
		// If we've reached this point, then the subquery is not shardable.
		// If the subquery was shardable, we would have returned true when examining the outer function call.
		return UnshardedExpression(), nil

	default:
		return UnshardedExpression(), fmt.Errorf("unexpected expression type %T in ShardingAnalyzer.Analyze", expr)
	}
}

// shardAndSquashFuncCall shards the given function call by cloning it and adding the shard label to the most outer matrix/vector selector.
func (summer *shardSummer) shardAndSquashFuncCall(ctx context.Context, expr *parser.Call) (mapped parser.Expr, finished bool, err error) {
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

	summer.stats.AddShardedQueries(summer.shards)

	children := make([]EmbeddedQuery, 0, summer.shards)

	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		cloned, err := CloneExpr(expr)
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
				subExprMapped, err := subSummer.Map(ctx, typedArg.Expr)
				if err != nil {
					return nil, true, err
				}
				typedArg.Expr = subExprMapped
				clonedCall.Args[i] = arg
			default:
				mapped, err := subSummer.Map(ctx, typedArg)
				if err != nil {
					return nil, true, err
				}
				clonedCall.Args[i] = mapped
			}
		}

		children = append(children, NewEmbeddedQuery(clonedCall, summer.shardLabeller.GetParams(i)))
	}

	squashed, err := summer.squasher.Squash(children...)
	if err != nil {
		return nil, true, err
	}
	return squashed, true, nil
}

// shardAggregate attempts to shard the given aggregation expression.
func (summer *shardSummer) shardAggregate(ctx context.Context, expr *parser.AggregateExpr) (mapped parser.Expr, finished bool, err error) {
	switch expr.Op {
	case parser.GROUP:
		mapped, err = summer.shardGroup(ctx, expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.SUM:
		mapped, err = summer.shardSum(ctx, expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.COUNT:
		mapped, err = summer.shardCount(ctx, expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.MAX, parser.MIN:
		mapped, err = summer.shardMinMax(ctx, expr)
		if err != nil {
			return nil, false, err
		}
		return mapped, true, nil
	case parser.AVG:
		mapped, err = summer.shardAvg(ctx, expr)
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
func (summer *shardSummer) shardGroup(ctx context.Context, expr *parser.AggregateExpr) (result *parser.AggregateExpr, err error) {
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

	summer.stats.AddShardedQueries(summer.shards)

	// Create a GROUP sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(ctx, expr, parser.GROUP)
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
		PosRange: expr.PosRange,
	}, nil
}

// shardSum attempts to shard the given SUM aggregation expression.
func (summer *shardSummer) shardSum(ctx context.Context, expr *parser.AggregateExpr) (result *parser.AggregateExpr, err error) {
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

	summer.stats.AddShardedQueries(summer.shards)

	// Create a SUM sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(ctx, expr, parser.SUM)
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
		PosRange: expr.PosRange,
	}, nil
}

// shardCount attempts to shard the given COUNT aggregation expression.
func (summer *shardSummer) shardCount(ctx context.Context, expr *parser.AggregateExpr) (result *parser.AggregateExpr, err error) {
	summer.stats.AddShardedQueries(summer.shards)

	// The COUNT aggregation can be parallelized as the SUM of per-shard COUNT.
	// Create a COUNT sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(ctx, expr, parser.COUNT)
	if err != nil {
		return nil, err
	}

	return &parser.AggregateExpr{
		Op:       parser.SUM,
		Expr:     sharded,
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
		PosRange: expr.PosRange,
	}, nil
}

// shardMinMax attempts to shard the given MIN/MAX aggregation expression.
func (summer *shardSummer) shardMinMax(ctx context.Context, expr *parser.AggregateExpr) (result parser.Expr, err error) {
	// We expect the given aggregation is either a MIN or MAX.
	if expr.Op != parser.MIN && expr.Op != parser.MAX {
		return nil, errors.Errorf("expected MIN or MAX aggregation while got %s", expr.Op.String())
	}

	summer.stats.AddShardedQueries(summer.shards)

	// The MIN/MAX aggregation can be parallelized as the MIN/MAX of per-shard MIN/MAX.
	// Create a MIN/MAX sub-query for each shard and squash it into a CONCAT expression.
	sharded, err := summer.shardAndSquashAggregateExpr(ctx, expr, expr.Op)
	if err != nil {
		return nil, err
	}

	return &parser.AggregateExpr{
		Op:       expr.Op,
		Expr:     sharded,
		Param:    expr.Param,
		Grouping: expr.Grouping,
		Without:  expr.Without,
		PosRange: expr.PosRange,
	}, nil
}

// shardAvg attempts to shard the given AVG aggregation expression.
func (summer *shardSummer) shardAvg(ctx context.Context, expr *parser.AggregateExpr) (result parser.Expr, err error) {
	// The AVG aggregation can be parallelized as per-shard SUM() divided by per-shard COUNT().
	sumExpr, err := summer.shardSum(ctx, expr)
	if err != nil {
		return nil, err
	}

	countExpr, err := summer.shardCount(ctx, expr)
	if err != nil {
		return nil, err
	}

	return &parser.ParenExpr{
		Expr: &parser.BinaryExpr{
			Op:             parser.DIV,
			LHS:            sumExpr,
			RHS:            countExpr,
			VectorMatching: &parser.VectorMatching{},
		},
	}, nil
}

// shardAndSquashAggregateExpr returns a squashed CONCAT expression including N embedded
// queries, where N is the number of shards and each sub-query queries a different shard
// with the given "op" aggregation operation.
func (summer *shardSummer) shardAndSquashAggregateExpr(ctx context.Context, expr *parser.AggregateExpr, op parser.ItemType) (parser.Expr, error) {
	children := make([]EmbeddedQuery, 0, summer.shards)

	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		sharded, err := cloneAndMap(ctx, NewASTExprMapper(summer.CopyWithCurShard(i)), expr.Expr)
		if err != nil {
			return nil, err
		}

		// Create the child expression, which runs the given aggregation operation
		// on a single shard. We need to preserve the grouping as it was
		// in the original one.
		aggExpr := &parser.AggregateExpr{
			Op:       op,
			Expr:     sharded,
			Without:  expr.Without,
			PosRange: expr.PosRange,

			// Clone the grouping slice, as MQE's aggregation operator mutates it to sort it and add __name__ if needed.
			// We don't need to clone this slice for the aggregate expression that wraps this one (created by the caller of shardAndSquashAggregateExpr),
			// as no other expression nodes will reference that slice, so it's safe for that one to be mutated.
			Grouping: slices.Clone(expr.Grouping),
		}
		children = append(children, NewEmbeddedQuery(aggExpr, summer.shardLabeller.GetParams(i)))
	}

	return summer.squasher.Squash(children...)
}

func isShardableBinOp(expr *parser.BinaryExpr) bool {
	switch expr.Op {
	case parser.GTR,
		parser.GTE,
		parser.LSS,
		parser.LTE:
		return true
	default:
		return false
	}
}

// shardBinOp attempts to shard the given binary operation expression.
func (summer *shardSummer) shardBinOp(ctx context.Context, expr *parser.BinaryExpr) (mapped parser.Expr, finished bool, err error) {
	if !isShardableBinOp(expr) {
		return expr, false, nil
	}

	mapped, err = summer.shardAndSquashBinOp(ctx, expr)
	if err != nil {
		return nil, false, err
	}
	return mapped, true, nil
}

// shardAndSquashBinOp returns a squashed CONCAT expression including N embedded
// queries, where N is the number of shards and each sub-query queries a different shard
// with the same binary operation.
func (summer *shardSummer) shardAndSquashBinOp(ctx context.Context, expr *parser.BinaryExpr) (parser.Expr, error) {
	if expr.VectorMatching != nil {
		// We shouldn't ever reach this point with a vector matching binary expression,
		// but it's better to check twice than completely mess it up with the results.
		return nil, fmt.Errorf("tried to shard a bin op with vector matching: %s", expr)
	}

	summer.stats.AddShardedQueries(summer.shards)
	children := make([]EmbeddedQuery, 0, summer.shards)
	// Create sub-query for each shard.
	for i := 0; i < summer.shards; i++ {
		shardedLHS, err := cloneAndMap(ctx, NewASTExprMapper(summer.CopyWithCurShard(i)), expr.LHS)
		if err != nil {
			return nil, err
		}
		shardedRHS, err := cloneAndMap(ctx, NewASTExprMapper(summer.CopyWithCurShard(i)), expr.RHS)
		if err != nil {
			return nil, err
		}

		binExpr := &parser.BinaryExpr{
			LHS:        shardedLHS,
			Op:         expr.Op,
			RHS:        shardedRHS,
			ReturnBool: expr.ReturnBool,
			// We don't need to set VectorMatching here: we expect that this method is only called for binary expressions
			// that are not vector/vector expressions.
		}
		children = append(children, NewEmbeddedQuery(binExpr, summer.shardLabeller.GetParams(i)))
	}

	return summer.squasher.Squash(children...)
}

func (summer *shardSummer) shardVectorSelector(selector *parser.VectorSelector) (parser.Expr, error) {
	shardMatcher, err := summer.shardLabeller.GetLabelMatcher(*summer.currentShard)
	if err != nil {
		return nil, err
	}
	if shardMatcher == nil {
		return selector, nil
	}

	shardedExpr, err := CloneExpr(selector)
	if err != nil {
		return nil, err
	}

	shardedSelector, ok := shardedExpr.(*parser.VectorSelector)
	if !ok {
		return nil, fmt.Errorf("expected cloneExpr to return a VectorSelector, got %T, this is a bug", shardedExpr)
	}

	shardedSelector.LabelMatchers = append(
		[]*labels.Matcher{shardMatcher},
		selector.LabelMatchers...,
	)

	return shardedSelector, nil
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
