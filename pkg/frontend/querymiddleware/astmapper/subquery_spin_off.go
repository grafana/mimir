// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

const (
	SubqueryMetricName      = "__subquery_spinoff__"
	SubqueryQueryLabelName  = "__query__"
	SubqueryRangeLabelName  = "__range__"
	SubqueryStepLabelName   = "__step__"
	SubqueryOffsetLabelName = "__offset__"

	DownstreamQueryMetricName = "__downstream_query__"
	DownstreamQueryLabelName  = "__query__"
)

type subquerySpinOffMapper struct {
	ctx             context.Context
	defaultStepFunc func(rangeMillis int64) int64

	logger log.Logger
	stats  *SubquerySpinOffMapperStats
}

// NewSubquerySpinOffMapper creates a new instant query mapper.
func NewSubquerySpinOffMapper(ctx context.Context, defaultStepFunc func(rangeMillis int64) int64, logger log.Logger, stats *SubquerySpinOffMapperStats) ASTMapper {
	queryMapper := NewASTExprMapper(
		&subquerySpinOffMapper{
			ctx:             ctx,
			defaultStepFunc: defaultStepFunc,
			logger:          logger,
			stats:           stats,
		},
	)

	return NewMultiMapper(
		queryMapper,
	)
}

// MapExpr implements the ASTMapper interface.
// The strategy here is to look for aggregated subqueries (all subqueries should be aggregated) and spin them off into separate queries.
// The frontend does not have internal control of the engine,
// so MapExpr has to remap subqueries into "fake metrics" that can be queried by a Queryable that we can inject into the engine.
// This "fake metric selector" is the "__subquery_spinoff__" metric.
// For everything else, we have to pass it through to the downstream execution path (other instant middlewares),
// so we remap them into a "__downstream_query__" selector.
//
// See sharding.go and embedded.go for another example of mapping into a fake metric selector.
func (m *subquerySpinOffMapper) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := m.ctx.Err(); err != nil {
		return nil, false, err
	}

	// Immediately clone the expr to avoid mutating the original
	expr, err = cloneExpr(expr)
	if err != nil {
		return nil, false, err
	}

	downstreamQuery := func(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
		if countSelectors(expr) == 0 {
			return expr, false, nil
		}
		selector := &parser.VectorSelector{
			Name: DownstreamQueryMetricName,
			LabelMatchers: []*labels.Matcher{
				labels.MustNewMatcher(labels.MatchEqual, DownstreamQueryLabelName, expr.String()),
			},
		}
		m.stats.AddDownstreamQuery()
		return selector, false, nil
	}

	switch e := expr.(type) {
	case *parser.Call:
		if len(e.Args) == 0 {
			return expr, false, nil
		}
		lastArgIdx := len(e.Args) - 1
		// The last argument will typically contain the subquery in an aggregation function
		// Examples: last_over_time(<subquery>[5m:]) or quantile_over_time(0.5, <subquery>[5m:])
		if sq, ok := e.Args[lastArgIdx].(*parser.SubqueryExpr); ok {
			canBeSpunOff, isConstant := subqueryCanBeSpunOff(*sq)
			if isConstant {
				return expr, false, nil
			}
			if !canBeSpunOff {
				return downstreamQuery(expr)
			}

			step := sq.Step
			if step == 0 {
				if m.defaultStepFunc == nil {
					return nil, false, errors.New("defaultStepFunc is not set")
				}
				step = time.Duration(m.defaultStepFunc(sq.Range.Milliseconds())) * time.Millisecond
			}

			// Filter out subqueries with less than 10 steps as they are not worth spinning off.
			numberOfSteps := int(sq.Range / step)
			if numberOfSteps < 10 {
				return downstreamQuery(expr)
			}

			selector := &parser.VectorSelector{
				Name: SubqueryMetricName,
				LabelMatchers: []*labels.Matcher{
					labels.MustNewMatcher(labels.MatchEqual, SubqueryQueryLabelName, sq.Expr.String()),
					labels.MustNewMatcher(labels.MatchEqual, SubqueryRangeLabelName, sq.Range.String()),
					labels.MustNewMatcher(labels.MatchEqual, SubqueryStepLabelName, step.String()),
				},
			}

			if sq.OriginalOffset != 0 {
				selector.LabelMatchers = append(selector.LabelMatchers, labels.MustNewMatcher(labels.MatchEqual, SubqueryOffsetLabelName, sq.OriginalOffset.String()))
				selector.OriginalOffset = sq.OriginalOffset
			}

			e.Args[lastArgIdx] = &parser.MatrixSelector{
				VectorSelector: selector,
				Range:          sq.Range,
			}
			m.stats.AddSpunOffSubquery()
			return e, true, nil
		}

		return downstreamQuery(expr)
	default:
		// If we encounter a parallelizable expression, we can pass it through to the downstream execution path.
		// Querysharding is proven to be fast enough that we don't need to spin off subqueries.
		if CanParallelize(expr, m.logger) {
			return downstreamQuery(expr)
		}
		// If there's no subquery in the children, we can abort early and pass the expression through to the downstream execution path.
		if !hasSubqueryInChildren(expr) {
			return downstreamQuery(expr)
		}
		return expr, false, nil
	}
}

func isComplexExpr(expr parser.Node) bool {
	switch e := expr.(type) {
	case *parser.SubqueryExpr:
		return true
	case *parser.AggregateExpr:
		return countSelectors(e.Expr) > 0
	case *parser.Call:
		for _, arg := range e.Args {
			if _, ok := arg.(*parser.MatrixSelector); ok || isComplexExpr(arg) {
				return true
			}
		}
		return false
	default:
		for _, child := range parser.Children(e) {
			if isComplexExpr(child) {
				return true
			}
		}
		return false
	}
}

func hasSubqueryInChildren(expr parser.Node) bool {
	switch e := expr.(type) {
	case *parser.SubqueryExpr:
		canBeSpunOff, _ := subqueryCanBeSpunOff(*e)
		return canBeSpunOff
	default:
		for _, child := range parser.Children(e) {
			if hasSubqueryInChildren(child) {
				return true
			}
		}
		return false
	}
}

func subqueryCanBeSpunOff(sq parser.SubqueryExpr) (spinoff, constant bool) {
	// @ is not supported
	if sq.StartOrEnd != 0 || sq.Timestamp != nil {
		return false, false
	}

	// Filter out subqueries with ranges less than 1 hour as they are not worth spinning off.
	if sq.Range < 1*time.Hour {
		return false, false
	}

	selectorsCt := countSelectors(sq.Expr)

	// Evaluate constants within the frontend engine
	if selectorsCt == 0 {
		return false, true
	}

	// Filter out subqueries that are just selectors, they are fast enough that they aren't worth spinning off.
	if selectorsCt == 1 && !isComplexExpr(sq.Expr) {
		return false, false
	}

	return true, false
}

func countSelectors(expr parser.Node) int {
	switch e := expr.(type) {
	case *parser.VectorSelector, *parser.MatrixSelector:
		return 1
	default:
		count := 0
		for _, child := range parser.Children(e) {
			count += countSelectors(child)
		}
		return count
	}
}
