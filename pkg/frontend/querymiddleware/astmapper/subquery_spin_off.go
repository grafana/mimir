// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"
	"time"

	"github.com/go-kit/log"
	"github.com/pkg/errors"
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
	wrapper         SubquerySpinOffWrapper
	defaultStepFunc func(rangeMillis int64) int64

	logger log.Logger
	stats  *SubquerySpinOffMapperStats
}

// NewSubquerySpinOffMapper creates a new instant query mapper.
//
// wrapper controls how the spun-off subqueries and downstream queries are represented in the mapped
// query.
func NewSubquerySpinOffMapper(wrapper SubquerySpinOffWrapper, defaultStepFunc func(rangeMillis int64) int64, logger log.Logger, stats *SubquerySpinOffMapperStats) ASTMapper {
	queryMapper := NewASTExprMapper(
		&subquerySpinOffMapper{
			wrapper:         wrapper,
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
// Subqueries that are worth spinning off are handed to the wrapper's WrapSubquery; everything else is passed through to
// the downstream execution path via the wrapper's WrapDownstreamQuery.
// How those spun-off subqueries and downstream queries are represented in the mapped query is up to the wrapper.
// For example, the query-frontend middleware uses the selectorSubquerySpinOffWrapper, which remaps them into the
// "fake metric" selectors "__subquery_spinoff__" and "__downstream_query__" that are resolved by a Queryable injected
// into the engine, because the frontend does not have internal control of the engine.
//
// See sharding.go and embedded.go for another example of mapping into a fake metric selector.
func (m *subquerySpinOffMapper) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	// Immediately clone the expr to avoid mutating the original
	expr, err = CloneExpr(expr)
	if err != nil {
		return nil, false, err
	}

	downstreamQuery := func(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
		if countSelectors(expr) == 0 {
			return expr, false, nil
		}
		m.stats.AddDownstreamQuery()
		return m.wrapper.WrapDownstreamQuery(expr), false, nil
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

			e.Args[lastArgIdx] = m.wrapper.WrapSubquery(sq, step)
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
