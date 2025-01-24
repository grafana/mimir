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

// NewSubqueryExtractor creates a new instant query mapper.
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
		if sq, ok := e.Args[lastArgIdx].(*parser.SubqueryExpr); ok {
			// Filter out subqueries with offsets, not supported yet
			if sq.OriginalOffset > 0 {
				return downstreamQuery(expr)
			}

			// Filter out subqueries with ranges less than 1 hour as they are not worth spinning off.
			if sq.Range < 1*time.Hour {
				return downstreamQuery(expr)
			}

			selectorsCt := countSelectors(sq.Expr)

			// Evaluate constants within the frontend engine
			if selectorsCt == 0 {
				return expr, false, nil
			}

			// Filter out subqueries that are just selectors, they are fast enough that they aren't worth spinning off.
			if selectorsCt == 1 && !isComplexExpr(sq.Expr) {
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

			e.Args[lastArgIdx] = &parser.MatrixSelector{
				VectorSelector: selector,
				Range:          sq.Range,
			}
			m.stats.AddSpunOffSubquery()
			return e, true, nil
		}

		return downstreamQuery(expr)
	default:
		// If there's no subquery in the children, we can just
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
		return true
	default:
		for _, child := range parser.Children(e) {
			if hasSubqueryInChildren(child) {
				return true
			}
		}
		return false
	}
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
