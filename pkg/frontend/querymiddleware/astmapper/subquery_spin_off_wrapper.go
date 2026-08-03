// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// SubquerySpinOffWrapper decides how the subquerySpinOffMapper represents the queries it spins off.
//
// The mapper itself is responsible for deciding which parts of a query should be spun off into separate
// subqueries and which should be passed through to the downstream execution path. It delegates the actual
// construction of the replacement expressions to a SubquerySpinOffWrapper, so that the same mapping logic
// can produce different representations depending on the caller. For example, the query-frontend middleware
// wraps them into fake metric selectors that are resolved by an injected Queryable.
type SubquerySpinOffWrapper interface {
	// WrapDownstreamQuery returns the expression to use in place of expr to execute it through the
	// downstream execution path rather than spinning it off.
	WrapDownstreamQuery(expr parser.Expr) (parser.Expr, error)

	// WrapSubquery returns the expression to use in place of the given subquery to spin it off into a
	// separate query. step is the resolved step of the subquery (the subquery's own step, or the default
	// step if it does not specify one).
	WrapSubquery(subquery *parser.SubqueryExpr, step time.Duration) (parser.Expr, error)
}

// selectorSubquerySpinOffWrapper is the SubquerySpinOffWrapper used by the query-frontend middleware.
// It wraps downstream queries and spun-off subqueries into fake metric selectors (__downstream_query__
// and __subquery_spinoff__ respectively) that are resolved by a Queryable injected into the engine.
type selectorSubquerySpinOffWrapper struct{}

// NewSelectorSubquerySpinOffWrapper returns a SubquerySpinOffWrapper that wraps downstream queries and
// spun-off subqueries into fake metric selectors resolved by an injected Queryable.
func NewSelectorSubquerySpinOffWrapper() SubquerySpinOffWrapper {
	return selectorSubquerySpinOffWrapper{}
}

func (selectorSubquerySpinOffWrapper) WrapDownstreamQuery(expr parser.Expr) (parser.Expr, error) {
	return &parser.VectorSelector{
		Name: DownstreamQueryMetricName,
		LabelMatchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, DownstreamQueryMetricName),
			labels.MustNewMatcher(labels.MatchEqual, DownstreamQueryLabelName, expr.String()),
		},
	}, nil
}

func (selectorSubquerySpinOffWrapper) WrapSubquery(subquery *parser.SubqueryExpr, step time.Duration) (parser.Expr, error) {
	selector := &parser.VectorSelector{
		Name: SubqueryMetricName,
		LabelMatchers: []*labels.Matcher{
			labels.MustNewMatcher(labels.MatchEqual, model.MetricNameLabel, SubqueryMetricName),
			labels.MustNewMatcher(labels.MatchEqual, SubqueryQueryLabelName, subquery.Expr.String()),
			labels.MustNewMatcher(labels.MatchEqual, SubqueryRangeLabelName, subquery.Range.String()),
			labels.MustNewMatcher(labels.MatchEqual, SubqueryStepLabelName, step.String()),
		},
	}

	if subquery.OriginalOffset != 0 {
		selector.LabelMatchers = append(selector.LabelMatchers, labels.MustNewMatcher(labels.MatchEqual, SubqueryOffsetLabelName, subquery.OriginalOffset.String()))
		selector.OriginalOffset = subquery.OriginalOffset
	}

	return &parser.MatrixSelector{
		VectorSelector: selector,
		Range:          subquery.Range,
	}, nil
}
