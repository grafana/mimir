// SPDX-License-Identifier: AGPL-3.0-only

package astmapper

import (
	"context"

	"github.com/go-kit/log"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

func NewQueryPrunerMatcherPropagate(ctx context.Context, logger log.Logger) ASTMapper {
	pruner := newQueryPrunerMatcherPropagate(ctx, logger)
	return NewASTExprMapper(pruner)
}

type queryPrunerMatcherPropagate struct {
	ctx    context.Context
	logger log.Logger
}

func newQueryPrunerMatcherPropagate(ctx context.Context, logger log.Logger) *queryPrunerMatcherPropagate {
	return &queryPrunerMatcherPropagate{
		ctx:    ctx,
		logger: logger,
	}
}

func (pruner *queryPrunerMatcherPropagate) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := pruner.ctx.Err(); err != nil {
		return nil, false, err
	}

	e, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return expr, false, nil
	}

	_, _, boolResult := pruner.propagateMatchersInBinaryExpr(e)
	return e, boolResult, nil
}

func (pruner *queryPrunerMatcherPropagate) propagateMatchersInBinaryExpr(e *parser.BinaryExpr) ([]*parser.VectorSelector, []*labels.Matcher, bool) {
	if e.Op == parser.LOR || e.Op == parser.LUNLESS {
		return nil, nil, false
	}

	vssL, matchersL, okL := pruner.extractVectorSelectors(e.LHS)
	vssR, matchersR, okR := pruner.extractVectorSelectors(e.RHS)
	switch {
	case !okL && !okR:
		return nil, nil, false
	case !okL:
		return vssR, matchersR, true
	case !okR:
		return vssL, matchersL, true
	}

	if e.VectorMatching == nil {
		return nil, nil, false
	}

	newMatchersL := pruner.getMatchersToPropagate(matchersR, e.VectorMatching)
	newMatchersR := pruner.getMatchersToPropagate(matchersL, e.VectorMatching)
	for _, vsL := range vssL {
		vsL.LabelMatchers = combineMatchers(vsL.LabelMatchers, newMatchersL)
	}
	for _, vsR := range vssR {
		vsR.LabelMatchers = combineMatchers(vsR.LabelMatchers, newMatchersR)
	}
	vss := append(vssL, vssR...)
	matchers := combineMatchers(newMatchersL, newMatchersR)
	return vss, matchers, true
}

func (pruner *queryPrunerMatcherPropagate) extractVectorSelectors(expr parser.Expr) ([]*parser.VectorSelector, []*labels.Matcher, bool) {
	vs, ok := expr.(*parser.VectorSelector)
	if ok {
		return []*parser.VectorSelector{vs}, vs.LabelMatchers, true
	}

	pe, ok := expr.(*parser.ParenExpr)
	if ok {
		return pruner.extractVectorSelectors(pe.Expr)
	}

	be, ok := expr.(*parser.BinaryExpr)
	if ok {
		return pruner.propagateMatchersInBinaryExpr(be)
	}

	return nil, nil, false
}

func (pruner *queryPrunerMatcherPropagate) getMatchersToPropagate(matchersSrc []*labels.Matcher, vectorMatching *parser.VectorMatching) []*labels.Matcher {
	setLabels := make(map[string]struct{})
	for _, m := range vectorMatching.MatchingLabels {
		setLabels[m] = struct{}{}
	}

	matchersToAdd := make([]*labels.Matcher, 0, len(matchersSrc))
	for _, m := range matchersSrc {
		if isMetricNameMatcher(m) {
			continue
		}
		_, exists := setLabels[m.Name]
		if vectorMatching.On {
			if !exists {
				continue
			}
		} else {
			if exists {
				continue
			}
		}
		matchersToAdd = append(matchersToAdd, m)
	}

	return matchersToAdd
}

func combineMatchers(matchers, matchersToAdd []*labels.Matcher) []*labels.Matcher {
	matchersMap := makeMatchersMap(matchers)
	newMatchers := make([]*labels.Matcher, 0, len(matchers)+len(matchersToAdd))
	newMatchers = append(newMatchers, matchers...)
	for _, m := range matchersToAdd {
		if _, ok := matchersMap[m.String()]; !ok {
			newMatchers = append(newMatchers, m)
		}
	}
	return newMatchers
}

func isMetricNameMatcher(m *labels.Matcher) bool {
	return m.Name == labels.MetricName
}

func makeMatchersMap(matchers []*labels.Matcher) map[string]*labels.Matcher {
	matchersMap := make(map[string]*labels.Matcher, len(matchers))
	for _, m := range matchers {
		matchersMap[m.String()] = m
	}
	return matchersMap
}
