// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// PropagateMatchers optimizes queries by propagating matchers across binary operations.
type PropagateMatchers struct{}

func (p *PropagateMatchers) Name() string {
	return "Matcher propagation across binary operations"
}

func (p *PropagateMatchers) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	mapper := &propagateMatchers{
		ctx: ctx,
	}
	ASTExprMapper := astmapper.NewASTExprMapper(mapper)
	return ASTExprMapper.Map(expr)
}

type propagateMatchers struct {
	ctx context.Context
}

func (mapper *propagateMatchers) MapExpr(expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	if err := mapper.ctx.Err(); err != nil {
		return nil, false, err
	}

	e, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return expr, false, nil
	}

	_, _, boolResult := mapper.propagateMatchersInBinaryExpr(e)
	return e, boolResult, nil
}

func (mapper *propagateMatchers) propagateMatchersInBinaryExpr(e *parser.BinaryExpr) ([]*parser.VectorSelector, []*labels.Matcher, bool) {
	if e.Op == parser.LOR || e.Op == parser.LUNLESS {
		return nil, nil, false
	}

	vssL, matchersL, okL := mapper.extractVectorSelectors(e.LHS)
	vssR, matchersR, okR := mapper.extractVectorSelectors(e.RHS)
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

	newMatchersL := mapper.getMatchersToPropagate(matchersR, e.VectorMatching.MatchingLabels, e.VectorMatching.On)
	newMatchersR := mapper.getMatchersToPropagate(matchersL, e.VectorMatching.MatchingLabels, e.VectorMatching.On)
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

func (mapper *propagateMatchers) extractVectorSelectors(expr parser.Expr) ([]*parser.VectorSelector, []*labels.Matcher, bool) {
	vs, ok := expr.(*parser.VectorSelector)
	if ok {
		return []*parser.VectorSelector{vs}, vs.LabelMatchers, true
	}

	pe, ok := expr.(*parser.ParenExpr)
	if ok {
		return mapper.extractVectorSelectors(pe.Expr)
	}

	agg, ok := expr.(*parser.AggregateExpr)
	if ok {
		if len(agg.Grouping) == 0 && !agg.Without {
			// Shortcut if there are no labels allowed to propagate inwards or outwards.
			return nil, nil, false
		}
		vss, labelMatchers, ok := mapper.extractVectorSelectors(agg.Expr)
		if !ok {
			return nil, nil, false
		}
		newMatchers := mapper.getMatchersToPropagate(labelMatchers, agg.Grouping, !agg.Without)
		return vss, newMatchers, ok
	}

	be, ok := expr.(*parser.BinaryExpr)
	if ok {
		return mapper.propagateMatchersInBinaryExpr(be)
	}

	return nil, nil, false
}

func (mapper *propagateMatchers) getMatchersToPropagate(matchersSrc []*labels.Matcher, labelsList []string, whitelist bool) []*labels.Matcher {
	labelsSet := make(map[string]struct{})
	for _, l := range labelsList {
		labelsSet[l] = struct{}{}
	}

	matchersToAdd := make([]*labels.Matcher, 0, len(labelsSet))
	for _, m := range matchersSrc {
		if isMetricNameMatcher(m) {
			continue
		}
		_, exists := labelsSet[m.Name]
		if whitelist {
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
