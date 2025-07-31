// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
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

// vectorSelectorWrapper is a struct to hold additional information about vector selectors,
// mainly the set of matchers that are allowed to propagate inwards and outwards, and whether
// that set is a whitelist or blacklist, which are used only for aggregate expressions and the
// expressions that contain them.
type vectorSelectorWrapper struct {
	vs        *parser.VectorSelector
	labelsSet map[string]struct{}
	whitelist bool
}

func (mapper *propagateMatchers) propagateMatchersInBinaryExpr(e *parser.BinaryExpr) ([]*vectorSelectorWrapper, []*labels.Matcher, bool) {
	if e.Op == parser.LOR {
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

	matchingLabelsSet := makeStringSet(e.VectorMatching.MatchingLabels)
	newMatchersR := mapper.getMatchersToPropagate(matchersL, matchingLabelsSet, e.VectorMatching.On)
	var newMatchersL []*labels.Matcher
	if e.Op == parser.LUNLESS {
		// For LUNLESS, we do not propagate matchers from the right-hand side to the left-hand side.
		newMatchersL = make([]*labels.Matcher, 0)
	} else {
		newMatchersL = mapper.getMatchersToPropagate(matchersR, matchingLabelsSet, e.VectorMatching.On)
		for _, vsL := range vssL {
			vsL.vs.LabelMatchers = combineMatchers(vsL.vs.LabelMatchers, newMatchersL, vsL.labelsSet, vsL.whitelist)
		}
	}
	for _, vsR := range vssR {
		vsR.vs.LabelMatchers = combineMatchers(vsR.vs.LabelMatchers, newMatchersR, vsR.labelsSet, vsR.whitelist)
	}
	vss := append(vssL, vssR...)
	matchers := combineMatchers(newMatchersL, newMatchersR, map[string]struct{}{}, false)
	return vss, matchers, true
}

// extractVectorSelectors returns the vector selectors found in the expression (wrapped with additional
// info, see vectorSelectorWrapper), along with the label matchers associated with them, and a boolean
// indicating whether any vector selectors were found.
func (mapper *propagateMatchers) extractVectorSelectors(expr parser.Expr) ([]*vectorSelectorWrapper, []*labels.Matcher, bool) {
	vs, ok := expr.(*parser.VectorSelector)
	if ok {
		return []*vectorSelectorWrapper{{vs: vs}}, vs.LabelMatchers, true
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
		groupingSet := makeStringSet(agg.Grouping)
		for _, vs := range vss {
			// TODO: what if these fields are already set?
			vs.labelsSet = groupingSet
			vs.whitelist = !agg.Without
		}
		newMatchers := mapper.getMatchersToPropagate(labelMatchers, groupingSet, !agg.Without)
		return vss, newMatchers, ok
	}

	be, ok := expr.(*parser.BinaryExpr)
	if ok {
		return mapper.propagateMatchersInBinaryExpr(be)
	}

	return nil, nil, false
}

func (mapper *propagateMatchers) getMatchersToPropagate(matchersSrc []*labels.Matcher, labelsSet map[string]struct{}, whitelist bool) []*labels.Matcher {
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

func combineMatchers(matchers, matchersToAdd []*labels.Matcher, labelsSet map[string]struct{}, whitelist bool) []*labels.Matcher {
	matchersMap := makeMatchersMap(matchers)
	newMatchers := make([]*labels.Matcher, 0, len(matchers)+len(matchersToAdd))
	newMatchers = append(newMatchers, matchers...)
	for _, m := range matchersToAdd {
		if _, ok := matchersMap[m.String()]; !ok {
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
			newMatchers = append(newMatchers, m)
		}
	}
	return newMatchers
}

func isMetricNameMatcher(m *labels.Matcher) bool {
	return m.Name == labels.MetricName
}

func makeStringSet(list []string) map[string]struct{} {
	set := make(map[string]struct{}, len(list))
	for _, l := range list {
		set[l] = struct{}{}
	}
	return set
}

func makeMatchersMap(matchers []*labels.Matcher) map[string]*labels.Matcher {
	matchersMap := make(map[string]*labels.Matcher, len(matchers))
	for _, m := range matchers {
		matchersMap[m.String()] = m
	}
	return matchersMap
}
