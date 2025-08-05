// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// PropagateMatchers optimizes queries by propagating matchers across binary operations.
type PropagateMatchers struct {
	mapper astmapper.ASTMapper
}

func NewPropagateMatchers() *PropagateMatchers {
	mapper := astmapper.NewASTExprMapper(&propagateMatchers{})
	return &PropagateMatchers{
		mapper: mapper,
	}
}

func (p *PropagateMatchers) Name() string {
	return "Matcher propagation across binary operations"
}

func (p *PropagateMatchers) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	return p.mapper.Map(ctx, expr)
}

type propagateMatchers struct{}

func (mapper *propagateMatchers) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	e, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return expr, false, nil
	}

	_, _, boolResult := mapper.propagateMatchersInBinaryExpr(e)
	return e, boolResult, nil
}

// enrichedVectorSelector is a struct to hold additional information about vector selectors,
// mainly the set of matchers that are allowed to propagate inwards and outwards (same set used
// for both), and whether that set is for including or excluding, which are used only for aggregate
// expressions and the expressions that contain them.
type enrichedVectorSelector struct {
	vs          *parser.VectorSelector
	labelsSet   map[string]struct{}
	include     bool
	containsAgg bool
}

func (mapper *propagateMatchers) propagateMatchersInBinaryExpr(e *parser.BinaryExpr) ([]*enrichedVectorSelector, []*labels.Matcher, bool) {
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
			vsL.vs.LabelMatchers = combineMatchers(vsL.vs.LabelMatchers, newMatchersL, vsL.labelsSet, vsL.include)
		}
	}
	for _, vsR := range vssR {
		vsR.vs.LabelMatchers = combineMatchers(vsR.vs.LabelMatchers, newMatchersR, vsR.labelsSet, vsR.include)
	}
	vss := append(vssL, vssR...)
	matchers := combineMatchers(newMatchersL, newMatchersR, map[string]struct{}{}, false)
	return vss, matchers, true
}

// extractVectorSelectors returns the vector selectors found in the expression (wrapped with additional
// info, see enrichedVectorSelector), along with the label matchers associated with them, and a boolean
// indicating whether any vector selectors were found.
func (mapper *propagateMatchers) extractVectorSelectors(expr parser.Expr) ([]*enrichedVectorSelector, []*labels.Matcher, bool) {
	vs, ok := expr.(*parser.VectorSelector)
	if ok {
		return []*enrichedVectorSelector{{vs: vs}}, vs.LabelMatchers, true
	}

	ms, ok := expr.(*parser.MatrixSelector)
	if ok {
		return mapper.extractVectorSelectors(ms.VectorSelector)
	}

	pe, ok := expr.(*parser.ParenExpr)
	if ok {
		return mapper.extractVectorSelectors(pe.Expr)
	}

	ue, ok := expr.(*parser.UnaryExpr)
	if ok {
		return mapper.extractVectorSelectors(ue.Expr)
	}

	agg, ok := expr.(*parser.AggregateExpr)
	if ok {
		include := !agg.Without
		if len(agg.Grouping) == 0 && include {
			// Shortcut if there are no labels allowed to propagate inwards or outwards.
			return nil, nil, false
		}
		vss, labelMatchers, ok := mapper.extractVectorSelectors(agg.Expr)
		if !ok {
			return nil, nil, false
		}
		groupingSet := makeStringSet(agg.Grouping)
		for _, vs := range vss {
			updateVecSelWithAggInfo(vs, groupingSet, include)
		}
		newMatchers := mapper.getMatchersToPropagate(labelMatchers, groupingSet, include)
		return vss, newMatchers, ok
	}

	be, ok := expr.(*parser.BinaryExpr)
	if ok {
		return mapper.propagateMatchersInBinaryExpr(be)
	}

	fn, ok := expr.(*parser.Call)
	if ok {
		if i := getIndexForEligibleFunction(fn.Func.Name); i >= 0 {
			return mapper.extractVectorSelectors(fn.Args[i])
		}
		return nil, nil, false
	}

	return nil, nil, false
}

func updateVecSelWithAggInfo(vs *enrichedVectorSelector, groupingSet map[string]struct{}, include bool) {
	if vs.containsAgg {
		switch {
		case vs.include && include:
			vs.labelsSet = getIntersection(vs.labelsSet, groupingSet)
		case !vs.include && !include:
			vs.labelsSet = getUnion(vs.labelsSet, groupingSet)
		case vs.include && !include:
			vs.labelsSet = getDifference(vs.labelsSet, groupingSet)
		case !vs.include && include:
			vs.labelsSet = getDifference(groupingSet, vs.labelsSet)
			vs.include = true
		}
		return
	}
	vs.labelsSet = groupingSet
	vs.include = include
	vs.containsAgg = true
}

func getIntersection(set1, set2 map[string]struct{}) map[string]struct{} {
	overlap := make(map[string]struct{}, len(set1))
	for key := range set1 {
		if _, exists := set2[key]; exists {
			overlap[key] = struct{}{}
		}
	}
	return overlap
}

func getUnion(set1, set2 map[string]struct{}) map[string]struct{} {
	union := make(map[string]struct{}, len(set1)+len(set2))
	for key := range set1 {
		union[key] = struct{}{}
	}
	for key := range set2 {
		union[key] = struct{}{}
	}
	return union
}

func getDifference(set1, set2 map[string]struct{}) map[string]struct{} {
	diff := make(map[string]struct{}, len(set1))
	for key := range set1 {
		if _, exists := set2[key]; !exists {
			diff[key] = struct{}{}
		}
	}
	return diff
}

// TODO: Consider more functions, and add tests for these.
func getIndexForEligibleFunction(funcName string) int {
	// If the function is eligible for matcher propagation, return the index of the argument
	// that contains the vector selector (may be embedded in other kinds of expressions).
	// Otherwise, return -1.
	switch funcName {
	// Mathematical
	case "abs", "sgn", "sqrt", "exp", "deriv", "ln", "log2", "log10", "ceil", "floor", "round", "clamp", "clamp_min", "clamp_max":
		return 0
	// Trigonometric
	case "acos", "acosh", "asin", "asinh", "atan", "atanh", "cos", "cosh", "sin", "sinh", "tan", "tanh", "deg", "rad":
		return 0
	// Differences between samples (using matrix selectors)
	case "rate", "delta", "increase", "idelta", "irate":
		return 0
	default:
		return -1
	}
}

func (mapper *propagateMatchers) getMatchersToPropagate(matchersSrc []*labels.Matcher, labelsSet map[string]struct{}, include bool) []*labels.Matcher {
	matchersToAdd := make([]*labels.Matcher, 0, len(labelsSet))
	for _, m := range matchersSrc {
		if isMetricNameMatcher(m) {
			continue
		}
		_, exists := labelsSet[m.Name]
		if include {
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

func combineMatchers(matchers, matchersToAdd []*labels.Matcher, labelsSet map[string]struct{}, include bool) []*labels.Matcher {
	matchersMap := makeMatchersMap(matchers)
	newMatchers := make([]*labels.Matcher, 0, len(matchers)+len(matchersToAdd))
	newMatchers = append(newMatchers, matchers...)
	for _, m := range matchersToAdd {
		if _, ok := matchersMap[m.String()]; !ok {
			_, exists := labelsSet[m.Name]
			if include {
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
