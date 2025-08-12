// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// PropagateMatchers optimizes queries by propagating matchers across binary operations.
type PropagateMatchers struct {
	mapper *ASTExprMapperWithState
}

func NewPropagateMatchers() *PropagateMatchers {
	return &PropagateMatchers{
		mapper: NewPropagateMatchersMapper(),
	}
}

func (p *PropagateMatchers) Name() string {
	return "Matcher propagation across binary operations"
}

func (p *PropagateMatchers) Apply(ctx context.Context, expr parser.Expr) (parser.Expr, error) {
	return p.mapper.Map(ctx, expr)
}

func NewPropagateMatchersMapper() *ASTExprMapperWithState {
	mapper := &propagateMatchers{}
	return NewASTExprMapperWithState(mapper)
}

type propagateMatchers struct {
	changed bool
}

func (mapper *propagateMatchers) HasChanged() bool {
	return mapper.changed
}

func (mapper *propagateMatchers) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	e, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return expr, false, nil
	}

	mapper.propagateMatchersInBinaryExpr(e)
	return e, true, nil
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

func (mapper *propagateMatchers) propagateMatchersInBinaryExpr(e *parser.BinaryExpr) ([]*enrichedVectorSelector, []*labels.Matcher) {
	if e.Op == parser.LOR {
		return nil, nil
	}

	vssL, matchersL := mapper.extractVectorSelectors(e.LHS)
	okL := len(vssL) > 0
	vssR, matchersR := mapper.extractVectorSelectors(e.RHS)
	okR := len(vssR) > 0
	switch {
	case !okL && !okR:
		return nil, nil
	case !okL:
		return vssR, matchersR
	case !okR:
		return vssL, matchersL
	}

	if e.VectorMatching == nil {
		return nil, nil
	}

	matchingLabelsSet := makeStringSet(e.VectorMatching.MatchingLabels)
	var newMatchersL []*labels.Matcher
	if e.Op == parser.LUNLESS {
		// For LUNLESS, we do not propagate matchers from the right-hand side to the left-hand side.
		newMatchersL = make([]*labels.Matcher, 0)
	} else {
		newMatchersL = mapper.getMatchersToPropagate(matchersR, matchingLabelsSet, e.VectorMatching.On)
		for _, vsL := range vssL {
			if newLabelMatchers, changed := combineMatchers(vsL.vs.LabelMatchers, newMatchersL, vsL.labelsSet, vsL.include); changed {
				vsL.vs.LabelMatchers = newLabelMatchers
				mapper.changed = true
			}
		}
	}
	newMatchersR := mapper.getMatchersToPropagate(matchersL, matchingLabelsSet, e.VectorMatching.On)
	for _, vsR := range vssR {
		if newLabelMatchers, changed := combineMatchers(vsR.vs.LabelMatchers, newMatchersR, vsR.labelsSet, vsR.include); changed {
			vsR.vs.LabelMatchers = newLabelMatchers
			mapper.changed = true
		}
	}
	vss := append(vssL, vssR...)
	matchers, _ := combineMatchers(newMatchersL, newMatchersR, map[string]struct{}{}, false)
	return vss, matchers
}

// extractVectorSelectors returns the vector selectors found in the expression (wrapped with additional
// info, see enrichedVectorSelector), along with the label matchers associated with them, and a boolean
// indicating whether any vector selectors were found.
func (mapper *propagateMatchers) extractVectorSelectors(expr parser.Expr) ([]*enrichedVectorSelector, []*labels.Matcher) {
	switch e := expr.(type) {
	case *parser.VectorSelector:
		return []*enrichedVectorSelector{{vs: e}}, e.LabelMatchers
	case *parser.MatrixSelector:
		return mapper.extractVectorSelectors(e.VectorSelector)
	case *parser.ParenExpr:
		return mapper.extractVectorSelectors(e.Expr)
	case *parser.UnaryExpr:
		return mapper.extractVectorSelectors(e.Expr)
	case *parser.Call:
		if i := getIndexForEligibleFunction(e.Func.Name); i >= 0 {
			return mapper.extractVectorSelectors(e.Args[i])
		}
		return nil, nil
	case *parser.AggregateExpr:
		return mapper.extractVectorSelectorsFromAggregateExpr(e)
	case *parser.BinaryExpr:
		return mapper.propagateMatchersInBinaryExpr(e)
	// Explicitly define what is not handled to avoid confusion.
	case *parser.StepInvariantExpr:
		// Used only for optimizations and not produced directly by parser, and should not contain
		// any vector selectors anyway.
		return nil, nil
	case *parser.SubqueryExpr:
		// We do not support subqueries for now due to complexity.
		return nil, nil
	default:
		return nil, nil
	}
}

func (mapper *propagateMatchers) extractVectorSelectorsFromAggregateExpr(e *parser.AggregateExpr) ([]*enrichedVectorSelector, []*labels.Matcher) {
	include := !e.Without
	if len(e.Grouping) == 0 && include {
		// Shortcut if there are no labels allowed to propagate inwards or outwards.
		return nil, nil
	}
	vss, labelMatchers := mapper.extractVectorSelectors(e.Expr)
	if len(vss) == 0 {
		return nil, nil
	}
	groupingSet := makeStringSet(e.Grouping)
	for _, vs := range vss {
		updateVecSelWithAggInfo(vs, groupingSet, include)
	}
	newMatchers := mapper.getMatchersToPropagate(labelMatchers, groupingSet, include)
	return vss, newMatchers
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

func combineMatchers(matchers, matchersToAdd []*labels.Matcher, labelsSet map[string]struct{}, include bool) ([]*labels.Matcher, bool) {
	matchersMap := makeMatchersMap(matchers)
	newMatchers := make([]*labels.Matcher, 0, len(matchers)+len(matchersToAdd))
	newMatchers = append(newMatchers, matchers...)
	changed := false
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
			changed = true
		}
	}
	return newMatchers, changed
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
