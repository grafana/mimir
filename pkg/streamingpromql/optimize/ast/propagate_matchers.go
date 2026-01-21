// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
)

// NewPropagateMatchersMapper optimizes queries by propagating matchers across binary operations.
func NewPropagateMatchersMapper() *astmapper.ASTExprMapperWithState {
	mapper := &propagateMatchers{}
	return astmapper.NewASTExprMapperWithState(mapper)
}

type propagateMatchers struct {
	changed bool
}

func (mapper *propagateMatchers) HasChanged() bool {
	return mapper.changed
}

func (mapper *propagateMatchers) Stats() (int, int, int) {
	return 0, 0, 0
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
// mainly the set of labels that are allowed to propagate inwards and outwards (same set used
// for both), and whether that set is for including or excluding, which are used only for aggregate
// expressions and the expressions that contain them.
type enrichedVectorSelector struct {
	vs          *parser.VectorSelector
	labelsSet   stringSet
	include     bool
	containsAgg bool
}

func (mapper *propagateMatchers) propagateMatchersInBinaryExpr(e *parser.BinaryExpr) ([]*enrichedVectorSelector, []*labels.Matcher) {
	if e.Op == parser.LOR {
		return nil, nil
	}

	vssL, matchersL := mapper.extractVectorSelectors(e.LHS)
	hasVectorSelectorsL := len(vssL) > 0
	vssR, matchersR := mapper.extractVectorSelectors(e.RHS)
	hasVectorSelectorsR := len(vssR) > 0
	switch {
	case !hasVectorSelectorsL && !hasVectorSelectorsR:
		return nil, nil
	case !hasVectorSelectorsL:
		return vssR, matchersR
	case !hasVectorSelectorsR:
		return vssL, matchersL
	}

	matchingLabelsSet := newStringSet(e.VectorMatching.MatchingLabels)
	var newMatchersL []*labels.Matcher
	if e.Op == parser.LUNLESS {
		// For LUNLESS, we cannot propagate matchers from the right-hand side to the left-hand side for correctness reasons.
		// e.g. `up unless down{foo="bar"}` must remain unchanged, but `up{foo="bar"} unless down` can become `up{foo="bar"} unless down{foo="bar"}`.
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
	matchers, _ := combineMatchers(newMatchersL, newMatchersR, stringSet{}, false)
	return vss, matchers
}

// extractVectorSelectors returns the vector selectors found in the expression (wrapped with additional
// info, see enrichedVectorSelector), along with the label matchers associated with them.
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
	case *parser.SubqueryExpr:
		return mapper.extractVectorSelectors(e.Expr)
	case *parser.Call:
		if i, _ := VectorSelectorArgumentIndex(e.Func.Name); i >= 0 {
			if len(e.Args) <= i {
				return nil, nil
			}
			return mapper.extractVectorSelectors(e.Args[i])
		}
		return nil, nil
	case *parser.AggregateExpr:
		return mapper.extractVectorSelectorsFromAggregateExpr(e)
	case *parser.BinaryExpr:
		return mapper.propagateMatchersInBinaryExpr(e)
	// Explicitly define what is not handled to avoid confusion.
	case *parser.StepInvariantExpr:
		// Used only for optimizations and not produced directly by parser,
		// but may be added in preprocessing step.
		return mapper.extractVectorSelectors(e.Expr)
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
	groupingSet := newStringSet(e.Grouping)
	for _, vs := range vss {
		updateVectorSelectorWithAggregationInfo(vs, groupingSet, include)
	}
	newMatchers := mapper.getMatchersToPropagate(labelMatchers, groupingSet, include)
	return vss, newMatchers
}

func updateVectorSelectorWithAggregationInfo(vs *enrichedVectorSelector, groupingSet stringSet, include bool) {
	if vs.containsAgg {
		switch {
		case vs.include && include:
			vs.labelsSet = vs.labelsSet.Intersection(groupingSet)
		case !vs.include && !include:
			vs.labelsSet = vs.labelsSet.Union(groupingSet)
		case vs.include && !include:
			vs.labelsSet = vs.labelsSet.Difference(groupingSet)
		case !vs.include && include:
			vs.labelsSet = groupingSet.Difference(vs.labelsSet)
			vs.include = true
		}
		return
	}
	vs.labelsSet = groupingSet
	vs.include = include
	vs.containsAgg = true
}

// VectorSelectorArgumentIndex returns the index of the argument that contains the vector selector,
// or -1 if the function does not support matcher propagation. The second return value is an error
// if the function is not recognized (for tests).
func VectorSelectorArgumentIndex(funcName string) (int, error) {
	// If the function is eligible for matcher propagation, return the index of the argument
	// that contains the vector selector (may be embedded in other kinds of expressions).
	// Otherwise, return -1.
	switch funcName {
	// Simple
	case "changes", "resets":
		return 0, nil
	// Mathematical
	case "abs", "sgn", "sqrt", "exp", "deriv", "ln", "log2", "log10", "ceil", "floor", "clamp", "clamp_min", "clamp_max":
		return 0, nil
	case "round":
		return 0, nil
	// More complicated mathematical functions
	case "double_exponential_smoothing", "predict_linear":
		return 0, nil
	// Trigonometric
	case "acos", "acosh", "asin", "asinh", "atan", "atanh", "cos", "cosh", "sin", "sinh", "tan", "tanh", "deg", "rad":
		return 0, nil
	// Differences between samples (using matrix selectors)
	case "rate", "delta", "increase", "idelta", "irate":
		return 0, nil
	// Histogram-related
	case "histogram_avg", "histogram_count", "histogram_sum", "histogram_stddev", "histogram_stdvar":
		return 0, nil
	case "histogram_quantile":
		return 1, nil
	case "histogram_fraction":
		return 2, nil
	// Aggregation over time
	case "avg_over_time", "min_over_time", "max_over_time", "sum_over_time", "count_over_time", "stddev_over_time", "stdvar_over_time", "first_over_time", "last_over_time", "present_over_time", "mad_over_time", "ts_of_min_over_time", "ts_of_max_over_time", "ts_of_first_over_time", "ts_of_last_over_time":
		return 0, nil
	case "quantile_over_time":
		return 1, nil
	// Time
	case "minute", "hour", "day_of_week", "day_of_month", "day_of_year", "days_in_month", "month", "year":
		return 0, nil
	case "timestamp":
		return 0, nil
	// No vector/matrix selectors
	case "pi", "time", "vector":
		return -1, nil
	// Explicitly not supported because it's not valid to propagate matchers across these functions.
	case "scalar", "absent", "absent_over_time":
		return -1, nil
	// Explicitly not supported because we want to avoid unexpected interactions with labels or ordering.
	case "label_join", "label_replace", "info", "sort", "sort_desc", "sort_by_label", "sort_by_label_desc":
		return -1, nil
	default:
		return -1, fmt.Errorf("function support unknown: %s", funcName)
	}
}

func (mapper *propagateMatchers) getMatchersToPropagate(matchersSrc []*labels.Matcher, labelsSet stringSet, include bool) []*labels.Matcher {
	var length int
	if include {
		length = min(len(labelsSet), len(matchersSrc))
	} else {
		length = len(matchersSrc)
	}
	matchersToAdd := make([]*labels.Matcher, 0, length)
	for _, m := range matchersSrc {
		if isMetricNameMatcher(m) {
			continue
		}
		if include != labelsSet.Contains(m.Name) {
			continue
		}
		matchersToAdd = append(matchersToAdd, m)
	}

	return matchersToAdd
}

// Note that this function mutates the matchers slice, so be careful where it is used.
func combineMatchers(matchers, matchersToAdd []*labels.Matcher, labelsSet stringSet, include bool) ([]*labels.Matcher, bool) {
	matchersMap := makeMatchersMap(matchers)
	changed := false
	for _, m := range matchersToAdd {
		if _, ok := matchersMap[m.String()]; ok {
			continue
		}
		if include != labelsSet.Contains(m.Name) {
			continue
		}
		matchers = append(matchers, m)
		changed = true
	}
	return matchers, changed
}

func isMetricNameMatcher(m *labels.Matcher) bool {
	return m.Name == model.MetricNameLabel
}

type stringSet map[string]struct{}

func newStringSet(list []string) stringSet {
	set := make(stringSet, len(list))
	for _, l := range list {
		set.Add(l)
	}
	return set
}

func (s stringSet) Add(key string) {
	s[key] = struct{}{}
}

func (s stringSet) Contains(key string) bool {
	_, exists := s[key]
	return exists
}

func (s stringSet) Intersection(s2 stringSet) stringSet {
	overlap := make(stringSet, len(s))
	for key := range s {
		if s2.Contains(key) {
			overlap.Add(key)
		}
	}
	return overlap
}

func (s stringSet) Union(s2 stringSet) stringSet {
	union := make(stringSet, len(s)+len(s2))
	for key := range s {
		union.Add(key)
	}
	for key := range s2 {
		union.Add(key)
	}
	return union
}

func (s stringSet) Difference(s2 stringSet) stringSet {
	diff := make(stringSet, len(s))
	for key := range s {
		if !s2.Contains(key) {
			diff.Add(key)
		}
	}
	return diff
}

func makeMatchersMap(matchers []*labels.Matcher) map[string]*labels.Matcher {
	matchersMap := make(map[string]*labels.Matcher, len(matchers))
	for _, m := range matchers {
		matchersMap[m.String()] = m
	}
	return matchersMap
}
