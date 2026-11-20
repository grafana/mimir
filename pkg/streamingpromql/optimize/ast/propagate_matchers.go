// SPDX-License-Identifier: AGPL-3.0-only

package ast

import (
	"context"
	"fmt"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"

	"github.com/grafana/mimir/pkg/frontend/querymiddleware/astmapper"
	"github.com/grafana/mimir/pkg/streamingpromql/planning"
	"github.com/grafana/mimir/pkg/streamingpromql/planning/core"
)

// PropagateMatchers optimizes queries by propagating matchers across binary operations.
type PropagateMatchers struct {
	propagateMatchersAttempts prometheus.Counter
	propagateMatchersRewrites prometheus.Counter
}

func NewPropagateMatchers(reg prometheus.Registerer) *PropagateMatchers {
	return &PropagateMatchers{
		propagateMatchersAttempts: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_propagate_matchers_attempted_total",
			Help: "Total number of queries that the optimization pass has attempted to rewrite by propagating matchers.",
		}),
		propagateMatchersRewrites: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_mimir_query_engine_propagate_matchers_rewritten_total",
			Help: "Total number of queries where the optimization pass has rewritten the query by propagating matchers.",
		}),
	}
}

func (p *PropagateMatchers) Name() string {
	return "Propagate matchers"
}

func (p *PropagateMatchers) Apply(ctx context.Context, expr parser.Expr, _ *planning.QueryParameters) (parser.Expr, error) {
	p.propagateMatchersAttempts.Inc()
	mapper := NewPropagateMatchersMapper()
	newExpr, err := mapper.Map(ctx, expr)
	if mapper.HasChanged() {
		p.propagateMatchersRewrites.Inc()
	}
	return newExpr, err
}

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

func (mapper *propagateMatchers) MapExpr(ctx context.Context, expr parser.Expr) (mapped parser.Expr, finished bool, err error) {
	e, ok := expr.(*parser.BinaryExpr)
	if !ok {
		return expr, false, nil
	}

	mapper.propagateMatchersInBinaryExpr(e)
	return e, true, nil
}

// labelSet is a set of label names: the names in names if include is true, or all names not in
// names if include is false. The zero value is the set of all label names.
type labelSet struct {
	names   stringSet
	include bool
}

func (s labelSet) contains(name string) bool {
	return s.include == s.names.Contains(name)
}

func (s labelSet) isEmpty() bool {
	return s.include && len(s.names) == 0
}

func (s labelSet) intersect(o labelSet) labelSet {
	switch {
	case s.include && o.include:
		return labelSet{names: s.names.Intersection(o.names), include: true}
	case s.include:
		return labelSet{names: s.names.Difference(o.names), include: true}
	case o.include:
		return labelSet{names: o.names.Difference(s.names), include: true}
	default:
		return labelSet{names: s.names.Union(o.names)}
	}
}

// filter returns the matchers on labels in s. Matchers on internal labels such as __name__ are
// never propagated, because different selectors usually select different metric names.
func (s labelSet) filter(matchers []*labels.Matcher) []*labels.Matcher {
	filtered := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		if !isInternalMatcher(m) && s.contains(m.Name) {
			filtered = append(filtered, m)
		}
	}
	return filtered
}

// enrichedVectorSelector is a vector selector with the labels that matchers from outside the
// expressions that contain it can be propagated into it on. These are the labels whose values in
// the series of the selector are kept in the results of those expressions.
type enrichedVectorSelector struct {
	vs           *parser.VectorSelector
	propagatable labelSet
}

// extracted is what the pass knows about an expression after it has propagated matchers inside it.
type extracted struct {
	// selectors are the vector selectors in the expression that matchers from outside it can be propagated into.
	selectors []*enrichedVectorSelector
	// matchers hold for every series in the result of the expression, so they can be propagated outwards.
	matchers []*labels.Matcher
}

// through returns what the pass knows about a parent expression whose result keeps only the
// labels in kept from the series in the result of x. required is false if the parent expression
// can return series that do not come from x, because then the matchers of x do not hold for its result.
func (x extracted) through(kept labelSet, required bool) extracted {
	var out extracted
	for _, vs := range x.selectors {
		vs.propagatable = vs.propagatable.intersect(kept)
		if !vs.propagatable.isEmpty() {
			out.selectors = append(out.selectors, vs)
		}
	}
	if required {
		out.matchers = kept.filter(x.matchers)
	}
	return out
}

func merge(a, b extracted) extracted {
	matchers, _ := combineMatchers(a.matchers, b.matchers, labelSet{})
	return extracted{selectors: append(a.selectors, b.selectors...), matchers: matchers}
}

func (mapper *propagateMatchers) propagateMatchersInBinaryExpr(e *parser.BinaryExpr) extracted {
	l := mapper.propagateMatchersAndExtractVectorSelectors(e.LHS)
	r := mapper.propagateMatchersAndExtractVectorSelectors(e.RHS)
	if e.Op == parser.LOR {
		// `or` returns the series without a match from both sides, so matchers cannot propagate
		// across it or out of it. The calls above still process the binary expressions inside each side.
		return extracted{}
	}

	matching := e.VectorMatching
	if matching == nil {
		// One side is a scalar, so it has no vector selectors.
		return merge(l.through(labelSet{}, true), r.through(labelSet{}, true))
	}

	// A side is required if every series in the result comes from a series on that side. Then the
	// matchers of that side hold for the result. If a side is required, series on the other side
	// without a match produce no output, so the matchers of the required side can narrow the other side.
	// fill_left and fill_right use the fill value in place of the missing side for series without a match,
	// and `unless` only returns LHS series without a match.
	requiredL := matching.FillValues.LHS == nil
	requiredR := e.Op != parser.LUNLESS && matching.FillValues.RHS == nil
	on := labelSet{names: newStringSet(matching.MatchingLabels), include: matching.On}
	if requiredR {
		mapper.addMatchers(l.selectors, on.filter(r.matchers))
	}
	// With only fill_right, narrowing the RHS would not change the results, but it would hide the error that both engines
	// return for duplicate series in a right-side match group without a match when a fill modifier is set.
	if requiredL && matching.FillValues.RHS == nil {
		mapper.addMatchers(r.selectors, on.filter(l.matchers))
	}
	return merge(l.through(keptLabels(e, true), requiredL), r.through(keptLabels(e, false), requiredR))
}

// addMatchers adds the matchers to each vector selector, if they are on labels that can be propagated into it.
func (mapper *propagateMatchers) addMatchers(vss []*enrichedVectorSelector, matchers []*labels.Matcher) {
	for _, vs := range vss {
		if newLabelMatchers, changed := combineMatchers(vs.vs.LabelMatchers, matchers, vs.propagatable); changed {
			vs.vs.LabelMatchers = newLabelMatchers
			mapper.changed = true
		}
	}
}

// keptLabels returns which labels of the series on one side of a binary expression keep their
// values in its result. lhs is true for the left-hand side.
func keptLabels(e *parser.BinaryExpr, lhs bool) labelSet {
	matching := e.VectorMatching
	if matching == nil {
		return labelSet{}
	}
	if (lhs && matching.Card == parser.CardManyToOne) || (!lhs && matching.Card == parser.CardOneToMany) {
		// The "many" side of group_left() or group_right() keeps all of its labels in the result,
		// except the included labels, which are taken from the "one" side.
		return labelSet{names: newStringSet(matching.Include)}
	}
	if lhs && (e.Op == parser.LAND || e.Op == parser.LUNLESS) {
		// `and` and `unless` return the left-hand side series unchanged.
		return labelSet{}
	}
	// Otherwise, with on() or ignoring(), only the matching labels are guaranteed to have the same
	// value on this side and in the result. Other labels on this side do not affect the result
	// (e.g. the "one" side of `a * on() group_left() b`), so filtering this side on an outer
	// matcher would drop series that should match.
	matchingLabelsSet := newStringSet(matching.MatchingLabels)
	if len(matching.Include) > 0 {
		// The "one" side of group_left() or group_right() also sets the included labels in the result.
		if matching.On {
			return labelSet{names: matchingLabelsSet.Union(newStringSet(matching.Include)), include: true}
		}
		return labelSet{names: matchingLabelsSet.Difference(newStringSet(matching.Include))}
	}
	return labelSet{names: matchingLabelsSet, include: matching.On}
}

// propagateMatchersAndExtractVectorSelectors propagates matchers inside the binary expressions in
// expr, and returns what the pass knows about expr (see extracted).
func (mapper *propagateMatchers) propagateMatchersAndExtractVectorSelectors(expr parser.Expr) extracted {
	switch e := expr.(type) {
	case *parser.VectorSelector:
		return extracted{selectors: []*enrichedVectorSelector{{vs: e}}, matchers: e.LabelMatchers}
	case *parser.MatrixSelector:
		return mapper.propagateMatchersAndExtractVectorSelectors(e.VectorSelector)
	case *parser.ParenExpr:
		return mapper.propagateMatchersAndExtractVectorSelectors(e.Expr)
	case *parser.UnaryExpr:
		return mapper.propagateMatchersAndExtractVectorSelectors(e.Expr)
	case *parser.SubqueryExpr:
		return mapper.propagateMatchersAndExtractVectorSelectors(e.Expr)
	case *parser.Call:
		if i, _ := VectorSelectorArgumentIndex(e.Func.Name); i >= 0 {
			if len(e.Args) <= i {
				return extracted{}
			}
			return mapper.propagateMatchersAndExtractVectorSelectors(e.Args[i])
		}
		// Even for unsupported functions, recurse into all args to process any nested
		// BinaryExprs (e.g. `scalar(up{foo="bar"} * down)` nested inside another BinaryExpr).
		for _, arg := range e.Args {
			mapper.propagateMatchersAndExtractVectorSelectors(arg)
		}
		return extracted{}
	case *parser.AggregateExpr:
		// Only the grouping labels keep their values in the result. Without grouping labels, no
		// matchers can propagate, but the call still processes the binary expressions inside the aggregation.
		inner := mapper.propagateMatchersAndExtractVectorSelectors(e.Expr)
		return inner.through(labelSet{names: newStringSet(e.Grouping), include: !e.Without}, true)
	case *parser.BinaryExpr:
		return mapper.propagateMatchersInBinaryExpr(e)
	// Explicitly define what is not handled to avoid confusion.
	case *parser.StepInvariantExpr:
		// Used only for optimizations and not produced directly by parser,
		// but may be added in preprocessing step.
		return mapper.propagateMatchersAndExtractVectorSelectors(e.Expr)
	default:
		return extracted{}
	}
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
	// Integration over samples (using matrix selectors)
	case "integral":
		return 0, nil
	// Histogram-related
	case "histogram_avg", "histogram_count", "histogram_sum", "histogram_stddev", "histogram_stdvar", "histogram_quantiles":
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
	case "timestamp", "start_timestamp":
		return 0, nil
	// No vector/matrix selectors
	case "end", "max_of", "min_of", "pi", "range", "start", "step", "time", "vector":
		return -1, nil
	// Explicitly not supported because it's not valid to propagate matchers across these functions.
	case "scalar", "absent", "absent_over_time", core.ScalarEvaluationRootFunctionName:
		return -1, nil
	// Explicitly not supported because we want to avoid unexpected interactions with labels or ordering.
	case "label_join", "label_replace", "info", "sort", "sort_desc", "sort_by_label", "sort_by_label_desc":
		return -1, nil
	case core.VectorEvaluationRootFunctionName:
		return 0, nil
	default:
		return -1, fmt.Errorf("function support unknown: %s", funcName)
	}
}

// combineMatchers adds the matchers in matchersToAdd that are on labels in allowed and not already in matchers.
// Note that this function mutates the matchers slice, so be careful where it is used.
func combineMatchers(matchers, matchersToAdd []*labels.Matcher, allowed labelSet) ([]*labels.Matcher, bool) {
	matchersMap := makeMatchersMap(matchers)
	changed := false
	for _, m := range matchersToAdd {
		if _, ok := matchersMap[m.String()]; ok {
			continue
		}
		if !allowed.contains(m.Name) {
			continue
		}
		matchers = append(matchers, m)
		changed = true
	}
	return matchers, changed
}

func isInternalMatcher(m *labels.Matcher) bool {
	return strings.HasPrefix(m.Name, "__")
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
