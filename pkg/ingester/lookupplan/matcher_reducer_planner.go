// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"

	"github.com/grafana/dskit/tracing"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/grafana/mimir/pkg/util"
)

// MatcherReducerPlanner deduplicates matchers from the input plan,
// and removes matchers that select for all non-empty values if a more selective matcher for the same label name already exists.
// It returns the input index and scan matchers in the input order.
// If matchers are duplicated across the index and scan matchers, they will be returned as index-only matchers.
type MatcherReducerPlanner struct{}

// concreteLookupPlan implements LookupPlan by storing pre-computed index and scan matchers.
type concreteLookupPlan struct {
	indexMatchers []*labels.Matcher
	scanMatchers  []*labels.Matcher
}

func (p concreteLookupPlan) ScanMatchers() []*labels.Matcher {
	return p.scanMatchers
}

func (p concreteLookupPlan) IndexMatchers() []*labels.Matcher {
	return p.indexMatchers
}

// PlanIndexLookup takes an index.LookupPlan and removes matchers that match only supersets of other matchers.
// It does not modify plans if the input plan has any scan matchers. It does not guarantee matcher order of the output plan.
func (p MatcherReducerPlanner) PlanIndexLookup(ctx context.Context, inPlan index.LookupPlan, _, _ int64) (index.LookupPlan, error) {
	// For simplicity, we don't process plans with scan matchers
	if planningDisabled(ctx) || len(inPlan.ScanMatchers()) > 0 {
		return inPlan, nil
	}
	// If there's only one matcher, don't try to do any optimizations
	if len(inPlan.IndexMatchers()) <= 1 {
		return inPlan, nil
	}
	allowedMatchers := setReduceMatchers(inPlan.IndexMatchers())

	// Rebuild the index and scan matchers in the input order, less the filtered/deduplicated matchers
	// droppedMatchers is used for logging purposes to record all matchers have been removed from the plan.
	outMatchers, droppedMatchers := buildOutMatchers(inPlan.IndexMatchers(), allowedMatchers)

	span, _, traceSampled := tracing.SpanFromContext(ctx)
	if traceSampled && len(droppedMatchers) > 0 {
		span.AddEvent("dropped matchers", trace.WithAttributes(
			attribute.Stringer("matchers", util.MatchersStringer(droppedMatchers)),
		))
	}
	return &concreteLookupPlan{indexMatchers: outMatchers}, nil
}

// buildOutMatchers takes a slice of matchers, and returns a slice of matchers which are included in allowedOutMatchers,
// as well as a slice of droppedMatchers which were either duplicates or not included in allowedOutMatchers.
func buildOutMatchers(inMatchers []*labels.Matcher, allowedOutMatchers []*labels.Matcher) ([]*labels.Matcher, []*labels.Matcher) {
	outMatchers := make([]*labels.Matcher, 0, len(inMatchers))
	dedupedMatchers, dropped := dedupeMatchers(inMatchers)

	// allowedInResultSet maps the relevant values of matchers returned by setReduce.
	// The innermost map value tracks whether that matcher is already represented in outMatchers.
	// We do it this way instead of using the matcher string via m.String()
	// to avoid unnecessary memory allocations when building the string.
	allowedInResultSet := make(map[labels.MatchType]map[string]map[string]bool, 4)
	for _, m := range allowedOutMatchers {
		if _, ok := allowedInResultSet[m.Type][m.Name]; ok {
			allowedInResultSet[m.Type][m.Name][m.Value] = false
			continue
		}
		val := map[string]bool{m.Value: false}
		if _, ok := allowedInResultSet[m.Type]; ok {
			allowedInResultSet[m.Type][m.Name] = val
			continue
		}
		name := map[string]map[string]bool{m.Name: val}
		allowedInResultSet[m.Type] = name
	}
	// If we have reached the last deduped input matcher and are still not returning any matchers,
	// we should return at least one matcher. This can happen if all input matchers are wildcard matchers.
	for i, m := range dedupedMatchers {
		if i == len(dedupedMatchers)-1 && len(outMatchers) == 0 {
			outMatchers = append(outMatchers, m)
			continue
		}
		// allowedOutMatchers is used to both keep track of all unique matchers (evidenced by existence in the map),
		// and whether the matcher has already been seen and added to a set of output matchers (evidenced by the value in the map).
		// We only want to add the matcher if it hasn't already been added to an output slice.
		if alreadyInResultSet, allowed := allowedInResultSet[m.Type][m.Name][m.Value]; allowed && !alreadyInResultSet {
			outMatchers = append(outMatchers, m)
			allowedInResultSet[m.Type][m.Name][m.Value] = true
		} else {
			dropped = append(dropped, m)
		}
	}
	return outMatchers, dropped
}

// setReduceMatchers takes a slice of matchers, and returns only those matchers which would reduce the result set size.
// For each label name, matchers are dropped from the result set based on criteria for their type:
//   - equals matchers are never dropped, but if there is more than one unique equals matcher,
//     all other matchers for the label name are dropped because unique equals matchers have non-intersecting result sets.
//   - not-equals matchers are dropped if they match any equals matcher value,
//     or if any not-regex matchers also exclude the not-equals matcher value.
//   - regex and not-regex matchers are dropped if they match any equals matcher value.
func setReduceMatchers(ms []*labels.Matcher) []*labels.Matcher {
	// Group matchers by their label names so we can evaluate each label name independently
	matchersByName := make(map[string][]*labels.Matcher)
	for _, m := range ms {
		if _, ok := matchersByName[m.Name]; !ok {
			matchersByName[m.Name] = make([]*labels.Matcher, 0, 1)
		}
		matchersByName[m.Name] = append(matchersByName[m.Name], m)
	}
	outMatchers := make([]*labels.Matcher, 0, len(ms))
	for _, matchers := range matchersByName {
		matchersByType := groupMatchersByType(matchers)
		equalsMatchers, _ := dedupeMatchers(matchersByType[labels.MatchEqual])
		outMatchers = append(outMatchers, equalsMatchers...)
		// If we have more than one unique equals matcher, we can just return those;
		// we know we'll only return an empty set
		if len(equalsMatchers) > 1 {
			continue
		}
		outMatchers = append(outMatchers, filterRegexMatchers(matchersByType, labels.MatchRegexp)...)
		outMatchers = append(outMatchers, filterNotEqualsMatchers(matchersByType)...)
		outMatchers = append(outMatchers, filterRegexMatchers(matchersByType, labels.MatchNotRegexp)...)
	}
	return outMatchers
}

// dedupeMatchers dedupes matchers based on their type, name, and value.
func dedupeMatchers(ms []*labels.Matcher) ([]*labels.Matcher, []*labels.Matcher) {
	deduped := make(map[labels.MatchType]map[string]map[string]*labels.Matcher, 4)
	dropped := make([]*labels.Matcher, 0, 1)
	for _, m := range ms {
		if _, ok := deduped[m.Type][m.Name][m.Value]; ok {
			dropped = append(dropped, m)
			continue
		}
		if _, ok := deduped[m.Type][m.Name]; ok {
			deduped[m.Type][m.Name][m.Value] = m
			continue
		}
		valPtr := map[string]*labels.Matcher{m.Value: m}
		if _, ok := deduped[m.Type]; ok {
			deduped[m.Type][m.Name] = valPtr
			continue
		}
		name := map[string]map[string]*labels.Matcher{m.Name: valPtr}
		deduped[m.Type] = name
	}
	outMatchers := make([]*labels.Matcher, 0, len(deduped))
	for _, nameMap := range deduped {
		for _, valMap := range nameMap {
			for _, ptr := range valMap {
				outMatchers = append(outMatchers, ptr)
			}
		}
	}
	return outMatchers, dropped
}

// matcherMatchesAnyValues returns true if the given matcher matches a single value from the input matchers, and false otherwise.
func matcherMatchesAnyValues(matcher *labels.Matcher, matchers []*labels.Matcher) bool {
	for _, m := range matchers {
		if matcher.Matches(m.Value) {
			return true
		}
	}
	return false
}

// filterRegexMatchers returns a subset of regex matchers which would actually reduce the result set size for either positive or negative regex matchers.
// A regex matcher is dropped if:
//   - it is a positive regex matcher and matches all values (foo=~".*")
//   - it matches any equals matches, since the equals matcher will match a strict subset of values that the regex matcher would.
//
// Examples:
//   - {foo=~".*bar.*", foo="bar"}, foo=~".*bar.*" is dropped because foo="bar" is a subset of foo=~".*bar.*"
//   - {foo!~".*bar.*", foo="bar"}, foo!~".*bar.*" is not dropped because it covers a different set of values than foo="bar"
//   - {foo!~".*baz.*", foo="bar"}, foo!~".*baz.*" is dropped because foo="bar" is a subset of foo!~".*baz.*"
func filterRegexMatchers(mf map[labels.MatchType][]*labels.Matcher, regexType labels.MatchType) []*labels.Matcher {
	var matchers []*labels.Matcher
	switch regexType {
	case labels.MatchRegexp:
		matchers = mf[labels.MatchRegexp]
	case labels.MatchNotRegexp:
		matchers = mf[labels.MatchNotRegexp]
	default:
		// Do not filter anything if passed a non-regex type.
		panic("cannot filter unknown type regex matchers")
	}
	outMatchers := make([]*labels.Matcher, 0, len(matchers))
	for _, m := range matchers {
		// Always drop wildcard matchers
		if m.Type == labels.MatchRegexp && m.Value == ".*" {
			continue
		}
		// If m matches any equals matcher, that equals matcher is a subset of the regex,
		// and we should not add m to outMatchers.
		if !matcherMatchesAnyValues(m, mf[labels.MatchEqual]) {
			outMatchers = append(outMatchers, m)
		}
	}
	return outMatchers
}

// filterNotEqualsMatchers returns a subset of not-equals matchers which should actually reduce the result set size.
// This subset is determined by comparing each not-equals matchers against equals and not-regex matchers.
// A not-equals matcher is dropped on any of these conditions:
//   - there are any not-regex matchers which would also remove the not-equals matcher value from the result set
//   - any equals matcher for a value that is matched by the not-equals matcher
//
// Examples:
//   - for the matcher set {foo!="bar", foo="b"}, foo!="bar" does match the value "b", so foo!="bar" is dropped.
//   - for the matcher set {foo!="bar", foo="bar"}, foo!="bar" does not match the value "bar", so foo!="bar" is not dropped.
//   - for the matcher set {foo!="bar", foo!~".*bar.*"}, foo!~".*bar.*" will exclude more values from the result set than foo!="bar", so foo!="bar" is dropped.
func filterNotEqualsMatchers(mf map[labels.MatchType][]*labels.Matcher) []*labels.Matcher {
	outMatchers := make([]*labels.Matcher, 0, len(mf[labels.MatchNotEqual]))
	// If the value of a not-equals matcher is matched by the inverse of any not-regex matcher,
	// that not-equals matcher can be dropped, because it will select a subset of the regex.
	// Using the example of x!="a" and x!~"a.*",
	// x!~"a.*" will exclude more values than x!="a".
	for _, m := range mf[labels.MatchNotEqual] {
		noRegexMatchesValue := true
		for _, nr := range mf[labels.MatchNotRegexp] {
			if inv, err := nr.Inverse(); err == nil {
				if inv.Matches(m.Value) {
					noRegexMatchesValue = false
					break
				}
			}
		}
		// If the value of any equals matcher is matched by the not-equals matcher, only keep the equals matcher,
		// because the not-equals will not "subtract" anything from the final set.
		matchesNoEquals := !matcherMatchesAnyValues(m, mf[labels.MatchEqual])

		if noRegexMatchesValue && matchesNoEquals {
			outMatchers = append(outMatchers, m)
		}
	}
	return outMatchers
}

func groupMatchersByType(ms []*labels.Matcher) map[labels.MatchType][]*labels.Matcher {
	outGroups := make(map[labels.MatchType][]*labels.Matcher)
	for _, m := range ms {
		outGroups[m.Type] = append(outGroups[m.Type], m)
	}
	return outGroups
}
