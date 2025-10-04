// SPDX-License-Identifier: AGPL-3.0-only

package lookupplan

import (
	"context"
	"slices"

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

func (p MatcherReducerPlanner) PlanIndexLookup(ctx context.Context, inPlan index.LookupPlan, minT, maxT int64) (index.LookupPlan, error) {
	span, _, traceSampled := tracing.SpanFromContext(ctx)
	if planningDisabled(ctx) {
		return inPlan, nil
	}

	matchers := slices.Concat(inPlan.IndexMatchers(), inPlan.ScanMatchers())
	matchers = setReduceMatchers(matchers)

	// allowedOutMatchers is used to check which matchers can be returned.
	// We use the matcher string as the key because we can't rely on value equality for regex matchers.
	// If a matcher string's value is false, it hasn't been added to any result set.
	allowedOutMatchers := make(map[string]bool, len(matchers))
	for _, m := range matchers {
		allowedOutMatchers[m.String()] = false
	}

	// Rebuild the index and scan matchers in the input order, less the filtered/deduplicated matchers
	// droppedMatchers is used for logging purposes to record all matchers have been removed from the plan.
	outIndexMatchers, allowedOutMatchers, droppedMatchers := buildOutMatchers(inPlan.IndexMatchers(), allowedOutMatchers, nil)
	outScanMatchers, _, droppedMatchers := buildOutMatchers(inPlan.ScanMatchers(), allowedOutMatchers, droppedMatchers)

	if traceSampled && len(droppedMatchers) > 0 {
		span.AddEvent("dropped matchers", trace.WithAttributes(
			attribute.Stringer("matchers", util.MatchersStringer(droppedMatchers)),
		))
	}
	return &concreteLookupPlan{
		indexMatchers: outIndexMatchers,
		scanMatchers:  outScanMatchers,
	}, nil
}

// buildOutMatchers takes a slice of index or scan matchers, and returns the same slice in order,
// less any duplicate matchers.
func buildOutMatchers(inMatchers []*labels.Matcher, allowedOutMatchers map[string]bool, droppedMatchers []*labels.Matcher) ([]*labels.Matcher, map[string]bool, []*labels.Matcher) {
	outMatchers := make([]*labels.Matcher, 0, len(inMatchers))
	for _, m := range inMatchers {
		matcherStr := m.String()
		// allowedOutMatchers is used to both keep track of all unique matchers (evidenced by existence in the map),
		// and whether the matcher has already been seen and added to a set of output matchers (evidenced by the value in the map).
		// We only want to add the matcher if it hasn't already been added to an output slice.
		if matcherAlreadySeen, ok := allowedOutMatchers[matcherStr]; ok && !matcherAlreadySeen {
			outMatchers = append(outMatchers, m)
			allowedOutMatchers[matcherStr] = true
		} else {
			droppedMatchers = append(droppedMatchers, m)
		}
	}
	return outMatchers, allowedOutMatchers, droppedMatchers
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
		equalsMatchers := dedupeEqualsMatchers(matchersByType)
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

func dedupeEqualsMatchers(mf map[labels.MatchType][]*labels.Matcher) []*labels.Matcher {
	return dedupeMatchers(mf[labels.MatchEqual])
}

func dedupeMatchers(ms []*labels.Matcher) []*labels.Matcher {
	deduped := make(map[labels.Matcher]*labels.Matcher, len(ms))
	for _, m := range ms {
		deduped[*m] = m
	}
	outMatchers := make([]*labels.Matcher, 0, len(deduped))
	for _, ptr := range deduped {
		outMatchers = append(outMatchers, ptr)
	}
	return outMatchers
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
//   - it matches all values (foo=~".*")
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
		if m.Value == ".*" {
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
//   - for the matcher set {foo!="bar", foo="bar"}, foo!="bar" matches the value "bar", so foo!="bar" is not dropped.
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
