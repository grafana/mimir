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

	allowedOutMatchers := make(map[labels.Matcher]bool, len(matchers))
	for _, m := range matchers {
		allowedOutMatchers[*m] = false
	}

	// Rebuild the index and scan matchers in the input order, less the filtered/deduplicated matchers
	droppedMatchers := make([]*labels.Matcher, 0)
	outIndexMatchers, allowedOutMatchers, droppedMatchers := buildOutMatchers(inPlan.IndexMatchers(), allowedOutMatchers, droppedMatchers)
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
// less any duplicate matchers. allowedOutMatchers contains the set of matchers which can be returned.
// If dedupedMatchers[*m] is false, the matcher has not been added to any result set.
// droppedMatchers is used for logging purposes to record which matchers have been removed from the plan.
func buildOutMatchers(inMatchers []*labels.Matcher, allowedOutMatchers map[labels.Matcher]bool, droppedMatchers []*labels.Matcher) ([]*labels.Matcher, map[labels.Matcher]bool, []*labels.Matcher) {
	outMatchers := make([]*labels.Matcher, 0, len(inMatchers))
	for _, m := range inMatchers {
		// dedupedMatchers is used to both keep track of all unique matchers (evidenced by existence in the map),
		// and whether the matcher has already been added to a set of output matchers (evidenced by the value in the map).
		// We only want to add the matcher if it hasn't already been added to an output slice.
		if _, ok := allowedOutMatchers[*m]; ok && !allowedOutMatchers[*m] {
			outMatchers = append(outMatchers, m)
			allowedOutMatchers[*m] = true
		} else {
			droppedMatchers = append(droppedMatchers, m)
		}
	}
	return outMatchers, allowedOutMatchers, droppedMatchers
}

func setReduceMatchers(ms []*labels.Matcher) []*labels.Matcher {
	// Group matchers by their label names so we can evaluate each label name independently
	matchersByName := make(map[string][]*labels.Matcher)
	for _, m := range ms {
		if _, ok := matchersByName[m.Name]; !ok {
			matchersByName[m.Name] = make([]*labels.Matcher, 0, 1)
		}
		// Always drop wildcard matchers
		if m.Type == labels.MatchRegexp && m.Value == ".*" {
			continue
		}
		matchersByName[m.Name] = append(matchersByName[m.Name], m)
	}
	outMatchers := make([]*labels.Matcher, 0, len(ms))
	for _, matchers := range matchersByName {
		matchers = dedupeMatchers(matchers)
		matchersByType := groupMatchersByType(matchers)
		// If we have more than one unique equals matcher, we can just return those;
		// we know we'll only return an empty set
		outMatchers = append(outMatchers, matchersByType[labels.MatchEqual]...)
		if len(matchersByType[labels.MatchEqual]) > 1 {
			continue
		}
		outMatchers = append(outMatchers, filterRegexMatchers(matchersByType)...)
		outMatchers = append(outMatchers, filterNotEqualsMatchers(matchersByType)...)
		outMatchers = append(outMatchers, filterNotRegexMatchers(matchersByType)...)
	}
	return outMatchers
}

func filterRegexMatchers(mf map[labels.MatchType][]*labels.Matcher) []*labels.Matcher {
	outMatchers := make([]*labels.Matcher, 0, len(mf[labels.MatchRegexp]))
	for _, m := range mf[labels.MatchRegexp] {
		matchesNoEquals := true
		// If a regex matcher matches any equals matcher, that equals matcher is a subset of the regex,
		// and we can throw out the regex matcher.
		for _, em := range mf[labels.MatchEqual] {
			if m.Matches(em.Value) {
				matchesNoEquals = false
				break
			}
		}
		if matchesNoEquals {
			outMatchers = append(outMatchers, m)
		}
	}
	return outMatchers
}

// filterNotEqualsMatchers returns a subset of not-equals matchers which should actually reduce the result set size.
// This subset is determined by comparing each not-equals matchers against equals and not-regex matchers.
// A not-equals matcher is dropped if:
// - there exists any not-regex matcher which would also remove the not-equals matcher value from the result set, OR
// - there exists any equals matcher for a value that is matched by the not-equals matcher
func filterNotEqualsMatchers(mf map[labels.MatchType][]*labels.Matcher) []*labels.Matcher {
	outMatchers := make([]*labels.Matcher, 0, len(mf[labels.MatchNotEqual]))
	// If the value of a not-equals matcher is matched by the inverse of any not-regex matcher,
	// that not-equals matcher can be dropped, because it will select a subset of the regex.
	// Using the example of x!="a" and x!~"a.*",
	// x!~"a.*" will exclude more values than x!="a".
	for _, m := range mf[labels.MatchNotEqual] {
		matchesNoRegex := true
		for _, nr := range mf[labels.MatchNotRegexp] {
			if inv, err := nr.Inverse(); err == nil {
				if inv.Matches(m.Value) {
					matchesNoRegex = false
					break
				}
			}
		}
		matchesNoEquals := true
		// If the value of any equals matcher is matched by the not-equals matcher, only keep the equals matcher,
		// because the not-equals will not "subtract" anything from the final set.
		for _, e := range mf[labels.MatchEqual] {
			if m.Matches(e.Value) {
				matchesNoEquals = false
				break
			}
		}
		if matchesNoRegex && matchesNoEquals {
			outMatchers = append(outMatchers, m)
		}
	}
	return outMatchers
}

// filterNotRegexMatchers returns a subset of not-regex matchers which should actually reduce the result set size.
// A not-regex matcher is dropped if any equals matcher is matched by the not-regex matcher.
func filterNotRegexMatchers(mf map[labels.MatchType][]*labels.Matcher) []*labels.Matcher {
	outMatchers := make([]*labels.Matcher, 0, len(mf[labels.MatchNotRegexp]))
	for _, nr := range mf[labels.MatchNotRegexp] {
		matchesNoEquals := true
		// If the value of any equals matcher is matched by the not-regex matcher, drop the not-regex matcher,
		// because it will not subtract anything from the final set.
		for _, e := range mf[labels.MatchEqual] {
			if nr.Matches(e.Value) {
				matchesNoEquals = false
				break
			}
		}
		if matchesNoEquals {
			outMatchers = append(outMatchers, nr)
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
