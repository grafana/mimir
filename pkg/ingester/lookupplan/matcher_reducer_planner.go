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
	matchers = deduplicateAndFilterNonselectiveMatchers(matchers)

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

func deduplicateAndFilterNonselectiveMatchers(ms []*labels.Matcher) []*labels.Matcher {
	outMatchers := make([]*labels.Matcher, 0, len(ms))
	matchersByName := make(map[string][]*labels.Matcher)
	for _, m := range ms {
		if _, ok := matchersByName[m.Name]; !ok {
			matchersByName[m.Name] = make([]*labels.Matcher, 0, len(ms))
		}
		matchersByName[m.Name] = append(matchersByName[m.Name], m)
	}
	for _, matchers := range matchersByName {
		matchers = dedupeMatchers(matchers)
		// If we have more than one unique matcher at all, filter out all universally non-selective matchers
		// (i.e., =~".*", which matches everything including empty strings).
		if len(matchers) > 1 {
			relevantMatchers := make([]*labels.Matcher, 0, len(matchers))
			for _, m := range matchers {
				if isNonSelectiveMatcher(m) && m.Matches("") {
					continue
				}
				relevantMatchers = append(relevantMatchers, m)
			}
			matchers = relevantMatchers
		}

		matchersForName := make([]*labels.Matcher, 0, len(ms))
		nonSelectiveMatchers := make([]*labels.Matcher, 0, len(ms))
		for _, m := range matchers {
			if isNonSelectiveMatcher(m) {
				nonSelectiveMatchers = append(nonSelectiveMatchers, m)
				continue
			}
			// We always keep selective matchers
			matchersForName = append(matchersForName, m)
		}

		// We want to know if any selective matchers match empty strings.
		// If there are none, we can use that information to drop some nonselective matchers.
		var selectiveEmptyStringMatchersExist bool
		for _, m := range matchersForName {
			if m.Matches("") {
				selectiveEmptyStringMatchersExist = true
				break
			}
		}

		for i, m := range nonSelectiveMatchers {
			// If we're on the last matcher for the label name and have returned none so far,
			// we should keep the last one.
			if i == len(nonSelectiveMatchers)-1 && len(matchersForName) == 0 {
				matchersForName = append(matchersForName, m)
				continue
			}
			// If this matcher doesn't match empty strings and no other matchers match empty strings,
			// this isn't reducing the set size, so we should drop it.
			if !m.Matches("") && !selectiveEmptyStringMatchersExist {
				continue
			}

			matchersForName = append(matchersForName, m)
		}
		outMatchers = append(outMatchers, matchersForName...)
	}
	return outMatchers
}

func dedupeMatchers(ms []*labels.Matcher) []*labels.Matcher {
	deduped := make(map[labels.Matcher]bool, len(ms))
	for _, m := range ms {
		deduped[*m] = true
	}
	outMatchers := make([]*labels.Matcher, 0, len(deduped))
	for m := range deduped {
		outMatchers = append(outMatchers, &m)
	}
	return outMatchers
}

func isNonSelectiveMatcher(m *labels.Matcher) bool {
	switch m.Type {
	case labels.MatchRegexp:
		return m.Value == ".*"
	case labels.MatchNotRegexp, labels.MatchNotEqual:
		return m.Value == ""
	default:
		return false
	}
}
