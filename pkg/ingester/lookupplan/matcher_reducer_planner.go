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

	// For the purpose of deduping, we'll treat all matchers the same for now
	matchers := slices.Concat(inPlan.IndexMatchers(), inPlan.ScanMatchers())
	matchers = filterDuplicateLabelNameNonemptyMatchers(matchers)

	dedupedMatchers := make(map[labels.Matcher]bool, len(matchers))
	for _, m := range matchers {
		dedupedMatchers[*m] = false
	}
	droppedMatchers := make([]*labels.Matcher, 0)
	outIndexMatchers, dedupedMatchers, droppedMatchers := buildOutMatchers(inPlan.IndexMatchers(), dedupedMatchers, droppedMatchers)
	outScanMatchers, _, droppedMatchers := buildOutMatchers(inPlan.ScanMatchers(), dedupedMatchers, droppedMatchers)

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
// less any duplicate matchers. dedupedMatchers contains the set of deduplicated matchers
// with nonselective matchers removed. If dedupedMatchers[*m] is false, the matcher has not been added to any result set.
// droppedMatchers is used for logging purposes to record which matchers have been removed from the plan.
func buildOutMatchers(inMatchers []*labels.Matcher, dedupedMatchers map[labels.Matcher]bool, droppedMatchers []*labels.Matcher) ([]*labels.Matcher, map[labels.Matcher]bool, []*labels.Matcher) {
	outMatchers := make([]*labels.Matcher, 0)
	for _, m := range inMatchers {
		// dedupedMatchers is used to both keep track of all unique matchers (evidenced by existence in the map),
		// and whether the matcher has already been added to a set of output matchers (evidenced by the value in the map).
		// We only want to add the matcher if it hasn't already been added to an output slice.
		if _, ok := dedupedMatchers[*m]; ok && !dedupedMatchers[*m] {
			outMatchers = append(outMatchers, m)
			dedupedMatchers[*m] = true
		} else {
			droppedMatchers = append(droppedMatchers, m)
		}
	}
	return outMatchers, dedupedMatchers, droppedMatchers
}

// filterDuplicateLabelNameNonemptyMatchers returns a slice of the input matchers,
// excluding any matchers with duplicate label names where one of said matchers matches all nonempty values.
// For example, an input of namespace="foo", namespace!="" should return only namespace="foo".
// If multiple matchers all match any nonempty value, only one matcher for that label name will be returned.
// Note that the input order is not maintained.
func filterDuplicateLabelNameNonemptyMatchers(ms []*labels.Matcher) []*labels.Matcher {
	matchersByName := make(map[string][]*labels.Matcher)
	for _, m := range ms {
		if _, ok := matchersByName[m.Name]; !ok {
			matchersByName[m.Name] = make([]*labels.Matcher, 0, 1)
		}
		matchersByName[m.Name] = append(matchersByName[m.Name], m)
	}
	filteredMatchers := make([]*labels.Matcher, 0, len(ms))

	for _, matchers := range matchersByName {
		matchersToAddForName := make([]*labels.Matcher, 0, len(matchers))
		// We need to return at least one matcher for each label name,
		// so for each label name, we keep matchers to the output if they're selective.
		// If we get to the last matcher for the label name and haven't found any selective matchers yet,
		// we just keep the last matcher.
		for i, m := range matchers {
			if (i == len(matchers)-1 && len(matchersToAddForName) == 0) || !isNonSelectiveMatcher(m) {
				matchersToAddForName = append(matchersToAddForName, m)
			}
		}
		filteredMatchers = append(filteredMatchers, matchersToAddForName...)
	}
	return filteredMatchers
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
