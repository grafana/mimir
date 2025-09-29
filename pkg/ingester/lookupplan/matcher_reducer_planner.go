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

	dedupedMatchers := make(map[labels.Matcher][]bool, len(matchers))
	for _, m := range matchers {
		dedupedMatchers[*m] = []bool{true}
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

func buildOutMatchers(inMatchers []*labels.Matcher, dedupedMatchers map[labels.Matcher][]bool, droppedMatchers []*labels.Matcher) ([]*labels.Matcher, map[labels.Matcher][]bool, []*labels.Matcher) {
	outMatchers := make([]*labels.Matcher, 0)
	for _, m := range inMatchers {
		// We only want to add the matcher if it hasn't already been added to an output slice
		if _, ok := dedupedMatchers[*m]; ok && len(dedupedMatchers[*m]) < 2 {
			outMatchers = append(outMatchers, m)
			dedupedMatchers[*m] = append(dedupedMatchers[*m], true)
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
	filteredMatchers := make([]*labels.Matcher, 0)

	for _, matchers := range matchersByName {
		matchersToAddForName := make([]*labels.Matcher, 0)
		for i, m := range matchers {
			// If we're on the last element and haven't kept any matchers for this label name yet, keep this one.
			// Otherwise, only keep selective matchers.
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
