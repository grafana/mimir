package lookupplan

import (
	"context"
	"slices"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
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
	outIndexMatchers := make([]*labels.Matcher, 0)
	outScanMatchers := make([]*labels.Matcher, 0)
	for _, m := range inPlan.IndexMatchers() {
		// We only want to add the matcher if it hasn't already been added to an output slice
		if _, ok := dedupedMatchers[*m]; ok && len(dedupedMatchers[*m]) < 2 {
			outIndexMatchers = append(outIndexMatchers, m)
			dedupedMatchers[*m] = append(dedupedMatchers[*m], true)
		}
	}
	for _, m := range inPlan.ScanMatchers() {
		if _, ok := dedupedMatchers[*m]; ok && len(dedupedMatchers[*m]) < 2 {
			outScanMatchers = append(outScanMatchers, m)
			dedupedMatchers[*m] = append(dedupedMatchers[*m], true)
		}
	}
	return &concreteLookupPlan{
		indexMatchers: outIndexMatchers,
		scanMatchers:  outScanMatchers,
	}, nil
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
			if i == len(matchers)-1 && len(matchersToAddForName) == 0 {
				matchersToAddForName = append(matchersToAddForName, m)
			} else if !(
			// If the matcher matches all non-empty values, we skip this block
			(m.Type == labels.MatchRegexp && m.Value == ".*") ||
				((m.Type == labels.MatchNotRegexp || m.Type == labels.MatchNotEqual) && m.Value == "")) {
				matchersToAddForName = append(matchersToAddForName, m)
			}
		}
		filteredMatchers = append(filteredMatchers, matchersToAddForName...)
	}
	return filteredMatchers
}
