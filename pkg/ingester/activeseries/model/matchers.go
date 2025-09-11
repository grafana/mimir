// SPDX-License-Identifier: AGPL-3.0-only

package activeseriesmodel

import (
	"cmp"
	"maps"
	"slices"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
)

func NewMatchers(matchersConfig CustomTrackersConfig) *Matchers {
	names := make([]string, 0, len(matchersConfig.config))
	unoptimized := make([]indexedMatchers, 0, len(matchersConfig.config))

	// singleLabelMatchers is a map of label names to matchers that match on that single label.
	// This is an optimization under the assumption that there are more of these than labels in a series,
	// so we don't have to lookup the label value every time.
	// This does not include matchers that match on empty labels.
	singleLabelMatchers := make(map[string][]indexedMatchers)

	// matchersForExactValue is a list of matchers that have at least one SetMatcher or exact value matcher.
	matchersForExactValue := make([]indexedMatchers, 0, len(matchersConfig.config))
	// As we scan them, keep track of the amount of different values matched per label name.
	valuesMatchedPerLabelName := map[string]map[string]struct{}{}
	for name, ms := range matchersConfig.config {
		index := len(names)
		names = append(names, name)

		if len(ms) == 1 && !ms[0].Matches("") {
			// This is a single matcher that matches on a single label.
			// We can optimize it by treating it as a single label matcher.
			singleLabelMatchers[ms[0].Name] = append(singleLabelMatchers[ms[0].Name], indexedMatchers{labelsMatchers: ms, index: index})
			continue
		}

		optimized := false
	matcherLoop:
		for _, m := range ms {
			switch m.Type {
			case labels.MatchEqual:
				if m.Value != "" {
					matchersForExactValue = append(matchersForExactValue, indexedMatchers{labelsMatchers: ms, index: index})
					if valuesMatchedPerLabelName[m.Name] == nil {
						valuesMatchedPerLabelName[m.Name] = make(map[string]struct{})
					}
					valuesMatchedPerLabelName[m.Name][m.Value] = struct{}{}
					optimized = true
					break matcherLoop
				}
			case labels.MatchRegexp:
				sm := m.SetMatches()
				if len(sm) > 0 {
					matchersForExactValue = append(matchersForExactValue, indexedMatchers{labelsMatchers: ms, index: index})
					if valuesMatchedPerLabelName[m.Name] == nil {
						valuesMatchedPerLabelName[m.Name] = make(map[string]struct{})
					}
					for _, v := range sm {
						valuesMatchedPerLabelName[m.Name][v] = struct{}{}
					}
					optimized = true
					break matcherLoop
				}
			}
		}

		if !optimized {
			unoptimized = append(unoptimized, indexedMatchers{labelsMatchers: ms, index: index})
		}
	}

	// Get label names and sort them by popularity (desc).
	// We select labels that have the most different values matched first to make the decision tree higher and lower.
	labelNames := slices.SortedFunc(
		maps.Keys(valuesMatchedPerLabelName),
		func(a, b string) int {
			return cmp.Compare(len(valuesMatchedPerLabelName[b]), len(valuesMatchedPerLabelName[a]))
		},
	)

	// valueMatchers is a map of label names to a map of values to indexedMatchers.
	valueMatchers := make(map[string]map[string][]indexedMatchers, len(labelNames))
	// We can probably optimize this 3 nested loops, but:
	// - This is not the hot path
	// - Probably there are not so many labelNames.
	// - Probably there are not so many labelsMatchers
	for _, name := range labelNames {
		for mi := 0; mi < len(matchersForExactValue); {
			ms := matchersForExactValue[mi]
			removed := false
			for lmi, m := range ms.labelsMatchers {
				if m.Name != name {
					continue
				}
				var sm []string
				if m.Type == labels.MatchEqual && m.Value != "" {
					// This is not a set matcher, but a simple equality matcher.
					// We'll treat it as a set matcher with a single value.
					sm = []string{m.Value}
				} else if sm = m.SetMatches(); len(sm) == 0 {
					continue
				}

				if valueMatchers[name] == nil {
					valueMatchers[name] = make(map[string][]indexedMatchers)
				}
				var lms labelsMatchers
				if len(ms.labelsMatchers) > 1 {
					// Remove this matcher, keep the rest.
					lms = slices.Delete(slices.Clone(ms.labelsMatchers), lmi, lmi+1)
				}
				for _, v := range sm {
					valueMatchers[name][v] = append(valueMatchers[name][v], indexedMatchers{labelsMatchers: lms, index: ms.index})
				}
				// Remove this matcher from matchersForExactValue list, so we don't have to check it again.
				matchersForExactValue = slices.Delete(matchersForExactValue, mi, mi+1)
				// Don't increase mi, because we just removed an element.
				removed = true
				break
			}
			if !removed {
				mi++
			}
		}
	}
	if len(matchersForExactValue) > 0 {
		panic("this should not happen, we should've classified all matchers in the previous loop")
	}

	return &Matchers{
		cfg:   matchersConfig,
		names: names,
		// singleLabelMatchers is a map of label names to matchers that match on that single label.
		singleLabelMatchers: singleLabelMatchers,
		// valueMatchers is a map of label names to a map of values to indexedMatchers that match on that label and value.
		valueMatchers: valueMatchers,
		// unoptimized matchers are the rest of the matchers that are not optimized.
		unoptimized: unoptimized,
	}
}

type Matchers struct {
	cfg                 CustomTrackersConfig
	names               []string
	unoptimized         []indexedMatchers
	valueMatchers       map[string]map[string][]indexedMatchers
	singleLabelMatchers map[string][]indexedMatchers
}

type indexedMatchers struct {
	labelsMatchers
	// index is the index of the matcher name in the matchers names slice.
	index int
}

func (m *Matchers) MatcherNames() []string {
	return m.names
}

func (m *Matchers) Config() CustomTrackersConfig {
	return m.cfg
}

// Matches returns a PreAllocDynamicSlice containing only matcher indexes which are matching
func (m *Matchers) Matches(series labels.Labels) PreAllocDynamicSlice {
	var matches PreAllocDynamicSlice
	if len(m.valueMatchers)+len(m.singleLabelMatchers) > 0 {
		series.Range(func(l labels.Label) {
			// Are there valueMatchers for this label?
			if vm, ok := m.valueMatchers[l.Name]; ok {
				// Is this label value in the valueMatchers?
				// Check all matchers for this label value.
				for _, ms := range vm[l.Value] {
					if ms.Matches(series) {
						matches.Append(ms.index)
					}
				}
			}
			// Are there singleLabelMatchers for this label?
			if slm, ok := m.singleLabelMatchers[l.Name]; ok {
				// Check all singleLabelMatchers for this label.
				for _, ms := range slm {
					if ms.Matches(series) {
						matches.Append(ms.index)
					}
				}
			}
		})
	}
	for _, ms := range m.unoptimized {
		if ms.Matches(series) {
			matches.Append(ms.index)
		}
	}
	return matches
}

// labelsMatchers is like alertmanager's labels.Matchers but for Prometheus' labels.Matcher slice
type labelsMatchers []*labels.Matcher

// Matches checks whether all matchers are fulfilled against the given label set.
// This is like amlabels.Matchers.Matches but works with labels.Labels instead of requiring a model.LabelSet which is a map
func (ms labelsMatchers) Matches(lset labels.Labels) bool {
	for _, m := range ms {
		if !m.Matches(lset.Get(m.Name)) {
			return false
		}
	}
	return true
}

func amlabelMatcherToProm(m *amlabels.Matcher) *labels.Matcher {
	// labels.MatchType(m.Type) is a risky conversion because it depends on the iota order, but we have a test for it
	return labels.MustNewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
}

const preAllocatedSize = 3

// PreAllocDynamicSlice is a slice-like uint16 data structure that allocates space for the first `preAllocatedSize` elements.
// When more than `preAllocatedSize` elements are appended, it allocates a slice that escapes to heap.
// This trades in extra allocated space (2x more with `preAllocatedSize=3`) for zero-allocations in most of the cases,
// relying on the assumption that most of the matchers will not match more than `preAllocatedSize` trackers.
type PreAllocDynamicSlice struct {
	arr  [preAllocatedSize]uint16
	arrl byte
	rest []uint16
}

func (fs *PreAllocDynamicSlice) Append(val int) {
	if fs.arrl < preAllocatedSize {
		fs.arr[fs.arrl] = uint16(val)
		fs.arrl++
		return
	}
	fs.rest = append(fs.rest, uint16(val))
}

func (fs *PreAllocDynamicSlice) Get(idx int) uint16 {
	if idx < preAllocatedSize {
		return fs.arr[idx]
	}
	return fs.rest[idx-preAllocatedSize]
}

func (fs *PreAllocDynamicSlice) Len() int {
	return int(fs.arrl) + len(fs.rest)
}
