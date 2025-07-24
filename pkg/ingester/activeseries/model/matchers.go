// SPDX-License-Identifier: AGPL-3.0-only

package activeseriesmodel

import (
	"maps"
	"slices"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
)

type indexedMatchers struct {
	labelsMatchers
	index int
}

func NewMatchers(matchersConfig CustomTrackersConfig) *Matchers {
	// First assign indexes to all matchers, they're unoptimized by default.
	names := make([]string, len(matchersConfig.config))
	unoptimized := make([]indexedMatchers, len(matchersConfig.config))
	i := 0
	// As we scan them, find the most popular label names with SetMatchers.
	matchersCountWithSetMatchersForLabelName := map[string]int{}
	for name, ms := range matchersConfig.config {
		names[i], unoptimized[i] = name, indexedMatchers{ms, i}
		i++
		for _, m := range ms {
			if m.Type == labels.MatchEqual && m.Value != "" {
				matchersCountWithSetMatchersForLabelName[m.Name]++
			} else if sm := m.SetMatches(); len(sm) > 0 {
				matchersCountWithSetMatchersForLabelName[m.Name]++
			}
		}
	}
	// Get label names and sort them by popularity (desc).
	labelNames := slices.Collect(maps.Keys(matchersCountWithSetMatchersForLabelName))
	slices.SortFunc(labelNames, func(a, b string) int {
		return matchersCountWithSetMatchersForLabelName[b] - matchersCountWithSetMatchersForLabelName[a]
	})

	// setMatchers is a map of label names to a map of values to indexedMatchers.
	setMatchers := make(map[string]map[string][]indexedMatchers, len(labelNames))
	// We can probably optimize this 3 nested loops, but:
	// - This is not the hot path
	// - Probably there are not so many labelNames.
	// - Probably there are not so many labelsMatchers
	for _, name := range labelNames {
		for ui := 0; ui < len(unoptimized); {
			um := unoptimized[ui]
			for lmi, m := range um.labelsMatchers {
				if m.Name != name {
					ui++
					continue
				}
				var sm []string
				if m.Type == labels.MatchEqual && m.Value != "" {
					// This is not a set matcher, but a simple equality matcher.
					// We'll treat it as a set matcher with a single value.
					stack := [1]string{m.Value}
					sm = stack[:]
				} else if sm = m.SetMatches(); len(sm) == 0 {
					ui++
					continue
				}

				if setMatchers[name] == nil {
					setMatchers[name] = make(map[string][]indexedMatchers)
				}
				var lms labelsMatchers
				if len(um.labelsMatchers) > 1 {
					// Remove this matcher, keep the rest.
					lms = slices.Delete(slices.Clone(um.labelsMatchers), lmi, lmi+1)
				}
				for _, v := range sm {
					setMatchers[name][v] = append(setMatchers[name][v], indexedMatchers{labelsMatchers: lms, index: um.index})
				}
				// Remove this matcher from unoptimized list, so we don't have to check it again.
				unoptimized = slices.Delete(unoptimized, ui, ui+1)
				// Don't increase ui, because we just removed an element.
			}
		}
	}

	// singleLabelMatchers is a map of label names to matchers that match on that single label.
	// This is an optimization under the assumption that there are more of these than labels in a series,
	// so we don't have to lookup the label value every time.
	// This does not include matchers that match on empty labels.
	singleLabelMatchers := make(map[string][]indexedMatchers)
	for ui := 0; ui < len(unoptimized); {
		um := unoptimized[ui]
		if len(um.labelsMatchers) != 1 {
			ui++
			continue
		}
		m := um.labelsMatchers[0]
		if m.Matches("") {
			// Can't optimize these, because they match on empty label value, so we won't try it when iterating series labels.
			ui++
			continue
		}
		singleLabelMatchers[m.Name] = append(singleLabelMatchers[m.Name], um)
		unoptimized = slices.Delete(unoptimized, ui, ui+1)
	}

	return &Matchers{
		cfg:                 matchersConfig,
		names:               names,
		unoptimized:         unoptimized,
		setMatchers:         setMatchers,
		singleLabelMatchers: singleLabelMatchers,
	}
}

type Matchers struct {
	cfg                 CustomTrackersConfig
	names               []string
	unoptimized         []indexedMatchers
	setMatchers         map[string]map[string][]indexedMatchers
	singleLabelMatchers map[string][]indexedMatchers
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
	if len(m.setMatchers)+len(m.singleLabelMatchers) > 0 {
		series.Range(func(l labels.Label) {
			// Are there setMatchers for this label?
			if sm, ok := m.setMatchers[l.Name]; ok {
				// Is this label value in the setMatchers?
				// Check all matchers for this label value.
				for _, ms := range sm[l.Value] {
					if ms.Matches(series) {
						matches.Append(ms.index)
					}
				}
			}
			// Are there singleLabelMatchers for this label?
			if sm, ok := m.singleLabelMatchers[l.Name]; ok {
				// Check all singleLabelMatchers for this label.
				for _, ms := range sm {
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
