// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
	"sort"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
)

func NewMatchers(matchersConfig CustomTrackersConfig) *Matchers {
	asm := &Matchers{cfg: matchersConfig}
	for name, matchers := range matchersConfig.config {
		asm.matchers = append(asm.matchers, matchers)
		asm.names = append(asm.names, name)
	}
	// Sort the result to make it deterministic for tests.
	// Order doesn't matter for the functionality as long as the order remains consistent during the execution of the program.
	sort.Sort(asm)
	return asm
}

type Matchers struct {
	cfg      CustomTrackersConfig
	names    []string
	matchers []labelsMatchers
}

func (m *Matchers) MatcherNames() []string {
	return m.names
}

func (m *Matchers) Config() CustomTrackersConfig {
	return m.cfg
}

// Matches returns a fixedSlice containing only matcher indexes which are matching
func (m *Matchers) Matches(series labels.Labels) fixedSlice {
	if len(m.matchers) == 0 {
		return fixedSlice{}
	}
	var matches fixedSlice
	for i, sm := range m.matchers {
		if sm.Matches(series) {
			matches.append(i)
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

func (m *Matchers) Len() int {
	return len(m.names)
}

func (m *Matchers) Less(i, j int) bool {
	return m.names[i] < m.names[j]
}

func (m *Matchers) Swap(i, j int) {
	m.names[i], m.names[j] = m.names[j], m.names[i]
	m.matchers[i], m.matchers[j] = m.matchers[j], m.matchers[i]
}

func amlabelMatcherToProm(m *amlabels.Matcher) *labels.Matcher {
	// labels.MatchType(m.Type) is a risky conversion because it depends on the iota order, but we have a test for it
	return labels.MustNewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
}

const fixedSliceSize = 4

type fixedSlice struct {
	arr  [fixedSliceSize]int
	arrl int
	rest []int
}

func (fs *fixedSlice) append(val int) {
	if fs.arrl < fixedSliceSize {
		fs.arr[fs.arrl] = val
		fs.arrl++
		return
	}
	fs.rest = append(fs.rest, val)
}

func (fs *fixedSlice) get(idx int) int {
	if idx < fixedSliceSize {
		return fs.arr[idx]
	}
	return fs.rest[idx-fixedSliceSize]
}

func (fs *fixedSlice) len() int {
	return fs.arrl + len(fs.rest)
}
