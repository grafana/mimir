// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"sort"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
)

func NewActiveSeriesMatchers(matchersConfig *ActiveSeriesCustomTrackersConfig) *ActiveSeriesMatchers {
	asm := &ActiveSeriesMatchers{cfg: matchersConfig}
	for name, matchers := range (*matchersConfig).config {
		asm.matchers = append(asm.matchers, matchers)
		asm.names = append(asm.names, name)
	}
	// Sort the result to make it deterministic for tests.
	// Order doesn't matter for the functionality as long as the order remains consistent during the execution of the program.
	sort.Sort(asm)
	return asm
}

func (asm *ActiveSeriesMatchers) Equals(other *ActiveSeriesMatchers) bool {
	if asm == nil || other == nil {
		return asm == other
	}
	return asm.cfg.String() == other.cfg.String()
}

type ActiveSeriesMatchers struct {
	cfg      *ActiveSeriesCustomTrackersConfig
	names    []string
	matchers []labelsMatchers
}

func (asm *ActiveSeriesMatchers) MatcherNames() []string {
	return asm.names
}

func (asm *ActiveSeriesMatchers) Matches(series labels.Labels) []bool {
	if len(asm.matchers) == 0 {
		return nil
	}
	matches := make([]bool, len(asm.matchers))
	for i, sm := range asm.matchers {
		matches[i] = sm.Matches(series)
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

func (asm *ActiveSeriesMatchers) Len() int {
	return len(asm.names)
}

func (asm *ActiveSeriesMatchers) Less(i, j int) bool {
	return asm.names[i] < asm.names[j]
}

func (asm *ActiveSeriesMatchers) Swap(i, j int) {
	asm.names[i], asm.names[j] = asm.names[j], asm.names[i]
	asm.matchers[i], asm.matchers[j] = asm.matchers[j], asm.matchers[i]
}

func amlabelMatcherToProm(m *amlabels.Matcher) *labels.Matcher {
	// labels.MatchType(m.Type) is a risky conversion because it depends on the iota order, but we have a test for it
	return labels.MustNewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
}
