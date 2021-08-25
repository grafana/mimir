// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"sort"
	"strings"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/pkg/labels"
)

// ActiveSeriesCustomTrackersConfig configures the additional custom trackers for active series in the ingester.
type ActiveSeriesCustomTrackersConfig map[string]string

func (c *ActiveSeriesCustomTrackersConfig) String() string {
	if *c == nil {
		return ""
	}

	strs := make([]string, 0, len(*c))
	for name, matcher := range *c {
		strs = append(strs, fmt.Sprintf("%s:%s", name, matcher))
	}
	return strings.Join(strs, ";")
}

func (c *ActiveSeriesCustomTrackersConfig) Set(s string) error {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	if *c == nil {
		*c = map[string]string{}
	}

	pairs := strings.Split(s, ";")
	for i, p := range pairs {
		split := strings.SplitN(p, ":", 2)
		if len(split) != 2 {
			return fmt.Errorf("value should be <name>:<matcher>[;<name>:<matcher>]*, but colon was not found in the value %d: %q", i, p)
		}
		name, matcher := strings.TrimSpace(split[0]), strings.TrimSpace(split[1])
		if len(name) == 0 || len(matcher) == 0 {
			return fmt.Errorf("semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value %d: %q", i, p)
		}
		if _, ok := (*c)[name]; ok {
			return fmt.Errorf("matcher %q for active series custom trackers is provided twice", name)
		}
		(*c)[name] = matcher
	}
	return nil
}

func NewActiveSeriesMatchers(matchers ActiveSeriesCustomTrackersConfig) (*ActiveSeriesMatchers, error) {
	asm := &ActiveSeriesMatchers{}
	for name, matcher := range matchers {
		sm, err := amlabels.ParseMatchers(matcher)
		if err != nil {
			return nil, fmt.Errorf("can't build active series matcher %s: %w", name, err)
		}
		matchers := make(labelsMatchers, len(sm))
		for i, m := range sm {
			matchers[i] = amlabelMatcherToProm(m)
		}

		asm.matchers = append(asm.matchers, matchers)
		asm.names = append(asm.names, name)
	}
	// Sort the result to make it deterministic for tests.
	// Order doesn't matter for the functionality as long as the order remains consistent during the execution of the program.
	sort.Sort(asm)
	return asm, nil
}

type ActiveSeriesMatchers struct {
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
