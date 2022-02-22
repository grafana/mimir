// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"sort"
	"strings"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
)

func (asm *ActiveSeriesMatchers) String() string {
	if asm == nil {
		return ""
	}

	var sb strings.Builder
	for i, name := range asm.names {
		sb.WriteString(name)
		for _, labelMatcher := range asm.matchers[i] {
			sb.WriteString(labelMatcher.String())
		}
	}
	return sb.String()
}

func (asm *ActiveSeriesMatchers) Set(s string) error {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	if len(asm.names) != 0 {
		return fmt.Errorf("can't provide active series custom trackers flag multple times")
	}

	c := map[string]string{}

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
		if _, ok := c[name]; ok {
			return fmt.Errorf("matcher %q for active series custom trackers is provided twice", name)
		}
		c[name] = matcher
	}

	a, err := NewActiveSeriesMatchers(c)
	if err != nil {
		return err
	}
	*asm = *a

	return nil
}

func (asm *ActiveSeriesMatchers) ExampleDoc() (comment string, yaml interface{}) {
	return `The following configuration will count the active series coming from dev and prod namespaces for each tenant` +
			` and label them as {name="dev"} and {name="prod"} in the cortex_ingester_active_series_custom_tracker metric.`,
		map[string]string{
			"dev":  `{namespace=~"dev-.*"}`,
			"prod": `{namespace=~"prod-.*"}`,
		}
}

func NewActiveSeriesMatchers(matchersConfig map[string]string) (*ActiveSeriesMatchers, error) {
	asm := &ActiveSeriesMatchers{}
	for name, matcher := range matchersConfig {
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
	// The concatenation should happen after ordering, to ensure equality is not dependent on map traversal.
	asm.key = asm.String()

	return asm, nil
}

type ActiveSeriesMatchers struct {
	key      string
	names    []string
	matchers []labelsMatchers
}

func (asm *ActiveSeriesMatchers) Equals(other *ActiveSeriesMatchers) bool {
	if asm == nil || other == nil {
		return asm == other
	}
	return asm.key == other.key
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
// ActiveSeriesMatchers are marshaled in yaml as a map, with matcher names as keys and strings as matchers definitions.
func (asm *ActiveSeriesMatchers) UnmarshalYAML(unmarshal func(interface{}) error) error {
	m := map[string]string{}
	err := unmarshal(&m)
	if err != nil {
		return err
	}
	var newMatchers *ActiveSeriesMatchers
	newMatchers, err = NewActiveSeriesMatchers(m)
	if err != nil {
		return err
	}
	*asm = *newMatchers
	return nil
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
