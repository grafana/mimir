// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"sort"
	"strings"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
)

type ActiveSeriesCustomTrackersConfigValue map[string]labelsMatchers

type ActiveSeriesCustomTrackersConfig struct {
	config ActiveSeriesCustomTrackersConfigValue `yaml:"active_series_custom_trackers"`
	key    string
}

func (c *ActiveSeriesCustomTrackersConfig) String() string {
	if (*c).config == nil {
		return ""
	}
	keys := make([]string, len((*c).config))
	for name := range (*c).config {
		keys = append(keys, name)
	}
	// The map is traversed in an ordered fashion to make String representaton stable and comparable.
	sort.Strings(keys)

	var sb strings.Builder
	for _, name := range keys {
		sb.WriteString(name)
		for _, labelMatcher := range ((*c).config)[name] {
			sb.WriteString(labelMatcher.String())
		}
	}
	return sb.String()
}

func (c *ActiveSeriesCustomTrackersConfig) Set(s string) error {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	if (*c).config == nil {
		*c = ActiveSeriesCustomTrackersConfig{}
		(*c).config = map[string]labelsMatchers{}
	}

	source := map[string]string{}
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
		if _, ok := source[name]; ok {
			return fmt.Errorf("matcher %q for active series custom trackers is provided twice", name)
		}
		source[name] = matcher
	}
	config, err := NewActiveSeriesCustomTrackersConfig(source)
	if err != nil {
		return err
	}
	for name, matchers := range (*config).config {
		// This check is when the value comes from multiple flags
		if _, ok := (*c).config[name]; ok {
			return fmt.Errorf("matcher %q for active series custom trackers is provided twice", name)
		}
		(*c).config[name] = matchers
	}
	(*c).key = (*c).String()

	return nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
// ActiveSeriesCustomTrackersConfig are marshaled in yaml as a map[string]string, with matcher names as keys and strings as matchers definitions.
func (c *ActiveSeriesCustomTrackersConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	stringMap := map[string]string{}
	err := unmarshal(&stringMap)
	if err != nil {
		return err
	}
	config, err := NewActiveSeriesCustomTrackersConfig(stringMap)
	if err != nil {
		return err
	}
	*c = *config
	return nil
}

func NewActiveSeriesCustomTrackersConfig(m map[string]string) (*ActiveSeriesCustomTrackersConfig, error) {
	c := ActiveSeriesCustomTrackersConfig{}
	c.config = map[string]labelsMatchers{}
	for name, matcher := range m {
		sm, err := amlabels.ParseMatchers(matcher)
		if err != nil {
			return nil, fmt.Errorf("can't build active series matcher %s: %w", name, err)
		}
		matchers := make(labelsMatchers, len(sm))
		for i, m := range sm {
			matchers[i] = amlabelMatcherToProm(m)
		}
		c.config[name] = matchers
	}
	c.key = c.String()
	return &c, nil
}

func (c *ActiveSeriesCustomTrackersConfigValue) ExampleDoc() (comment string, yaml interface{}) {
	return `The following configuration will count the active series coming from dev and prod namespaces for each tenant` +
			` and label them as {name="dev"} and {name="prod"} in the cortex_ingester_active_series_custom_tracker metric.`,
		map[string]string{
			"dev":  `{namespace=~"dev-.*"}`,
			"prod": `{namespace=~"prod-.*"}`,
		}
}

func NewActiveSeriesMatchers(matchersConfig *ActiveSeriesCustomTrackersConfig) *ActiveSeriesMatchers {
	asm := &ActiveSeriesMatchers{}
	for name, matchers := range (*matchersConfig).config {
		asm.matchers = append(asm.matchers, matchers)
		asm.names = append(asm.names, name)
	}
	// Sort the result to make it deterministic for tests.
	// Order doesn't matter for the functionality as long as the order remains consistent during the execution of the program.
	sort.Sort(asm)
	// ActiveSeriesCustomTrackersConfig.key is suitable for fast equality checks.
	asm.key = matchersConfig.key

	return asm
}

func (asm *ActiveSeriesMatchers) Equals(other *ActiveSeriesMatchers) bool {
	if asm == nil || other == nil {
		return asm == other
	}
	return asm.key == other.key
}

type ActiveSeriesMatchers struct {
	key      string
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
