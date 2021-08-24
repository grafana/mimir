// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"strings"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/pkg/labels"
)

// ActiveSeriesCustomTrackersConfigs configures the additional custom trackers for active series in the ingester.
type ActiveSeriesCustomTrackersConfigs []ActiveSeriesCustomTrackerConfig

func (cfgs *ActiveSeriesCustomTrackersConfigs) String() string {
	strs := make([]string, len(*cfgs))
	for i, cfg := range *cfgs {
		strs[i] = cfg.String()
	}
	return strings.Join(strs, ";")
}

func (cfgs *ActiveSeriesCustomTrackersConfigs) Set(s string) error {
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
		*cfgs = append(*cfgs, ActiveSeriesCustomTrackerConfig{Name: name, Matcher: matcher})
	}
	return nil
}

// ActiveSeriesCustomTrackerConfig configures an additional custom tracker for active series in the ingester.
// Active series matched by the matcher will be exported in the metric labeled with the name provided.
type ActiveSeriesCustomTrackerConfig struct {
	Name    string `yaml:"name"`
	Matcher string `yaml:"matcher"`
}

func (cfg ActiveSeriesCustomTrackerConfig) String() string {
	return cfg.Name + ":" + cfg.Matcher
}

func NewActiveSeriesMatchers(cfgs ActiveSeriesCustomTrackersConfigs) (asm ActiveSeriesMatchers, _ error) {
	seenMatcherNames := map[string]int{}
	for i, cfg := range cfgs {
		if idx, seen := seenMatcherNames[cfg.Name]; seen {
			return asm, fmt.Errorf("active series matcher %d duplicates the name of matcher at position %d: %q", i, idx, cfg.Name)
		}
		seenMatcherNames[cfg.Name] = i

		sm, err := amlabels.ParseMatchers(cfg.Matcher)
		if err != nil {
			return asm, fmt.Errorf("can't build active series matcher %d: %w", i, err)
		}
		matchers := make(labelsMatchers, len(sm))
		for i, m := range sm {
			matchers[i] = amlabelMatcherToProm(m)
		}

		asm.matchers = append(asm.matchers, matchers)
		asm.names = append(asm.names, cfg.Name)
	}
	return asm, nil
}

type ActiveSeriesMatchers struct {
	names    []string
	matchers []labelsMatchers
}

func (asm ActiveSeriesMatchers) MatcherNames() []string {
	return asm.names
}

func (asm ActiveSeriesMatchers) Matches(series labels.Labels) []bool {
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

func amlabelMatcherToProm(m *amlabels.Matcher) *labels.Matcher {
	return labels.MustNewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
}
