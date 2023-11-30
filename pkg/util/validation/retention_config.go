package validation

import (
	"fmt"
	"sort"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/yaml.v3"
)

type labelsMatchers []*labels.Matcher

type Policy struct {
	Duration model.Duration
	Matchers []labelsMatchers
}

// RetentionPolicy is a map of retention policies, the key is the duration and the value is a list of regexes.
// We don't need to implement Set and String because we don't use it as flag.
// Why golang just can't have a sorted map by default?!!!
type RetentionCfg struct {
	source map[model.Duration][]string
	config []Policy
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
// RetentionCfg are marshaled in yaml as a map[time.Duration][]string, with retention duration as keys and promql queries as value vector.
func (c *RetentionCfg) UnmarshalYAML(value *yaml.Node) error {
	stringMap := map[model.Duration][]string{}
	err := value.DecodeWithOptions(&stringMap, yaml.DecodeOptions{KnownFields: true})
	if err != nil {
		return err
	}
	*c, err = NewRetentionConfig(stringMap)
	return err
}

// MarshalYAML implements yaml.Marshaler.
func (c RetentionCfg) MarshalYAML() (interface{}, error) {
	return c.source, nil
}

func NewRetentionConfig(m map[model.Duration][]string) (c RetentionCfg, err error) {
	c.source = m
	c.config = []Policy{}

	for duration, matchersEle := range m {
		policy := Policy{Duration: duration}
		policy.Matchers = make([]labelsMatchers, 0, len(matchersEle))
		for _, matcher := range matchersEle {
			sm, err := amlabels.ParseMatchers(matcher)
			if err != nil {
				return c, fmt.Errorf("can't build active series matcher %s: %w", duration, err)
			}
			ms := make(labelsMatchers, len(sm))
			for i, m := range sm {
				ms[i] = amlabelMatcherToProm(m)
			}
			policy.Matchers = append(policy.Matchers, ms)
		}
		c.config = append(c.config, policy)
	}
	sort.Slice(c.config, func(i, j int) bool {
		return c.config[i].Duration < c.config[j].Duration
	})
	return c, nil
}

func amlabelMatcherToProm(m *amlabels.Matcher) *labels.Matcher {
	// labels.MatchType(m.Type) is a risky conversion because it depends on the iota order, but we have a test for it
	return labels.MustNewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
}
