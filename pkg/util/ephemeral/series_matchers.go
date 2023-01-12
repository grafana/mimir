// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import (
	"fmt"
	"strings"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"

	"github.com/grafana/mimir/pkg/mimirpb"

	"github.com/prometheus/prometheus/model/labels"
	"golang.org/x/exp/slices"
	"gopkg.in/yaml.v3"
)

// SeriesMatchers configures matchers based on which series get marked as ephemeral.
type SeriesMatchers struct {
	source []string
	config []labelMatchers
	string string
}

// labelMatchers is like alertmanager's labels.Matchers but for Prometheus' labels.Matcher slice
type labelMatchers []*labels.Matcher

// matches checks whether all the matchers match the given label set.
func (ms labelMatchers) matches(lset []mimirpb.LabelAdapter) bool {
	for _, m := range ms {
		var lv string
		for _, l := range lset {
			if l.Name == m.Name {
				lv = l.Value
				break
			}
		}

		if !m.Matches(lv) {
			return false
		}
	}

	return true
}

func NewSeriesMatchers(m []string) (c SeriesMatchers, err error) {
	c.source = m
	c.config = []labelMatchers{}
	for _, matcher := range m {
		sm, err := amlabels.ParseMatchers(matcher)
		if err != nil {
			return c, fmt.Errorf("can't build ephemeral series matcher %q: %w", matcher, err)
		}
		matchers := make(labelMatchers, len(sm))
		for i, m := range sm {
			matchers[i] = amlabelMatcherToProm(m)
		}
		c.config = append(c.config, matchers)
	}
	c.string = ephemeralMatchersConfigString(c.source)
	return c, nil
}

func amlabelMatcherToProm(m *amlabels.Matcher) *labels.Matcher {
	// labels.MatchType(m.Type) is a risky conversion because it depends on the iota order, but we have a test for it
	return labels.MustNewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
}

func ephemeralMatchersConfigString(matchers []string) string {
	if len(matchers) == 0 {
		return ""
	}

	// making String representation stable and comparable.
	slices.Sort(matchers)

	var sb strings.Builder
	for i, matcher := range matchers {
		if i > 0 {
			sb.WriteByte(';')
		}
		sb.WriteString(matcher)
		sb.WriteByte(':')
	}

	return sb.String()
}

// String is a canonical representation of the config, it is compatible with flag definition.
// String is also needed to implement flag.Value.
func (c SeriesMatchers) String() string {
	return c.string
}

// Set implements flag.Value, and is used to set the config value from a flag value provided as string.
func (c *SeriesMatchers) Set(s string) error {
	if strings.TrimSpace(s) == "" {
		return nil
	}

	var err error
	*c, err = NewSeriesMatchers(strings.Split(s, ";"))
	return err
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
// EphemeralMatchers are marshaled in yaml as a []string.
func (c *SeriesMatchers) UnmarshalYAML(value *yaml.Node) error {
	stringSlice := []string{}
	err := value.DecodeWithOptions(&stringSlice, yaml.DecodeOptions{KnownFields: true})
	if err != nil {
		return err
	}
	*c, err = NewSeriesMatchers(stringSlice)
	return err
}

// MarshalYAML implements yaml.Marshaler.
func (c SeriesMatchers) MarshalYAML() (interface{}, error) {
	return c.source, nil
}

func (c SeriesMatchers) IsEphemeral(lset []mimirpb.LabelAdapter) bool {
	for _, m := range c.config {
		if m.matches(lset) {
			return true
		}
	}

	return false
}
