// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	amlabels "github.com/prometheus/alertmanager/pkg/labels"
	"github.com/prometheus/prometheus/model/labels"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/mimirpb"
)

// LabelMatchers configures matchers based on which series get marked as ephemeral.
type LabelMatchers struct {
	raw    map[Source][]string
	config map[Source][]matcherSet
	string string
}

// matcherSet is like alertmanager's labels.Matchers but for Prometheus' labels.Matcher slice
type matcherSet []*labels.Matcher

// matches checks whether all the matchers match the given label set.
func (ms matcherSet) matches(lset []mimirpb.LabelAdapter) bool {
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

// String is a canonical representation of the config, it is compatible with the flag definition.
// String is needed to implement flag.Value.
func (c *LabelMatchers) String() string {
	return c.string
}

// Set implements flag.Value, and is used to set the config value from a flag value provided as string.
// Set is needed to implement flag.Value.
func (c *LabelMatchers) Set(s string) error {
	if strings.TrimSpace(s) == "" {
		return nil
	}

	rawMatchers := map[Source][]string{}
	for _, matcherSet := range strings.Split(s, ";") {
		splits := strings.SplitN(matcherSet, ":", 2)
		if len(splits) < 2 {
			return fmt.Errorf("invalid matcher %q", matcherSet)
		}

		source, err := convertStringToSource(splits[0])
		if err != nil {
			return errors.Wrapf(err, "can't set matcher source %q", splits[0])
		}

		rawMatchers[source] = append(rawMatchers[source], splits[1])
	}

	var err error
	*c, err = parseLabelMatchers(rawMatchers)
	return err
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
func (c *LabelMatchers) UnmarshalYAML(value *yaml.Node) error {
	rawMatchers := map[Source][]string{}
	err := value.DecodeWithOptions(&rawMatchers, yaml.DecodeOptions{KnownFields: true})
	if err != nil {
		return err
	}
	*c, err = parseLabelMatchers(rawMatchers)
	return err
}

func parseLabelMatchers(configIn map[Source][]string) (c LabelMatchers, err error) {
	c.raw = configIn
	c.config = map[Source][]matcherSet{}

	for source, matcherSetsRaw := range configIn {
		for _, matcherSetRaw := range matcherSetsRaw {
			amMatchers, err := amlabels.ParseMatchers(matcherSetRaw)
			if err != nil {
				return c, fmt.Errorf("can't build ephemeral series matcher %q: %w", matcherSetRaw, err)
			}

			promMatchers := make(matcherSet, len(amMatchers))
			for i, m := range amMatchers {
				promMatchers[i] = amlabelMatcherToProm(m)
			}

			c.config[source] = append(c.config[source], promMatchers)
		}
	}

	c.string = matchersConfigString(c.raw)

	return c, nil
}

func amlabelMatcherToProm(m *amlabels.Matcher) *labels.Matcher {
	// labels.MatchType(m.Type) is a risky conversion because it depends on the iota order, but we have a test for it
	return labels.MustNewMatcher(labels.MatchType(m.Type), m.Name, m.Value)
}

func matchersConfigString(matchers map[Source][]string) string {
	if len(matchers) == 0 {
		return ""
	}

	// Sort sources to have a deterministic output.
	sources := make([]Source, 0, len(matchers))
	for source := range matchers {
		sources = append(sources, source)
	}
	sort.Slice(sources, func(i, j int) bool {
		return sources[i] < sources[j]
	})

	var sb strings.Builder
	for _, source := range sources {
		matcherSetsRaw := matchers[source]
		for _, matcherSetRaw := range matcherSetsRaw {
			if sb.Len() > 0 {
				sb.WriteByte(';')
			}
			sb.WriteString(source.String())
			sb.WriteByte(':')
			sb.WriteString(matcherSetRaw)
		}
	}

	return sb.String()
}

// MarshalYAML implements yaml.Marshaler.
func (c *LabelMatchers) MarshalYAML() (interface{}, error) {
	return c.raw, nil
}

func (c *LabelMatchers) ShouldMarkEphemeral(mimirPbSampleSource mimirpb.WriteRequest_SourceEnum, lset []mimirpb.LabelAdapter) bool {
	sampleSource := convertMimirpbSource(mimirPbSampleSource)

	for _, matcherSource := range []Source{sampleSource, ANY} {
		for _, m := range c.config[matcherSource] {
			if m.matches(lset) {
				return true
			}
		}
	}

	return false
}

func convertStringToSource(source string) (Source, error) {
	switch strings.ToLower(source) {
	case "any":
		return ANY, nil
	case "api":
		return API, nil
	case "rule":
		return RULE, nil
	}
	return INVALID, fmt.Errorf("invalid source %q", source)
}

func convertMimirpbSource(source mimirpb.WriteRequest_SourceEnum) Source {
	switch source {
	case mimirpb.API:
		return API
	case mimirpb.RULE:
		return RULE
	default:
		return INVALID
	}
}
