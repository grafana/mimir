// SPDX-License-Identifier: AGPL-3.0-only

package activeseries

import (
	"fmt"
	"sort"
	"strings"

	amlabels "github.com/prometheus/alertmanager/pkg/labels"
)

// ActiveSeriesCustomTrackersConfig configures active series custom trackers.
// It can be set using a flag, or parsed from yaml.
type ActiveSeriesCustomTrackersConfig struct {
	source map[string]string
	config map[string]labelsMatchers
	string string
}

// ExampleDoc provides an example doc for this config, especially valuable since it's custom-unmarshaled.
func (c *ActiveSeriesCustomTrackersConfig) ExampleDoc() (comment string, yaml interface{}) {
	return `The following configuration will count the active series coming from dev and prod namespaces for each tenant` +
			` and label them as {name="dev"} and {name="prod"} in the cortex_ingester_active_series_custom_tracker metric.`,
		map[string]string{
			"dev":  `{namespace=~"dev-.*"}`,
			"prod": `{namespace=~"prod-.*"}`,
		}
}

// String is a canonical representation of the config, it is compatible with flag definition.
// String is also needed to implement flag.Value.
func (c *ActiveSeriesCustomTrackersConfig) String() string {
	return c.string
}

func activeSeriesCustomTrackersConfigString(cfg map[string]string) string {
	if len(cfg) == 0 {
		return ""
	}

	keys := make([]string, 0, len(cfg))
	for name := range cfg {
		keys = append(keys, name)
	}

	// The map is traversed in an ordered fashion to make String representation stable and comparable.
	sort.Strings(keys)

	var sb strings.Builder
	for i, name := range keys {
		if i > 0 {
			sb.WriteByte(';')
		}
		sb.WriteString(name)
		sb.WriteByte(':')
		sb.WriteString(cfg[name])
	}

	return sb.String()
}

// Set implements flag.Value, and is used to set the config value from a flag value provided as string.
func (c *ActiveSeriesCustomTrackersConfig) Set(s string) error {
	if strings.TrimSpace(s) == "" {
		return nil
	}

	f, err := activeSeriesCustomTrackerFlagValueToMap(s)
	if err != nil {
		return err
	}

	nc, err := NewActiveSeriesCustomTrackersConfig(f)
	if err != nil {
		return err
	}

	if len(c.config) == 0 {
		// First flag, just set whatever we parsed.
		// This includes an updated string.
		*c = nc
		return nil
	}

	// Not the first flag, merge checking for duplications.
	for name := range nc.config {
		if _, ok := c.config[name]; ok {
			return fmt.Errorf("matcher %q for active series custom trackers is provided more than once", name)
		}
		c.config[name] = nc.config[name]
		c.source[name] = f[name]
	}

	// Recalculate the string after merging.
	c.string = activeSeriesCustomTrackersConfigString(c.source)
	return nil
}

func activeSeriesCustomTrackerFlagValueToMap(s string) (map[string]string, error) {
	source := map[string]string{}
	pairs := strings.Split(s, ";")
	for i, p := range pairs {
		split := strings.SplitN(p, ":", 2)
		if len(split) != 2 {
			return nil, fmt.Errorf("value should be <name>:<matcher>[;<name>:<matcher>]*, but colon was not found in the value %d: %q", i, p)
		}
		name, matcher := strings.TrimSpace(split[0]), strings.TrimSpace(split[1])
		if len(name) == 0 || len(matcher) == 0 {
			return nil, fmt.Errorf("semicolon-separated values should be <name>:<matcher>, but one of the sides was empty in the value %d: %q", i, p)
		}
		if _, ok := source[name]; ok {
			return nil, fmt.Errorf("matcher %q for active series custom trackers is provided twice", name)
		}
		source[name] = matcher
	}
	return source, nil
}

// UnmarshalYAML implements the yaml.Unmarshaler interface.
// ActiveSeriesCustomTrackersConfig are marshaled in yaml as a map[string]string, with matcher names as keys and strings as matchers definitions.
func (c *ActiveSeriesCustomTrackersConfig) UnmarshalYAML(unmarshal func(interface{}) error) error {
	stringMap := map[string]string{}
	err := unmarshal(&stringMap)
	if err != nil {
		return err
	}
	*c, err = NewActiveSeriesCustomTrackersConfig(stringMap)
	return err
}

func NewActiveSeriesCustomTrackersConfig(m map[string]string) (c ActiveSeriesCustomTrackersConfig, err error) {
	c.source = m
	c.config = map[string]labelsMatchers{}
	for name, matcher := range m {
		sm, err := amlabels.ParseMatchers(matcher)
		if err != nil {
			return c, fmt.Errorf("can't build active series matcher %s: %w", name, err)
		}
		matchers := make(labelsMatchers, len(sm))
		for i, m := range sm {
			matchers[i] = amlabelMatcherToProm(m)
		}
		c.config[name] = matchers
	}
	c.string = activeSeriesCustomTrackersConfigString(c.source)
	return c, nil
}
