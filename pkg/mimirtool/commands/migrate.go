// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"bytes"
	"fmt"

	"github.com/prometheus/alertmanager/config"
	"go.yaml.in/yaml/v3"
)

// shadowCfg is a special version of an Alertmanager configuration that uses yaml.Node
// to avoid issues when deserializing and re-serializing Alertmanager configurations,
// such as secrets being replaced with <secret>. We use this struct to double quote
// all matchers in a configuration file.
type shadowCfg struct {
	Global            yaml.Node            `yaml:"global,omitempty" json:"global,omitempty"`
	Route             *config.Route        `yaml:"route,omitempty" json:"route,omitempty"`
	InhibitRules      []config.InhibitRule `yaml:"inhibit_rules,omitempty" json:"inhibit_rules,omitempty"`
	Receivers         []yaml.Node          `yaml:"receivers,omitempty" json:"receivers,omitempty"`
	Templates         []string             `yaml:"templates" json:"templates"`
	MuteTimeIntervals []yaml.Node          `yaml:"mute_time_intervals,omitempty" json:"mute_time_intervals,omitempty"`
	TimeIntervals     []yaml.Node          `yaml:"time_intervals,omitempty" json:"time_intervals,omitempty"`
}

// migrateCfg double quotes all matchers in a configuration file. It uses the fact that MarshalYAML
// for labels.Matchers double quotes matchers, even if the original is unquoted.
func migrateCfg(cfg string) (string, error) {
	tmp := shadowCfg{}
	if err := yaml.Unmarshal([]byte(cfg), &tmp); err != nil {
		return "", fmt.Errorf("failed to load config from YAML: %w", err)
	}
	var buf bytes.Buffer
	enc := yaml.NewEncoder(&buf)
	enc.SetIndent(2)
	if err := enc.Encode(tmp); err != nil {
		return "", fmt.Errorf("failed to save config as YAML: %w", err)
	}
	if err := enc.Close(); err != nil {
		return "", fmt.Errorf("failed to save config as YAML: %w", err)
	}
	return buf.String(), nil
}
