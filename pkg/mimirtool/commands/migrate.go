// SPDX-License-Identifier: AGPL-3.0-only

package commands

import (
	"fmt"

	"github.com/prometheus/alertmanager/config"
	"gopkg.in/yaml.v2"
)

// shadowCfg is a special version of an Alertmanager configuration that uses yaml.MapSlice
// to avoid issues when deserializing and re-serializing Alertmanager configurations,
// such as secrets being replaced with <secret>. We use this struct to double quote
// all matchers in a configuration file.
type shadowCfg struct {
	Global            yaml.MapSlice        `yaml:"global,omitempty" json:"global,omitempty"`
	Route             *config.Route        `yaml:"route,omitempty" json:"route,omitempty"`
	InhibitRules      []config.InhibitRule `yaml:"inhibit_rules,omitempty" json:"inhibit_rules,omitempty"`
	Receivers         []yaml.MapSlice      `yaml:"receivers,omitempty" json:"receivers,omitempty"`
	Templates         []string             `yaml:"templates" json:"templates"`
	MuteTimeIntervals []yaml.MapSlice      `yaml:"mute_time_intervals,omitempty" json:"mute_time_intervals,omitempty"`
	TimeIntervals     []yaml.MapSlice      `yaml:"time_intervals,omitempty" json:"time_intervals,omitempty"`
}

// migrateCfg double quotes all matchers in a configuration file. It uses the fact that MarshalYAML
// for labels.Matchers double quotes matchers, even if the original is unquoted.
func migrateCfg(cfg string) (string, error) {
	tmp := shadowCfg{}
	if err := yaml.Unmarshal([]byte(cfg), &tmp); err != nil {
		return "", fmt.Errorf("failed to load config from YAML: %w", err)
	}
	b, err := yaml.Marshal(tmp)
	if err != nil {
		return "", fmt.Errorf("failed to save config as YAML: %w", err)
	}
	return string(b), nil
}
