// SPDX-License-Identifier: AGPL-3.0-only
// This file contains code for the migration from classic mode to UTF-8 strict
// mode in the Alertmanager. It is intended to be used to gather data about
// configurations that are incompatible with the UTF-8 matchers parser, so
// action can be taken to fix those configurations before enabling the mode.

package alertmanager

import (
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/alertmanager/matchers/compat"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/alertmanager/alertspb"
)

// matchersConfig is a simplified version of an Alertmanager configuration
// containing just the configuration options that are matchers. matchersConfig
// is used to validate that existing configurations are forwards compatible with
// the new UTF-8 parser in Alertmanager (see the matchers/parse package).
type matchersConfig struct {
	Route        *matchersRoute            `yaml:"route,omitempty" json:"route,omitempty"`
	InhibitRules []*matchersInhibitionRule `yaml:"inhibit_rules,omitempty" json:"inhibit_rules,omitempty"`
}

type matchersRoute struct {
	Matchers []string         `yaml:"matchers,omitempty" json:"matchers,omitempty"`
	Routes   []*matchersRoute `yaml:"routes,omitempty" json:"routes,omitempty"`
}

type matchersInhibitionRule struct {
	SourceMatchers []string `yaml:"source_matchers,omitempty" json:"source_matchers,omitempty"`
	TargetMatchers []string `yaml:"target_matchers,omitempty" json:"target_matchers,omitempty"`
}

// validateMatchersInConfigDesc validates that a configuration is forwards compatible with the
// UTF-8 matchers parser. It loads the configuration into an intermediate structure that is a
// subset of the full Alertmanager configuration containing just the fields needed to validate
// matchers. It then parses all matchers using the UTF-8 parser and returns an error if any of
// the matchers are incompatible or invalid.
func validateMatchersInConfigDesc(logger log.Logger, origin string, cfg alertspb.AlertConfigDesc) error {
	logger = log.With(logger, "user", cfg.User)
	parseFn := compat.UTF8MatchersParser(logger)
	matchersCfg := matchersConfig{}
	if err := yaml.Unmarshal([]byte(cfg.RawConfig), &matchersCfg); err != nil {
		return fmt.Errorf("Failed to load configuration in validateMatchersInConfigDesc: %w", err)
	}
	if err := validateRoute(parseFn, origin, matchersCfg.Route, cfg.User); err != nil {
		return err
	}
	if err := validateInhibitionRules(parseFn, origin, matchersCfg.InhibitRules, cfg.User); err != nil {
		return err
	}
	return nil
}

func validateRoute(parseFn compat.ParseMatchers, origin string, r *matchersRoute, user string) error {
	if r == nil {
		// This shouldn't be possible, but if somehow a tenant does have a nil route this prevents
		// a nil pointer dereference and a subsequent panic.
		return nil
	}
	for _, m := range r.Matchers {
		if _, err := parseFn(m, origin); err != nil {
			return fmt.Errorf("Invalid matcher in route: %s: %w", m, err)
		}
	}
	for _, route := range r.Routes {
		if err := validateRoute(parseFn, origin, route, user); err != nil {
			return err
		}
	}
	return nil
}

func validateInhibitionRules(parseFn compat.ParseMatchers, origin string, rules []*matchersInhibitionRule, _ string) error {
	for _, r := range rules {
		for _, m := range r.SourceMatchers {
			if _, err := parseFn(m, origin); err != nil {
				return fmt.Errorf("Invalid matcher in inhibition rule source matchers: %s: %w", m, err)
			}
		}
		for _, m := range r.TargetMatchers {
			if _, err := parseFn(m, origin); err != nil {
				return fmt.Errorf("Invalid matcher in inhibition rule target matchers: %s: %w", m, err)
			}
		}
	}
	return nil
}
