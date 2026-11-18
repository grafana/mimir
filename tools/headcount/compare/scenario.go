// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"slices"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util/promqlext"
)

// scenarioQueryPrefix marks a scenario's pairs in the query label.
const scenarioQueryPrefix = "scenario:"

// scenario is one hand-written pair from the -scenario file, run every round next to the drawn pairs.
type scenario struct {
	Name     string   `yaml:"name"`
	Selector string   `yaml:"selector"`
	GroupBy  string   `yaml:"group_by"`
	Windows  []string `yaml:"windows"`
	Kinds    []string `yaml:"kinds"`
	Limit    int      `yaml:"limit"`

	matchers []*labels.Matcher
	lengths  []time.Duration
}

// loadScenarios reads and checks the -scenario file: a YAML list of scenarios.
func loadScenarios(path string) ([]scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	var out []scenario
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%s: no scenarios", path)
	}
	seen := map[string]bool{}
	for i := range out {
		s := &out[i]
		if err := s.check(); err != nil {
			return nil, fmt.Errorf("%s: scenario %d (%q): %w", path, i+1, s.Name, err)
		}
		if seen[s.Name] {
			return nil, fmt.Errorf("%s: scenario name %q used twice", path, s.Name)
		}
		seen[s.Name] = true
	}
	return out, nil
}

func (s *scenario) check() error {
	if s.Name == "" {
		return errors.New("name is required")
	}
	var err error
	if s.matchers, err = promqlext.NewPromQLParser().ParseMetricSelector(s.Selector); err != nil {
		return fmt.Errorf("selector: %w", err)
	}
	if s.GroupBy == "" {
		s.GroupBy = model.MetricNameLabel
	}
	for _, w := range s.Windows {
		d, err := model.ParseDuration(w)
		if err != nil || d <= 0 {
			return fmt.Errorf("window %q is not a positive duration", w)
		}
		s.lengths = append(s.lengths, time.Duration(d))
	}
	for _, k := range s.Kinds {
		if !slices.Contains(allWindowKinds, k) {
			return fmt.Errorf("unknown window kind %q, want one of %v", k, allWindowKinds)
		}
	}
	if s.Limit < 0 {
		return errors.New("limit must not be negative")
	}
	return nil
}

// scenarioPairs gives each scenario one pair, on a window drawn from the kinds and lengths it allows. It also returns the scenarios no window fits.
func scenarioPairs(scenarios []scenario, wcfg windowConfig, now time.Time, rng *rand.Rand) (pairs []pair, unplaced []string) {
	for _, s := range scenarios {
		cfg := wcfg
		if len(s.lengths) > 0 {
			cfg.lengths = s.lengths
		}
		var fits []window
		for _, w := range cfg.windows(now) {
			if len(s.Kinds) > 0 && !slices.Contains(s.Kinds, w.kind) {
				continue
			}
			fits = append(fits, w)
		}
		if len(fits) == 0 {
			unplaced = append(unplaced, s.Name)
			continue
		}
		pairs = append(pairs, pair{
			query:    scenarioQueryPrefix + s.Name,
			matchers: s.matchers,
			groupBy:  s.GroupBy,
			window:   fits[rng.IntN(len(fits))],
			limit:    s.Limit,
		})
	}
	return pairs, unplaced
}
