// SPDX-License-Identifier: AGPL-3.0-only

package costattributionmodel

import (
	"fmt"

	"github.com/prometheus/common/model"
)

// TrackerConfig defines the configuration for a single named cost attribution tracker.
type TrackerConfig struct {
	Labels         Labels         `yaml:"labels" json:"labels"`
	MaxCardinality int            `yaml:"max_cardinality,omitempty" json:"max_cardinality,omitempty"`
	Cooldown       model.Duration `yaml:"cooldown,omitempty" json:"cooldown,omitempty"`
}

// TrackerConfigs is a map of tracker name to TrackerConfig.
type TrackerConfigs map[string]TrackerConfig

// DefaultTrackerName is the name used for the default cost attribution tracker
// configured via cost_attribution_labels.
const DefaultTrackerName = "cost-attribution"

func (tc TrackerConfigs) Validate() error {
	for name, cfg := range tc {
		if name == "" {
			return fmt.Errorf("cost attribution tracker name must not be empty")
		}
		if !model.LabelValue(name).IsValid() {
			return fmt.Errorf("invalid cost attribution tracker name: %q", name)
		}
		if err := cfg.Labels.Validate(); err != nil {
			return fmt.Errorf("cost attribution tracker %q: %w", name, err)
		}
		if len(cfg.Labels) == 0 {
			return fmt.Errorf("cost attribution tracker %q must have at least one label", name)
		}
	}
	return nil
}
