// SPDX-License-Identifier: AGPL-3.0-only

package costattributionmodel

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"

	"github.com/prometheus/common/model"
)

// TrackerConfig defines the configuration for a single named cost attribution tracker.
type TrackerConfig struct {
	Labels Labels `yaml:"labels" json:"labels"`
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

// String implements flag.Value. Returns JSON representation.
func (tc TrackerConfigs) String() string {
	if len(tc) == 0 {
		return ""
	}
	b, _ := json.Marshal(tc)
	return string(b)
}

// Set implements flag.Value. Parses JSON input.
func (tc *TrackerConfigs) Set(s string) error {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	var parsed TrackerConfigs
	if err := json.Unmarshal([]byte(s), &parsed); err != nil {
		return fmt.Errorf("failed to parse cost attribution trackers: %w", err)
	}
	if err := parsed.Validate(); err != nil {
		return err
	}
	*tc = parsed
	return nil
}

// MergeTrackerConfigs returns a new TrackerConfigs containing the merge of the two
// TrackerConfigs in input. If a key exists in both, second wins.
func MergeTrackerConfigs(first, second TrackerConfigs) TrackerConfigs {
	if len(first) == 0 && len(second) == 0 {
		return nil
	}
	if len(second) == 0 {
		return first
	}
	if len(first) == 0 {
		return second
	}
	merged := make(TrackerConfigs, len(first)+len(second))
	maps.Copy(merged, first)
	maps.Copy(merged, second)
	return merged
}
