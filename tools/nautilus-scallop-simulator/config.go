// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"sort"
)

//go:embed fixtures/*.json
var fixtureFiles embed.FS

type Fixture struct {
	Name                    string           `json:"name"`
	Ticks                   int              `json:"ticks"`
	TickSeconds             int              `json:"tick_seconds"`
	Partitions              int              `json:"partitions"`
	Readcaches              int              `json:"readcaches"`
	InitialRanges           int              `json:"initial_ranges_per_tenant"`
	ImbalanceThreshold      float64          `json:"imbalance_threshold"`
	ExcludeFromWeightSearch bool             `json:"exclude_from_weight_search,omitempty"`
	Tenants                 []TenantWorkload `json:"tenants"`
}

type TenantWorkload struct {
	ID         string              `json:"id"`
	Baseline   float64             `json:"baseline"`
	Components []GaussianComponent `json:"components"`
}

type GaussianComponent struct {
	Center    float64           `json:"center"`
	Width     float64           `json:"width"`
	Amplitude TemporalAmplitude `json:"amplitude"`
}

type TemporalAmplitude struct {
	Kind string `json:"kind"`

	Value float64 `json:"value,omitempty"`

	StartTick  int     `json:"start_tick,omitempty"`
	EndTick    int     `json:"end_tick,omitempty"`
	StartValue float64 `json:"start_value,omitempty"`
	EndValue   float64 `json:"end_value,omitempty"`

	CenterTick float64 `json:"center_tick,omitempty"`
	TimeWidth  float64 `json:"time_width,omitempty"`
	Peak       float64 `json:"peak,omitempty"`
}

// loadEmbeddedFixtures loads and validates the checked-in workload catalog used by every reproducible search.
func loadEmbeddedFixtures() ([]Fixture, error) {
	names, err := fs.Glob(fixtureFiles, "fixtures/*.json")
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	fixtures := make([]Fixture, 0, len(names))
	for _, name := range names {
		data, err := fixtureFiles.ReadFile(name)
		if err != nil {
			return nil, err
		}
		var fixture Fixture
		if err := json.Unmarshal(data, &fixture); err != nil {
			return nil, fmt.Errorf("decode %s: %w", name, err)
		}
		if err := fixture.validate(); err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		fixtures = append(fixtures, fixture)
	}
	return fixtures, nil
}

// weightSearchFixtures removes scale-only scenarios that would make exhaustive policy calibration impractical.
func weightSearchFixtures(fixtures []Fixture) []Fixture {
	selected := make([]Fixture, 0, len(fixtures))
	for _, fixture := range fixtures {
		if !fixture.ExcludeFromWeightSearch {
			selected = append(selected, fixture)
		}
	}
	return selected
}

// validate rejects incomplete or nonsensical fixture topology and workload definitions before simulation.
func (f Fixture) validate() error {
	if f.Name == "" {
		return fmt.Errorf("name is required")
	}
	if f.Ticks <= 1 || f.TickSeconds <= 0 {
		return fmt.Errorf("ticks must be > 1 and tick_seconds must be positive")
	}
	if f.Partitions <= 1 || f.Readcaches <= 0 || f.Readcaches > f.Partitions {
		return fmt.Errorf("invalid topology: partitions=%d readcaches=%d", f.Partitions, f.Readcaches)
	}
	if f.InitialRanges <= 0 {
		return fmt.Errorf("initial ranges must be positive")
	}
	if f.ImbalanceThreshold <= 0 {
		return fmt.Errorf("imbalance threshold must be positive")
	}
	if len(f.Tenants) == 0 {
		return fmt.Errorf("at least one tenant is required")
	}
	seen := map[string]struct{}{}
	for _, tenant := range f.Tenants {
		if tenant.ID == "" {
			return fmt.Errorf("tenant ID is required")
		}
		if _, duplicate := seen[tenant.ID]; duplicate {
			return fmt.Errorf("tenant %q is duplicated", tenant.ID)
		}
		seen[tenant.ID] = struct{}{}
		if tenant.Baseline < 0 {
			return fmt.Errorf("tenant %q baseline must be non-negative", tenant.ID)
		}
		for _, component := range tenant.Components {
			if component.Center < 0 || component.Center >= 1 || component.Width <= 0 || component.Width > 1 {
				return fmt.Errorf("tenant %q has invalid Gaussian center/width", tenant.ID)
			}
			if err := component.Amplitude.validate(); err != nil {
				return fmt.Errorf("tenant %q: %w", tenant.ID, err)
			}
		}
	}
	return nil
}

// validate ensures an amplitude profile has the parameters required by its temporal model.
func (a TemporalAmplitude) validate() error {
	switch a.Kind {
	case "constant":
		if a.Value < 0 {
			return fmt.Errorf("constant amplitude must be non-negative")
		}
	case "linear":
		if a.EndTick <= a.StartTick || a.StartValue < 0 || a.EndValue < 0 {
			return fmt.Errorf("linear amplitude requires an increasing tick interval and non-negative values")
		}
	case "gaussian":
		if a.TimeWidth <= 0 || a.Peak < 0 {
			return fmt.Errorf("Gaussian temporal amplitude requires positive width and non-negative peak")
		}
	default:
		return fmt.Errorf("unknown temporal amplitude kind %q", a.Kind)
	}
	return nil
}
