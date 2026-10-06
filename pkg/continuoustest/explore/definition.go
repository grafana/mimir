// SPDX-License-Identifier: AGPL-3.0-only

package explore

import (
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"time"

	"github.com/prometheus/prometheus/model/histogram"
	"go.yaml.in/yaml/v3"
)

//go:embed default_definition.yaml
var defaultDefinitionYAML []byte

type SampleType string

const (
	SampleTypeInt   SampleType = "int"
	SampleTypeFloat SampleType = "float"
)

type Kind string

const (
	KindCounter Kind = "counter"
	KindGauge   Kind = "gauge"
)

// Definition describes the behavior space to explore and the paces at which it can be explored.
type Definition struct {
	NativeHistograms NativeHistogramsDefinition `yaml:"native_histograms"`
	Paces            map[string]Pace            `yaml:"paces"`
}

// NativeHistogramsDefinition lists the values of each native histogram dimension to explore.
type NativeHistogramsDefinition struct {
	SampleTypes []SampleType `yaml:"sample_types"`
	Kinds       []Kind       `yaml:"kinds"`
	Schemas     []int32      `yaml:"schemas"`
}

// Pace controls how quickly the behavior space is explored and how many series are written.
type Pace struct {
	// SlotDuration is how often a new scenario starts.
	SlotDuration time.Duration `yaml:"slot_duration"`
	// ScenarioLifetime is how long a scenario keeps writing after it starts.
	ScenarioLifetime time.Duration `yaml:"scenario_lifetime"`
	// WriteInterval is the interval between two samples of the same series.
	WriteInterval time.Duration `yaml:"write_interval"`
	// SeriesPerScenario is the number of series written by each scenario.
	SeriesPerScenario int `yaml:"series_per_scenario"`
	// MaxActiveSeries is an upper bound on the number of series written at the same time. A pace
	// whose scenarios would exceed it is rejected.
	MaxActiveSeries int `yaml:"max_active_series"`
}

// MaxActiveScenarios returns the maximum number of scenarios writing at the same time.
func (p Pace) MaxActiveScenarios() int {
	return int((p.ScenarioLifetime + p.SlotDuration - 1) / p.SlotDuration)
}

func (p Pace) validate() error {
	if p.SlotDuration <= 0 || p.ScenarioLifetime <= 0 || p.WriteInterval <= 0 {
		return errors.New("slot_duration, scenario_lifetime and write_interval must be positive")
	}
	if p.SlotDuration%p.WriteInterval != 0 || p.ScenarioLifetime%p.WriteInterval != 0 {
		return errors.New("slot_duration and scenario_lifetime must be multiples of write_interval")
	}
	if p.SeriesPerScenario <= 0 {
		return errors.New("series_per_scenario must be positive")
	}
	if active := p.MaxActiveScenarios() * p.SeriesPerScenario; active > p.MaxActiveSeries {
		return fmt.Errorf("up to %d series can be active at the same time, which exceeds max_active_series %d", active, p.MaxActiveSeries)
	}
	return nil
}

func (d NativeHistogramsDefinition) validate() error {
	if err := validateValues("sample_types", d.SampleTypes, []SampleType{SampleTypeInt, SampleTypeFloat}); err != nil {
		return err
	}
	if err := validateValues("kinds", d.Kinds, []Kind{KindCounter, KindGauge}); err != nil {
		return err
	}
	if err := validateValues("schemas", d.Schemas, nil); err != nil {
		return err
	}
	for _, s := range d.Schemas {
		if !histogram.IsExponentialSchema(s) {
			return fmt.Errorf("schemas: %d is not an exponential schema, must be between %d and %d", s, histogram.ExponentialSchemaMin, histogram.ExponentialSchemaMax)
		}
	}
	return nil
}

// validateValues checks that values is non-empty and free of duplicates. If allowed is non-nil,
// it also checks that every value is one of allowed.
func validateValues[T comparable](name string, values, allowed []T) error {
	if len(values) == 0 {
		return fmt.Errorf("%s: at least one value is required", name)
	}
	for i, v := range values {
		if allowed != nil && !slices.Contains(allowed, v) {
			return fmt.Errorf("%s: unknown value %v, supported values are %v", name, v, allowed)
		}
		if slices.Contains(values[:i], v) {
			return fmt.Errorf("%s: duplicate value %v", name, v)
		}
	}
	return nil
}

// dimensions returns the native histogram dimensions in a fixed order. The order is part of the
// schedule: changing it changes which scenario runs in each slot.
func (d NativeHistogramsDefinition) dimensions() []dimension {
	dims := []dimension{
		{name: labelSampleType},
		{name: labelKind},
		{name: labelSchema},
	}
	for _, v := range d.SampleTypes {
		dims[0].values = append(dims[0].values, string(v))
	}
	for _, v := range d.Kinds {
		dims[1].values = append(dims[1].values, string(v))
	}
	for _, v := range d.Schemas {
		dims[2].values = append(dims[2].values, strconv.Itoa(int(v)))
	}
	return dims
}

// ParseDefinition parses and validates a definition.
func ParseDefinition(data []byte) (Definition, error) {
	var def Definition
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&def); err != nil {
		return Definition{}, fmt.Errorf("parsing definition: %w", err)
	}
	if err := def.NativeHistograms.validate(); err != nil {
		return Definition{}, fmt.Errorf("native_histograms: %w", err)
	}
	if len(def.Paces) == 0 {
		return Definition{}, errors.New("paces: at least one pace is required")
	}
	for name, p := range def.Paces {
		if err := p.validate(); err != nil {
			return Definition{}, fmt.Errorf("paces: %s: %w", name, err)
		}
	}
	return def, nil
}

// LoadDefinition loads the definition from path, or the built-in default definition if path is empty.
func LoadDefinition(path string) (Definition, error) {
	if path == "" {
		return ParseDefinition(defaultDefinitionYAML)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Definition{}, fmt.Errorf("reading definition file: %w", err)
	}
	return ParseDefinition(data)
}
