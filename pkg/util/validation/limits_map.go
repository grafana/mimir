// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

// LimitsMap is a generic map that can hold either float64 or int as values.
type LimitsMap[T float64 | int] struct {
	data      map[string]T
	validator func(k string, v T) error
}

func NewLimitsMap[T float64 | int](validator func(k string, v T) error) LimitsMap[T] {
	return LimitsMap[T]{
		data:      make(map[string]T),
		validator: validator,
	}
}

// IsInitialized returns true if the map is initialized.
func (m LimitsMap[T]) IsInitialized() bool {
	return m.data != nil
}

// String implements flag.Value
func (m LimitsMap[T]) String() string {
	out, err := json.Marshal(m.data)
	if err != nil {
		return fmt.Sprintf("failed to marshal: %v", err)
	}
	return string(out)
}

// Set implements flag.Value
func (m LimitsMap[T]) Set(s string) error {
	newMap := make(map[string]T)
	if err := json.Unmarshal([]byte(s), &newMap); err != nil {
		return err
	}
	return m.updateMap(newMap)
}

// Clone returns a copy of the LimitsMap.
func (m LimitsMap[T]) Clone() LimitsMap[T] {
	newMap := make(map[string]T, len(m.data))
	for k, v := range m.data {
		newMap[k] = v
	}
	return LimitsMap[T]{data: newMap, validator: m.validator}
}

// UnmarshalYAML implements yaml.Unmarshaler.
func (m LimitsMap[T]) UnmarshalYAML(value *yaml.Node) error {
	newMap := make(map[string]T)
	if err := value.DecodeWithOptions(newMap, yaml.DecodeOptions{KnownFields: true}); err != nil {
		return err
	}
	return m.updateMap(newMap)
}

func (m LimitsMap[T]) updateMap(newMap map[string]T) error {
	// Validate first, as we don't want to allow partial updates.
	if m.validator != nil {
		for k, v := range newMap {
			if err := m.validator(k, v); err != nil {
				return err
			}
		}
	}

	for k, v := range newMap {
		m.data[k] = v
	}

	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (m LimitsMap[T]) MarshalYAML() (interface{}, error) {
	return m.data, nil
}

// Equal compares two LimitsMap. This is needed to allow cmp.Equal to compare two LimitsMap.
func (m LimitsMap[T]) Equal(other LimitsMap[T]) bool {
	if len(m.data) != len(other.data) {
		return false
	}

	for k, v := range m.data {
		if other.data[k] != v {
			return false
		}
	}

	return true
}
