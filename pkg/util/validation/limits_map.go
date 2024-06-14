// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"fmt"

	"gopkg.in/yaml.v3"
)

// LimitsMap is a generic map that can hold either float64 or int64 as values.
type LimitsMap[T float64 | int64] struct {
	data      map[string]T
	validator func(k string, v T) error
}

func NewLimitsMap[T float64 | int64](validator func(k string, v T) error) LimitsMap[T] {
	return LimitsMap[T]{
		data:      make(map[string]T),
		validator: validator,
	}
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

// UnmarshalYAML implements yaml.Unmarshaler.
func (m LimitsMap[T]) UnmarshalYAML(value *yaml.Node) error {
	newMap := make(map[string]T)
	if err := value.DecodeWithOptions(newMap, yaml.DecodeOptions{KnownFields: true}); err != nil {
		return err
	}
	return m.updateMap(newMap)
}

func (m LimitsMap[T]) updateMap(newMap map[string]T) error {
	for k, v := range newMap {
		if m.validator != nil {
			if err := m.validator(k, v); err != nil {
				return err
			}
		}

		m.data[k] = v
	}
	return nil
}

// MarshalYAML implements yaml.Marshaler.
func (m LimitsMap[T]) MarshalYAML() (interface{}, error) {
	return m.data, nil
}
