// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/pflag"
	"go.yaml.in/yaml/v3"
)

var (
	_ interface {
		yaml.Unmarshaler
		yaml.Marshaler
		json.Unmarshaler
		json.Marshaler
		pflag.Value
	} = new(StartTimestampStrategy)
)

// StartTimestampStrategy defines what the ingester and block-builder do with the start timestamp of incoming samples.
type StartTimestampStrategy int

const (
	StartTimestampStrategyZeroSample StartTimestampStrategy = iota
	StartTimestampStrategyIgnore
	StartTimestampStrategyStore
)

func (s *StartTimestampStrategy) UnmarshalYAML(value *yaml.Node) error {
	var repr string
	if err := value.Decode(&repr); err != nil {
		return err
	}
	return s.Set(repr)
}

func (s StartTimestampStrategy) MarshalYAML() (any, error) {
	return s.String(), nil
}

func (s *StartTimestampStrategy) UnmarshalJSON(bytes []byte) error {
	var repr string
	if err := json.Unmarshal(bytes, &repr); err != nil {
		return err
	}
	return s.Set(repr)
}

// UnmarshalMapstructure implements [mapstructure.Unmarshaler].
func (s *StartTimestampStrategy) UnmarshalMapstructure(input any) error {
	repr, ok := input.(string)
	if !ok {
		return fmt.Errorf("expected a string, got %T", input)
	}
	return s.Set(repr)
}

func (s StartTimestampStrategy) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s StartTimestampStrategy) String() string {
	switch s {
	case StartTimestampStrategyZeroSample:
		return "zero_sample"
	case StartTimestampStrategyIgnore:
		return "ignore"
	case StartTimestampStrategyStore:
		return "store"
	default:
		panic(fmt.Errorf("unrecognized StartTimestampStrategy: %d", s))
	}
}

func (s *StartTimestampStrategy) Set(text string) error {
	switch text {
	case "", "zero_sample":
		*s = StartTimestampStrategyZeroSample
	case "ignore":
		*s = StartTimestampStrategyIgnore
	case "store":
		*s = StartTimestampStrategyStore
	default:
		return fmt.Errorf("unrecognized StartTimestampStrategy: %q", text)
	}
	return nil
}

func (s StartTimestampStrategy) Type() string {
	return "startTimestampStrategy"
}
