// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"

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
	} = new(LabelValueLengthOverLimitStrategy)
)

type LabelValueLengthOverLimitStrategy int

const (
	LabelValueLengthOverLimitStrategyError LabelValueLengthOverLimitStrategy = iota
	LabelValueLengthOverLimitStrategyTruncate
	LabelValueLengthOverLimitStrategyDrop
)

func (s *LabelValueLengthOverLimitStrategy) UnmarshalYAML(value *yaml.Node) error {
	var repr string
	if err := value.Decode(&repr); err != nil {
		return err
	}
	return s.Set(repr)
}

func (s LabelValueLengthOverLimitStrategy) MarshalYAML() (any, error) {
	return s.String(), nil
}

func (s *LabelValueLengthOverLimitStrategy) UnmarshalJSON(bytes []byte) error {
	var repr string
	if err := json.Unmarshal(bytes, &repr); err != nil {
		return err
	}
	return s.Set(repr)
}

// UnmarshalMapstructure implements [mapstructure.Unmarshaler].
func (s *LabelValueLengthOverLimitStrategy) UnmarshalMapstructure(input any) error {
	repr, ok := input.(string)
	if !ok {
		return fmt.Errorf("expected a string, got %T", input)
	}
	return s.Set(repr)
}

func (s LabelValueLengthOverLimitStrategy) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s LabelValueLengthOverLimitStrategy) String() string {
	switch s {
	case LabelValueLengthOverLimitStrategyError:
		return "error"
	case LabelValueLengthOverLimitStrategyTruncate:
		return "truncate"
	case LabelValueLengthOverLimitStrategyDrop:
		return "drop"
	default:
		panic(fmt.Errorf("unrecognized LabelValueLengthOverLimitStrategy: %d", s))
	}
}

func (s *LabelValueLengthOverLimitStrategy) Set(text string) error {
	switch text {
	case "", "error":
		*s = LabelValueLengthOverLimitStrategyError
	case "truncate":
		*s = LabelValueLengthOverLimitStrategyTruncate
	case "drop":
		*s = LabelValueLengthOverLimitStrategyDrop
	default:
		return fmt.Errorf("unrecognized LabelValueLengthOverLimitStrategy: %q", text)
	}
	return nil
}

func (s LabelValueLengthOverLimitStrategy) Type() string {
	return "labelValueLengthOverLimitStrategy"
}

// Generate implements testing/quick.Generator.
func (LabelValueLengthOverLimitStrategy) Generate(rand *rand.Rand, _ int) reflect.Value {
	return reflect.ValueOf(LabelValueLengthOverLimitStrategy(rand.Intn(3)))
}
