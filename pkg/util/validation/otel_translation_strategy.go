// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"

	"github.com/prometheus/otlptranslator"
	"github.com/spf13/pflag"
	"go.yaml.in/yaml/v3"
)

var (
	_ interface {
		yaml.Unmarshaler
		json.Unmarshaler
		pflag.Value
	} = new(OTelTranslationStrategyValue)
)

// OTelTranslationStrategyValue wraps otlptranslator.TranslationStrategyOption for use in limits.
type OTelTranslationStrategyValue otlptranslator.TranslationStrategyOption

func (s *OTelTranslationStrategyValue) UnmarshalYAML(value *yaml.Node) error {
	var repr string
	if err := value.Decode(&repr); err != nil {
		return err
	}
	return s.Set(repr)
}

func (s *OTelTranslationStrategyValue) UnmarshalJSON(bytes []byte) error {
	var repr string
	if err := json.Unmarshal(bytes, &repr); err != nil {
		return err
	}
	return s.Set(repr)
}

// UnmarshalMapstructure implements [mapstructure.Unmarshaler].
func (s *OTelTranslationStrategyValue) UnmarshalMapstructure(input any) error {
	repr, ok := input.(string)
	if !ok {
		return fmt.Errorf("expected a string, got %T", input)
	}
	return s.Set(repr)
}

func (s OTelTranslationStrategyValue) String() string {
	return string(s)
}

func (s *OTelTranslationStrategyValue) Set(text string) error {
	switch text {
	case "", string(otlptranslator.NoUTF8EscapingWithSuffixes), string(otlptranslator.UnderscoreEscapingWithSuffixes), string(otlptranslator.UnderscoreEscapingWithoutSuffixes), string(otlptranslator.NoTranslation):
		*s = OTelTranslationStrategyValue(text)
	default:
		return fmt.Errorf("unrecognized OTel translation strategy %q", text)
	}
	return nil
}

func (s OTelTranslationStrategyValue) Type() string {
	return "otelTranslationStrategy"
}

// Generate implements testing/quick.Generator.
func (OTelTranslationStrategyValue) Generate(rand *rand.Rand, _ int) reflect.Value {
	choices := []otlptranslator.TranslationStrategyOption{
		otlptranslator.NoUTF8EscapingWithSuffixes,
		otlptranslator.UnderscoreEscapingWithSuffixes,
		otlptranslator.UnderscoreEscapingWithoutSuffixes,
		otlptranslator.NoTranslation,
	}
	return reflect.ValueOf(OTelTranslationStrategyValue(choices[rand.Intn(len(choices))]))
}
