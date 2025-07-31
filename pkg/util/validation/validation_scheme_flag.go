// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"fmt"

	"github.com/prometheus/common/model"
	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
)

var (
	_ interface {
		yaml.Marshaler
		yaml.Unmarshaler
		json.Marshaler
		json.Unmarshaler
		pflag.Value
	} = new(ValidationSchemeValue)
)

// ValidationSchemeValue wraps model.ValidationScheme for use in limits.
type ValidationSchemeValue model.ValidationScheme

func (s ValidationSchemeValue) MarshalYAML() (any, error) {
	return model.ValidationScheme(s).MarshalYAML()
}

func (s *ValidationSchemeValue) UnmarshalYAML(value *yaml.Node) error {
	var repr model.ValidationScheme
	if err := value.Decode(&repr); err != nil {
		return err
	}
	*s = ValidationSchemeValue(repr)
	return nil
}

func (s ValidationSchemeValue) MarshalJSON() ([]byte, error) {
	switch model.ValidationScheme(s) {
	case model.UTF8Validation, model.LegacyValidation:
		return json.Marshal(s.String())
	case model.UnsetValidation:
		return json.Marshal("")
	default:
		return nil, fmt.Errorf("unrecognized name validation scheme: %s", s)
	}
}

func (s *ValidationSchemeValue) UnmarshalJSON(bytes []byte) error {
	var repr string
	if err := json.Unmarshal(bytes, &repr); err != nil {
		return err
	}
	return s.Set(repr)
}

func (s ValidationSchemeValue) String() string {
	return model.ValidationScheme(s).String()
}

func (s *ValidationSchemeValue) Set(text string) error {
	switch text {
	case "":
		// Don't change value.
	case model.LegacyValidation.String():
		*s = ValidationSchemeValue(model.LegacyValidation)
	case model.UTF8Validation.String():
		*s = ValidationSchemeValue(model.UTF8Validation)
	default:
		return fmt.Errorf("unrecognized validation scheme %s", text)
	}
	return nil
}

func (s ValidationSchemeValue) Type() string {
	return "validationScheme"
}
