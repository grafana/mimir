// SPDX-License-Identifier: AGPL-3.0-only

package ephemeral

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type Source uint8

const (
	INVALID Source = iota
	ANY
	API
	RULE
)

var ValidSources = []Source{ANY, API, RULE}
var ValidSourceStrings []string
var Sources = []Source{INVALID, ANY, API, RULE}

func init() {
	for _, s := range ValidSources {
		ValidSourceStrings = append(ValidSourceStrings, s.String())
	}
}

func (s Source) String() string {
	switch s {
	case ANY:
		return "any"
	case API:
		return "api"
	case RULE:
		return "rule"
	default:
		return "unknown"
	}
}

// MarshalYAML implements yaml.Marshaler.
func (s Source) MarshalYAML() (interface{}, error) {
	return s.String(), nil
}

func (s *Source) UnmarshalYAML(value *yaml.Node) error {
	source, err := convertStringToSource(value.Value)
	if err != nil {
		return errors.Wrapf(err, "can't unmarshal source %q", value.Value)
	}
	*s = source
	return nil
}

func (s Source) MarshalText() (text []byte, err error) {
	return []byte(s.String()), nil
}

func (s *Source) UnmarshalText(text []byte) error {
	source, err := convertStringToSource(string(text))
	if err != nil {
		return errors.Wrapf(err, "can't unmarshal source %q", string(text))
	}
	*s = source
	return nil
}
