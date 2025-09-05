// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"fmt"

	"github.com/spf13/pflag"
	"gopkg.in/yaml.v3"
)

var (
	_ interface {
		yaml.Unmarshaler
		json.Unmarshaler
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

func (s *LabelValueLengthOverLimitStrategy) UnmarshalJSON(bytes []byte) error {
	var repr string
	if err := json.Unmarshal(bytes, &repr); err != nil {
		return err
	}
	return s.Set(repr)
}

func (s LabelValueLengthOverLimitStrategy) String() string {
	return string(s)
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
