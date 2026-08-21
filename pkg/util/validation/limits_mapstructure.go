// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"flag"
	"fmt"
	"reflect"

	"github.com/grafana/dskit/runtimeconfig/mapstructure"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/relabel"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/dskit/flagext"

	"github.com/grafana/mimir/pkg/ruler/notifier"
)

// UnmarshalMapstructure implements [mapstructure.Unmarshaler]. input is the raw
// per-tenant configuration map. It applies exactly the same defaults, extension
// handling, migration, validation and canonicalization as
// UnmarshalYAML/UnmarshalJSON.
func (l *Limits) UnmarshalMapstructure(input any) error {
	return l.unmarshal(func(v any) error {
		dec, err := NewLimitsMapDecoder(v)
		if err != nil {
			return err
		}
		return dec.Decode(input)
	})
}

// NewLimitsMapDecoder builds a mapstructure decoder that behaves like YAML
// decoding for limits.
func NewLimitsMapDecoder(out any) (*mapstructure.Decoder, error) {
	return mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		DecodeHook:  limitsMapstructureDecodeHook,
		Result:      out,
		TagName:     "yaml",
		Squash:      true,
		ErrorUnused: true,
		ZeroFields:  false,
		MatchName:   func(mapKey, fieldName string) bool { return mapKey == fieldName },
	})
}

// DecodeLimitsMap is a helper for [NewLimitsMapDecoder].
func DecodeLimitsMap(m map[string]any, out any) error {
	dec, err := NewLimitsMapDecoder(out)
	if err != nil {
		return err
	}
	return dec.Decode(m)
}

var limitsMapstructureDecodeHook = mapstructure.ComposeDecodeHookFunc(
	// go.yaml.in/yaml/v3 natively decodes duration strings (e.g. "1m") into
	// time.Duration; mirror that here for the raw time.Duration fields.
	mapstructure.StringToTimeDurationHookFunc(),
	mapstructure.DecodeHookFuncValue(func(from, to reflect.Value) (any, error) {
		if from.IsValid() {
			if dec, ok := limitsFieldDecoders[to.Type()]; ok {
				return dec(from, to)
			}
		}
		return from.Interface(), nil
	}),
)

// limitsFieldDecoders maps each external field type used in Limits to the
// decoder that handles it. It is built once.
var limitsFieldDecoders = map[reflect.Type]mapstructure.DecodeHookFuncValue{
	reflect.TypeFor[model.Duration]():         mapDecodeAsFlagValue,
	reflect.TypeFor[model.ValidationScheme](): mapDecodeAsFlagValue,
	// StringSliceCSV parses a comma-separated string in its Set, which is far
	// cheaper than a YAML round-trip.
	reflect.TypeFor[flagext.StringSliceCSV](): mapDecodeAsFlagValue,
	// CIDRSliceCSV must round-trip through YAML rather than Set: its
	// UnmarshalYAML treats an empty string (the marshaled form of an unset
	// value) as "no CIDRs", whereas Set("") fails to parse an empty CIDR.
	reflect.TypeFor[flagext.CIDRSliceCSV](): mapDecodeAsYAML,

	// flagext.StringSlice is a plain []string with no custom unmarshaler, so
	// mapstructure decodes it natively (no entry here).
	reflect.TypeFor[flagext.LimitsMap[int]]():     mapDecodeAsYAML,
	reflect.TypeFor[flagext.LimitsMap[float64]](): mapDecodeAsYAML,
	reflect.TypeFor[flagext.LimitsMap[string]]():  mapDecodeAsYAML,
	reflect.TypeFor[[]*relabel.Config]():          mapDecodeAsYAML,
	// AlertmanagerClientConfig embeds another config with yaml:",inline", which
	// mapstructure does not treat as squash; round-trip the whole (rare, small)
	// subtree through YAML instead.
	reflect.TypeFor[notifier.AlertmanagerClientConfig](): mapDecodeAsYAML,
}

func mapDecodeAsYAML(from reflect.Value, to reflect.Value) (any, error) {
	b, err := yaml.Marshal(from.Interface())
	if err != nil {
		return nil, err
	}
	// Seed with the current destination value so any pre-initialized state is
	// preserved.
	v := reflect.New(to.Type())
	if to.IsValid() && to.CanInterface() {
		v.Elem().Set(to)
	}
	if err := yaml.Unmarshal(b, v.Interface()); err != nil {
		return nil, err
	}
	return v.Elem().Interface(), nil
}

func mapDecodeAsFlagValue(from reflect.Value, to reflect.Value) (any, error) {
	s, ok := from.Interface().(string)
	if !ok {
		return nil, fmt.Errorf("expected a string, got %T", from.Interface())
	}
	v := reflect.New(to.Type()).Interface().(flag.Value)
	err := v.Set(s)
	return v, err
}
