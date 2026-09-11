// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"flag"
	"reflect"
	"time"

	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/runtimeconfig/mapstructure"
	"github.com/prometheus/common/model"
	"go.yaml.in/yaml/v3"

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
		if !from.IsValid() {
			return from.Interface(), nil
		}
		if dec, ok := limitsFieldDecoders[to.Type()]; ok {
			return dec(from, to)
		}
		// Any other type that defines its own YAML unmarshaling can't be safely
		// decoded by mapstructure, so round-trip it through YAML to match the
		// YAML loader exactly.
		if needsYAMLDecode(to.Type()) {
			return mapDecodeAsYAML(from, to)
		}
		return from.Interface(), nil
	}),
)

// legacyYAMLUnmarshaler is the pre-v3 (function-based) YAML unmarshaler
// interface, still implemented by several dskit/flagext types.
type legacyYAMLUnmarshaler interface {
	UnmarshalYAML(unmarshal func(any) error) error
}

func needsYAMLDecode(t reflect.Type) bool {
	if implements[mapstructure.Unmarshaler](t) {
		return false
	}
	return implements[yaml.Unmarshaler](t) || implements[legacyYAMLUnmarshaler](t)
}

// implements reports whether t or *t implements the interface I.
func implements[I any](t reflect.Type) bool {
	i := reflect.TypeFor[I]()
	return t.Implements(i) || reflect.PointerTo(t).Implements(i)
}

// limitsFieldDecoders maps each external field type used in Limits to the
// decoder that handles it. It is built once.
var limitsFieldDecoders = map[reflect.Type]mapstructure.DecodeHookFuncValue{
	reflect.TypeFor[model.Duration]():         mapDecodeAsFlagValue,
	reflect.TypeFor[model.ValidationScheme](): mapDecodeAsFlagValue,
	reflect.TypeFor[time.Time]():              mapDecodeTime,
	// StringSliceCSV parses a comma-separated string in its Set, which is far
	// cheaper than a YAML round-trip.
	reflect.TypeFor[flagext.StringSliceCSV](): mapDecodeAsFlagValue,
	// TODO: Remove once dskit/runtimeconfig/mapstructure supports ",inline"
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
		// The value isn't a string. Fall back to YAML decoding.
		return mapDecodeAsYAML(from, to)
	}
	v := reflect.New(to.Type()).Interface().(flag.Value)
	err := v.Set(s)
	return v, err
}

// mapDecodeTime converts a quoted (string) timestamp into time.Time. Unquoted
// YAML timestamps decode into time.Time natively and pass through unchanged;
// only strings need converting, which we do via a YAML round-trip so we accept
// exactly the timestamp formats the YAML loader does.
func mapDecodeTime(from reflect.Value, to reflect.Value) (any, error) {
	if _, ok := from.Interface().(string); ok {
		return mapDecodeAsYAML(from, to)
	}
	return from.Interface(), nil
}
