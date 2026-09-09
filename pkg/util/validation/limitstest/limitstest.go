// SPDX-License-Identifier: AGPL-3.0-only

package limitstest

import (
	"fmt"
	"maps"
	"math/rand"
	"net"
	"reflect"
	"testing/quick"
	"time"
	"unsafe"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util/validation"
)

// Generator produces random validation.Limits, encoded as map[string]any to
// be passed to a runtimeconfig MapLoader.
type Generator struct {
	// ValueFuncs maps a reflect.Type to a generator for that type. It is
	// consulted first, before the testing/quick.Generator interface and before
	// the generic reflection-based generation. Downstream callers can add
	// entries for their own field or extension types.
	ValueFuncs map[reflect.Type]ValueFunc

	// SkipFields is the set of validation.Limits field names that are left at
	// their default value instead of being randomized.
	SkipFields map[string]bool

	// Temperature controls how many random perturbances to introduce in the
	// generated limits, from 0 (nothing) to 2 (maximum).
	Temperature float64
}

// ValueFunc returns a random reflect.Value of a specific type.
type ValueFunc func(*rand.Rand) reflect.Value

// GenerateLimits produces a random validation.Limits, encoded as a
// map[string]any to be passed to a runtimeconfig MapLoader.
func GenerateLimits(r *rand.Rand, defaults validation.Limits) map[string]any {
	return NewGenerator().Limits(r, defaults)
}

// NewGenerator returns a Generator preconfigured for the standard Mimir
// validation.Limits field types. The returned Generator may be customized
// before use.
func NewGenerator() *Generator {
	return &Generator{
		ValueFuncs: defaultValueFuncs(),
		SkipFields: map[string]bool{
			// Decoded by the mapstructure loader through a full YAML round-trip
			// (mapDecodeAsYAML), so they are equivalent to the YAML loader by
			// construction, and generating valid random instances is awkward.
			// They are still exercised at their default value.
			"MetricRelabelConfigs":          true,
			"RulerAlertmanagerClientConfig": true,
		},
		Temperature: 1,
	}
}

const (
	weightFieldPerturb     = 40
	weightExtensionPerturb = 60
	weightDurationAsInt    = 30
	weightTimeAsString     = 30
)

// Generator produces a random validation.Limits, encoded as a map[string]any to
// be passed to a runtimeconfig MapLoader.
func (g *Generator) Limits(r *rand.Rand, defaults validation.Limits) map[string]any {
	l := g.limits(r, defaults)
	m := marshalToMap(&l)

	// Convert to type with custom encodings, then merge the resulting map into
	// m.
	encType := g.mapToStructWithCustomEncoding(reflect.TypeFor[validation.Limits](), r)
	encoded := deepConvert(reflect.ValueOf(l), encType)
	maps.Copy(m, marshalToMap(encoded.Interface()))

	return m
}

func (g *Generator) limits(r *rand.Rand, defaults validation.Limits) validation.Limits {
	l := defaults
	l.RegisterExtensionsDefaults()

	v := reflect.ValueOf(&l).Elem()
	tp := v.Type()
	for i := range v.NumField() {
		fv := v.Field(i)
		if !fv.CanSet() {
			continue
		}
		if g.SkipFields[tp.Field(i).Name] {
			continue
		}
		if g.coin(r, weightFieldPerturb) {
			fv.Set(g.Value(fv.Type(), r))
		}
	}

	g.perturbExtensions(&l, r)
	return l
}

func (g *Generator) chance(weight int) int {
	c := int(float64(weight)*g.Temperature + 0.5)
	if c > 100 {
		return 100
	}
	if c < 0 {
		return 0
	}
	return c
}

func (g *Generator) coin(r *rand.Rand, weight int) bool {
	return r.Intn(100) < g.chance(weight)
}

var generatorType = reflect.TypeFor[quick.Generator]()

// Value returns a random reflect.Value of the given type. It consults
// ValueFuncs, then the testing/quick.Generator interface, then falls back to
// generic reflection-based generation. Structs only have their exported fields
// populated (unexported fields are left zero, which is also what
// YAML/mapstructure decoding leaves them as). Any type it doesn't know how to
// build panics, so new, unhandled types are caught rather than silently zeroed.
func (g *Generator) Value(t reflect.Type, r *rand.Rand) reflect.Value {
	if fn, ok := g.ValueFuncs[t]; ok {
		return fn(r)
	}
	if t.Implements(generatorType) {
		if v, ok := quick.Value(t, r); ok {
			return v
		}
		panic("limitstest: quick.Value failed for testing/quick.Generator type " + t.String())
	}
	switch t.Kind() {
	case reflect.Bool:
		v := reflect.New(t).Elem()
		v.SetBool(r.Intn(2) == 0)
		return v
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v := reflect.New(t).Elem()
		v.SetInt(int64(r.Intn(1 << 20)))
		return v
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v := reflect.New(t).Elem()
		v.SetUint(uint64(r.Intn(1 << 20)))
		return v
	case reflect.Float32, reflect.Float64:
		v := reflect.New(t).Elem()
		v.SetFloat(float64(r.Intn(1 << 20)))
		return v
	case reflect.String:
		v := reflect.New(t).Elem()
		v.SetString(RandString(r))
		return v
	case reflect.Pointer:
		if r.Intn(3) == 0 {
			return reflect.Zero(t)
		}
		p := reflect.New(t.Elem())
		p.Elem().Set(g.Value(t.Elem(), r))
		return p
	case reflect.Slice:
		n := r.Intn(4)
		s := reflect.MakeSlice(t, n, n)
		for i := range n {
			s.Index(i).Set(g.Value(t.Elem(), r))
		}
		return s
	case reflect.Map:
		m := reflect.MakeMap(t)
		for i := r.Intn(4); i > 0; i-- {
			m.SetMapIndex(g.Value(t.Key(), r), g.Value(t.Elem(), r))
		}
		return m
	case reflect.Struct:
		v := reflect.New(t).Elem()
		for i := 0; i < t.NumField(); i++ {
			fv := v.Field(i)
			if !fv.CanSet() {
				continue
			}
			fv.Set(g.Value(t.Field(i).Type, r))
		}
		return v
	default:
		panic("limitstest: don't know how to generate values of type " + t.String())
	}
}

// perturbExtensions randomizes a subset of the extension values stored in l.
func (g *Generator) perturbExtensions(l *validation.Limits, r *rand.Rand) {
	ext := limitsExtensions(l)
	for name, val := range ext {
		if g.coin(r, weightExtensionPerturb) {
			ext[name] = g.Value(reflect.TypeOf(val), r).Interface()
		}
	}
}

func limitsExtensions(l *validation.Limits) map[string]any {
	f := reflect.ValueOf(l).Elem().FieldByName("extensions")
	f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
	return f.Interface().(map[string]any)
}

// defaultValueFuncs returns generators for the field types used by
// validation.Limits that are owned by other modules (and therefore can't
// implement testing/quick.Generator here) or that carry unexported state.
func defaultValueFuncs() map[reflect.Type]ValueFunc {
	return map[reflect.Type]ValueFunc{
		reflect.TypeFor[model.Duration](): func(r *rand.Rand) reflect.Value {
			return reflect.ValueOf(model.Duration(time.Duration(r.Intn(100000)) * time.Second))
		},
		reflect.TypeFor[time.Duration](): func(r *rand.Rand) reflect.Value {
			return reflect.ValueOf(time.Duration(r.Intn(100000)) * time.Second)
		},
		reflect.TypeFor[time.Time](): func(r *rand.Rand) reflect.Value {
			return reflect.ValueOf(randTime(r))
		},
		reflect.TypeFor[model.ValidationScheme](): func(r *rand.Rand) reflect.Value {
			choices := []model.ValidationScheme{model.UnsetValidation, model.LegacyValidation, model.UTF8Validation}
			return reflect.ValueOf(choices[r.Intn(len(choices))])
		},
		reflect.TypeFor[flagext.LimitsMap[int]](): func(r *rand.Rand) reflect.Value {
			data := map[string]int{}
			for i := r.Intn(3); i > 0; i-- {
				data[RandKey(r)] = r.Intn(1000) + 1
			}
			return reflect.ValueOf(flagext.NewLimitsMapWithData(data, nil))
		},
		reflect.TypeFor[flagext.LimitsMap[float64]](): func(r *rand.Rand) reflect.Value {
			data := map[string]float64{}
			for i := r.Intn(3); i > 0; i-- {
				data[RandKey(r)] = float64(r.Intn(1000) + 1)
			}
			return reflect.ValueOf(flagext.NewLimitsMapWithData(data, nil))
		},
		reflect.TypeFor[flagext.LimitsMap[string]](): func(r *rand.Rand) reflect.Value {
			data := map[string]string{}
			for i := r.Intn(3); i > 0; i-- {
				data[RandKey(r)] = RandKey(r)
			}
			return reflect.ValueOf(flagext.NewLimitsMapWithData(data, nil))
		},
		reflect.TypeFor[flagext.StringSliceCSV](): func(r *rand.Rand) reflect.Value {
			n := r.Intn(4)
			s := make(flagext.StringSliceCSV, n)
			for i := range s {
				s[i] = RandKey(r)
			}
			return reflect.ValueOf(s)
		},
		reflect.TypeFor[flagext.CIDRSliceCSV](): func(r *rand.Rand) reflect.Value {
			var c flagext.CIDRSliceCSV
			for i := r.Intn(3); i > 0; i-- {
				cidr := randCIDR(r)
				if err := c.Set(cidr); err != nil {
					panic(fmt.Sprintf("limitstest: generated invalid CIDR %q: %v", cidr, err))
				}
			}
			return reflect.ValueOf(c)
		},
	}
}

// randTime returns a random time truncated to whole seconds (so it marshals to a
// clean RFC3339 timestamp).
func randTime(r *rand.Rand) time.Time {
	base := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	return base.Add(time.Duration(r.Intn(1_000_000_000)) * time.Second)
}

// randCIDR returns a random, syntactically valid IPv4 or IPv6 CIDR string.
func randCIDR(r *rand.Rand) string {
	if r.Intn(2) == 0 {
		return fmt.Sprintf("%d.%d.%d.%d/%d", r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(256), r.Intn(33))
	}
	b := make([]byte, net.IPv6len)
	for i := range b {
		b[i] = byte(r.Intn(256))
	}
	// Keep the high byte in the global-unicast range so the address always
	// stringifies as IPv6 (never as an IPv4-mapped/compatible address, whose
	// textual form would be incompatible with an IPv6-sized prefix length).
	b[0] = byte(0x20 + r.Intn(0x10))
	return fmt.Sprintf("%s/%d", net.IP(b).String(), r.Intn(129))
}

const alphabet = "abcdefghABCDEFGH0123456789_"

// RandString returns a random (possibly empty) short string. Exported so
// Generator.ValueFunc implementations supplied by callers can reuse it.
func RandString(r *rand.Rand) string {
	b := make([]byte, r.Intn(8))
	for i := range b {
		b[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(b)
}

// RandKey returns a random non-empty short string, suitable for map keys.
func RandKey(r *rand.Rand) string {
	b := make([]byte, r.Intn(7)+1)
	for i := range b {
		b[i] = alphabet[r.Intn(len(alphabet))]
	}
	return string(b)
}

// durationAsZeroInt encodes a duration as the bare integer 0.
type durationAsZeroInt model.Duration

func (durationAsZeroInt) MarshalYAML() (any, error) { return 0, nil }

// timeAsString encodes a timestamp as a quoted RFC3339 string instead of a
// native YAML timestamp.
type timeAsString time.Time

func (t timeAsString) MarshalYAML() (any, error) { return time.Time(t).Format(time.RFC3339), nil }
func (t timeAsString) IsZero() bool              { return time.Time(t).IsZero() }

var (
	durationType     = reflect.TypeFor[model.Duration]()
	timeType         = reflect.TypeFor[time.Time]()
	durationAsIntTyp = reflect.TypeFor[durationAsZeroInt]()
	timeAsStringTyp  = reflect.TypeFor[timeAsString]()

	yamlMarshalerType   = reflect.TypeFor[yaml.Marshaler]()
	yamlUnmarshalerType = reflect.TypeFor[yaml.Unmarshaler]()
)

// mapToTypeWithCustomEncoding maps t to a derived type, such that:
//
//   - Unexported fields are dropped (they aren't serialized).
//   - Types with an associated custom encoding are converted to use the custom
//     encoding, with some probability.
func (g *Generator) mapToTypeWithCustomEncoding(t reflect.Type, r *rand.Rand) reflect.Type {
	switch t {
	case durationType:
		if g.coin(r, weightDurationAsInt) {
			return durationAsIntTyp
		}
		return t
	case timeType:
		if g.coin(r, weightTimeAsString) {
			return timeAsStringTyp
		}
		return t
	}
	if isOpaque(t) {
		return t
	}

	switch t.Kind() {
	case reflect.Pointer:
		if e := g.mapToTypeWithCustomEncoding(t.Elem(), r); e != t.Elem() {
			return reflect.PointerTo(e)
		}
	case reflect.Slice:
		if e := g.mapToTypeWithCustomEncoding(t.Elem(), r); e != t.Elem() {
			return reflect.SliceOf(e)
		}
	case reflect.Array:
		if e := g.mapToTypeWithCustomEncoding(t.Elem(), r); e != t.Elem() {
			return reflect.ArrayOf(t.Len(), e)
		}
	case reflect.Map:
		if e := g.mapToTypeWithCustomEncoding(t.Elem(), r); e != t.Elem() {
			return reflect.MapOf(t.Key(), e)
		}
	case reflect.Struct:
		return g.mapToStructWithCustomEncoding(t, r)
	}
	return t
}

func (g *Generator) mapToStructWithCustomEncoding(t reflect.Type, r *rand.Rand) reflect.Type {
	fields := make([]reflect.StructField, 0, t.NumField())
	changed := false
	for f := range t.Fields() {
		f := f
		if f.PkgPath != "" {
			changed = true // dropping an unexported field changes the type
			continue
		}
		ft := g.mapToTypeWithCustomEncoding(f.Type, r)
		if ft != f.Type {
			changed = true
		}
		fields = append(fields, reflect.StructField{
			Name:      f.Name,
			Type:      ft,
			Tag:       f.Tag,
			Anonymous: f.Anonymous,
		})
	}
	if !changed {
		return t
	}
	return reflect.StructOf(fields)
}

func isOpaque(t reflect.Type) bool {
	pt := reflect.PointerTo(t)
	return t.Implements(yamlMarshalerType) || pt.Implements(yamlMarshalerType) ||
		t.Implements(yamlUnmarshalerType) || pt.Implements(yamlUnmarshalerType)
}

func deepConvert(src reflect.Value, dst reflect.Type) reflect.Value {
	switch dst {
	case src.Type(), durationAsIntTyp, timeAsStringTyp:
		return src.Convert(dst)
	}

	switch dst.Kind() {
	case reflect.Pointer:
		if src.IsNil() {
			return reflect.Zero(dst)
		}
		p := reflect.New(dst.Elem())
		p.Elem().Set(deepConvert(src.Elem(), dst.Elem()))
		return p
	case reflect.Slice:
		if src.IsNil() {
			return reflect.Zero(dst)
		}
		out := reflect.MakeSlice(dst, src.Len(), src.Len())
		for i := range src.Len() {
			out.Index(i).Set(deepConvert(src.Index(i), dst.Elem()))
		}
		return out
	case reflect.Array:
		out := reflect.New(dst).Elem()
		for i := range src.Len() {
			out.Index(i).Set(deepConvert(src.Index(i), dst.Elem()))
		}
		return out
	case reflect.Map:
		if src.IsNil() {
			return reflect.Zero(dst)
		}
		out := reflect.MakeMapWithSize(dst, src.Len())
		for iter := src.MapRange(); iter.Next(); {
			out.SetMapIndex(iter.Key(), deepConvert(iter.Value(), dst.Elem()))
		}
		return out
	case reflect.Struct:
		out := reflect.New(dst).Elem()
		for i := range dst.NumField() {
			df := dst.Field(i)
			sf := src.FieldByName(df.Name)
			if !sf.IsValid() {
				continue
			}
			out.Field(i).Set(deepConvert(sf, df.Type))
		}
		return out
	default:
		if src.Type().ConvertibleTo(dst) {
			return src.Convert(dst)
		}
		return src
	}
}

// marshalToMap marshals v to YAML and parses it back into a generic map, exactly
// the shape a runtime-config loader receives.
func marshalToMap(v any) map[string]any {
	b, err := yaml.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("limitstest: marshaling: %v", err))
	}
	var m map[string]any
	if err := yaml.Unmarshal(b, &m); err != nil {
		panic(fmt.Sprintf("limitstest: unmarshaling into map: %v", err))
	}
	if m == nil {
		m = map[string]any{}
	}
	return m
}
