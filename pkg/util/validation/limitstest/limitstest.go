// SPDX-License-Identifier: AGPL-3.0-only

package limitstest

import (
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"testing/quick"
	"time"
	"unsafe"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"

	"github.com/grafana/mimir/pkg/util/validation"
)

// Generator produces random validation.Limits values.
//
// The zero value is not usable; call New and customize the returned Generator.
type Generator struct {
	// ValueFuncs maps a reflect.Type to a generator for that type. It is
	// consulted first, before the testing/quick.Generator interface and before
	// the generic reflection-based generation. Downstream callers can add
	// entries for their own field or extension types.
	ValueFuncs map[reflect.Type]ValueFunc

	// SkipFields is the set of validation.Limits field names that are left at
	// their default value instead of being randomized.
	SkipFields map[string]bool

	// FieldPerturbChance is the percentage chance (0-100) that any given
	// top-level Limits field is randomized rather than left at its default.
	FieldPerturbChance int

	// ExtensionPerturbChance is the percentage chance (0-100) that any given
	// registered extension value is randomized rather than left at its default.
	ExtensionPerturbChance int
}

// ValueFunc returns a random reflect.Value of a specific type.
type ValueFunc func(*rand.Rand) reflect.Value

// New returns a Generator preconfigured for the standard Mimir
// validation.Limits field types. The returned Generator may be customized
// further (its maps are safe to mutate) before use.
func New() *Generator {
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
		FieldPerturbChance:     40,
		ExtensionPerturbChance: 60,
	}
}

func (g *Generator) Limits(r *rand.Rand, defaults validation.Limits) validation.Limits {
	l := defaults

	// Give l its own extensions map (RegisterExtensionsDefaults allocates a
	// fresh one) so perturbing it doesn't mutate the shared defaults.
	l.RegisterExtensionsDefaults()

	v := reflect.ValueOf(&l).Elem()
	tp := v.Type()
	for i := 0; i < v.NumField(); i++ {
		fv := v.Field(i)
		if !fv.CanSet() {
			// Unexported bookkeeping fields (extensions map, atomic pointers,
			// cached hash). They are handled elsewhere or reset on decode.
			continue
		}
		if g.SkipFields[tp.Field(i).Name] {
			continue
		}
		if r.Intn(100) < g.FieldPerturbChance {
			fv.Set(g.Value(fv.Type(), r))
		}
	}

	g.perturbExtensions(&l, r)
	return l
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
		for i := 0; i < n; i++ {
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
		if r.Intn(100) < g.ExtensionPerturbChance {
			ext[name] = g.Value(reflect.TypeOf(val), r).Interface()
		}
	}
}

// limitsExtensions returns the (unexported) extensions map of l so the
// generator can randomize the extension values. There is no exported accessor
// for it, and exercising the extensions is the whole point of reusing this
// generator downstream, so we reach in.
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
