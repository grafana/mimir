// SPDX-License-Identifier: AGPL-3.0-only

package validation_test

import (
	"flag"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/grafana/mimir/pkg/util/validation/limitstest"
)

// TestLimits_YAMLAndMapstructureDecodingAreEquivalent checks that decoding
// per-tenant limits by round-tripping through YAML yields the same
// configuration as decoding directly from the merged map using mapstructure.
//
// The two decoders correspond to the -runtime-config.loader=yaml and
// -runtime-config.loader=map selectors respectively (see
// pkg/mimir/runtime_config.go). Both go through the exact same
// (*Limits).unmarshal machinery — defaults, extension handling, migration,
// validation and canonicalization — so the only meaningful difference between
// them is the raw decode step, which is what this test stresses.
//
// The generated limits are produced by reflecting over validation.Limits (see
// limitstest), so new fields are covered automatically: any field type the
// generator doesn't know how to produce makes the test fail loudly, forcing it
// to be handled explicitly.
func TestLimits_YAMLAndMapstructureDecodingAreEquivalent(t *testing.T) {
	defaults := defaultLimitsForEquivalence(t)
	validation.SetDefaultLimitsForYAMLUnmarshalling(defaults)

	gen := limitstest.New()

	f := func(l validation.Limits) bool {
		// The generated limits are the "input" a user could write: marshal them
		// to YAML, which is exactly what a runtime config file would contain.
		b, err := yaml.Marshal(&l)
		require.NoError(t, err)

		// -runtime-config.loader=yaml: decode the YAML document straight into Limits.
		var viaYAML validation.Limits
		errYAML := yaml.Unmarshal(b, &viaYAML)

		// -runtime-config.loader=map: decode the YAML into a generic map (this is
		// what dskit's runtimeconfig hands to the map loader) and then decode
		// that map with mapstructure.
		var raw map[string]any
		require.NoError(t, yaml.Unmarshal(b, &raw))
		var viaMap validation.Limits
		errMap := viaMap.UnmarshalMapstructure(raw)

		// Both loaders must agree on whether the config is valid at all.
		require.Equalf(t, errYAML == nil, errMap == nil,
			"YAML and mapstructure loaders disagree on validity.\ninput:\n%s\nyaml loader err: %v\nmap loader err: %v",
			b, errYAML, errMap)

		// If both rejected the config there's nothing else to compare.
		if errYAML != nil {
			return true
		}

		// Both accepted it: the resulting configuration must be equivalent. We
		// compare the marshaled forms so we don't depend on unexported
		// bookkeeping fields (atomic pointers, cached hashes, ...) which are
		// derived from the same content anyway.
		yamlOut, err := yaml.Marshal(&viaYAML)
		require.NoError(t, err)
		mapOut, err := yaml.Marshal(&viaMap)
		require.NoError(t, err)
		require.Equalf(t, string(yamlOut), string(mapOut),
			"YAML and mapstructure loaders produced different configs.\ninput:\n%s", b)
		return true
	}

	cfg := &quick.Config{
		MaxCount: 2000,
		Values: func(args []reflect.Value, r *rand.Rand) {
			args[0] = reflect.ValueOf(gen.Limits(r, defaults))
		},
	}
	require.NoError(t, quick.Check(f, cfg))
}

func defaultLimitsForEquivalence(t *testing.T) validation.Limits {
	t.Helper()
	var l validation.Limits
	l.RegisterFlags(flag.NewFlagSet("test", flag.PanicOnError))
	l.RegisterExtensionsDefaults()
	return l
}
