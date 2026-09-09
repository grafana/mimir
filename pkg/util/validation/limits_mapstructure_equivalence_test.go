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

	f := func(config map[string]any) bool {
		// Simulate the real runtimeconfig scenario: config is read from YAML,
		// then unmarshaled into a map[string]any, then either re-encoded as
		// YAML (viaYAML) or passed directly as a map (viaMap).
		b, err := yaml.Marshal(config)
		require.NoError(t, err)
		config = map[string]any{}
		require.NoError(t, yaml.Unmarshal(b, &config))

		var viaYAML validation.Limits
		b, err = yaml.Marshal(config)
		require.NoError(t, err)
		errYAML := yaml.Unmarshal(b, &viaYAML)

		var viaMap validation.Limits
		errMap := viaMap.UnmarshalMapstructure(config)

		require.Equalf(t, errYAML == nil, errMap == nil, "YAML and mapstructure loaders disagree on validity.\ninput:\n%s\nyaml loader err: %v\nmap loader err: %v", b, errYAML, errMap)
		if errYAML != nil {
			return true
		}

		// Compare the marshaled forms so we don't depend on unexported fields.
		yamlOut, err := yaml.Marshal(&viaYAML)
		require.NoError(t, err)
		mapOut, err := yaml.Marshal(&viaMap)
		require.NoError(t, err)
		require.Equalf(t, string(yamlOut), string(mapOut), "YAML and mapstructure loaders produced different configs.\ninput:\n%s", b)

		return true
	}

	cfg := &quick.Config{
		MaxCount: 2000,
		Values: func(args []reflect.Value, r *rand.Rand) {
			args[0] = reflect.ValueOf(limitstest.GenerateLimits(r, defaults))
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
