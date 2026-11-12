// SPDX-License-Identifier: AGPL-3.0-only

package mimir

import (
	"bytes"
	"flag"
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	"github.com/grafana/mimir/pkg/util/validation"
	"github.com/grafana/mimir/pkg/util/validation/limitstest"
)

// TestRuntimeConfigLoader_YAMLAndMapLoadersAreEquivalent checks that loading a
// runtime config file with -runtime-config.loader=yaml (round-tripping through
// YAML) yields the same configuration as loading it with
// -runtime-config.loader=map (decoding directly from the merged map using
// mapstructure).
//
// The generated limits are produced by reflecting over validation.Limits (see
// limitstest), so new fields are covered automatically: any field type the
// generator doesn't know how to produce makes the test fail loudly, forcing it
// to be handled explicitly.
func TestRuntimeConfigLoader_YAMLAndMapLoadersAreEquivalent(t *testing.T) {
	t.Cleanup(func() { validation.SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits()) })

	// roundTrip simulates the runtime config file being read from YAML into a map[string]any.
	roundTrip := func(m map[string]any) map[string]any {
		out := map[string]any{}
		require.NoError(t, yaml.Unmarshal(mustMarshalYAML(t, m), &out))
		return out
	}

	base := newDefaultLimitsForEquivalence(t)

	f := func(config map[string]any) bool {
		file := mustMarshalYAML(t, config)
		loader := &runtimeConfigLoader{}

		validation.SetDefaultLimitsForYAMLUnmarshalling(newDefaultLimitsForEquivalence(t))
		viaYAML, errYAML := loader.load(bytes.NewReader(file))

		validation.SetDefaultLimitsForYAMLUnmarshalling(newDefaultLimitsForEquivalence(t))
		viaMap, errMap := loader.loadFromMap(config)

		require.Equalf(t, errYAML == nil, errMap == nil, "YAML and map loaders disagree on validity.\ninput:\n%s\nyaml loader err: %v\nmap loader err: %v", file, errYAML, errMap)
		if errYAML != nil {
			return true
		}

		// Compare the marshaled forms so we don't depend on unexported fields.
		yamlOut, mapOut := mustMarshalYAML(t, viaYAML), mustMarshalYAML(t, viaMap)
		require.Equalf(t, string(yamlOut), string(mapOut), "YAML and map loaders produced different configs.\ninput:\n%s", file)

		return true
	}

	cfg := &quick.Config{
		MaxCount: 100,
		Values: func(args []reflect.Value, r *rand.Rand) {
			tenants := map[string]any{}
			for i := range 2 + r.Intn(5) {
				tenants[fmt.Sprintf("tenant-%d", i)] = roundTrip(limitstest.GenerateLimits(r, base))
			}
			args[0] = reflect.ValueOf(map[string]any{"overrides": tenants})
		},
	}
	require.NoError(t, quick.Check(f, cfg))
}

func newDefaultLimitsForEquivalence(t *testing.T) validation.Limits {
	t.Helper()

	var l validation.Limits
	fs := flag.NewFlagSet("test", flag.PanicOnError)
	l.RegisterFlags(fs)
	l.RegisterExtensionsDefaults()
	// Set some defaults for fields with reference types. Regression test
	// for a bug by which mapstructure would mutate the default limits in place,
	// thus affecting other Limits.
	require.NoError(t, fs.Parse([]string{
		// flagext.StringSliceCSV
		"-query-frontend.enabled-promql-experimental-functions=info,sort_by_label",
		"-query-frontend.enabled-promql-extended-range-selectors=smoothed",
		"-query-frontend.enabled-promql-binop-fill-modifiers=fill,fill_left",
		"-ruler.protected-namespaces=protected-1,protected-2",
		"-distributor.otel-promote-resource-attributes=k8s.cluster.name,host.name",
		// flagext.StringSlice
		"-distributor.drop-label=dropped-1",
		"-distributor.drop-label=dropped-2",
		// flagext.CIDRSliceCSV
		"-alertmanager.receivers-firewall-block-cidr-networks=10.0.0.0/8,192.168.0.0/16",
		// flagext.LimitsMap
		`-ruler.max-rules-per-rule-group-by-namespace={"ns-1":10,"ns-2":20}`,
		`-ruler.max-rule-groups-per-tenant-by-namespace={"ns-1":10,"ns-2":20}`,
	}))
	return l
}

func mustMarshalYAML(t *testing.T, v any) []byte {
	t.Helper()
	b, err := yaml.Marshal(v)
	require.NoError(t, err)
	return b
}
