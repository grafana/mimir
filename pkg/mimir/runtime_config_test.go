// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/runtime_config_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/grafana/dskit/clusterutil"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"

	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestMain(m *testing.M) {
	validation.SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())

	m.Run()
}

// Given limits are usually loaded via a config file, and that
// a configmap is limited to 1MB, we need to minimise the limits file.
// One way to do it is via YAML anchors.
func TestRuntimeConfigLoader_ShouldLoadAnchoredYAML(t *testing.T) {
	yamlFile := strings.NewReader(`
overrides:
  '1234': &id001
    ingestion_burst_size: 15000
    ingestion_rate: 1500
    max_global_series_per_metric: 7000
    max_global_series_per_user: 15000
    ruler_max_rule_groups_per_tenant: 20
    ruler_max_rules_per_rule_group: 20
  '1235': *id001
  '1236': *id001
`)

	loader := &runtimeConfigLoader{}
	runtimeCfg, err := loader.load(yamlFile)
	require.NoError(t, err)

	expected := getDefaultLimits()
	expected.IngestionRate = 1500
	expected.IngestionBurstSize = 15000
	expected.MaxGlobalSeriesPerUser = 15000
	expected.MaxGlobalSeriesPerMetric = 7000
	expected.RulerMaxRulesPerRuleGroup = 20
	expected.RulerMaxRuleGroupsPerTenant = 20
	expected.OTelMetricSuffixesEnabled = nil
	expected.NameValidationScheme = model.UnsetValidation

	loadedLimits := runtimeCfg.(*runtimeConfigValues).TenantLimits
	require.Equal(t, 3, len(loadedLimits))

	compareOptions := []cmp.Option{
		cmp.AllowUnexported(validation.Limits{}),
		cmpopts.IgnoreFields(validation.Limits{}, "activeSeriesMergedCustomTrackersConfig", "costAttributionMergedTrackers"),
	}

	require.Empty(t, cmp.Diff(expected, *loadedLimits["1234"], compareOptions...))
	require.Empty(t, cmp.Diff(expected, *loadedLimits["1235"], compareOptions...))
	require.Empty(t, cmp.Diff(expected, *loadedLimits["1236"], compareOptions...))
}

func TestRuntimeConfigLoader_LoadsMetadataTenantOverrides(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected map[string]validation.Limits
	}{
		"omitted bool stays nil for metadata override": {
			input: `
overrides:
  'tenant-a:source=test-run':
    ingestion_rate: 200
`,
			expected: map[string]validation.Limits{
				"tenant-a:source=test-run": func() validation.Limits {
					limits := getDefaultLimits()
					limits.IngestionRate = 200
					limits.OTelMetricSuffixesEnabled = nil
					limits.NameValidationScheme = model.UnsetValidation
					return limits
				}(),
			},
		},
		"explicit bool values are preserved for tenant and global metadata overrides": {
			input: `
overrides:
  ':source=test-run':
    otel_metric_suffixes_enabled: false
  'tenant-a:source=load-test':
    otel_metric_suffixes_enabled: true
`,
			expected: map[string]validation.Limits{
				":source=test-run": func() validation.Limits {
					limits := getDefaultLimits()
					limits.OTelMetricSuffixesEnabled = boolPtr(false)
					limits.NameValidationScheme = model.UnsetValidation
					return limits
				}(),
				"tenant-a:source=load-test": func() validation.Limits {
					limits := getDefaultLimits()
					limits.OTelMetricSuffixesEnabled = boolPtr(true)
					limits.NameValidationScheme = model.UnsetValidation
					return limits
				}(),
			},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			loader := &runtimeConfigLoader{}
			runtimeCfg, err := loader.load(strings.NewReader(tc.input))
			require.NoError(t, err)

			loadedLimits := runtimeCfg.(*runtimeConfigValues).TenantLimits
			require.Len(t, loadedLimits, len(tc.expected))

			for userID, expected := range tc.expected {
				require.Contains(t, loadedLimits, userID)
				require.Empty(t, cmp.Diff(expected, *loadedLimits[userID], runtimeConfigCompareOptions()...))
			}
		})
	}
}

func TestRuntimeConfigLoader_ShouldLoadEmptyFile(t *testing.T) {
	yamlFile := strings.NewReader(`
# This is an empty YAML.
`)

	loader := &runtimeConfigLoader{}
	actual, err := loader.load(yamlFile)
	require.NoError(t, err)
	assert.Equal(t, &runtimeConfigValues{}, actual)
}

func TestRuntimeConfigLoader_MissingPointerFieldsAreNil(t *testing.T) {
	yamlFile := strings.NewReader(`
# This is an empty YAML.
`)
	loader := &runtimeConfigLoader{}
	actual, err := loader.load(yamlFile)
	require.NoError(t, err)

	actualCfg, ok := actual.(*runtimeConfigValues)
	require.Truef(t, ok, "expected to be able to cast %+v to runtimeConfigValues", actual)

	// Ensure that when settings are omitted, the pointers are nil. See #4228
	assert.Nil(t, actualCfg.IngesterLimits)
}

func TestRuntimeConfigLoader_ShouldReturnErrorOnMultipleDocumentsInTheConfig(t *testing.T) {
	cases := []string{
		`
---
---
`, `
---
overrides:
  '1234':
    ingestion_burst_size: 123
---
overrides:
  '1234':
    ingestion_burst_size: 123
`, `
---
# This is an empty YAML.
---
overrides:
  '1234':
    ingestion_burst_size: 123
`, `
---
overrides:
  '1234':
    ingestion_burst_size: 123
---
# This is an empty YAML.
`,
	}

	for _, tc := range cases {
		loader := &runtimeConfigLoader{}
		actual, err := loader.load(strings.NewReader(tc))
		assert.Equal(t, errMultipleDocuments, err)
		assert.Nil(t, actual)
	}
}

func TestRuntimeConfigLoader_RunsValidation(t *testing.T) {
	for _, tc := range []struct {
		name     string
		validate func(limits *validation.Limits) error
		hasError bool
	}{
		{
			name: "successful validate doesn't return error",
			validate: func(*validation.Limits) error {
				return nil
			},
		},
		{
			name: "no validate function doesn't return error",
		},
		{
			name: "unsuccessful validate returns error",
			validate: func(*validation.Limits) error {
				return errors.New("validation failed")
			},
			hasError: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			loader := &runtimeConfigLoader{
				validate: tc.validate,
			}
			_, err := loader.load(strings.NewReader(`
overrides:
  '1234':
    ingestion_burst_size: 123
`))
			if tc.hasError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRuntimeConfigLoader_ActiveSeriesCustomTrackersMergingShouldNotInterfereBetweenTenants(t *testing.T) {
	// Write the runtime config to a temporary file.
	runtimeConfigFile := filepath.Join(t.TempDir(), "runtime-config")
	require.NoError(t, os.WriteFile(runtimeConfigFile, []byte(`
overrides:
  'user-1': &user1
    active_series_custom_trackers:
      base:   '{foo="user_1_base"}'
      common: '{foo="user_1_base"}'

    active_series_additional_custom_trackers:
      additional: '{foo="user_1_additional"}'
      common:     '{foo="user_1_additional"}'

  # An user inheriting from another one.
  'user-2': *user1

  # An user with only base trackers configured.
  'user-3':
    active_series_custom_trackers:
      base:   '{foo="user_1_base"}'
      common: '{foo="user_1_base"}'

  # An user with only additional trackers configured.
  'user-4':
    active_series_additional_custom_trackers:
      additional: '{foo="user_1_additional"}'
      common:     '{foo="user_1_additional"}'

  # An user disabling default base trackers.
  'user-5':
    active_series_custom_trackers: {}

  # An user disabling default base trackers and adding additional trackers.
  'user-6':
    active_series_custom_trackers: {}

    active_series_additional_custom_trackers:
      additional: '{foo="user_1_additional"}'
      common:     '{foo="user_1_additional"}'
`), os.ModePerm))

	// Start the runtime config manager.
	cfg := Config{}
	flagext.DefaultValues(&cfg)
	defaultTrackers, err := asmodel.NewCustomTrackersConfig(map[string]string{"default": `{foo="default"}`})
	require.NoError(t, err)
	cfg.LimitsConfig.ActiveSeriesBaseCustomTrackersConfig = defaultTrackers

	require.NoError(t, cfg.RuntimeConfig.LoadPath.Set(runtimeConfigFile))
	validation.SetDefaultLimitsForYAMLUnmarshalling(cfg.LimitsConfig)

	manager, err := NewRuntimeManager(&cfg, "test", nil, log.NewNopLogger())
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(context.Background(), manager))
	t.Cleanup(func() {
		require.NoError(t, services.StopAndAwaitTerminated(context.Background(), manager))
	})

	overrides := validation.NewOverrides(cfg.LimitsConfig, newTenantLimits(manager))

	require.Equal(t, `additional:{foo="user_1_additional"};base:{foo="user_1_base"};common:{foo="user_1_additional"}`, overrides.ActiveSeriesCustomTrackersConfig("user-1").String())
	require.Equal(t, `additional:{foo="user_1_additional"};base:{foo="user_1_base"};common:{foo="user_1_additional"}`, overrides.ActiveSeriesCustomTrackersConfig("user-2").String())
	require.Equal(t, `base:{foo="user_1_base"};common:{foo="user_1_base"}`, overrides.ActiveSeriesCustomTrackersConfig("user-3").String())
	require.Equal(t, `additional:{foo="user_1_additional"};common:{foo="user_1_additional"};default:{foo="default"}`, overrides.ActiveSeriesCustomTrackersConfig("user-4").String())
	require.Equal(t, ``, overrides.ActiveSeriesCustomTrackersConfig("user-5").String())
	require.Equal(t, `additional:{foo="user_1_additional"};common:{foo="user_1_additional"}`, overrides.ActiveSeriesCustomTrackersConfig("user-6").String())
	require.Equal(t, `default:{foo="default"}`, overrides.ActiveSeriesCustomTrackersConfig("user-without-overrides").String())
}

func TestRuntimeConfigHTTPClientClusterValidation(t *testing.T) {
	const minimalConfig = "overrides: {}\n"

	t.Run("with cluster validation label", func(t *testing.T) {
		var gotHeader string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotHeader = r.Header.Get(clusterutil.ClusterValidationLabelHeader)
			_, _ = w.Write([]byte(minimalConfig))
		}))
		t.Cleanup(srv.Close)

		cfg := Config{}
		flagext.DefaultValues(&cfg)
		cfg.RuntimeConfig.HTTPClientClusterValidation.Label = "test-cluster"
		require.NoError(t, cfg.RuntimeConfig.LoadPath.Set(srv.URL))

		manager, err := NewRuntimeManager(&cfg, "test", nil, log.NewNopLogger())
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), manager))
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), manager)) })

		require.Equal(t, "test-cluster", gotHeader)
	})

	t.Run("without cluster validation label", func(t *testing.T) {
		var headerPresent bool
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, headerPresent = r.Header[clusterutil.ClusterValidationLabelHeader]
			_, _ = w.Write([]byte(minimalConfig))
		}))
		t.Cleanup(srv.Close)

		cfg := Config{}
		flagext.DefaultValues(&cfg)
		require.NoError(t, cfg.RuntimeConfig.LoadPath.Set(srv.URL))

		manager, err := NewRuntimeManager(&cfg, "test", nil, log.NewNopLogger())
		require.NoError(t, err)
		require.NoError(t, services.StartAndAwaitRunning(context.Background(), manager))
		t.Cleanup(func() { require.NoError(t, services.StopAndAwaitTerminated(context.Background(), manager)) })

		require.False(t, headerPresent)
	})
}

func getDefaultLimits() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}

func runtimeConfigCompareOptions() []cmp.Option {
	return []cmp.Option{
		cmp.AllowUnexported(validation.Limits{}),
		cmpopts.IgnoreFields(validation.Limits{}, "activeSeriesMergedCustomTrackersConfig", "costAttributionMergedTrackers"),
	}
}

func boolPtr(v bool) *bool {
	return &v
}

func BenchmarkRuntimeConfigLoader_Load(b *testing.B) {
	for _, tenants := range []int{100, 1000, 5000, 20000} {
		b.Run(fmt.Sprintf("tenants=%d", tenants), func(b *testing.B) {
			overrides := make(map[string]map[string]int, tenants)
			for i := 0; i < tenants; i++ {
				overrides[fmt.Sprintf("tenant-%d", i)] = map[string]int{"ingestion_rate": 1000 + i, "max_global_series_per_user": 150000 + i}
			}
			data, err := yaml.Marshal(map[string]any{"overrides": overrides})
			require.NoError(b, err)
			loader := &runtimeConfigLoader{}

			b.ReportAllocs()
			for b.Loop() {
				if _, err := loader.load(bytes.NewReader(data)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkRuntimeConfigLoader_LoadFromMap simulates the runtime config manager
// reloading a file where only a few tenants change between reloads, which is the
// common case when the file is regenerated periodically.
func BenchmarkRuntimeConfigLoader_LoadFromMap(b *testing.B) {
	for _, tenants := range []int{1000, 5000, 20000} {
		for _, changedTenants := range []int{1, 100} {
			b.Run(fmt.Sprintf("tenants=%d/changed=%d", tenants, changedTenants), func(b *testing.B) {
				// Alternate between two files differing by changedTenants, so every reload sees a change.
				files := make([][]byte, 2)
				for v := range files {
					overrides := make(map[string]map[string]any, tenants)
					for i := 0; i < tenants; i++ {
						rate := 1000 + i
						if i < changedTenants {
							rate += v
						}
						overrides[fmt.Sprintf("tenant-%d", i)] = map[string]any{
							"ingestion_rate":                    rate,
							"ingestion_burst_size":              10 * rate,
							"max_global_series_per_user":        150000 + i,
							"compactor_blocks_retention_period": "30d",
							"max_label_names_per_series":        40,
						}
					}
					var err error
					files[v], err = yaml.Marshal(map[string]any{"overrides": overrides})
					require.NoError(b, err)
				}
				loader := &runtimeConfigLoader{}
				// Measure steady-state reloads rather than the initial load.
				for _, f := range files {
					m := map[string]any{}
					require.NoError(b, yaml.Unmarshal(f, &m))
					_, err := loader.loadFromMap(m)
					require.NoError(b, err)
				}

				b.ReportAllocs()
				i := 0
				for b.Loop() {
					// Parse inside the loop, like the runtime config manager does on every reload.
					m := map[string]any{}
					if err := yaml.Unmarshal(files[i%2], &m); err != nil {
						b.Fatal(err)
					}
					if _, err := loader.loadFromMap(m); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		}
	}
}

func TestRuntimeConfigLoader_LoadFromMapReusesUnchangedTenants(t *testing.T) {
	parse := func(t *testing.T, s string) map[string]any {
		m := map[string]any{}
		require.NoError(t, yaml.Unmarshal([]byte(s), &m))
		return m
	}

	loader := &runtimeConfigLoader{}
	first, err := loader.loadFromMap(parse(t, `
overrides:
  unchanged:
    ingestion_rate: 100
    active_series_custom_trackers:
      foo: '{foo="bar"}'
  changed:
    ingestion_rate: 200
  removed:
    ingestion_rate: 300
`))
	require.NoError(t, err)

	secondFile := `
overrides:
  unchanged:
    ingestion_rate: 100
    active_series_custom_trackers:
      foo: '{foo="bar"}'
  changed:
    ingestion_rate: 201
  added:
    ingestion_rate: 400
`
	second, err := loader.loadFromMap(parse(t, secondFile))
	require.NoError(t, err)

	firstLimits := first.(*runtimeConfigValues).TenantLimits
	secondLimits := second.(*runtimeConfigValues).TenantLimits
	require.Same(t, firstLimits["unchanged"], secondLimits["unchanged"])
	require.NotSame(t, firstLimits["changed"], secondLimits["changed"])
	require.Equal(t, 201.0, secondLimits["changed"].IngestionRate)
	require.Equal(t, 400.0, secondLimits["added"].IngestionRate)
	require.NotContains(t, secondLimits, "removed")

	// The incremental result must match loading the same file from scratch.
	fresh, err := (&runtimeConfigLoader{}).loadFromMap(parse(t, secondFile))
	require.NoError(t, err)
	require.Equal(t, string(mustMarshalYAML(t, fresh)), string(mustMarshalYAML(t, second)))
}

func TestRuntimeConfigLoader_LoadFromMapDoesNotCacheFailedLoads(t *testing.T) {
	parse := func(t *testing.T, s string) map[string]any {
		m := map[string]any{}
		require.NoError(t, yaml.Unmarshal([]byte(s), &m))
		return m
	}
	validationErr := errors.New("ingestion rate too high")
	loader := &runtimeConfigLoader{validate: func(l *validation.Limits) error {
		if l.IngestionRate > 1000 {
			return validationErr
		}
		return nil
	}}

	_, err := loader.loadFromMap(parse(t, "overrides:\n  a:\n    ingestion_rate: 100\n"))
	require.NoError(t, err)

	invalid := "overrides:\n  a:\n    ingestion_rate: 100\n  b:\n    ingestion_rate: 2000\n"
	_, err = loader.loadFromMap(parse(t, invalid))
	require.ErrorIs(t, err, validationErr)

	// A tenant that failed validation must be validated again on the next reload.
	_, err = loader.loadFromMap(parse(t, invalid))
	require.ErrorIs(t, err, validationErr)
}

func TestRawConfigEqual(t *testing.T) {
	for name, tc := range map[string]struct {
		a, b  any
		equal bool
	}{
		"equal nested":         {map[string]any{"a": []any{1, "x", nil}, "b": map[string]any{"c": 1.5}}, map[string]any{"a": []any{1, "x", nil}, "b": map[string]any{"c": 1.5}}, true},
		"different scalar":     {map[string]any{"a": 1}, map[string]any{"a": 2}, false},
		"different types":      {map[string]any{"a": 1}, map[string]any{"a": "1"}, false},
		"missing key":          {map[string]any{"a": 1}, map[string]any{"b": 1}, false},
		"different length":     {[]any{1}, []any{1, 2}, false},
		"map vs nil":           {map[string]any{}, nil, false},
		"nil vs map":           {nil, map[string]any{}, false},
		"empty maps":           {map[string]any{}, map[string]any{}, true},
		"non-basic types":      {map[string]any{"t": int32(1)}, map[string]any{"t": int32(1)}, true},
		"non-basic types diff": {map[string]any{"t": int32(1)}, map[string]any{"t": int32(2)}, false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.equal, rawConfigEqual(tc.a, tc.b))
		})
	}
}
