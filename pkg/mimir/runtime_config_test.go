// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/cortex/runtime_config_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package mimir

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/grafana/dskit/flagext"
	"github.com/grafana/dskit/services"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

	loadedLimits := runtimeCfg.(*runtimeConfigValues).TenantLimits
	require.Equal(t, 3, len(loadedLimits))

	compareOptions := []cmp.Option{
		cmp.AllowUnexported(validation.Limits{}),
		cmpopts.IgnoreFields(validation.Limits{}, "activeSeriesMergedCustomTrackersConfig"),
	}

	require.True(t, cmp.Equal(expected, *loadedLimits["1234"], compareOptions...))
	require.True(t, cmp.Equal(expected, *loadedLimits["1235"], compareOptions...))
	require.True(t, cmp.Equal(expected, *loadedLimits["1236"], compareOptions...))
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
		validate func(limits validation.Limits) error
		hasError bool
	}{
		{
			name: "successful validate doesn't return error",
			validate: func(validation.Limits) error {
				return nil
			},
		},
		{
			name: "no validate function doesn't return error",
		},
		{
			name: "unsuccessful validate returns error",
			validate: func(validation.Limits) error {
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

	overrides, err := validation.NewOverrides(cfg.LimitsConfig, newTenantLimits(manager))
	require.NoError(t, err)

	require.Equal(t, `additional:{foo="user_1_additional"};base:{foo="user_1_base"};common:{foo="user_1_additional"}`, overrides.ActiveSeriesCustomTrackersConfig("user-1").String())
	require.Equal(t, `additional:{foo="user_1_additional"};base:{foo="user_1_base"};common:{foo="user_1_additional"}`, overrides.ActiveSeriesCustomTrackersConfig("user-2").String())
	require.Equal(t, `base:{foo="user_1_base"};common:{foo="user_1_base"}`, overrides.ActiveSeriesCustomTrackersConfig("user-3").String())
	require.Equal(t, `additional:{foo="user_1_additional"};common:{foo="user_1_additional"};default:{foo="default"}`, overrides.ActiveSeriesCustomTrackersConfig("user-4").String())
	require.Equal(t, ``, overrides.ActiveSeriesCustomTrackersConfig("user-5").String())
	require.Equal(t, `additional:{foo="user_1_additional"};common:{foo="user_1_additional"}`, overrides.ActiveSeriesCustomTrackersConfig("user-6").String())
	require.Equal(t, `default:{foo="default"}`, overrides.ActiveSeriesCustomTrackersConfig("user-without-overrides").String())
}

func getDefaultLimits() validation.Limits {
	limits := validation.Limits{}
	flagext.DefaultValues(&limits)
	return limits
}
