// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/grafana/dskit/flagext"
	"github.com/pkg/errors"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"

	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
)

func TestMain(m *testing.M) {
	SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())

	m.Run()
}

func TestOverridesManager_GetOverrides(t *testing.T) {
	tenantLimits := map[string]*Limits{}

	defaults := Limits{
		MaxLabelNamesPerSeries: 100,
	}
	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	require.Equal(t, 100, ov.MaxLabelNamesPerSeries("user1"))
	require.Equal(t, 0, ov.MaxLabelValueLength("user1"))

	// Update limits for tenant user1. We only update single field, the rest is copied from defaults.
	// (That is how limits work when loaded from YAML)
	l := defaults
	l.MaxLabelValueLength = 150

	tenantLimits["user1"] = &l

	// Checking whether overrides were enforced
	require.Equal(t, 100, ov.MaxLabelNamesPerSeries("user1"))
	require.Equal(t, 150, ov.MaxLabelValueLength("user1"))

	// Verifying user2 limits are not impacted by overrides
	require.Equal(t, 100, ov.MaxLabelNamesPerSeries("user2"))
	require.Equal(t, 0, ov.MaxLabelValueLength("user2"))
}

func TestLimitsLoadingFromYaml(t *testing.T) {
	inp := `ingestion_rate: 0.5`

	l := Limits{}
	dec := yaml.NewDecoder(strings.NewReader(inp))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&l))

	assert.Equal(t, 0.5, l.IngestionRate, "from yaml")
	assert.Equal(t, 1024, l.MaxLabelNameLength, "from defaults")
}

func TestLimitsLoadingFromJson(t *testing.T) {
	inp := `{"ingestion_rate": 0.5}`

	l := Limits{}
	err := json.Unmarshal([]byte(inp), &l)
	require.NoError(t, err)

	assert.Equal(t, 0.5, l.IngestionRate, "from json")
	assert.Equal(t, 1024, l.MaxLabelNameLength, "from defaults")

	// Unmarshal should fail if input contains unknown struct fields and
	// the decoder flag `json.Decoder.DisallowUnknownFields()` is set
	inp = `{"unknown_fields": 100}`
	l = Limits{}
	dec := json.NewDecoder(strings.NewReader(inp))
	dec.DisallowUnknownFields()
	err = dec.Decode(&l)
	assert.Error(t, err)
}

func TestLimitsTagsYamlMatchJson(t *testing.T) {
	limits := reflect.TypeOf(Limits{})
	n := limits.NumField()
	var mismatch []string

	for i := 0; i < n; i++ {
		field := limits.Field(i)

		// Note that we aren't requiring YAML and JSON tags to match, just that
		// they either both exist or both don't exist.
		hasYAMLTag := field.Tag.Get("yaml") != ""
		hasJSONTag := field.Tag.Get("json") != ""

		if hasYAMLTag != hasJSONTag {
			mismatch = append(mismatch, field.Name)
		}
	}

	assert.Empty(t, mismatch, "expected no mismatched JSON and YAML tags")
}

func TestLimitsStringDurationYamlMatchJson(t *testing.T) {
	inputYAML := `
max_query_lookback: 1s
max_partial_query_length: 1s
`
	inputJSON := `{"max_query_lookback": "1s", "max_partial_query_length": "1s"}`

	limitsYAML := getDefaultLimits()
	err := yaml.Unmarshal([]byte(inputYAML), &limitsYAML)
	require.NoError(t, err, "expected to be able to unmarshal from YAML")

	limitsJSON := getDefaultLimits()
	err = json.Unmarshal([]byte(inputJSON), &limitsJSON)
	require.NoError(t, err, "expected to be able to unmarshal from JSON")

	assert.True(t, cmp.Equal(limitsYAML, limitsJSON, cmp.AllowUnexported(Limits{})), "expected YAML and JSON to match")
}

func TestLimitsAlwaysUsesPromDuration(t *testing.T) {
	stdlibDuration := reflect.TypeOf(time.Duration(0))
	limits := reflect.TypeOf(Limits{})
	n := limits.NumField()
	var badDurationType []string

	for i := 0; i < n; i++ {
		field := limits.Field(i)
		if field.Type == stdlibDuration {
			badDurationType = append(badDurationType, field.Name)
		}
	}

	assert.Empty(t, badDurationType, "some Limits fields are using stdlib time.Duration instead of model.Duration")
}

func TestMetricRelabelConfigLimitsLoadingFromYaml(t *testing.T) {
	inp := `
metric_relabel_configs:
- action: drop
  source_labels: [le]
  regex: .+
`
	exp := relabel.DefaultRelabelConfig
	exp.Action = relabel.Drop
	regex, err := relabel.NewRegexp(".+")
	require.NoError(t, err)
	exp.Regex = regex
	exp.SourceLabels = model.LabelNames([]model.LabelName{"le"})

	l := Limits{}
	dec := yaml.NewDecoder(strings.NewReader(inp))
	dec.KnownFields(true)
	require.NoError(t, dec.Decode(&l))

	assert.Equal(t, []*relabel.Config{&exp}, l.MetricRelabelConfigs)
}

func TestSmallestPositiveIntPerTenant(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxQueryParallelism: 5,
		},
		"tenant-b": {
			MaxQueryParallelism: 10,
		},
	}

	defaults := Limits{
		MaxQueryParallelism: 0,
	}
	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  int
	}{
		{tenantIDs: []string{}, expLimit: 0},
		{tenantIDs: []string{"tenant-a"}, expLimit: 5},
		{tenantIDs: []string{"tenant-b"}, expLimit: 10},
		{tenantIDs: []string{"tenant-c"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: 5},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b", "tenant-c"}, expLimit: 0},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveIntPerTenant(tc.tenantIDs, ov.MaxQueryParallelism))
	}
}

func TestSmallestPositiveNonZeroIntPerTenant(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxQueriersPerTenant: 5,
		},
		"tenant-b": {
			MaxQueriersPerTenant: 10,
		},
	}

	defaults := Limits{
		MaxQueriersPerTenant: 0,
	}
	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  int
	}{
		{tenantIDs: []string{}, expLimit: 0},
		{tenantIDs: []string{"tenant-a"}, expLimit: 5},
		{tenantIDs: []string{"tenant-b"}, expLimit: 10},
		{tenantIDs: []string{"tenant-c"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: 5},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: 0},
		{tenantIDs: []string{"tenant-a", "tenant-b", "tenant-c"}, expLimit: 5},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveNonZeroIntPerTenant(tc.tenantIDs, ov.MaxQueriersPerUser))
	}
}

func TestSmallestPositiveNonZeroDurationPerTenant(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxPartialQueryLength: model.Duration(time.Hour),
		},
		"tenant-b": {
			MaxPartialQueryLength: model.Duration(4 * time.Hour),
		},
	}

	defaults := Limits{
		MaxPartialQueryLength: 0,
	}
	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  time.Duration
	}{
		{tenantIDs: []string{}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a"}, expLimit: time.Hour},
		{tenantIDs: []string{"tenant-b"}, expLimit: 4 * time.Hour},
		{tenantIDs: []string{"tenant-c"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: time.Hour},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a", "tenant-b", "tenant-c"}, expLimit: time.Hour},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveNonZeroDurationPerTenant(tc.tenantIDs, ov.MaxPartialQueryLength))
	}
}

func TestMinDurationPerTenant(t *testing.T) {
	defaults := Limits{ResultsCacheTTLForCardinalityQuery: 0}
	tenantLimits := map[string]*Limits{
		"tenant-a": {ResultsCacheTTLForCardinalityQuery: 0},
		"tenant-b": {ResultsCacheTTLForCardinalityQuery: model.Duration(time.Minute)},
		"tenant-c": {ResultsCacheTTLForCardinalityQuery: model.Duration(time.Hour)},
	}

	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  time.Duration
	}{
		{tenantIDs: []string{}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-b"}, expLimit: time.Minute},
		{tenantIDs: []string{"tenant-c"}, expLimit: time.Hour},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-c", "tenant-b"}, expLimit: time.Minute},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-c", "tenant-b", "tenant-a"}, expLimit: time.Duration(0)},
	} {
		assert.Equal(t, tc.expLimit, MinDurationPerTenant(tc.tenantIDs, ov.ResultsCacheTTLForCardinalityQuery))
	}
}

func TestMaxTotalQueryLengthWithoutDefault(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxTotalQueryLength: model.Duration(4 * time.Hour),
		},
	}
	defaults := Limits{}

	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  time.Duration
	}{
		{tenantIDs: []string{}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a"}, expLimit: 4 * time.Hour},
		{tenantIDs: []string{"tenant-b"}, expLimit: time.Duration(0)},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveNonZeroDurationPerTenant(tc.tenantIDs, ov.MaxTotalQueryLength))
	}
}

func TestMaxTotalQueryLengthWithDefault(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxTotalQueryLength: model.Duration(4 * time.Hour),
		},
	}
	defaults := Limits{
		MaxTotalQueryLength: model.Duration(3 * time.Hour),
	}

	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  time.Duration
	}{
		{tenantIDs: []string{}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a"}, expLimit: 4 * time.Hour},
		{tenantIDs: []string{"tenant-b"}, expLimit: 3 * time.Hour},
	} {
		assert.Equal(t, tc.expLimit, SmallestPositiveNonZeroDurationPerTenant(tc.tenantIDs, ov.MaxTotalQueryLength))
	}
}

func TestMaxPartialQueryLengthWithDefault(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxPartialQueryLength: model.Duration(1 * time.Hour),
		},
	}
	defaults := Limits{
		MaxPartialQueryLength: model.Duration(6 * time.Hour),
	}

	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	assert.Equal(t, 6*time.Hour, ov.MaxPartialQueryLength(""))
	assert.Equal(t, 1*time.Hour, ov.MaxPartialQueryLength("tenant-a"))
	assert.Equal(t, 6*time.Hour, ov.MaxPartialQueryLength("tenant-b"))
}

func TestMaxPartialQueryLengthWithoutDefault(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			MaxPartialQueryLength: model.Duration(3 * time.Hour),
		},
	}
	defaults := Limits{}

	ov, err := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))
	require.NoError(t, err)

	assert.Equal(t, time.Duration(0), ov.MaxPartialQueryLength(""))
	assert.Equal(t, 3*time.Hour, ov.MaxPartialQueryLength("tenant-a"))
	assert.Equal(t, time.Duration(0), ov.MaxPartialQueryLength("tenant-b"))
}

func TestAlertmanagerNotificationLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		inputYAML         string
		expectedRateLimit rate.Limit
		expectedBurstSize int
	}{
		"no email specific limit": {
			inputYAML: `
alertmanager_notification_rate_limit: 100
`,
			expectedRateLimit: 100,
			expectedBurstSize: 100,
		},
		"zero limit": {
			inputYAML: `
alertmanager_notification_rate_limit: 100

alertmanager_notification_rate_limit_per_integration:
  email: 0
`,
			expectedRateLimit: rate.Inf,
			expectedBurstSize: maxInt,
		},

		"negative limit": {
			inputYAML: `
alertmanager_notification_rate_limit_per_integration:
  email: -10
`,
			expectedRateLimit: 0,
			expectedBurstSize: 0,
		},

		"positive limit, negative burst": {
			inputYAML: `
alertmanager_notification_rate_limit_per_integration:
  email: 222
`,
			expectedRateLimit: 222,
			expectedBurstSize: 222,
		},

		"infinte limit": {
			inputYAML: `
alertmanager_notification_rate_limit_per_integration:
  email: .inf
`,
			expectedRateLimit: rate.Inf,
			expectedBurstSize: maxInt,
		},
	} {
		t.Run(name, func(t *testing.T) {
			limitsYAML := Limits{}
			err := yaml.Unmarshal([]byte(tc.inputYAML), &limitsYAML)
			require.NoError(t, err, "expected to be able to unmarshal from YAML")

			ov, err := NewOverrides(limitsYAML, nil)
			require.NoError(t, err)

			require.Equal(t, tc.expectedRateLimit, ov.NotificationRateLimit("user", "email"))
			require.Equal(t, tc.expectedBurstSize, ov.NotificationBurstSize("user", "email"))
		})
	}
}

func TestAlertmanagerNotificationLimitsOverrides(t *testing.T) {
	baseYaml := `
alertmanager_notification_rate_limit: 5

alertmanager_notification_rate_limit_per_integration:
 email: 100
`

	overrideGenericLimitsOnly := `
testuser:
  alertmanager_notification_rate_limit: 333
`

	overrideEmailLimits := `
testuser:
  alertmanager_notification_rate_limit_per_integration:
    email: 7777
`

	overrideGenericLimitsAndEmailLimits := `
testuser:
  alertmanager_notification_rate_limit: 333

  alertmanager_notification_rate_limit_per_integration:
    email: 7777
`

	differentUserOverride := `
differentuser:
  alertmanager_notification_rate_limit_per_integration:
    email: 500
`

	for name, tc := range map[string]struct {
		testedIntegration string
		overrides         string
		expectedRateLimit rate.Limit
		expectedBurstSize int
	}{
		"no overrides, pushover": {
			testedIntegration: "pushover",
			expectedRateLimit: 5,
			expectedBurstSize: 5,
		},

		"no overrides, email": {
			testedIntegration: "email",
			expectedRateLimit: 100,
			expectedBurstSize: 100,
		},

		"generic override, pushover": {
			testedIntegration: "pushover",
			overrides:         overrideGenericLimitsOnly,
			expectedRateLimit: 333,
			expectedBurstSize: 333,
		},

		"generic override, email": {
			testedIntegration: "email",
			overrides:         overrideGenericLimitsOnly,
			expectedRateLimit: 100, // there is email-specific override in default config.
			expectedBurstSize: 100,
		},

		"email limit override, pushover": {
			testedIntegration: "pushover",
			overrides:         overrideEmailLimits,
			expectedRateLimit: 5, // loaded from defaults when parsing YAML
			expectedBurstSize: 5,
		},

		"email limit override, email": {
			testedIntegration: "email",
			overrides:         overrideEmailLimits,
			expectedRateLimit: 7777,
			expectedBurstSize: 7777,
		},

		"generic and email limit override, pushover": {
			testedIntegration: "pushover",
			overrides:         overrideGenericLimitsAndEmailLimits,
			expectedRateLimit: 333,
			expectedBurstSize: 333,
		},

		"generic and email limit override, email": {
			testedIntegration: "email",
			overrides:         overrideGenericLimitsAndEmailLimits,
			expectedRateLimit: 7777,
			expectedBurstSize: 7777,
		},

		"partial email limit override": {
			testedIntegration: "email",
			overrides: `
testuser:
  alertmanager_notification_rate_limit_per_integration:
    email: 500
`,
			expectedRateLimit: 500, // overridden
			expectedBurstSize: 500, // same as rate limit
		},

		"different user override, pushover": {
			testedIntegration: "pushover",
			overrides:         differentUserOverride,
			expectedRateLimit: 5,
			expectedBurstSize: 5,
		},

		"different user override, email": {
			testedIntegration: "email",
			overrides:         differentUserOverride,
			expectedRateLimit: 100,
			expectedBurstSize: 100,
		},
	} {
		t.Run(name, func(t *testing.T) {
			// Reset the default limits at the end of the test.
			t.Cleanup(func() {
				SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())
			})

			SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())

			var limitsYAML Limits
			err := yaml.Unmarshal([]byte(baseYaml), &limitsYAML)
			require.NoError(t, err, "expected to be able to unmarshal from YAML")

			SetDefaultLimitsForYAMLUnmarshalling(limitsYAML)

			overrides := map[string]*Limits{}
			err = yaml.Unmarshal([]byte(tc.overrides), &overrides)
			require.NoError(t, err, "parsing overrides")

			tl := NewMockTenantLimits(overrides)

			ov, err := NewOverrides(limitsYAML, tl)
			require.NoError(t, err)

			require.Equal(t, tc.expectedRateLimit, ov.NotificationRateLimit("testuser", tc.testedIntegration))
			require.Equal(t, tc.expectedBurstSize, ov.NotificationBurstSize("testuser", tc.testedIntegration))
		})
	}
}

func TestRulerMaxRulesPerRuleGroupLimits(t *testing.T) {
	tc := map[string]struct {
		inputYAML         string
		expectedLimit     int
		expectedNamespace string
	}{
		"no namespace specific limit": {
			inputYAML: `
ruler_max_rules_per_rule_group: 100
`,
			expectedLimit:     100,
			expectedNamespace: "mynamespace",
		},
		"zero limit for the right namespace": {
			inputYAML: `
ruler_max_rules_per_rule_group: 100

ruler_max_rules_per_rule_group_by_namespace:
  mynamespace: 0
`,
			expectedLimit:     0,
			expectedNamespace: "mynamespace",
		},
		"other namespaces are not affected": {
			inputYAML: `
ruler_max_rules_per_rule_group: 100

ruler_max_rules_per_rule_group_by_namespace:
  mynamespace: 10
`,
			expectedLimit:     100,
			expectedNamespace: "othernamespace",
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			limitsYAML := Limits{}
			require.NoError(t, yaml.Unmarshal([]byte(tt.inputYAML), &limitsYAML))

			ov, err := NewOverrides(limitsYAML, nil)
			require.NoError(t, err)

			require.Equal(t, tt.expectedLimit, ov.RulerMaxRulesPerRuleGroup("user", tt.expectedNamespace))
		})
	}
}

func TestRulerMaxRulesPerRuleGroupLimitsOverrides(t *testing.T) {
	baseYaml := `
ruler_max_rules_per_rule_group: 5

ruler_max_rules_per_rule_group_by_namespace:
  mynamespace: 10
`

	overrideGenericLimitsOnly := `
testuser:
  ruler_max_rules_per_rule_group: 333
`

	overrideNamespaceLimits := `
testuser:
  ruler_max_rules_per_rule_group_by_namespace:
    mynamespace: 7777
`

	overrideGenericLimitsAndNamespaceLimits := `
testuser:
  ruler_max_rules_per_rule_group: 333

  ruler_max_rules_per_rule_group_by_namespace:
    mynamespace: 7777
`

	differentUserOverride := `
differentuser:
  ruler_max_rules_per_rule_group_by_namespace:
    mynamespace: 500
`

	tc := map[string]struct {
		overrides      string
		inputNamespace string
		expectedLimit  int
	}{
		"no overrides, mynamespace": {
			inputNamespace: "mynamespace",
			expectedLimit:  10,
		},
		"no overrides, othernamespace": {
			inputNamespace: "othernamespace",
			expectedLimit:  5,
		},
		"generic override, mynamespace": {
			inputNamespace: "mynamespace",
			overrides:      overrideGenericLimitsOnly,
			expectedLimit:  10,
		},
		"generic override, othernamespace": {
			inputNamespace: "othernamespace",
			overrides:      overrideGenericLimitsOnly,
			expectedLimit:  333,
		},
		"namespace limit override, mynamespace": {
			inputNamespace: "mynamespace",
			overrides:      overrideNamespaceLimits,
			expectedLimit:  7777,
		},
		"namespace limit override, othernamespace": {
			inputNamespace: "othernamespace",
			overrides:      overrideNamespaceLimits,
			expectedLimit:  5,
		},
		"generic and namespace limit override, mynamespace": {
			inputNamespace: "mynamespace",
			overrides:      overrideGenericLimitsAndNamespaceLimits,
			expectedLimit:  7777,
		},
		"generic and namespace limit override, othernamespace": {
			inputNamespace: "othernamespace",
			overrides:      overrideGenericLimitsAndNamespaceLimits,
			expectedLimit:  333,
		},
		"different user override, mynamespace": {
			inputNamespace: "mynamespace",
			overrides:      differentUserOverride,
			expectedLimit:  10,
		},
		"different user override, othernamespace": {
			inputNamespace: "othernamespace",
			overrides:      differentUserOverride,
			expectedLimit:  5,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() {
				SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())
			})

			SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())

			var limitsYAML Limits
			err := yaml.Unmarshal([]byte(baseYaml), &limitsYAML)
			require.NoError(t, err)

			SetDefaultLimitsForYAMLUnmarshalling(limitsYAML)

			overrides := map[string]*Limits{}
			err = yaml.Unmarshal([]byte(tt.overrides), &overrides)
			require.NoError(t, err)

			tl := NewMockTenantLimits(overrides)
			ov, err := NewOverrides(limitsYAML, tl)
			require.NoError(t, err)

			require.Equal(t, tt.expectedLimit, ov.RulerMaxRulesPerRuleGroup("testuser", tt.inputNamespace))
		})
	}
}

func TestRulerMaxRuleGroupsPerTenantLimits(t *testing.T) {
	tc := map[string]struct {
		inputYAML         string
		expectedLimit     int
		expectedNamespace string
	}{
		"no namespace specific limit": {
			inputYAML: `
ruler_max_rule_groups_per_tenant: 200
`,
			expectedLimit:     200,
			expectedNamespace: "mynamespace",
		},
		"zero limit for the right namespace": {
			inputYAML: `
ruler_max_rule_groups_per_tenant: 200

ruler_max_rule_groups_per_tenant_by_namespace:
  mynamespace: 1
`,
			expectedLimit:     1,
			expectedNamespace: "mynamespace",
		},
		"other namespaces are not affected": {
			inputYAML: `
ruler_max_rule_groups_per_tenant: 200

ruler_max_rule_groups_per_tenant_by_namespace:
  mynamespace: 20
`,
			expectedLimit:     200,
			expectedNamespace: "othernamespace",
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			limitsYAML := Limits{}
			require.NoError(t, yaml.Unmarshal([]byte(tt.inputYAML), &limitsYAML))

			ov, err := NewOverrides(limitsYAML, nil)
			require.NoError(t, err)

			require.Equal(t, tt.expectedLimit, ov.RulerMaxRuleGroupsPerTenant("user", tt.expectedNamespace))
		})
	}
}

func TestRulerMaxRuleGroupsPerTenantLimitsOverrides(t *testing.T) {
	baseYaml := `
ruler_max_rule_groups_per_tenant: 20

ruler_max_rule_groups_per_tenant_by_namespace:
  mynamespace: 20
`

	overrideGenericLimitsOnly := `
testuser:
  ruler_max_rule_groups_per_tenant: 444
`

	overrideNamespaceLimits := `
testuser:
  ruler_max_rule_groups_per_tenant_by_namespace:
    mynamespace: 8888
`

	overrideGenericLimitsAndNamespaceLimits := `
testuser:
  ruler_max_rule_groups_per_tenant: 444

  ruler_max_rule_groups_per_tenant_by_namespace:
    mynamespace: 8888
`

	differentUserOverride := `
differentuser:
  ruler_max_rule_groups_per_tenant_by_namespace:
    mynamespace: 600
`

	tc := map[string]struct {
		overrides      string
		inputNamespace string
		expectedLimit  int
	}{
		"no overrides, mynamespace": {
			inputNamespace: "mynamespace",
			expectedLimit:  20,
		},
		"no overrides, othernamespace": {
			inputNamespace: "othernamespace",
			expectedLimit:  20,
		},
		"generic override, mynamespace": {
			inputNamespace: "mynamespace",
			overrides:      overrideGenericLimitsOnly,
			expectedLimit:  20,
		},
		"generic override, othernamespace": {
			inputNamespace: "othernamespace",
			overrides:      overrideGenericLimitsOnly,
			expectedLimit:  444,
		},
		"namespace limit override, mynamespace": {
			inputNamespace: "mynamespace",
			overrides:      overrideNamespaceLimits,
			expectedLimit:  8888,
		},
		"namespace limit override, othernamespace": {
			inputNamespace: "othernamespace",
			overrides:      overrideNamespaceLimits,
			expectedLimit:  20,
		},
		"generic and namespace limit override, mynamespace": {
			inputNamespace: "mynamespace",
			overrides:      overrideGenericLimitsAndNamespaceLimits,
			expectedLimit:  8888,
		},
		"generic and namespace limit override, othernamespace": {
			inputNamespace: "othernamespace",
			overrides:      overrideGenericLimitsAndNamespaceLimits,
			expectedLimit:  444,
		},
		"different user override, mynamespace": {
			inputNamespace: "mynamespace",
			overrides:      differentUserOverride,
			expectedLimit:  20,
		},
		"different user override, othernamespace": {
			inputNamespace: "othernamespace",
			overrides:      differentUserOverride,
			expectedLimit:  20,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() {
				SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())
			})

			SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())

			var limitsYAML Limits
			err := yaml.Unmarshal([]byte(baseYaml), &limitsYAML)
			require.NoError(t, err)

			SetDefaultLimitsForYAMLUnmarshalling(limitsYAML)

			overrides := map[string]*Limits{}
			err = yaml.Unmarshal([]byte(tt.overrides), &overrides)
			require.NoError(t, err)

			tl := NewMockTenantLimits(overrides)
			ov, err := NewOverrides(limitsYAML, tl)
			require.NoError(t, err)

			require.Equal(t, tt.expectedLimit, ov.RulerMaxRuleGroupsPerTenant("testuser", tt.inputNamespace))
		})
	}
}

func TestRulerProtectedNamespacesOverrides(t *testing.T) {
	tc := map[string]struct {
		inputYAML          string
		overrides          string
		expectedNamespaces []string
	}{
		"no user specific protected namespaces": {
			inputYAML: `
ruler_protected_namespaces: "ns1,ns2"
`,
			expectedNamespaces: []string{"ns1", "ns2"},
		},
		"default limit for not specific user": {
			inputYAML: `
ruler_protected_namespaces: "ns1,ns2"
`,
			overrides: `
randomuser:
  ruler_protected_namespaces: "ns3"
`,
			expectedNamespaces: []string{"ns1", "ns2"},
		},
		"overridden limit for specific user": {
			inputYAML: `
ruler_protected_namespaces: "ns1,ns2"
`,
			overrides: `
user1:
  ruler_protected_namespaces: "ns3"
`,
			expectedNamespaces: []string{"ns3"},
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			var LimitsYAML Limits
			err := yaml.Unmarshal([]byte(tt.inputYAML), &LimitsYAML)
			require.NoError(t, err)

			SetDefaultLimitsForYAMLUnmarshalling(LimitsYAML)

			overrides := map[string]*Limits{}
			err = yaml.Unmarshal([]byte(tt.overrides), &overrides)
			require.NoError(t, err)

			tl := NewMockTenantLimits(overrides)
			ov, err := NewOverrides(LimitsYAML, tl)
			require.NoError(t, err)

			require.Equal(t, tt.expectedNamespaces, ov.RulerProtectedNamespaces("user1"))
		})
	}
}

func TestRulerMaxConcurrentRuleEvaluationsPerTenantOverrides(t *testing.T) {
	tc := map[string]struct {
		inputYAML                    string
		overrides                    string
		expectedPerTenantConcurrency int64
	}{
		"no user specific concurrency": {
			inputYAML: `
ruler_max_independent_rule_evaluation_concurrency_per_tenant: 5
`,
			expectedPerTenantConcurrency: 5,
		},
		"default limit for not specific user": {
			inputYAML: `
ruler_max_independent_rule_evaluation_concurrency_per_tenant: 5
`,
			overrides: `
randomuser:
  ruler_max_independent_rule_evaluation_concurrency_per_tenant: 10
`,
			expectedPerTenantConcurrency: 5,
		},
		"overridden limit for specific user": {
			inputYAML: `
ruler_max_independent_rule_evaluation_concurrency_per_tenant: 5
`,
			overrides: `
user1:
  ruler_max_independent_rule_evaluation_concurrency_per_tenant: 15
`,
			expectedPerTenantConcurrency: 15,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			var LimitsYAML Limits
			err := yaml.Unmarshal([]byte(tt.inputYAML), &LimitsYAML)
			require.NoError(t, err)

			SetDefaultLimitsForYAMLUnmarshalling(LimitsYAML)

			overrides := map[string]*Limits{}
			err = yaml.Unmarshal([]byte(tt.overrides), &overrides)
			require.NoError(t, err)

			tl := NewMockTenantLimits(overrides)
			ov, err := NewOverrides(LimitsYAML, tl)
			require.NoError(t, err)

			require.Equal(t, tt.expectedPerTenantConcurrency, ov.RulerMaxIndependentRuleEvaluationConcurrencyPerTenant("user1"))
		})
	}
}

func TestCustomTrackerConfigDeserialize(t *testing.T) {
	expectedConfig, err := asmodel.NewCustomTrackersConfig(map[string]string{"baz": `{foo="bar"}`})
	require.NoError(t, err, "creating expected config")
	cfg := `
    user:
        active_series_custom_trackers:
            baz: '{foo="bar"}'
    `

	overrides := map[string]*Limits{}
	require.NoError(t, yaml.Unmarshal([]byte(cfg), &overrides), "parsing overrides")

	assert.False(t, overrides["user"].ActiveSeriesCustomTrackersConfig.Empty())
	assert.Equal(t, expectedConfig.String(), overrides["user"].ActiveSeriesCustomTrackersConfig.String())
}

func TestUnmarshalYAML_ShouldValidateConfig(t *testing.T) {
	tests := map[string]struct {
		cfg         string
		expectedErr string
	}{
		"should fail on invalid metric_relabel_configs": {
			cfg: `
metric_relabel_configs:
  -
`,
			expectedErr: "invalid metric_relabel_configs",
		},
		"should fail on negative max_estimated_fetched_chunks_per_query_multiplier": {
			cfg:         `max_estimated_fetched_chunks_per_query_multiplier: -0.1`,
			expectedErr: errInvalidMaxEstimatedChunksPerQueryMultiplier.Error(),
		},
		"should pass on max_estimated_fetched_chunks_per_query_multiplier = 0": {
			cfg:         `max_estimated_fetched_chunks_per_query_multiplier: 0`,
			expectedErr: "",
		},
		"should fail on max_estimated_fetched_chunks_per_query_multiplier greater than 0 but less than 1": {
			cfg:         `max_estimated_fetched_chunks_per_query_multiplier: 0.9`,
			expectedErr: errInvalidMaxEstimatedChunksPerQueryMultiplier.Error(),
		},
		"should pass on max_estimated_fetched_chunks_per_query_multiplier = 1": {
			cfg:         `max_estimated_fetched_chunks_per_query_multiplier: 1`,
			expectedErr: "",
		},
		"should pass on max_estimated_fetched_chunks_per_query_multiplier greater than 1": {
			cfg:         `max_estimated_fetched_chunks_per_query_multiplier: 1.1`,
			expectedErr: "",
		},
		"should fail on invalid ingest_storage_read_consistency": {
			cfg:         `ingest_storage_read_consistency: xyz`,
			expectedErr: errInvalidIngestStorageReadConsistency.Error(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := getDefaultLimits()
			err := yaml.Unmarshal([]byte(testData.cfg), &limits)

			if testData.expectedErr != "" {
				require.ErrorContains(t, err, testData.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUnmarshalJSON_ShouldValidateConfig(t *testing.T) {
	tests := map[string]struct {
		cfg         string
		expectedErr string
	}{
		"should fail on invalid metric_relabel_configs": {
			cfg:         `{"metric_relabel_configs": [null]}`,
			expectedErr: "invalid metric_relabel_configs",
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			limits := getDefaultLimits()
			err := json.Unmarshal([]byte(testData.cfg), &limits)

			if testData.expectedErr != "" {
				require.ErrorContains(t, err, testData.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type structExtension struct {
	Foo int `yaml:"foo" json:"foo"`
}

func (te structExtension) Default() structExtension {
	return structExtension{Foo: 42}
}

type stringExtension string

func (stringExtension) Default() stringExtension {
	return "default string extension value"
}

func TestExtensions(t *testing.T) {
	t.Cleanup(func() {
		registeredExtensions = map[string]registeredExtension{}
		limitsExtensionsFields = nil
	})

	getExtensionStruct := MustRegisterExtension[structExtension]("test_extension_struct")
	getExtensionString := MustRegisterExtension[stringExtension]("test_extension_string")

	// Unmarshal a config with extensions.
	// JSON is a valid YAML, so we can use it here to avoid having to fight the whitespaces.

	cfg := `{"user": {"test_extension_struct": {"foo": 1}, "test_extension_string": "bar"}}`
	t.Run("yaml", func(t *testing.T) {
		overrides := map[string]*Limits{}
		require.NoError(t, yaml.Unmarshal([]byte(cfg), &overrides), "parsing overrides")

		// Check that getExtensionStruct(*Limits) actually returns the proper type with filled extensions.
		assert.Equal(t, structExtension{Foo: 1}, getExtensionStruct(overrides["user"]))
		assert.Equal(t, stringExtension("bar"), getExtensionString(overrides["user"]))
	})

	t.Run("json", func(t *testing.T) {
		overrides := map[string]*Limits{}
		require.NoError(t, json.Unmarshal([]byte(cfg), &overrides), "parsing overrides")

		// Check that getExtensionStruct(*Limits) actually returns the proper type with filled extensions.
		assert.Equal(t, structExtension{Foo: 1}, getExtensionStruct(overrides["user"]))
		assert.Equal(t, stringExtension("bar"), getExtensionString(overrides["user"]))
	})

	t.Run("can't register twice", func(t *testing.T) {
		require.Panics(t, func() {
			MustRegisterExtension[structExtension]("foo")
			MustRegisterExtension[structExtension]("foo")
		})
	})

	t.Run("can't register name that is already a Limits JSON/YAML key", func(t *testing.T) {
		require.Panics(t, func() {
			MustRegisterExtension[stringExtension]("max_global_series_per_user")
		})
	})

	t.Run("can't register empty name", func(t *testing.T) {
		require.Panics(t, func() {
			MustRegisterExtension[stringExtension]("")
		})
	})

	t.Run("default value", func(t *testing.T) {
		var limits Limits
		require.NoError(t, json.Unmarshal([]byte(`{}`), &limits), "parsing overrides")
		require.Equal(t, structExtension{Foo: 42}, getExtensionStruct(&limits))
		require.Equal(t, stringExtension("default string extension value"), getExtensionString(&limits))
	})

	t.Run("default value after registering extension defaults", func(t *testing.T) {
		var limits Limits

		limits.RegisterExtensionsDefaults()

		require.Equal(t, structExtension{Foo: 42}, getExtensionStruct(&limits))
		require.Equal(t, stringExtension("default string extension value"), getExtensionString(&limits))
	})

	t.Run("empty value from empty yaml", func(t *testing.T) {
		// Reset the default limits at the end of the test.
		t.Cleanup(func() {
			SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())
		})

		SetDefaultLimitsForYAMLUnmarshalling(Limits{
			RequestRate: 100,
		})
		var limits map[string]Limits

		require.NoError(t, yaml.Unmarshal([]byte(`foo:`), &limits), "parsing overrides")

		fooLimits, ok := limits["foo"]
		require.True(t, ok, "foo limits should be present")
		require.Equal(t, float64(0), fooLimits.RequestRate)

		require.Equal(t, structExtension{}, getExtensionStruct(&fooLimits))
		require.Equal(t, stringExtension(""), getExtensionString(&fooLimits))
	})

	t.Run("default limits does not interfere with tenants extensions", func(t *testing.T) {
		// This test makes sure that sharing the default limits does not leak extensions values between tenants.
		// Since we assign l = *defaultLimits before unmarshaling,
		// there's a chance of unmarshaling on top of a reference that is already being used in different tenant's limits.
		// This shouldn't happen, but let's have a test to make sure that it doesnt.
		def := getDefaultLimits()
		require.NoError(t, json.Unmarshal([]byte(`{"test_extension_string": "default"}`), &def), "parsing overrides")
		require.Equal(t, stringExtension("default"), getExtensionString(&def))

		// Reset the default limits at the end of the test.
		t.Cleanup(func() {
			SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())
		})

		SetDefaultLimitsForYAMLUnmarshalling(def)

		cfg := `{"one": {"test_extension_string": "one"}, "two": {"test_extension_string": "two"}}`
		overrides := map[string]*Limits{}
		require.NoError(t, yaml.Unmarshal([]byte(cfg), &overrides), "parsing overrides")
		require.Equal(t, stringExtension("one"), getExtensionString(overrides["one"]))
		require.Equal(t, stringExtension("two"), getExtensionString(overrides["two"]))

		cfg = `{"three": {"test_extension_string": "three"}}`
		overrides2 := map[string]*Limits{}
		require.NoError(t, yaml.Unmarshal([]byte(cfg), &overrides2), "parsing overrides")
		require.Equal(t, stringExtension("three"), getExtensionString(overrides2["three"]))

		// Previous values did not change.
		require.Equal(t, stringExtension("one"), getExtensionString(overrides["one"]))
		require.Equal(t, stringExtension("two"), getExtensionString(overrides["two"]))

		// Default value did not change.
		require.Equal(t, stringExtension("default"), getExtensionString(&def))
	})

	t.Run("getter works with nil Limits returning default values", func(t *testing.T) {
		require.Equal(t, structExtension{}.Default(), getExtensionStruct(nil))
	})
}

func TestExtensionMarshalling(t *testing.T) {
	t.Cleanup(func() {
		registeredExtensions = map[string]registeredExtension{}
		limitsExtensionsFields = nil
	})

	MustRegisterExtension[structExtension]("test_extension_struct")
	MustRegisterExtension[stringExtension]("test_extension_string")

	t.Run("marshal limits with no extension values", func(t *testing.T) {
		overrides := map[string]*Limits{
			"test": {},
		}

		val, err := yaml.Marshal(overrides)
		require.NoError(t, err)
		fmt.Println(string(val))
		require.Contains(t, string(val),
			`test:
    test_extension_struct:
        foo: 0
    test_extension_string: ""
    request_rate: 0`)

		val, err = json.Marshal(overrides)
		require.NoError(t, err)
		require.Contains(t, string(val), `{"test":{"test_extension_struct":{"foo":0},"test_extension_string":"","request_rate":0,`)
	})

	t.Run("marshal limits with partial extension values", func(t *testing.T) {
		overrides := map[string]*Limits{
			"test": {
				extensions: map[string]interface{}{
					"test_extension_struct": structExtension{Foo: 421237},
				},
			},
		}

		val, err := yaml.Marshal(overrides)
		require.NoError(t, err)
		require.Contains(t, string(val),
			`test:
    test_extension_struct:
        foo: 421237
    test_extension_string: ""
    request_rate: 0
    request_burst_size: 0`)

		val, err = json.Marshal(overrides)
		require.NoError(t, err)
		require.Contains(t, string(val), `{"test":{"test_extension_struct":{"foo":421237},"test_extension_string":"","request_rate":0,"request_burst_size":0,`)
	})

	t.Run("marshal limits with default extension values", func(t *testing.T) {
		overrides := map[string]*Limits{}
		require.NoError(t, yaml.Unmarshal([]byte(`{"user": {}}`), &overrides), "parsing overrides")

		val, err := yaml.Marshal(overrides)
		require.NoError(t, err)
		require.Contains(t, string(val),
			`user:
    test_extension_struct:
        foo: 42
    test_extension_string: default string extension value
    request_rate: 0
    request_burst_size: 0`)

		val, err = json.Marshal(overrides)
		require.NoError(t, err)
		require.Contains(t, string(val), `{"user":{"test_extension_struct":{"foo":42},"test_extension_string":"default string extension value","request_rate":0,"request_burst_size":0,`)
	})
}

func TestIsLimitError(t *testing.T) {
	const msg = "this is an error"
	testCases := map[string]struct {
		err             error
		expectedOutcome bool
	}{
		"a random error is not a LimitError": {
			err:             errors.New(msg),
			expectedOutcome: false,
		},
		"errors implementing LimitError interface are LimitErrors": {
			err:             NewLimitError(msg),
			expectedOutcome: true,
		},
		"wrapped LimitErrors are LimitErrors": {
			err:             errors.Wrap(NewLimitError(msg), "wrapped"),
			expectedOutcome: true,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, testData.expectedOutcome, IsLimitError(testData.err))
		})
	}
}

func TestIntStringYAMLAlertmanagerSizeLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		inputYAML          string
		expectedConfigSize int
		expectedStateSize  int
	}{
		"when using integers": {
			inputYAML: `
alertmanager_max_grafana_config_size_bytes: 4096000
alertmanager_max_grafana_state_size_bytes: 2048000
`,
			expectedConfigSize: 4096000,
			expectedStateSize:  2048000,
		},
		"when using strings": {
			inputYAML: `
alertmanager_max_grafana_config_size_bytes: "4096000"
alertmanager_max_grafana_state_size_bytes: "2048000"
`,
			expectedConfigSize: 4096000,
			expectedStateSize:  2048000,
		},
		"when nothing is given, defaults to 0": {
			inputYAML:          "",
			expectedConfigSize: 0,
			expectedStateSize:  0,
		},
	} {
		t.Run(name, func(t *testing.T) {
			limitsYAML := Limits{}
			err := yaml.Unmarshal([]byte(tc.inputYAML), &limitsYAML)
			require.NoError(t, err, "expected to be able to unmarshal from YAML")

			ov, err := NewOverrides(limitsYAML, nil)
			require.NoError(t, err)

			require.Equal(t, tc.expectedConfigSize, ov.AlertmanagerMaxGrafanaConfigSize("user"))
			require.Equal(t, tc.expectedStateSize, ov.AlertmanagerMaxGrafanaStateSize("user"))
		})
	}
}

func TestIntStringJSONAMLAlertmanagerSizeLimits(t *testing.T) {
	for name, tc := range map[string]struct {
		inputJSON          string
		expectedConfigSize int
		expectedStateSize  int
	}{
		"when using integers": {
			inputJSON: `{
"alertmanager_max_grafana_config_size_bytes": 4096000,
"alertmanager_max_grafana_state_size_bytes": 2048000
			}`,
			expectedConfigSize: 4096000,
			expectedStateSize:  2048000,
		},
		"when using strings": {
			inputJSON: `{
"alertmanager_max_grafana_config_size_bytes": "4096000",
"alertmanager_max_grafana_state_size_bytes": "2048000"
			}`,
			expectedConfigSize: 4096000,
			expectedStateSize:  2048000,
		},
		"when nothing is given, defaults to 0": {
			inputJSON:          "{}",
			expectedConfigSize: 0,
			expectedStateSize:  0,
		},
	} {
		t.Run(name, func(t *testing.T) {
			limitsJSON := Limits{}
			err := json.Unmarshal([]byte(tc.inputJSON), &limitsJSON)
			require.NoError(t, err, "expected to be able to unmarshal from JSON")

			ov, err := NewOverrides(limitsJSON, nil)
			require.NoError(t, err)

			require.Equal(t, tc.expectedConfigSize, ov.AlertmanagerMaxGrafanaConfigSize("user"))
			require.Equal(t, tc.expectedStateSize, ov.AlertmanagerMaxGrafanaStateSize("user"))
		})
	}
}

func getDefaultLimits() Limits {
	limits := Limits{}
	flagext.DefaultValues(&limits)
	return limits
}
