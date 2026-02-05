// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/limits_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/common/model"
	"github.com/prometheus/otlptranslator"
	"github.com/prometheus/prometheus/model/relabel"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
	"gopkg.in/yaml.v3"

	"github.com/grafana/mimir/pkg/costattribution/costattributionmodel"
	asmodel "github.com/grafana/mimir/pkg/ingester/activeseries/model"
	"github.com/grafana/mimir/pkg/ruler/notifier"
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
	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

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
	testCases := []struct {
		name     string
		input    string
		testFunc func(t *testing.T, l Limits)
	}{
		{
			name:  "defaults",
			input: `{}`,
			testFunc: func(t *testing.T, l Limits) {
				assert.Equal(t, 1024, l.MaxLabelNameLength)
				assert.Equal(t, model.LegacyValidation, l.NameValidationScheme)
				assert.True(t, l.OTelLabelNameUnderscoreSanitization)
				assert.True(t, l.OTelLabelNamePreserveMultipleUnderscores)
			},
		},
		{
			name:  "ingestion_rate",
			input: `ingestion_rate: 0.5`,
			testFunc: func(t *testing.T, l Limits) {
				assert.Equal(t, 0.5, l.IngestionRate)
			},
		},
		{
			name:  "name_validation_scheme: legacy",
			input: `name_validation_scheme: "legacy"`,
			testFunc: func(t *testing.T, l Limits) {
				assert.Equal(t, model.LegacyValidation, l.NameValidationScheme)
			},
		},
		{
			name:  "name_validation_scheme: utf8",
			input: `name_validation_scheme: "utf8"`,
			testFunc: func(t *testing.T, l Limits) {
				assert.Equal(t, model.UTF8Validation, l.NameValidationScheme)
			},
		},
		{
			name:  "otel_label_name_underscore_sanitization: true",
			input: `otel_label_name_underscore_sanitization: true`,
			testFunc: func(t *testing.T, l Limits) {
				assert.True(t, l.OTelLabelNameUnderscoreSanitization)
			},
		},
		{
			name:  "otel_label_name_preserve_multiple_underscores: true",
			input: `otel_label_name_preserve_multiple_underscores: true`,
			testFunc: func(t *testing.T, l Limits) {
				assert.True(t, l.OTelLabelNamePreserveMultipleUnderscores)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := Limits{}
			dec := yaml.NewDecoder(strings.NewReader(tc.input))
			dec.KnownFields(true)
			require.NoError(t, dec.Decode(&l))
			tc.testFunc(t, l)
		})
	}
}

func TestLimitsLoadingFromJson(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		testFunc func(t *testing.T, l Limits)
	}{
		{
			name:  "defaults",
			input: `{}`,
			testFunc: func(t *testing.T, l Limits) {
				assert.Equal(t, 1024, l.MaxLabelNameLength)
				assert.Equal(t, model.LegacyValidation, l.NameValidationScheme)
				assert.True(t, l.OTelLabelNameUnderscoreSanitization)
				assert.True(t, l.OTelLabelNamePreserveMultipleUnderscores)
			},
		},
		{
			name:  "ingestion_rate",
			input: `{"ingestion_rate": 0.5}`,
			testFunc: func(t *testing.T, l Limits) {
				assert.Equal(t, 0.5, l.IngestionRate)
			},
		},
		{
			name:  "name_validation_scheme: utf8",
			input: `{"name_validation_scheme": "utf8"}`,
			testFunc: func(t *testing.T, l Limits) {
				assert.Equal(t, model.UTF8Validation, l.NameValidationScheme)
			},
		},
		{
			name:  "otel_label_name_underscore_sanitization: true",
			input: `{"otel_label_name_underscore_sanitization": true}`,
			testFunc: func(t *testing.T, l Limits) {
				assert.True(t, l.OTelLabelNameUnderscoreSanitization)
			},
		},
		{
			name:  "otel_label_name_preserve_multiple_underscores: true",
			input: `{"otel_label_name_preserve_multiple_underscores": true}`,
			testFunc: func(t *testing.T, l Limits) {
				assert.True(t, l.OTelLabelNamePreserveMultipleUnderscores)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			l := Limits{}
			err := json.Unmarshal([]byte(tc.input), &l)
			require.NoError(t, err)
			tc.testFunc(t, l)
		})
	}

	// Unmarshal should fail if input contains unknown struct fields and
	// the decoder flag `json.Decoder.DisallowUnknownFields()` is set
	inp := `{"unknown_fields": 100}`
	l := Limits{}
	dec := json.NewDecoder(strings.NewReader(inp))
	dec.DisallowUnknownFields()
	assert.Error(t, dec.Decode(&l))
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

	// Excluding activeSeriesMergedCustomTrackersConfig because it's not comparable, but we
	// don't care about it in this test (it's not exported to JSON or YAML).
	assert.True(t, cmp.Equal(limitsYAML, limitsJSON, cmp.AllowUnexported(Limits{}), cmpopts.IgnoreFields(Limits{}, "activeSeriesMergedCustomTrackersConfig")), "expected YAML and JSON to match")
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
	exp.NameValidationScheme = model.LegacyValidation

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
	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

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
	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

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
	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

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

func TestLargestPositiveNonZeroDurationPerTenant(t *testing.T) {
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
	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

	for _, tc := range []struct {
		tenantIDs []string
		expLimit  time.Duration
	}{
		{tenantIDs: []string{}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a"}, expLimit: time.Hour},
		{tenantIDs: []string{"tenant-b"}, expLimit: 4 * time.Hour},
		{tenantIDs: []string{"tenant-c"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a", "tenant-b"}, expLimit: 4 * time.Hour},
		{tenantIDs: []string{"tenant-c", "tenant-d", "tenant-e"}, expLimit: time.Duration(0)},
		{tenantIDs: []string{"tenant-a", "tenant-b", "tenant-c"}, expLimit: 4 * time.Hour},
	} {
		assert.Equal(t, tc.expLimit, LargestPositiveNonZeroDurationPerTenant(tc.tenantIDs, ov.MaxPartialQueryLength))
	}
}

func TestMinDurationPerTenant(t *testing.T) {
	defaults := Limits{ResultsCacheTTLForCardinalityQuery: 0}
	tenantLimits := map[string]*Limits{
		"tenant-a": {ResultsCacheTTLForCardinalityQuery: 0},
		"tenant-b": {ResultsCacheTTLForCardinalityQuery: model.Duration(time.Minute)},
		"tenant-c": {ResultsCacheTTLForCardinalityQuery: model.Duration(time.Hour)},
	}

	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

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

	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

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

	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

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

	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

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

	ov := NewOverrides(defaults, NewMockTenantLimits(tenantLimits))

	assert.Equal(t, time.Duration(0), ov.MaxPartialQueryLength(""))
	assert.Equal(t, 3*time.Hour, ov.MaxPartialQueryLength("tenant-a"))
	assert.Equal(t, time.Duration(0), ov.MaxPartialQueryLength("tenant-b"))
}

func TestDistributorIngestionArtificialDelay(t *testing.T) {
	tests := map[string]struct {
		tenantID      string
		tenantLimits  func(*Limits)
		expectedDelay time.Duration
	}{
		"should not apply delay by default": {
			tenantID:      "tenant-a",
			tenantLimits:  func(*Limits) {},
			expectedDelay: 0,
		},
		"should apply delay if a plain delay has been configured for the tenant": {
			tenantID: "tenant-a",
			tenantLimits: func(l *Limits) {
				l.IngestionArtificialDelay = model.Duration(time.Second)
			},
			expectedDelay: time.Second,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			tenantLimits := &Limits{}
			flagext.DefaultValues(tenantLimits)
			testData.tenantLimits(tenantLimits)

			ov := NewOverrides(Limits{}, NewMockTenantLimits(map[string]*Limits{testData.tenantID: tenantLimits}))
			require.Equal(t, testData.expectedDelay, ov.DistributorIngestionArtificialDelay(testData.tenantID))
		})
	}
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

			ov := NewOverrides(limitsYAML, nil)

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

			ov := NewOverrides(limitsYAML, tl)

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

			ov := NewOverrides(limitsYAML, nil)

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
			ov := NewOverrides(limitsYAML, tl)

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

			ov := NewOverrides(limitsYAML, nil)

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
			ov := NewOverrides(limitsYAML, tl)

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
			ov := NewOverrides(LimitsYAML, tl)

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
			ov := NewOverrides(LimitsYAML, tl)

			require.Equal(t, tt.expectedPerTenantConcurrency, ov.RulerMaxIndependentRuleEvaluationConcurrencyPerTenant("user1"))
		})
	}
}

func TestRulerMinRuleEvaluationIntervalPerTenantOverrides(t *testing.T) {
	tc := map[string]struct {
		inputYAML                         string
		overrides                         string
		expectedMinRuleEvaluationInterval time.Duration
	}{
		"no user specific minimum": {
			inputYAML: `
ruler_min_rule_evaluation_interval: 15s
`,
			expectedMinRuleEvaluationInterval: 15 * time.Second,
		},
		"default limit if user not specified": {
			inputYAML: `
ruler_min_rule_evaluation_interval: 5s
`,
			overrides: `
randomuser:
  ruler_min_rule_evaluation_interval: 5m
`,
			expectedMinRuleEvaluationInterval: 5 * time.Second,
		},
		"overridden limit for specific user": {
			inputYAML: `
ruler_min_rule_evaluation_interval: 5s
`,
			overrides: `
user1:
  ruler_min_rule_evaluation_interval: 10m
`,
			expectedMinRuleEvaluationInterval: 10 * time.Minute,
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
			ov := NewOverrides(LimitsYAML, tl)

			require.Equal(t, tt.expectedMinRuleEvaluationInterval, ov.RulerMinRuleEvaluationInterval("user1"))
		})
	}
}

func TestRulerMaxRuleEvaluationResults(t *testing.T) {
	tc := map[string]struct {
		inputYAML     string
		overrides     string
		expectedLimit int
	}{
		"default limit": {
			inputYAML: `
ruler_max_rule_evaluation_results: 100
`,
			expectedLimit: 100,
		},
		"zero disables limit": {
			inputYAML: `
ruler_max_rule_evaluation_results: 0
`,
			expectedLimit: 0,
		},
		"user specific limit overrides default": {
			inputYAML: `
ruler_max_rule_evaluation_results: 100
`,
			overrides: `
user1:
  ruler_max_rule_evaluation_results: 500
`,
			expectedLimit: 500,
		},
		"zero user limit overrides default": {
			inputYAML: `
ruler_max_rule_evaluation_results: 100
`,
			overrides: `
user1:
  ruler_max_rule_evaluation_results: 0
`,
			expectedLimit: 0,
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			LimitsYAML := Limits{}
			err := yaml.Unmarshal([]byte(tt.inputYAML), &LimitsYAML)
			require.NoError(t, err)

			var overrides map[string]*Limits
			if tt.overrides != "" {
				err = yaml.Unmarshal([]byte(tt.overrides), &overrides)
				require.NoError(t, err)
			}

			tl := NewMockTenantLimits(overrides)
			ov := NewOverrides(LimitsYAML, tl)

			require.Equal(t, tt.expectedLimit, ov.RulerMaxRuleEvaluationResults("user1"))
		})
	}
}

func TestRulerAlertmanagerClientConfig(t *testing.T) {
	tc := map[string]struct {
		baseYAML       string
		overrides      string
		expectedConfig notifier.AlertmanagerClientConfig
	}{
		"no override provided": {
			baseYAML:       ``,
			expectedConfig: notifier.DefaultAlertmanagerClientConfig,
		},
		"no user specific client config": {
			baseYAML: `
ruler_alertmanager_client_config:
  alertmanager_url: http://custom-url:8080
  proxy_url: http://some-proxy:1234
  oauth2:
    client_id: myclient
    client_secret: mysecret
    token_url: http://token-url
    scopes: abc,def
    endpoint_params:
      key1: value1
`,
			expectedConfig: notifier.AlertmanagerClientConfig{
				AlertmanagerURL: "http://custom-url:8080",
				NotifierConfig: notifier.Config{
					ProxyURL:   "http://some-proxy:1234",
					TLSEnabled: true,
					OAuth2: notifier.OAuth2Config{
						ClientID:     "myclient",
						ClientSecret: flagext.SecretWithValue("mysecret"),
						TokenURL:     "http://token-url",
						Scopes:       []string{"abc", "def"},
						EndpointParams: flagext.NewLimitsMapWithData(map[string]string{
							"key1": "value1",
						}, nil),
					},
				},
			},
		},
		"overridden config for specific user": {
			baseYAML: `
ruler_alertmanager_client_config:
  alertmanager_url: http://some-base-url:8080
`,
			overrides: `
user1:
  ruler_alertmanager_client_config:
    alertmanager_url: http://custom-url-for-this-tenant:8080
    proxy_url: http://some-proxy:1234
    oauth2:
      client_id: myclient
      client_secret: mysecret
      token_url: http://token-url
      scopes: abc,def
      endpoint_params:
        key1: value1
`,
			expectedConfig: notifier.AlertmanagerClientConfig{
				AlertmanagerURL: "http://custom-url-for-this-tenant:8080",
				NotifierConfig: notifier.Config{
					ProxyURL:   "http://some-proxy:1234",
					TLSEnabled: true,
					OAuth2: notifier.OAuth2Config{
						ClientID:     "myclient",
						ClientSecret: flagext.SecretWithValue("mysecret"),
						TokenURL:     "http://token-url",
						Scopes:       []string{"abc", "def"},
						EndpointParams: flagext.NewLimitsMapWithData(map[string]string{
							"key1": "value1",
						}, nil),
					},
				},
			},
		},
	}

	for name, tt := range tc {
		t.Run(name, func(t *testing.T) {
			t.Cleanup(func() {
				SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())
			})

			SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits())

			var limitsYAML Limits
			limitsYAML.RulerAlertmanagerClientConfig.NotifierConfig.OAuth2.EndpointParams = flagext.NewLimitsMap[string](nil)
			err := yaml.Unmarshal([]byte(tt.baseYAML), &limitsYAML)
			require.NoError(t, err)

			SetDefaultLimitsForYAMLUnmarshalling(limitsYAML)

			overrides := map[string]*Limits{}
			err = yaml.Unmarshal([]byte(tt.overrides), &overrides)
			require.NoError(t, err)

			tl := NewMockTenantLimits(overrides)
			ov := NewOverrides(limitsYAML, tl)
			require.NoError(t, err)

			require.Equal(t, tt.expectedConfig, ov.RulerAlertmanagerClientConfig("user1"))
			require.True(t, tt.expectedConfig.Equal(ov.RulerAlertmanagerClientConfig("user1")))
		})
	}
}

func TestActiveSeriesCustomTrackersConfig(t *testing.T) {
	tests := map[string]struct {
		cfg                      string
		expectedBaseConfig       string
		expectedAdditionalConfig string
		expectedMergedConfig     string
	}{
		"no base and no additional config": {
			cfg: `
# Set another unrelated field to trigger the limits unmarshalling.
max_global_series_per_user: 10
`,
			expectedBaseConfig:       "",
			expectedAdditionalConfig: "",
			expectedMergedConfig:     "",
		},
		"only base config is set": {
			cfg: `
active_series_custom_trackers:
  base_1: '{foo="base_1"}'`,
			expectedBaseConfig:       `base_1:{foo="base_1"}`,
			expectedAdditionalConfig: "",
			expectedMergedConfig:     `base_1:{foo="base_1"}`,
		},
		"only additional config is set": {
			cfg: `
active_series_additional_custom_trackers:
  additional_1: '{foo="additional_1"}'`,
			expectedBaseConfig:       "",
			expectedAdditionalConfig: `additional_1:{foo="additional_1"}`,
			expectedMergedConfig:     `additional_1:{foo="additional_1"}`,
		},
		"both base and additional configs are set": {
			cfg: `
active_series_custom_trackers:
  base_1: '{foo="base_1"}'
  common_1: '{foo="base"}'

active_series_additional_custom_trackers:
  additional_1: '{foo="additional_1"}'
  common_1: '{foo="additional"}'`,
			expectedBaseConfig:       `base_1:{foo="base_1"};common_1:{foo="base"}`,
			expectedAdditionalConfig: `additional_1:{foo="additional_1"};common_1:{foo="additional"}`,
			expectedMergedConfig:     `additional_1:{foo="additional_1"};base_1:{foo="base_1"};common_1:{foo="additional"}`,
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			for _, withDefaultValues := range []bool{true, false} {
				t.Run(fmt.Sprintf("with default values: %t", withDefaultValues), func(t *testing.T) {
					limitsYAML := Limits{}
					if withDefaultValues {
						flagext.DefaultValues(&limitsYAML)
					}
					require.NoError(t, yaml.Unmarshal([]byte(testData.cfg), &limitsYAML))

					overrides := NewOverrides(limitsYAML, nil)

					// We expect the pointer holder to be always initialised, either when initializing default values
					// or by the unmarshalling.
					require.NotNil(t, overrides.getOverridesForUser("user").activeSeriesMergedCustomTrackersConfig)

					assert.Equal(t, testData.expectedBaseConfig, overrides.getOverridesForUser("test").ActiveSeriesBaseCustomTrackersConfig.String())
					assert.Equal(t, testData.expectedAdditionalConfig, overrides.getOverridesForUser("user").ActiveSeriesAdditionalCustomTrackersConfig.String())
					assert.Equal(t, testData.expectedMergedConfig, overrides.ActiveSeriesCustomTrackersConfig("user").String())
				})
			}
		})
	}
}

func TestActiveSeriesCustomTrackersConfig_Concurrency(t *testing.T) {
	const (
		numRuns             = 100
		numGoroutinesPerRun = 10
	)

	cfg := `
active_series_custom_trackers:
  base_1: '{foo="base_1"}'
  common_1: '{foo="base"}'

active_series_additional_custom_trackers:
  additional_1: '{foo="additional_1"}'
  common_1: '{foo="additional"}'`

	for r := 0; r < numRuns; r++ {
		limitsYAML := Limits{}
		require.NoError(t, yaml.Unmarshal([]byte(cfg), &limitsYAML))

		overrides := NewOverrides(limitsYAML, nil)

		start := make(chan struct{})
		wg := sync.WaitGroup{}
		wg.Add(numGoroutinesPerRun)

		// Kick off goroutines.
		for g := 0; g < numGoroutinesPerRun; g++ {
			go func() {
				defer wg.Done()
				<-start
				overrides.ActiveSeriesCustomTrackersConfig("user")
			}()
		}

		// Unblock calls and wait until done.
		close(start)
		wg.Wait()
	}
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

func TestLimits_Validate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cfg         Limits
		verify      func(*testing.T, Limits)
		expectedErr error
	}{
		"should fail if max update timeout jitter is negative": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.HATrackerUpdateTimeoutJitterMax = -1

				return cfg
			}(),
			expectedErr: errNegativeUpdateTimeoutJitterMax,
		},
		"should fail if failover timeout is < update timeout + jitter + 1 sec": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.HATrackerFailoverTimeout = model.Duration(5 * time.Second)
				cfg.HATrackerUpdateTimeout = model.Duration(4 * time.Second)
				cfg.HATrackerUpdateTimeoutJitterMax = model.Duration(2 * time.Second)

				return cfg
			}(),
			expectedErr: fmt.Errorf(errInvalidFailoverTimeout, 5*time.Second, 7*time.Second),
		},
		"should pass if failover timeout is >= update timeout + jitter + 1 sec": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.HATrackerFailoverTimeout = model.Duration(7 * time.Second)
				cfg.HATrackerUpdateTimeout = model.Duration(4 * time.Second)
				cfg.HATrackerUpdateTimeoutJitterMax = model.Duration(2 * time.Second)

				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass if otel_translation_strategy is UnderscoreEscapingWithoutSuffixes and name_validation_scheme is legacy and metric name suffixes are disabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.LegacyValidation
				cfg.OTelMetricSuffixesEnabled = false
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.UnderscoreEscapingWithoutSuffixes)
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass if otel_translation_strategy is UnderscoreEscapingWithSuffixes and name_validation_scheme is legacy and metric name suffixes are enabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.LegacyValidation
				cfg.OTelMetricSuffixesEnabled = true
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.UnderscoreEscapingWithSuffixes)
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass if otel_translation_strategy is NoUTF8EscapingWithSuffixes and name_validation_scheme is utf8 and metric name suffixes are enabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.UTF8Validation
				cfg.OTelMetricSuffixesEnabled = true
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.NoUTF8EscapingWithSuffixes)
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass if otel_translation_strategy is NoTranslation and name_validation_scheme is utf8 and metric name suffixes are disabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.UTF8Validation
				cfg.OTelMetricSuffixesEnabled = false
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.NoTranslation)
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass if otel_translation_strategy is unspecified and name_validation_scheme is legacy and metric name suffixes are disabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.LegacyValidation
				cfg.OTelMetricSuffixesEnabled = false
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue("")
				return cfg
			}(),
			verify: func(t *testing.T, cfg Limits) {
				t.Helper()
				assert.Equal(t, OTelTranslationStrategyValue(""), cfg.OTelTranslationStrategy)
			},
			expectedErr: nil,
		},
		"should pass if otel_translation_strategy is unspecified and name_validation_scheme is legacy and metric name suffixes are enabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.LegacyValidation
				cfg.OTelMetricSuffixesEnabled = true
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue("")
				return cfg
			}(),
			verify: func(t *testing.T, cfg Limits) {
				t.Helper()
				assert.Equal(t, OTelTranslationStrategyValue(""), cfg.OTelTranslationStrategy)
			},
			expectedErr: nil,
		},
		"should pass if otel_translation_strategy is unspecified and name_validation_scheme is utf8 and metric name suffixes are enabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.UTF8Validation
				cfg.OTelMetricSuffixesEnabled = true
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue("")
				return cfg
			}(),
			verify: func(t *testing.T, cfg Limits) {
				t.Helper()
				assert.Equal(t, OTelTranslationStrategyValue(""), cfg.OTelTranslationStrategy)
			},
			expectedErr: nil,
		},
		"should pass if otel_translation_strategy is unspecified and name_validation_scheme is utf8 and metric name suffixes are disabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.UTF8Validation
				cfg.OTelMetricSuffixesEnabled = false
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue("")
				return cfg
			}(),
			verify: func(t *testing.T, cfg Limits) {
				t.Helper()
				assert.Equal(t, OTelTranslationStrategyValue(""), cfg.OTelTranslationStrategy)
			},
			expectedErr: nil,
		},
		"should fail if otel_translation_strategy is UnderscoreEscapingWithoutSuffixes and name_validation_scheme is utf8 and metric name suffixes are disabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.UTF8Validation
				cfg.OTelMetricSuffixesEnabled = false
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.UnderscoreEscapingWithoutSuffixes)
				return cfg
			}(),
			expectedErr: fmt.Errorf("OTLP translation strategy UnderscoreEscapingWithoutSuffixes is not allowed unless validation scheme is legacy"),
		},
		"should fail if otel_translation_strategy is UnderscoreEscapingWithoutSuffixes and name_validation_scheme is legacy and metric name suffixes are enabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.LegacyValidation
				cfg.OTelMetricSuffixesEnabled = true
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.UnderscoreEscapingWithoutSuffixes)
				return cfg
			}(),
			expectedErr: fmt.Errorf("OTLP translation strategy UnderscoreEscapingWithoutSuffixes is not allowed unless metric suffixes are disabled"),
		},
		"should fail if otel_translation_strategy is UnderscoreEscapingWithSuffixes and name_validation_scheme is utf8 and metric name suffixes are enabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.UTF8Validation
				cfg.OTelMetricSuffixesEnabled = true
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.UnderscoreEscapingWithSuffixes)
				return cfg
			}(),
			expectedErr: fmt.Errorf("OTLP translation strategy UnderscoreEscapingWithSuffixes is not allowed unless validation scheme is legacy"),
		},
		"should fail if otel_translation_strategy is UnderscoreEscapingWithSuffixes and name_validation_scheme is legacy and metric name suffixes are disabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.LegacyValidation
				cfg.OTelMetricSuffixesEnabled = false
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.UnderscoreEscapingWithSuffixes)
				return cfg
			}(),
			expectedErr: fmt.Errorf("OTLP translation strategy UnderscoreEscapingWithSuffixes is not allowed unless metric suffixes are enabled"),
		},
		"should fail if otel_translation_strategy is NoUTF8EscapingWithSuffixes and name_validation_scheme is legacy and metric name suffixes are enabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.LegacyValidation
				cfg.OTelMetricSuffixesEnabled = true
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.NoUTF8EscapingWithSuffixes)
				return cfg
			}(),
			expectedErr: fmt.Errorf("OTLP translation strategy NoUTF8EscapingWithSuffixes is not allowed unless validation scheme is utf8"),
		},
		"should fail if otel_translation_strategy is NoUTF8EscapingWithSuffixes and name_validation_scheme is utf8 and metric name suffixes are disabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.UTF8Validation
				cfg.OTelMetricSuffixesEnabled = false
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.NoUTF8EscapingWithSuffixes)
				return cfg
			}(),
			expectedErr: fmt.Errorf("OTLP translation strategy NoUTF8EscapingWithSuffixes is not allowed unless metric suffixes are enabled"),
		},
		"should fail if otel_translation_strategy is NoTranslation and name_validation_scheme is legacy and metric name suffixes are disabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.LegacyValidation
				cfg.OTelMetricSuffixesEnabled = false
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.NoTranslation)
				return cfg
			}(),
			expectedErr: fmt.Errorf("OTLP translation strategy NoTranslation is not allowed unless validation scheme is utf8"),
		},
		"should fail if otel_translation_strategy is NoTranslation and name_validation_scheme is utf8 and metric name suffixes are enabled": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.NameValidationScheme = model.UTF8Validation
				cfg.OTelMetricSuffixesEnabled = true
				cfg.OTelTranslationStrategy = OTelTranslationStrategyValue(otlptranslator.NoTranslation)
				return cfg
			}(),
			expectedErr: fmt.Errorf("OTLP translation strategy NoTranslation is not allowed unless metric suffixes are disabled"),
		},
		"should fail if label_value_length_over_limit_strategy=truncate and hash suffix would be longer than max label value limit": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.LabelValueLengthOverLimitStrategy = LabelValueLengthOverLimitStrategyTruncate
				cfg.MaxLabelValueLength = LabelValueHashLen - 1
				return cfg
			}(),
			expectedErr: errors.New(`cannot set -validation.label-value-length-over-limit-strategy to "truncate": label value hash suffix would exceed max label value length of 70`),
		},
		"should fail if label_value_length_over_limit_strategy=drop and hash suffix would be longer than max label value limit": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.LabelValueLengthOverLimitStrategy = LabelValueLengthOverLimitStrategyDrop
				cfg.MaxLabelValueLength = LabelValueHashLen - 1
				return cfg
			}(),
			expectedErr: errors.New(`cannot set -validation.label-value-length-over-limit-strategy to "drop": label value hash suffix would exceed max label value length of 70`),
		},
		"should pass if label_value_length_over_limit_strategy=truncate and hash suffix would be shorter than or equal to max label value limit": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.LabelValueLengthOverLimitStrategy = LabelValueLengthOverLimitStrategyTruncate
				cfg.MaxLabelValueLength = LabelValueHashLen
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass if label_value_length_over_limit_strategy=drop and hash suffix would be shorter than or equal to max label value limit": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.LabelValueLengthOverLimitStrategy = LabelValueLengthOverLimitStrategyDrop
				cfg.MaxLabelValueLength = LabelValueHashLen + 1
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass if cost_attribution_labels_struct is correct": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.CostAttributionLabelsStructured = costattributionmodel.Labels{
					{Input: "team", Output: "my_team"},
					{Input: "service", Output: "my_service"},
				}
				return cfg
			}(),
			expectedErr: nil,
		},
		"should pass if the first cost attribution label is invalid": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.CostAttributionLabelsStructured = costattributionmodel.Labels{
					{Input: "__team__", Output: "my_team"},
					{Input: "service", Output: "my_service"},
				}
				return cfg
			}(),
			expectedErr: nil,
		},
		"should fail if the second cost attribution label is invalid": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.CostAttributionLabelsStructured = costattributionmodel.Labels{
					{Input: "team", Output: "my_team"},
					{Input: "service", Output: "__my_service__"},
				}
				return cfg
			}(),
			expectedErr: errors.New(`invalid cost attribution output label: "service:__my_service__"`),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			err := testData.cfg.Validate()
			if testData.expectedErr != nil {
				require.EqualError(t, err, testData.expectedErr.Error())
				return
			}
			require.NoError(t, err)

			if testData.verify != nil {
				testData.verify(t, testData.cfg)
			}
		})
	}
}

func TestLimits_ValidateMaxUsageGroupsCardinality(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		cfg         Limits
		expectedErr string
	}{
		"additional config within limit": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.MaxActiveSeriesAdditionalCustomTrackers = 5
				additionalConfig := map[string]string{
					"tracker1": `{foo="bar"}`,
					"tracker2": `{baz="qux"}`,
				}
				var err error
				cfg.ActiveSeriesAdditionalCustomTrackersConfig, err = asmodel.NewCustomTrackersConfig(additionalConfig)
				if err != nil {
					panic(err)
				}
				return cfg
			}(),
		},
		"additional config exceeds limit": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.MaxActiveSeriesAdditionalCustomTrackers = 2
				additionalConfig := map[string]string{
					"tracker1": `{foo="bar"}`,
					"tracker2": `{baz="qux"}`,
					"tracker3": `{hello="world"}`,
				}
				var err error
				cfg.ActiveSeriesAdditionalCustomTrackersConfig, err = asmodel.NewCustomTrackersConfig(additionalConfig)
				if err != nil {
					panic(err)
				}
				return cfg
			}(),
			expectedErr: "active_series_additional_custom_trackers validation failed: the number of custom trackers [3] exceeds the configured limit [2]",
		},
		"base config not affected by limit": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.MaxActiveSeriesAdditionalCustomTrackers = 2
				baseConfig := map[string]string{
					"tracker1": `{foo="bar"}`,
					"tracker2": `{baz="qux"}`,
					"tracker3": `{hello="world"}`,
					"tracker4": `{ping="pong"}`,
					"tracker5": `{alpha="beta"}`,
				}
				var err error
				cfg.ActiveSeriesBaseCustomTrackersConfig, err = asmodel.NewCustomTrackersConfig(baseConfig)
				if err != nil {
					panic(err)
				}
				return cfg
			}(),
		},
		"limit only applies to additional config not merged": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.MaxActiveSeriesAdditionalCustomTrackers = 2
				baseConfig := map[string]string{
					"tracker1": `{foo="bar"}`,
					"tracker2": `{baz="qux"}`,
					"tracker3": `{hello="world"}`,
				}
				additionalConfig := map[string]string{
					"tracker4": `{ping="pong"}`,
					"tracker5": `{alpha="beta"}`,
				}
				var err error
				cfg.ActiveSeriesBaseCustomTrackersConfig, err = asmodel.NewCustomTrackersConfig(baseConfig)
				if err != nil {
					panic(err)
				}
				cfg.ActiveSeriesAdditionalCustomTrackersConfig, err = asmodel.NewCustomTrackersConfig(additionalConfig)
				if err != nil {
					panic(err)
				}
				return cfg
			}(),
		},
		"limit 0 means unlimited for additional config": {
			cfg: func() Limits {
				cfg := Limits{}
				flagext.DefaultValues(&cfg)
				cfg.MaxActiveSeriesAdditionalCustomTrackers = 0
				additionalConfig := map[string]string{
					"tracker1":  `{foo="bar"}`,
					"tracker2":  `{baz="qux"}`,
					"tracker3":  `{hello="world"}`,
					"tracker4":  `{ping="pong"}`,
					"tracker5":  `{alpha="beta"}`,
					"tracker6":  `{gamma="delta"}`,
					"tracker7":  `{epsilon="zeta"}`,
					"tracker8":  `{eta="theta"}`,
					"tracker9":  `{iota="kappa"}`,
					"tracker10": `{lambda="mu"}`,
				}
				var err error
				cfg.ActiveSeriesAdditionalCustomTrackersConfig, err = asmodel.NewCustomTrackersConfig(additionalConfig)
				if err != nil {
					panic(err)
				}
				return cfg
			}(),
		},
	}

	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			t.Parallel()
			err := testData.cfg.Validate()
			if testData.expectedErr != "" {
				require.EqualError(t, err, testData.expectedErr)
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
		require.Contains(t, string(val),
			`test:
    test_extension_struct:
        foo: 0
    test_extension_string: ""
    max_active_series_per_user: 0
    request_rate: 0`)

		val, err = json.Marshal(overrides)
		require.NoError(t, err)
		require.Contains(t, string(val), `{"test":{"test_extension_struct":{"foo":0},"test_extension_string":"","max_active_series_per_user":0,`)
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
    max_active_series_per_user: 0
    request_rate: 0
    request_burst_size: 0`)

		val, err = json.Marshal(overrides)
		require.NoError(t, err)
		require.Contains(t, string(val), `{"test":{"test_extension_struct":{"foo":421237},"test_extension_string":"","max_active_series_per_user":0,"request_rate":0,`)
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
    max_active_series_per_user: 0
    request_rate: 0
    request_burst_size: 0`)

		val, err = json.Marshal(overrides)
		require.NoError(t, err)
		require.Contains(t, string(val), `{"user":{"test_extension_struct":{"foo":42},"test_extension_string":"default string extension value","max_active_series_per_user":0,"request_rate":0,`)
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
			err:             fmt.Errorf("wrapped: %w", NewLimitError(msg)),
			expectedOutcome: true,
		},
	}
	for testName, testData := range testCases {
		t.Run(testName, func(t *testing.T) {
			require.Equal(t, testData.expectedOutcome, IsLimitError(testData.err))
		})
	}
}

func TestAlertmanagerSizeLimitsUnmarshal(t *testing.T) {
	for name, tc := range map[string]struct {
		inputYAML          string
		expectedConfigSize int
		expectedStateSize  int
	}{
		"when using strings": {
			inputYAML: `
alertmanager_max_grafana_config_size_bytes: "4MiB"
alertmanager_max_grafana_state_size_bytes: "2MiB"
`,
			expectedConfigSize: 1024 * 1024 * 4,
			expectedStateSize:  1024 * 1024 * 2,
		},
		"when using 0B, returns 0": {
			inputYAML: `
alertmanager_max_grafana_config_size_bytes: "0"
alertmanager_max_grafana_state_size_bytes: "0"
`,
			expectedConfigSize: 0,
			expectedStateSize:  0,
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

			ov := NewOverrides(limitsYAML, nil)

			require.Equal(t, tc.expectedConfigSize, ov.AlertmanagerMaxGrafanaConfigSize("user"))
			require.Equal(t, tc.expectedStateSize, ov.AlertmanagerMaxGrafanaStateSize("user"))
		})
	}
}

func TestBlockedRequestsUnmarshal(t *testing.T) {
	inputYAML := `
user1:
  blocked_requests:
    - path: /api/v1/query
      method: POST
      query_params:
        foo:
          value: bar
    - query_params:
        first:
          value: bar.*
          is_regexp: true
        other:
          value: bar
          is_regexp: false
`
	overrides := map[string]*Limits{}
	err := yaml.Unmarshal([]byte(inputYAML), &overrides)
	require.NoError(t, err)
	tl := NewMockTenantLimits(overrides)
	ov := NewOverrides(getDefaultLimits(), tl)

	blockedRequests := ov.BlockedRequests("user1")
	require.Len(t, blockedRequests, 2)
	require.Equal(t, BlockedRequest{
		Path:   "/api/v1/query",
		Method: "POST",
		QueryParams: map[string]BlockedRequestQueryParam{
			"foo": {
				Value: "bar",
			},
		},
	}, blockedRequests[0])
	require.Equal(t, BlockedRequest{
		QueryParams: map[string]BlockedRequestQueryParam{
			"first": {
				Value:    "bar.*",
				IsRegexp: true,
			},
			"other": {
				Value:    "bar",
				IsRegexp: false,
			},
		},
	}, blockedRequests[1])
}

func TestLimitsCanonicalizeQueries(t *testing.T) {
	testCases := []struct {
		name            string
		inputQueries    BlockedQueriesConfig
		expectedQueries BlockedQueriesConfig
	}{
		{
			name: "mixed queries",
			inputQueries: BlockedQueriesConfig{
				// Valid exact queries are canonicalized.
				{Pattern: `up{pod="test", job="test"}`, Regex: false},
				// Invalid exact queries are unchanged.
				{Pattern: `up{pod="test", job="test"`, Regex: false},
				// Regex queries are unchanged.
				{Pattern: `up{pod="test", job=~".*"}`, Regex: true},
			},
			expectedQueries: BlockedQueriesConfig{
				// Order is preserved.
				{Pattern: `up{job="test",pod="test"}`, Regex: false},
				{Pattern: `up{pod="test", job="test"`, Regex: false},
				{Pattern: `up{pod="test", job=~".*"}`, Regex: true},
			},
		},
		{
			name:            "empty blocked queries list",
			inputQueries:    BlockedQueriesConfig{},
			expectedQueries: BlockedQueriesConfig{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			limits := Limits{
				BlockedQueries: tc.inputQueries,
			}
			limits.canonicalizeQueries()
			require.Equal(t, tc.expectedQueries, limits.BlockedQueries)
		})
	}
}

func TestOverrides_OTelTranslationStrategy(t *testing.T) {
	testCases := []struct {
		name                        string
		limits                      map[string]*Limits
		tenantID                    string
		expectedTranslationStrategy otlptranslator.TranslationStrategyOption
	}{
		{
			name: "explicit strategy takes precedence",
			limits: map[string]*Limits{
				"tenant1": {
					OTelTranslationStrategy:   OTelTranslationStrategyValue(otlptranslator.UnderscoreEscapingWithSuffixes),
					NameValidationScheme:      model.UTF8Validation,
					OTelMetricSuffixesEnabled: false,
				},
			},
			tenantID:                    "tenant1",
			expectedTranslationStrategy: otlptranslator.UnderscoreEscapingWithSuffixes,
		},
		{
			name: "auto-deduced: legacy validation + suffixes enabled",
			limits: map[string]*Limits{
				"tenant1": {
					OTelTranslationStrategy:   OTelTranslationStrategyValue(""),
					NameValidationScheme:      model.LegacyValidation,
					OTelMetricSuffixesEnabled: true,
				},
			},
			tenantID:                    "tenant1",
			expectedTranslationStrategy: otlptranslator.UnderscoreEscapingWithSuffixes,
		},
		{
			name: "auto-deduced: legacy validation + suffixes disabled",
			limits: map[string]*Limits{
				"tenant1": {
					OTelTranslationStrategy:   OTelTranslationStrategyValue(""),
					NameValidationScheme:      model.LegacyValidation,
					OTelMetricSuffixesEnabled: false,
				},
			},
			tenantID:                    "tenant1",
			expectedTranslationStrategy: otlptranslator.UnderscoreEscapingWithoutSuffixes,
		},
		{
			name: "auto-deduced: utf8 validation + suffixes enabled",
			limits: map[string]*Limits{
				"tenant1": {
					OTelTranslationStrategy:   OTelTranslationStrategyValue(""),
					NameValidationScheme:      model.UTF8Validation,
					OTelMetricSuffixesEnabled: true,
				},
			},
			tenantID:                    "tenant1",
			expectedTranslationStrategy: otlptranslator.NoUTF8EscapingWithSuffixes,
		},
		{
			name: "auto-deduced: utf8 validation + suffixes disabled",
			limits: map[string]*Limits{
				"tenant1": {
					OTelTranslationStrategy:   OTelTranslationStrategyValue(""),
					NameValidationScheme:      model.UTF8Validation,
					OTelMetricSuffixesEnabled: false,
				},
			},
			tenantID:                    "tenant1",
			expectedTranslationStrategy: otlptranslator.NoTranslation,
		},
		{
			name:                        "uses defaults for unknown tenant",
			limits:                      map[string]*Limits{},
			tenantID:                    "unknown",
			expectedTranslationStrategy: otlptranslator.UnderscoreEscapingWithoutSuffixes, // Default: legacy + suffixes disabled
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			defaults := getDefaultLimits()
			overrides := NewOverrides(defaults, NewMockTenantLimits(tc.limits))

			result := overrides.OTelTranslationStrategy(tc.tenantID)
			assert.Equal(t, tc.expectedTranslationStrategy, result)
		})
	}

	t.Run("panics on unknown name validation scheme", func(t *testing.T) {
		limits := map[string]*Limits{
			"tenant1": {
				OTelTranslationStrategy:   OTelTranslationStrategyValue(""),
				NameValidationScheme:      model.ValidationScheme(999), // Invalid scheme
				OTelMetricSuffixesEnabled: true,
			},
		}

		defaults := getDefaultLimits()
		overrides := NewOverrides(defaults, NewMockTenantLimits(limits))

		assert.Panics(t, func() {
			overrides.OTelTranslationStrategy("tenant1")
		})
	})
}

func getDefaultLimits() Limits {
	limits := Limits{}
	flagext.DefaultValues(&limits)
	return limits
}
