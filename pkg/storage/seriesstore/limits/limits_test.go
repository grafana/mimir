// SPDX-License-Identifier: AGPL-3.0-only

package limits

import (
	"math"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParsesPrometheusDurations(t *testing.T) {
	for value, expected := range map[string]int64{
		"0": 0, "2h": 7_200_000, "1h30m": 5_400_000, "10000d": 10_000 * 86_400_000, "250ms": 250,
	} {
		got, err := ParseDurationMs(value)
		require.NoError(t, err, value)
		require.Equal(t, expected, got, value)
	}
	for _, value := range []string{"5", "h"} {
		_, err := ParseDurationMs(value)
		require.Error(t, err, value)
	}
}

func overridesYAML(t *testing.T, yaml string) map[string]any {
	document, err := ParseDocument([]byte(yaml))
	require.NoError(t, err)
	return document
}

func TestTenantOverridesReplaceOnlyTheirFields(t *testing.T) {
	args := Args{
		OutOfOrderTimeWindow:               "2h",
		CreationGracePeriod:                "10m",
		PastGracePeriod:                    "0",
		MaxGlobalSeriesPerUser:             150_000,
		MaxGlobalExemplarsPerUser:          100_000,
		MaxGlobalMetadataPerUser:           30_000,
		MaxGlobalMetadataPerMetric:         10,
		IngestionPartitionsTenantShardSize: 1,
		ActiveSeriesCustomTrackers:         StringSlice{`a:{job="a"};b:{job="b"}`},
		MaxCostAttributionCardinality:      2000,
		CostAttributionCooldown:            "0",
	}
	defaults, err := args.ToLimits()
	require.NoError(t, err)
	overrides := NewOverrides(defaults)
	// Large YAML limits are floats, and numeric tenant IDs are map keys.
	require.NoError(t, overrides.ApplyRuntimeConfig(overridesYAML(t, `
ingester_limits:
  max_series: 3e+06
overrides:
  "18657":
    max_global_exemplars_per_user: 2.5e+06
    native_histograms_ingestion_enabled: true
    ingestion_partitions_tenant_shard_size: 3091
    early_head_compaction_owned_series_threshold: 1.5e+07
    active_series_additional_custom_trackers:
      c: '{job="c"}'
  9960:
    out_of_order_time_window: 30m
`)))
	big := overrides.Tenant("18657")
	require.Equal(t, int64(2_500_000), big.Limits.MaxGlobalExemplarsPerUser)
	require.True(t, big.Limits.NativeHistogramsIngestionEnabled)
	require.Equal(t, int64(7_200_000), big.Limits.OutOfOrderTimeWindowMs)
	require.Equal(t, int64(30_000), big.Limits.MaxGlobalMetadataPerUser)
	require.Equal(t, int64(15_000_000), big.Limits.EarlyHeadCompactionOwnedSeriesThreshold)
	require.Equal(t, []string{"a", "b", "c"}, big.CustomTrackers.Names())
	small := overrides.Tenant("9960")
	require.Equal(t, int64(30*60_000), small.Limits.OutOfOrderTimeWindowMs)
	require.False(t, small.Limits.NativeHistogramsIngestionEnabled)
	other := overrides.Tenant("other")
	require.Equal(t, int64(100_000), other.Limits.MaxGlobalExemplarsPerUser)
	require.Equal(t, []string{"a", "b"}, other.CustomTrackers.Names())
}

func TestReloadDropsRemovedTenants(t *testing.T) {
	overrides := DefaultOverrides()
	before := overrides.Generation()
	require.NoError(t, overrides.ApplyRuntimeConfig(overridesYAML(t, "overrides:\n  t:\n    max_global_exemplars_per_user: 5\n")))
	require.Equal(t, int64(5), overrides.Tenant("t").Limits.MaxGlobalExemplarsPerUser)
	require.Greater(t, overrides.Generation(), before)
	require.NoError(t, overrides.ApplyRuntimeConfig(overridesYAML(t, "overrides: {}\n")))
	require.Equal(t, int64(0), overrides.Tenant("t").Limits.MaxGlobalExemplarsPerUser)
}

func TestRejectsInvalidOverrideValues(t *testing.T) {
	require.Error(t, DefaultOverrides().ApplyRuntimeConfig(overridesYAML(t, "overrides:\n  t:\n    out_of_order_time_window: soon\n")))
}

func TestConvertsGlobalLimitsLikeThePartitionRingStrategy(t *testing.T) {
	overrides := DefaultOverrides()
	l := DefaultLimits()
	l.MaxGlobalExemplarsPerUser = 100
	l.MaxGlobalMetadataPerUser = 30
	// No known partitions: exemplars fall back to the global limit, metadata is unlimited.
	require.Equal(t, 100, overrides.MaxExemplars(&l))
	require.Equal(t, math.MaxInt32, overrides.MaxMetadataPerUser(&l))
	overrides.SetActivePartitions(10)
	require.Equal(t, 10, overrides.MaxExemplars(&l))
	require.Equal(t, 3, overrides.MaxMetadataPerUser(&l))
	l.IngestionPartitionsTenantShardSize = 3
	require.Equal(t, 33, overrides.MaxExemplars(&l))
	// A shard size above the active partitions uses all of them.
	l.IngestionPartitionsTenantShardSize = 3091
	require.Equal(t, 10, overrides.MaxExemplars(&l))
	// 0 means unlimited metadata and disabled exemplars.
	l.MaxGlobalExemplarsPerUser = 0
	l.MaxGlobalMetadataPerMetric = 0
	require.Equal(t, 0, overrides.MaxExemplars(&l))
	require.Equal(t, math.MaxInt32, overrides.MaxMetadataPerMetric(&l))
}

func TestInstanceLimitsComeFromFlagsAndTheRuntimeConfig(t *testing.T) {
	instance := DefaultInstanceLimits()
	instance.MaxTenants = 7
	overrides := NewOverridesWithInstanceLimits(DefaultLimits(), instance)
	require.NoError(t, overrides.ApplyRuntimeConfig(overridesYAML(t, "ingester_limits:\n  max_series: 3e+06\n  max_tenants: 500\n")))
	got := overrides.InstanceLimits()
	require.Equal(t, int64(3_000_000), got.MaxSeries)
	require.Equal(t, int64(500), got.MaxTenants)
	require.Equal(t, int64(30_000), got.MaxInflightPushRequests)
	require.NoError(t, overrides.ApplyRuntimeConfig(map[string]any{}))
	require.Equal(t, int64(7), overrides.InstanceLimits().MaxTenants)
}

func TestRejectsMoreAdditionalTrackersThanAllowed(t *testing.T) {
	l := DefaultLimits()
	l.MaxActiveSeriesAdditionalCustomTrackers = 1
	overrides := NewOverrides(l)
	yaml := "overrides:\n  t:\n    active_series_additional_custom_trackers:\n      a: '{x=\"1\"}'\n      b: '{x=\"2\"}'\n"
	err := overrides.ApplyRuntimeConfig(overridesYAML(t, yaml))
	require.ErrorContains(t, err, "exceeds the configured limit [1]")
	require.NoError(t, overrides.ApplyRuntimeConfig(overridesYAML(t, strings.Replace(yaml, "      b: '{x=\"2\"}'\n", "", 1))))
}
