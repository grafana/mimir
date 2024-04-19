// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/exporter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package exporter

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/kv/consul"
	"github.com/grafana/dskit/ring"
	"github.com/grafana/dskit/services"
	"github.com/grafana/dskit/test"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestOverridesExporter_noConfig(t *testing.T) {
	exporter, err := NewOverridesExporter(Config{EnabledMetrics: defaultEnabledMetricNames}, &validation.Limits{}, nil, log.NewNopLogger(), nil)
	require.NoError(t, err)

	// With no updated override configurations, there should be no override metrics
	count := testutil.CollectAndCount(exporter, "cortex_limits_overrides")
	assert.Equal(t, 0, count)

	// The defaults should exist though
	count = testutil.CollectAndCount(exporter, "cortex_limits_defaults")
	assert.Equal(t, 10, count)
}

func TestOverridesExporter_emptyConfig(t *testing.T) {
	exporter, err := NewOverridesExporter(Config{EnabledMetrics: defaultEnabledMetricNames}, &validation.Limits{}, validation.NewMockTenantLimits(nil), log.NewNopLogger(), nil)
	require.NoError(t, err)

	// With no updated override configurations, there should be no override metrics
	count := testutil.CollectAndCount(exporter, "cortex_limits_overrides")
	assert.Equal(t, 0, count)

	// The defaults should exist though
	count = testutil.CollectAndCount(exporter, "cortex_limits_defaults")
	assert.Equal(t, 10, count)
}

func TestOverridesExporter_withConfig(t *testing.T) {
	tenantLimits := map[string]*validation.Limits{
		"tenant-a": {
			IngestionRate:                10,
			IngestionBurstSize:           11,
			MaxGlobalSeriesPerUser:       12,
			MaxGlobalSeriesPerMetric:     13,
			MaxGlobalExemplarsPerUser:    14,
			MaxChunksPerQuery:            15,
			MaxFetchedSeriesPerQuery:     16,
			MaxFetchedChunkBytesPerQuery: 17,
			RulerMaxRulesPerRuleGroup:    19,
			RulerMaxRuleGroupsPerTenant:  20,
		},
	}

	exporter, err := NewOverridesExporter(Config{EnabledMetrics: defaultEnabledMetricNames}, &validation.Limits{
		IngestionRate:                22,
		IngestionBurstSize:           23,
		MaxGlobalSeriesPerUser:       24,
		MaxGlobalSeriesPerMetric:     25,
		MaxGlobalExemplarsPerUser:    26,
		MaxChunksPerQuery:            27,
		MaxFetchedSeriesPerQuery:     28,
		MaxFetchedChunkBytesPerQuery: 29,
		RulerMaxRulesPerRuleGroup:    31,
		RulerMaxRuleGroupsPerTenant:  32,
	}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	require.NoError(t, err)
	limitsMetrics := `
# HELP cortex_limits_overrides Resource limit overrides applied to tenants
# TYPE cortex_limits_overrides gauge
cortex_limits_overrides{limit_name="ingestion_rate",user="tenant-a"} 10
cortex_limits_overrides{limit_name="ingestion_burst_size",user="tenant-a"} 11
cortex_limits_overrides{limit_name="max_global_series_per_user",user="tenant-a"} 12
cortex_limits_overrides{limit_name="max_global_series_per_metric",user="tenant-a"} 13
cortex_limits_overrides{limit_name="max_global_exemplars_per_user",user="tenant-a"} 14
cortex_limits_overrides{limit_name="max_fetched_chunks_per_query",user="tenant-a"} 15
cortex_limits_overrides{limit_name="max_fetched_series_per_query",user="tenant-a"} 16
cortex_limits_overrides{limit_name="max_fetched_chunk_bytes_per_query",user="tenant-a"} 17
cortex_limits_overrides{limit_name="ruler_max_rules_per_rule_group",user="tenant-a"} 19
cortex_limits_overrides{limit_name="ruler_max_rule_groups_per_tenant",user="tenant-a"} 20
`

	// Make sure each override matches the values from the supplied `Limit`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_overrides")
	assert.NoError(t, err)

	limitsMetrics = `
# HELP cortex_limits_defaults Resource limit defaults for tenants without overrides
# TYPE cortex_limits_defaults gauge
cortex_limits_defaults{limit_name="ingestion_rate"} 22
cortex_limits_defaults{limit_name="ingestion_burst_size"} 23
cortex_limits_defaults{limit_name="max_global_series_per_user"} 24
cortex_limits_defaults{limit_name="max_global_series_per_metric"} 25
cortex_limits_defaults{limit_name="max_global_exemplars_per_user"} 26
cortex_limits_defaults{limit_name="max_fetched_chunks_per_query"} 27
cortex_limits_defaults{limit_name="max_fetched_series_per_query"} 28
cortex_limits_defaults{limit_name="max_fetched_chunk_bytes_per_query"} 29
cortex_limits_defaults{limit_name="ruler_max_rules_per_rule_group"} 31
cortex_limits_defaults{limit_name="ruler_max_rule_groups_per_tenant"} 32
`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_defaults")
	assert.NoError(t, err)
}

func TestOverridesExporter_withEnabledMetricsConfig(t *testing.T) {
	tenantLimits := map[string]*validation.Limits{
		"tenant-a": {
			RequestRate:                                10,
			RequestBurstSize:                           11,
			MaxGlobalMetricsWithMetadataPerUser:        12,
			MaxGlobalMetadataPerMetric:                 13,
			NotificationRateLimit:                      40,
			AlertmanagerMaxDispatcherAggregationGroups: 41,
			AlertmanagerMaxAlertsCount:                 42,
			AlertmanagerMaxAlertsSizeBytes:             43,
		},
	}

	exporter, err := NewOverridesExporter(Config{
		EnabledMetrics: []string{
			maxGlobalMetricsWithMetadataPerUser,
			maxGlobalMetadataPerMetric,
			requestRate,
			requestBurstSize,
			notificationRateLimit,
			alertmanagerMaxDispatcherAggregationGroups,
			alertmanagerMaxAlertsCount,
			alertmanagerMaxAlertsSizeBytes,
		},
	}, &validation.Limits{
		RequestRate:                                22,
		RequestBurstSize:                           23,
		MaxGlobalMetricsWithMetadataPerUser:        24,
		MaxGlobalMetadataPerMetric:                 25,
		NotificationRateLimit:                      50,
		AlertmanagerMaxDispatcherAggregationGroups: 51,
		AlertmanagerMaxAlertsCount:                 52,
		AlertmanagerMaxAlertsSizeBytes:             53,
	}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	require.NoError(t, err)
	limitsMetrics := `
# HELP cortex_limits_overrides Resource limit overrides applied to tenants
# TYPE cortex_limits_overrides gauge
cortex_limits_overrides{limit_name="request_rate",user="tenant-a"} 10
cortex_limits_overrides{limit_name="request_burst_size",user="tenant-a"} 11
cortex_limits_overrides{limit_name="max_global_metadata_per_user",user="tenant-a"} 12
cortex_limits_overrides{limit_name="max_global_metadata_per_metric",user="tenant-a"} 13
cortex_limits_overrides{limit_name="alertmanager_notification_rate_limit",user="tenant-a"} 40
cortex_limits_overrides{limit_name="alertmanager_max_dispatcher_aggregation_groups",user="tenant-a"} 41
cortex_limits_overrides{limit_name="alertmanager_max_alerts_count",user="tenant-a"} 42
cortex_limits_overrides{limit_name="alertmanager_max_alerts_size_bytes",user="tenant-a"} 43
`

	// Make sure each override matches the values from the supplied `Limit`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_overrides")
	assert.NoError(t, err)

	limitsMetrics = `
# HELP cortex_limits_defaults Resource limit defaults for tenants without overrides
# TYPE cortex_limits_defaults gauge
cortex_limits_defaults{limit_name="request_rate"} 22
cortex_limits_defaults{limit_name="request_burst_size"} 23
cortex_limits_defaults{limit_name="max_global_metadata_per_user"} 24
cortex_limits_defaults{limit_name="max_global_metadata_per_metric"} 25
cortex_limits_defaults{limit_name="alertmanager_notification_rate_limit"} 50
cortex_limits_defaults{limit_name="alertmanager_max_dispatcher_aggregation_groups"} 51
cortex_limits_defaults{limit_name="alertmanager_max_alerts_count"} 52
cortex_limits_defaults{limit_name="alertmanager_max_alerts_size_bytes"} 53
`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_defaults")
	assert.NoError(t, err)
}

func TestOverridesExporter_withExtraMetrics(t *testing.T) {
	tenantLimits := map[string]*validation.Limits{
		"tenant-a": {
			IngestionRate:                10,
			IngestionBurstSize:           11,
			MaxGlobalSeriesPerUser:       12,
			MaxGlobalSeriesPerMetric:     13,
			MaxGlobalExemplarsPerUser:    14,
			MaxChunksPerQuery:            15,
			MaxFetchedSeriesPerQuery:     16,
			MaxFetchedChunkBytesPerQuery: 17,
			RulerMaxRulesPerRuleGroup:    19,
			RulerMaxRuleGroupsPerTenant:  20,
		},
	}

	config := Config{
		EnabledMetrics: defaultEnabledMetricNames,
		ExtraMetrics: []ExportedMetric{
			{
				Name: "custom_extra_limit",
				Get: func(_ *validation.Limits) float64 {
					return 1234.0
				},
			},
		},
	}

	exporter, err := NewOverridesExporter(config, &validation.Limits{
		IngestionRate:                22,
		IngestionBurstSize:           23,
		MaxGlobalSeriesPerUser:       24,
		MaxGlobalSeriesPerMetric:     25,
		MaxGlobalExemplarsPerUser:    26,
		MaxChunksPerQuery:            27,
		MaxFetchedSeriesPerQuery:     28,
		MaxFetchedChunkBytesPerQuery: 29,
		RulerMaxRulesPerRuleGroup:    31,
		RulerMaxRuleGroupsPerTenant:  32,
	}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	require.NoError(t, err)
	limitsMetrics := `
# HELP cortex_limits_overrides Resource limit overrides applied to tenants
# TYPE cortex_limits_overrides gauge
cortex_limits_overrides{limit_name="custom_extra_limit",user="tenant-a"} 1234
cortex_limits_overrides{limit_name="ingestion_rate",user="tenant-a"} 10
cortex_limits_overrides{limit_name="ingestion_burst_size",user="tenant-a"} 11
cortex_limits_overrides{limit_name="max_global_series_per_user",user="tenant-a"} 12
cortex_limits_overrides{limit_name="max_global_series_per_metric",user="tenant-a"} 13
cortex_limits_overrides{limit_name="max_global_exemplars_per_user",user="tenant-a"} 14
cortex_limits_overrides{limit_name="max_fetched_chunks_per_query",user="tenant-a"} 15
cortex_limits_overrides{limit_name="max_fetched_series_per_query",user="tenant-a"} 16
cortex_limits_overrides{limit_name="max_fetched_chunk_bytes_per_query",user="tenant-a"} 17
cortex_limits_overrides{limit_name="ruler_max_rules_per_rule_group",user="tenant-a"} 19
cortex_limits_overrides{limit_name="ruler_max_rule_groups_per_tenant",user="tenant-a"} 20
`

	// Make sure each override matches the values from the supplied `Limit`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_overrides")
	assert.NoError(t, err)

	limitsMetrics = `
# HELP cortex_limits_defaults Resource limit defaults for tenants without overrides
# TYPE cortex_limits_defaults gauge
cortex_limits_defaults{limit_name="custom_extra_limit"} 1234
cortex_limits_defaults{limit_name="ingestion_rate"} 22
cortex_limits_defaults{limit_name="ingestion_burst_size"} 23
cortex_limits_defaults{limit_name="max_global_series_per_user"} 24
cortex_limits_defaults{limit_name="max_global_series_per_metric"} 25
cortex_limits_defaults{limit_name="max_global_exemplars_per_user"} 26
cortex_limits_defaults{limit_name="max_fetched_chunks_per_query"} 27
cortex_limits_defaults{limit_name="max_fetched_series_per_query"} 28
cortex_limits_defaults{limit_name="max_fetched_chunk_bytes_per_query"} 29
cortex_limits_defaults{limit_name="ruler_max_rules_per_rule_group"} 31
cortex_limits_defaults{limit_name="ruler_max_rule_groups_per_tenant"} 32
`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_defaults")
	assert.NoError(t, err)
}

func TestOverridesExporter_withRing(t *testing.T) {
	tenantLimits := map[string]*validation.Limits{
		"tenant-a": {},
	}

	ringStore, closer := consul.NewInMemoryClient(ring.GetCodec(), log.NewNopLogger(), nil)
	t.Cleanup(func() { assert.NoError(t, closer.Close()) })

	cfg1 := Config{
		Ring:           RingConfig{Enabled: true},
		EnabledMetrics: []string{},
	}
	cfg1.Ring.Common.KVStore.Mock = ringStore
	cfg1.Ring.Common.InstancePort = 1234
	cfg1.Ring.Common.HeartbeatPeriod = 15 * time.Second
	cfg1.Ring.Common.HeartbeatTimeout = 1 * time.Minute

	// Create an empty ring.
	ctx := context.Background()
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(interface{}) (out interface{}, retry bool, err error) {
		return ring.NewDesc(), true, nil
	}))

	// Create an overrides-exporter.
	cfg1.Ring.Common.InstanceID = "overrides-exporter-1"
	cfg1.Ring.Common.InstanceAddr = "1.2.3.1"
	e1, err := NewOverridesExporter(cfg1, &validation.Limits{}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	l1 := e1.ring.lifecycler
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, e1))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, e1)) })

	// Wait until it has received the ring update.
	test.Poll(t, time.Second, true, func() interface{} {
		rs, _ := e1.ring.client.GetAllHealthy(ringOp)
		return rs.Includes(l1.GetInstanceAddr())
	})

	// Set leader token.
	require.NoError(t, ringStore.CAS(ctx, ringKey, func(in interface{}) (out interface{}, retry bool, err error) {
		desc := in.(*ring.Desc)
		instance := desc.Ingesters[l1.GetInstanceID()]
		instance.Tokens = []uint32{leaderToken + 1}
		desc.Ingesters[l1.GetInstanceID()] = instance
		return desc, true, nil
	}))

	// Wait for update of token.
	test.Poll(t, time.Second, []uint32{leaderToken + 1}, func() interface{} {
		rs, _ := e1.ring.client.GetAllHealthy(ringOp)
		return rs.Instances[0].Tokens
	})

	// This instance is now the only ring member and should export metrics.
	require.True(t, hasOverrideMetrics(e1))

	// Register a second instance.
	cfg2 := cfg1
	cfg2.Ring.Common.InstanceID = "overrides-exporter-2"
	cfg2.Ring.Common.InstanceAddr = "1.2.3.2"
	e2, err := NewOverridesExporter(cfg2, &validation.Limits{}, validation.NewMockTenantLimits(tenantLimits), log.NewNopLogger(), nil)
	require.NoError(t, err)
	require.NoError(t, services.StartAndAwaitRunning(ctx, e2))
	t.Cleanup(func() { assert.NoError(t, services.StopAndAwaitTerminated(ctx, e2)) })

	// Wait until it has registered itself to the ring and both overrides-exporter instances got the updated ring.
	test.Poll(t, time.Second, true, func() interface{} {
		rs1, _ := e1.ring.client.GetAllHealthy(ringOp)
		rs2, _ := e2.ring.client.GetAllHealthy(ringOp)
		return rs1.Includes(e2.ring.lifecycler.GetInstanceAddr()) && rs2.Includes(e1.ring.lifecycler.GetInstanceAddr())
	})

	// Only the leader instance (owner of the special token) should export metrics.
	require.True(t, hasOverrideMetrics(e1))
	require.False(t, hasOverrideMetrics(e2))
}

func hasOverrideMetrics(e1 prometheus.Collector) bool {
	return testutil.CollectAndCount(e1, "cortex_limits_overrides") > 0
}
