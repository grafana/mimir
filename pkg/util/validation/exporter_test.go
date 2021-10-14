// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/exporter_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"bytes"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
)

func TestOverridesExporter_noConfig(t *testing.T) {
	exporter := NewOverridesExporter(&Limits{}, newMockTenantLimits(nil))

	// With no updated override configurations, there should be no override metrics
	count := testutil.CollectAndCount(exporter, "cortex_limits_overrides")
	assert.Equal(t, 0, count)
	// The defaults should exist though
	count = testutil.CollectAndCount(exporter, "cortex_limits_defaults")
	assert.Equal(t, 11, count)
}

func TestOverridesExporter_withConfig(t *testing.T) {
	tenantLimits := map[string]*Limits{
		"tenant-a": {
			IngestionRate:                10,
			IngestionBurstSize:           11,
			MaxLocalSeriesPerUser:        12,
			MaxLocalSeriesPerMetric:      13,
			MaxGlobalSeriesPerUser:       14,
			MaxGlobalSeriesPerMetric:     15,
			MaxFetchedSeriesPerQuery:     16,
			MaxFetchedChunkBytesPerQuery: 17,
			MaxSeriesPerQuery:            18,
			RulerMaxRulesPerRuleGroup:    20,
			RulerMaxRuleGroupsPerTenant:  21,
		},
	}

	exporter := NewOverridesExporter(&Limits{
		IngestionRate:                22,
		IngestionBurstSize:           23,
		MaxLocalSeriesPerUser:        24,
		MaxLocalSeriesPerMetric:      25,
		MaxGlobalSeriesPerUser:       26,
		MaxGlobalSeriesPerMetric:     27,
		MaxFetchedSeriesPerQuery:     28,
		MaxFetchedChunkBytesPerQuery: 29,
		MaxSeriesPerQuery:            30,
		RulerMaxRulesPerRuleGroup:    32,
		RulerMaxRuleGroupsPerTenant:  33,
	}, newMockTenantLimits(tenantLimits))
	limitsMetrics := `
# HELP cortex_limits_overrides Resource limit overrides applied to tenants
# TYPE cortex_limits_overrides gauge
cortex_limits_overrides{limit_name="ingestion_rate",user="tenant-a"} 10
cortex_limits_overrides{limit_name="ingestion_burst_size",user="tenant-a"} 11
cortex_limits_overrides{limit_name="max_local_series_per_user",user="tenant-a"} 12
cortex_limits_overrides{limit_name="max_local_series_per_metric",user="tenant-a"} 13
cortex_limits_overrides{limit_name="max_global_series_per_user",user="tenant-a"} 14
cortex_limits_overrides{limit_name="max_global_series_per_metric",user="tenant-a"} 15
cortex_limits_overrides{limit_name="max_fetched_series_per_query",user="tenant-a"} 16
cortex_limits_overrides{limit_name="max_fetched_chunk_bytes_per_query",user="tenant-a"} 17
cortex_limits_overrides{limit_name="max_series_per_query",user="tenant-a"} 18
cortex_limits_overrides{limit_name="ruler_max_rules_per_rule_group",user="tenant-a"} 20
cortex_limits_overrides{limit_name="ruler_max_rule_groups_per_tenant",user="tenant-a"} 21
`

	// Make sure each override matches the values from the supplied `Limit`
	err := testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_overrides")
	assert.NoError(t, err)

	limitsMetrics = `
# HELP cortex_limits_defaults Resource limit defaults for tenants without overrides
# TYPE cortex_limits_defaults gauge
cortex_limits_defaults{limit_name="ingestion_rate"} 22
cortex_limits_defaults{limit_name="ingestion_burst_size"} 23
cortex_limits_defaults{limit_name="max_local_series_per_user"} 24
cortex_limits_defaults{limit_name="max_local_series_per_metric"} 25
cortex_limits_defaults{limit_name="max_global_series_per_user"} 26
cortex_limits_defaults{limit_name="max_global_series_per_metric"} 27
cortex_limits_defaults{limit_name="max_fetched_series_per_query"} 28
cortex_limits_defaults{limit_name="max_fetched_chunk_bytes_per_query"} 29
cortex_limits_defaults{limit_name="max_series_per_query"} 30
cortex_limits_defaults{limit_name="ruler_max_rules_per_rule_group"} 32
cortex_limits_defaults{limit_name="ruler_max_rule_groups_per_tenant"} 33
`
	err = testutil.CollectAndCompare(exporter, bytes.NewBufferString(limitsMetrics), "cortex_limits_defaults")
	assert.NoError(t, err)
}
