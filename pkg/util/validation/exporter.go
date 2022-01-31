// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/validation/exporter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package validation

import (
	"github.com/prometheus/client_golang/prometheus"
)

// OverridesExporter exposes per-tenant resource limit overrides as Prometheus metrics
type OverridesExporter struct {
	defaultLimits       *Limits
	tenantLimits        TenantLimits
	overrideDescription *prometheus.Desc
	defaultsDescription *prometheus.Desc
}

// NewOverridesExporter creates an OverridesExporter that reads updates to per-tenant
// limits using the provided function.
func NewOverridesExporter(defaultLimits *Limits, tenantLimits TenantLimits) *OverridesExporter {
	return &OverridesExporter{
		defaultLimits: defaultLimits,
		tenantLimits:  tenantLimits,
		overrideDescription: prometheus.NewDesc(
			"cortex_limits_overrides",
			"Resource limit overrides applied to tenants",
			[]string{"limit_name", "user"},
			nil,
		),
		defaultsDescription: prometheus.NewDesc(
			"cortex_limits_defaults",
			"Resource limit defaults for tenants without overrides",
			[]string{"limit_name"},
			nil,
		),
	}
}

func (oe *OverridesExporter) Describe(ch chan<- *prometheus.Desc) {
	ch <- oe.defaultsDescription
	ch <- oe.overrideDescription
}

func (oe *OverridesExporter) Collect(ch chan<- prometheus.Metric) {
	// Write path limits
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, oe.defaultLimits.IngestionRate, "ingestion_rate")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.IngestionBurstSize), "ingestion_burst_size")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalSeriesPerUser), "max_global_series_per_user")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalSeriesPerMetric), "max_global_series_per_metric")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxGlobalExemplarsPerUser), "max_global_exemplars_per_user")

	// Read path limits
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxChunksPerQuery), "max_fetched_chunks_per_query")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxFetchedSeriesPerQuery), "max_fetched_series_per_query")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.MaxFetchedChunkBytesPerQuery), "max_fetched_chunk_bytes_per_query")

	// Ruler limits
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.RulerMaxRulesPerRuleGroup), "ruler_max_rules_per_rule_group")
	ch <- prometheus.MustNewConstMetric(oe.defaultsDescription, prometheus.GaugeValue, float64(oe.defaultLimits.RulerMaxRuleGroupsPerTenant), "ruler_max_rule_groups_per_tenant")

	allLimits := oe.tenantLimits.AllByUserID()
	for tenant, limits := range allLimits {
		// Write path limits
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, limits.IngestionRate, "ingestion_rate", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.IngestionBurstSize), "ingestion_burst_size", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerUser), "max_global_series_per_user", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalSeriesPerMetric), "max_global_series_per_metric", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxGlobalExemplarsPerUser), "max_global_exemplars_per_user", tenant)

		// Read path limits
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxChunksPerQuery), "max_fetched_chunks_per_query", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxFetchedSeriesPerQuery), "max_fetched_series_per_query", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.MaxFetchedChunkBytesPerQuery), "max_fetched_chunk_bytes_per_query", tenant)

		// Ruler limits
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.RulerMaxRulesPerRuleGroup), "ruler_max_rules_per_rule_group", tenant)
		ch <- prometheus.MustNewConstMetric(oe.overrideDescription, prometheus.GaugeValue, float64(limits.RulerMaxRuleGroupsPerTenant), "ruler_max_rule_groups_per_tenant", tenant)
	}
}
