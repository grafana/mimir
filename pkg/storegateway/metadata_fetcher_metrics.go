// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/metadata_fetcher_metrics.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package storegateway

import (
	"github.com/go-kit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// MetadataFetcherMetrics aggregates metrics exported by Thanos MetaFetcher
// and re-exports those aggregates as Mimir metrics.
type MetadataFetcherMetrics struct {
	regs *dskit_metrics.TenantRegistries

	// Exported metrics, gathered from Thanos MetaFetcher
	syncs           *prometheus.Desc
	syncFailures    *prometheus.Desc
	syncDuration    *prometheus.Desc
	synced          *prometheus.Desc
	metaLoadsTotal  *prometheus.Desc
	metaCacheLoads  *prometheus.Desc
	metaCacheMisses *prometheus.Desc
	metaDiskLoads   *prometheus.Desc
	metaDiskMisses  *prometheus.Desc
}

func NewMetadataFetcherMetrics(logger log.Logger) *MetadataFetcherMetrics {
	return &MetadataFetcherMetrics{
		regs: dskit_metrics.NewTenantRegistries(logger),

		// When mapping new metadata fetcher metrics from Thanos, please remember to add these metrics
		// to our internal fetcherMetrics implementation too.
		syncs: prometheus.NewDesc(
			"cortex_blocks_meta_syncs_total",
			"Total blocks metadata synchronization attempts",
			nil, nil),
		syncFailures: prometheus.NewDesc(
			"cortex_blocks_meta_sync_failures_total",
			"Total blocks metadata synchronization failures",
			nil, nil),
		syncDuration: prometheus.NewDesc(
			"cortex_blocks_meta_sync_duration_seconds",
			"Duration of the blocks metadata synchronization in seconds",
			nil, nil),
		synced: prometheus.NewDesc(
			"cortex_blocks_meta_synced",
			"Reflects current state of synced blocks (over all tenants).",
			[]string{"state"}, nil),
		metaLoadsTotal: prometheus.NewDesc(
			"cortex_blocks_meta_loads_total",
			"Total number of block metadata load attempts across all users",
			nil, nil),
		metaCacheLoads: prometheus.NewDesc(
			"cortex_blocks_meta_cache_loads_total",
			"Total number of block metadata loads served from in-memory cache",
			nil, nil),
		metaCacheMisses: prometheus.NewDesc(
			"cortex_blocks_meta_cache_misses_total",
			"Total number of block metadata loads that missed in-memory cache",
			nil, nil),
		metaDiskLoads: prometheus.NewDesc(
			"cortex_blocks_meta_disk_loads_total",
			"Total number of block metadata loads served from local disk",
			nil, nil),
		metaDiskMisses: prometheus.NewDesc(
			"cortex_blocks_meta_disk_misses_total",
			"Total number of block metadata loads that missed local disk",
			nil, nil),
	}
}

func (m *MetadataFetcherMetrics) AddUserRegistry(user string, reg *prometheus.Registry) {
	m.regs.AddTenantRegistry(user, reg)
}

func (m *MetadataFetcherMetrics) RemoveUserRegistry(user string) {
	m.regs.RemoveTenantRegistry(user, false)
}

func (m *MetadataFetcherMetrics) Describe(out chan<- *prometheus.Desc) {
	out <- m.syncs
	out <- m.syncFailures
	out <- m.syncDuration
	out <- m.synced
	out <- m.metaLoadsTotal
	out <- m.metaCacheLoads
	out <- m.metaCacheMisses
	out <- m.metaDiskLoads
	out <- m.metaDiskMisses
}

func (m *MetadataFetcherMetrics) Collect(out chan<- prometheus.Metric) {
	data := m.regs.BuildMetricFamiliesPerTenant()

	data.SendSumOfCounters(out, m.syncs, "blocks_meta_syncs_total")
	data.SendSumOfCounters(out, m.syncFailures, "blocks_meta_sync_failures_total")
	data.SendSumOfHistograms(out, m.syncDuration, "blocks_meta_sync_duration_seconds")
	data.SendSumOfGaugesWithLabels(out, m.synced, "blocks_meta_synced", "state")
	data.SendSumOfCounters(out, m.metaLoadsTotal, "blocks_meta_loads_total")
	data.SendSumOfCounters(out, m.metaCacheLoads, "blocks_meta_cache_loads_total")
	data.SendSumOfCounters(out, m.metaCacheMisses, "blocks_meta_cache_misses_total")
	data.SendSumOfCounters(out, m.metaDiskLoads, "blocks_meta_disk_loads_total")
	data.SendSumOfCounters(out, m.metaDiskMisses, "blocks_meta_disk_misses_total")
}
