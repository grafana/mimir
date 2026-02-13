// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ingester/metrics.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"github.com/go-kit/log"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

// TSDBMetrics is a per-tenant TSDB metrics collector. Each tenant has its own registry that TSDB code uses.
type TSDBMetrics struct {
	regs *dskit_metrics.TenantRegistries

	// Metrics aggregated from TSDB.
	tsdbCompactionsTotal              *prometheus.Desc
	tsdbCompactionDuration            *prometheus.Desc
	tsdbFsyncDuration                 *prometheus.Desc
	tsdbPageFlushes                   *prometheus.Desc
	tsdbPageCompletions               *prometheus.Desc
	tsdbWALTruncateFail               *prometheus.Desc
	tsdbWALTruncateTotal              *prometheus.Desc
	tsdbWALTruncateDuration           *prometheus.Desc
	tsdbWALCorruptionsTotal           *prometheus.Desc
	tsdbWALWritesFailed               *prometheus.Desc
	tsdbHeadTruncateFail              *prometheus.Desc
	tsdbHeadTruncateTotal             *prometheus.Desc
	tsdbHeadGcDuration                *prometheus.Desc
	tsdbActiveAppenders               *prometheus.Desc
	tsdbSeriesNotFound                *prometheus.Desc
	tsdbChunks                        *prometheus.Desc
	tsdbChunksCreatedTotal            *prometheus.Desc
	tsdbChunksRemovedTotal            *prometheus.Desc
	tsdbMmapChunkCorruptionTotal      *prometheus.Desc
	tsdbMmapChunkQueueOperationsTotal *prometheus.Desc
	tsdbMmapChunksTotal               *prometheus.Desc
	tsdbOOOHistogram                  *prometheus.Desc

	tsdbExemplarsTotal          *prometheus.Desc
	tsdbExemplarsInStorage      *prometheus.Desc
	tsdbExemplarSeriesInStorage *prometheus.Desc
	tsdbExemplarLastTs          *prometheus.Desc
	tsdbExemplarsOutOfOrder     *prometheus.Desc

	// Follow dskit_metrics are from https://github.com/prometheus/prometheus/blob/fbe960f2c1ad9d6f5fe2f267d2559bf7ecfab6df/tsdb/db.go#L179
	tsdbLoadedBlocks       *prometheus.Desc
	tsdbSymbolTableSize    *prometheus.Desc
	tsdbReloads            *prometheus.Desc
	tsdbReloadsFailed      *prometheus.Desc
	tsdbTimeRetentionCount *prometheus.Desc
	tsdbBlocksBytes        *prometheus.Desc

	tsdbOOOAppendedSamples *prometheus.Desc

	checkpointDeleteFail    *prometheus.Desc
	checkpointDeleteTotal   *prometheus.Desc
	checkpointCreationFail  *prometheus.Desc
	checkpointCreationTotal *prometheus.Desc

	memSeries             *prometheus.Desc
	memSeriesCreatedTotal *prometheus.Desc
	memSeriesRemovedTotal *prometheus.Desc

	tsdbWalReplayUnknownRefsTotal *prometheus.Desc
	tsdbWblReplayUnknownRefsTotal *prometheus.Desc

	tsdbHeadStatisticsLastUpdate   *prometheus.Desc
	tsdbHeadStatisticsTimeToUpdate *prometheus.Desc
}

func NewTSDBMetrics(r prometheus.Registerer, logger log.Logger) *TSDBMetrics {
	m := &TSDBMetrics{
		regs: dskit_metrics.NewTenantRegistries(logger),

		tsdbCompactionsTotal: prometheus.NewDesc(
			"tsdb_compactions_total",
			"Total number of TSDB compactions that were executed.",
			nil, nil),
		tsdbCompactionDuration: prometheus.NewDesc(
			"tsdb_compaction_duration_seconds",
			"Duration of TSDB compaction runs.",
			nil, nil),
		tsdbFsyncDuration: prometheus.NewDesc(
			"tsdb_wal_fsync_duration_seconds",
			"Duration of TSDB WAL fsync.",
			nil, nil),
		tsdbPageFlushes: prometheus.NewDesc(
			"tsdb_wal_page_flushes_total",
			"Total number of TSDB WAL page flushes.",
			nil, nil),
		tsdbPageCompletions: prometheus.NewDesc(
			"tsdb_wal_completed_pages_total",
			"Total number of TSDB WAL completed pages.",
			nil, nil),
		tsdbWALTruncateFail: prometheus.NewDesc(
			"tsdb_wal_truncations_failed_total",
			"Total number of TSDB WAL truncations that failed.",
			nil, nil),
		tsdbWALTruncateTotal: prometheus.NewDesc(
			"tsdb_wal_truncations_total",
			"Total number of TSDB  WAL truncations attempted.",
			nil, nil),
		tsdbWALTruncateDuration: prometheus.NewDesc(
			"tsdb_wal_truncate_duration_seconds",
			"Duration of TSDB WAL truncation.",
			nil, nil),
		tsdbWALCorruptionsTotal: prometheus.NewDesc(
			"tsdb_wal_corruptions_total",
			"Total number of TSDB WAL corruptions.",
			nil, nil),
		tsdbWALWritesFailed: prometheus.NewDesc(
			"tsdb_wal_writes_failed_total",
			"Total number of TSDB WAL writes that failed.",
			nil, nil),
		tsdbHeadTruncateFail: prometheus.NewDesc(
			"tsdb_head_truncations_failed_total",
			"Total number of TSDB head truncations that failed.",
			nil, nil),
		tsdbHeadTruncateTotal: prometheus.NewDesc(
			"tsdb_head_truncations_total",
			"Total number of TSDB head truncations attempted.",
			nil, nil),
		tsdbHeadGcDuration: prometheus.NewDesc(
			"tsdb_head_gc_duration_seconds",
			"Runtime of garbage collection in the TSDB head.",
			nil, nil),
		tsdbActiveAppenders: prometheus.NewDesc(
			"tsdb_head_active_appenders",
			"Number of currently active TSDB appender transactions.",
			nil, nil),
		tsdbSeriesNotFound: prometheus.NewDesc(
			"tsdb_head_series_not_found_total",
			"Total number of TSDB requests for series that were not found.",
			nil, nil),
		tsdbChunks: prometheus.NewDesc(
			"tsdb_head_chunks",
			"Total number of chunks in the TSDB head block.",
			nil, nil),
		tsdbChunksCreatedTotal: prometheus.NewDesc(
			"tsdb_head_chunks_created_total",
			"Total number of series created in the TSDB head.",
			[]string{"user"}, nil),
		tsdbChunksRemovedTotal: prometheus.NewDesc(
			"tsdb_head_chunks_removed_total",
			"Total number of series removed in the TSDB head.",
			[]string{"user"}, nil),
		tsdbMmapChunkCorruptionTotal: prometheus.NewDesc(
			"tsdb_mmap_chunk_corruptions_total",
			"Total number of memory-mapped TSDB chunk corruptions.",
			nil, nil),
		tsdbMmapChunkQueueOperationsTotal: prometheus.NewDesc(
			"tsdb_mmap_chunk_write_queue_operations_total",
			"Total number of memory-mapped TSDB chunk operations.",
			[]string{"operation"}, nil),
		tsdbMmapChunksTotal: prometheus.NewDesc(
			"tsdb_mmap_chunks_total",
			"Total number of chunks that were memory-mapped.",
			nil, nil),
		tsdbOOOHistogram: prometheus.NewDesc(
			"tsdb_sample_out_of_order_delta_seconds",
			"Delta in seconds by which a sample is considered out-of-order.",
			nil, nil),
		tsdbLoadedBlocks: prometheus.NewDesc(
			"tsdb_blocks_loaded",
			"Number of currently loaded data blocks",
			nil, nil),
		tsdbReloads: prometheus.NewDesc(
			"tsdb_reloads_total",
			"Number of times the database reloaded block data from disk.",
			nil, nil),
		tsdbReloadsFailed: prometheus.NewDesc(
			"tsdb_reloads_failures_total",
			"Number of times the database failed to reloadBlocks block data from disk.",
			nil, nil),
		tsdbSymbolTableSize: prometheus.NewDesc(
			"tsdb_symbol_table_size_bytes",
			"Size of symbol table in memory for loaded blocks",
			[]string{"user"}, nil),
		tsdbBlocksBytes: prometheus.NewDesc(
			"tsdb_storage_blocks_bytes",
			"The number of bytes that are currently used for local storage by all blocks.",
			[]string{"user"}, nil),
		tsdbTimeRetentionCount: prometheus.NewDesc(
			"tsdb_time_retentions_total",
			"The number of times that blocks were deleted because the maximum time limit was exceeded.",
			nil, nil),
		checkpointDeleteFail: prometheus.NewDesc(
			"tsdb_checkpoint_deletions_failed_total",
			"Total number of TSDB checkpoint deletions that failed.",
			nil, nil),
		checkpointDeleteTotal: prometheus.NewDesc(
			"tsdb_checkpoint_deletions_total",
			"Total number of TSDB checkpoint deletions attempted.",
			nil, nil),
		checkpointCreationFail: prometheus.NewDesc(
			"tsdb_checkpoint_creations_failed_total",
			"Total number of TSDB checkpoint creations that failed.",
			nil, nil),
		checkpointCreationTotal: prometheus.NewDesc(
			"tsdb_checkpoint_creations_total",
			"Total number of TSDB checkpoint creations attempted.",
			nil, nil),

		// The most useful exemplar dskit_metrics are per-user. The rest
		// are global to reduce dskit_metrics overhead.
		tsdbExemplarsTotal: prometheus.NewDesc(
			"tsdb_exemplar_exemplars_appended_total",
			"Total number of TSDB exemplars appended.",
			[]string{"user"}, nil), // see distributor_exemplars_in for per-user rate
		tsdbExemplarsInStorage: prometheus.NewDesc(
			"tsdb_exemplar_exemplars_in_storage",
			"Number of TSDB exemplars currently in storage.",
			nil, nil),
		tsdbExemplarSeriesInStorage: prometheus.NewDesc(
			"tsdb_exemplar_series_with_exemplars_in_storage",
			"Number of TSDB series with exemplars currently in storage.",
			[]string{"user"}, nil),
		tsdbExemplarLastTs: prometheus.NewDesc(
			"tsdb_exemplar_last_exemplars_timestamp_seconds",
			"The timestamp of the oldest exemplar stored in circular storage. "+
				"Useful to check for what time range the current exemplar buffer limit allows. "+
				"This usually means the last timestamp for all exemplars for a typical setup. "+
				"This is not true though if one of the series timestamp is in future compared to rest series.",
			[]string{"user"}, nil),
		tsdbExemplarsOutOfOrder: prometheus.NewDesc(
			"tsdb_exemplar_out_of_order_exemplars_total",
			"Total number of out-of-order exemplar ingestion failed attempts.",
			nil, nil),

		tsdbOOOAppendedSamples: prometheus.NewDesc(
			"tsdb_out_of_order_samples_appended_total",
			"Total number of out-of-order samples appended.",
			[]string{"user"}, nil),

		memSeries: prometheus.NewDesc(
			"memory_series",
			"The current number of series in memory.",
			nil, nil),
		memSeriesCreatedTotal: prometheus.NewDesc(
			"memory_series_created_total",
			"The total number of series that were created per user.",
			[]string{"user"}, nil),
		memSeriesRemovedTotal: prometheus.NewDesc(
			"memory_series_removed_total",
			"The total number of series that were removed per user.",
			[]string{"user"}, nil),

		tsdbWalReplayUnknownRefsTotal: prometheus.NewDesc(
			"tsdb_wal_replay_unknown_refs_total",
			"Total number of unknown series references encountered during WAL replay.",
			[]string{"user", "type"}, nil),
		tsdbWblReplayUnknownRefsTotal: prometheus.NewDesc(
			"tsdb_wbl_replay_unknown_refs_total",
			"Total number of unknown series references encountered during WBL replay.",
			[]string{"user", "type"}, nil),

		tsdbHeadStatisticsLastUpdate: prometheus.NewDesc(
			"tsdb_head_statistics_last_update_timestamp_seconds",
			"Timestamp of the last update of head statistics.",
			[]string{"user"}, nil,
		),
		tsdbHeadStatisticsTimeToUpdate: prometheus.NewDesc(
			"tsdb_head_statistics_time_to_update_seconds",
			"Time spent updating head statistics.",
			[]string{"user"}, nil,
		),
	}

	if r != nil {
		r.MustRegister(m)
	}
	return m
}

func (sm *TSDBMetrics) Describe(out chan<- *prometheus.Desc) {
	out <- sm.tsdbCompactionsTotal
	out <- sm.tsdbCompactionDuration
	out <- sm.tsdbFsyncDuration
	out <- sm.tsdbPageFlushes
	out <- sm.tsdbPageCompletions
	out <- sm.tsdbWALTruncateFail
	out <- sm.tsdbWALTruncateTotal
	out <- sm.tsdbWALTruncateDuration
	out <- sm.tsdbWALCorruptionsTotal
	out <- sm.tsdbWALWritesFailed
	out <- sm.tsdbHeadTruncateFail
	out <- sm.tsdbHeadTruncateTotal
	out <- sm.tsdbHeadGcDuration
	out <- sm.tsdbActiveAppenders
	out <- sm.tsdbSeriesNotFound
	out <- sm.tsdbChunks
	out <- sm.tsdbChunksCreatedTotal
	out <- sm.tsdbChunksRemovedTotal
	out <- sm.tsdbMmapChunkCorruptionTotal
	out <- sm.tsdbMmapChunkQueueOperationsTotal
	out <- sm.tsdbMmapChunksTotal
	out <- sm.tsdbOOOHistogram
	out <- sm.tsdbLoadedBlocks
	out <- sm.tsdbSymbolTableSize
	out <- sm.tsdbReloads
	out <- sm.tsdbReloadsFailed
	out <- sm.tsdbTimeRetentionCount
	out <- sm.tsdbBlocksBytes
	out <- sm.checkpointDeleteFail
	out <- sm.checkpointDeleteTotal
	out <- sm.checkpointCreationFail
	out <- sm.checkpointCreationTotal

	out <- sm.tsdbExemplarsTotal
	out <- sm.tsdbExemplarsInStorage
	out <- sm.tsdbExemplarSeriesInStorage
	out <- sm.tsdbExemplarLastTs
	out <- sm.tsdbExemplarsOutOfOrder

	out <- sm.tsdbOOOAppendedSamples

	out <- sm.memSeries
	out <- sm.memSeriesCreatedTotal
	out <- sm.memSeriesRemovedTotal

	out <- sm.tsdbWalReplayUnknownRefsTotal
	out <- sm.tsdbWblReplayUnknownRefsTotal

	out <- sm.tsdbHeadStatisticsLastUpdate
	out <- sm.tsdbHeadStatisticsTimeToUpdate
}

func (sm *TSDBMetrics) Collect(out chan<- prometheus.Metric) {
	data := sm.regs.BuildMetricFamiliesPerTenant()

	data.SendSumOfCounters(out, sm.tsdbCompactionsTotal, "prometheus_tsdb_compactions_total")
	data.SendSumOfHistograms(out, sm.tsdbCompactionDuration, "prometheus_tsdb_compaction_duration_seconds")
	data.SendSumOfSummaries(out, sm.tsdbFsyncDuration, "prometheus_tsdb_wal_fsync_duration_seconds")
	data.SendSumOfCounters(out, sm.tsdbPageFlushes, "prometheus_tsdb_wal_page_flushes_total")
	data.SendSumOfCounters(out, sm.tsdbPageCompletions, "prometheus_tsdb_wal_completed_pages_total")
	data.SendSumOfCounters(out, sm.tsdbWALTruncateFail, "prometheus_tsdb_wal_truncations_failed_total")
	data.SendSumOfCounters(out, sm.tsdbWALTruncateTotal, "prometheus_tsdb_wal_truncations_total")
	data.SendSumOfSummaries(out, sm.tsdbWALTruncateDuration, "prometheus_tsdb_wal_truncate_duration_seconds")
	data.SendSumOfCounters(out, sm.tsdbWALCorruptionsTotal, "prometheus_tsdb_wal_corruptions_total")
	data.SendSumOfCounters(out, sm.tsdbWALWritesFailed, "prometheus_tsdb_wal_writes_failed_total")
	data.SendSumOfCounters(out, sm.tsdbHeadTruncateFail, "prometheus_tsdb_head_truncations_failed_total")
	data.SendSumOfCounters(out, sm.tsdbHeadTruncateTotal, "prometheus_tsdb_head_truncations_total")
	data.SendSumOfSummaries(out, sm.tsdbHeadGcDuration, "prometheus_tsdb_head_gc_duration_seconds")
	data.SendSumOfGauges(out, sm.tsdbActiveAppenders, "prometheus_tsdb_head_active_appenders")
	data.SendSumOfCounters(out, sm.tsdbSeriesNotFound, "prometheus_tsdb_head_series_not_found_total")
	data.SendSumOfGauges(out, sm.tsdbChunks, "prometheus_tsdb_head_chunks")
	data.SendSumOfCountersPerTenant(out, sm.tsdbChunksCreatedTotal, "prometheus_tsdb_head_chunks_created_total")
	data.SendSumOfCountersPerTenant(out, sm.tsdbChunksRemovedTotal, "prometheus_tsdb_head_chunks_removed_total")
	data.SendSumOfCounters(out, sm.tsdbMmapChunkCorruptionTotal, "prometheus_tsdb_mmap_chunk_corruptions_total")
	data.SendSumOfCountersWithLabels(out, sm.tsdbMmapChunkQueueOperationsTotal, "prometheus_tsdb_chunk_write_queue_operations_total", "operation")
	data.SendSumOfCounters(out, sm.tsdbMmapChunksTotal, "prometheus_tsdb_mmap_chunks_total")
	data.SendSumOfHistograms(out, sm.tsdbOOOHistogram, "prometheus_tsdb_sample_ooo_delta")
	data.SendSumOfGauges(out, sm.tsdbLoadedBlocks, "prometheus_tsdb_blocks_loaded")
	data.SendSumOfGaugesPerTenant(out, sm.tsdbSymbolTableSize, "prometheus_tsdb_symbol_table_size_bytes")
	data.SendSumOfCounters(out, sm.tsdbReloads, "prometheus_tsdb_reloads_total")
	data.SendSumOfCounters(out, sm.tsdbReloadsFailed, "prometheus_tsdb_reloads_failures_total")
	data.SendSumOfCounters(out, sm.tsdbTimeRetentionCount, "prometheus_tsdb_time_retentions_total")
	data.SendSumOfGaugesPerTenant(out, sm.tsdbBlocksBytes, "prometheus_tsdb_storage_blocks_bytes")
	data.SendSumOfCounters(out, sm.checkpointDeleteFail, "prometheus_tsdb_checkpoint_deletions_failed_total")
	data.SendSumOfCounters(out, sm.checkpointDeleteTotal, "prometheus_tsdb_checkpoint_deletions_total")
	data.SendSumOfCounters(out, sm.checkpointCreationFail, "prometheus_tsdb_checkpoint_creations_failed_total")
	data.SendSumOfCounters(out, sm.checkpointCreationTotal, "prometheus_tsdb_checkpoint_creations_total")
	data.SendSumOfCountersPerTenant(out, sm.tsdbExemplarsTotal, "prometheus_tsdb_exemplar_exemplars_appended_total")
	data.SendSumOfGauges(out, sm.tsdbExemplarsInStorage, "prometheus_tsdb_exemplar_exemplars_in_storage")
	data.SendSumOfGaugesPerTenant(out, sm.tsdbExemplarSeriesInStorage, "prometheus_tsdb_exemplar_series_with_exemplars_in_storage")
	data.SendSumOfGaugesPerTenant(out, sm.tsdbExemplarLastTs, "prometheus_tsdb_exemplar_last_exemplars_timestamp_seconds")
	data.SendSumOfCounters(out, sm.tsdbExemplarsOutOfOrder, "prometheus_tsdb_exemplar_out_of_order_exemplars_total")
	data.SendSumOfCountersPerTenant(out, sm.tsdbOOOAppendedSamples, "prometheus_tsdb_head_out_of_order_samples_appended_total")
	data.SendSumOfGauges(out, sm.memSeries, "prometheus_tsdb_head_series")
	data.SendSumOfCountersPerTenant(out, sm.memSeriesCreatedTotal, "prometheus_tsdb_head_series_created_total")
	data.SendSumOfCountersPerTenant(out, sm.memSeriesRemovedTotal, "prometheus_tsdb_head_series_removed_total")
	data.SendSumOfCountersPerTenant(out, sm.tsdbWalReplayUnknownRefsTotal, "prometheus_tsdb_wal_replay_unknown_refs_total", dskit_metrics.WithLabels("type"))
	data.SendSumOfCountersPerTenant(out, sm.tsdbWblReplayUnknownRefsTotal, "prometheus_tsdb_wbl_replay_unknown_refs_total", dskit_metrics.WithLabels("type"))
	data.SendMaxOfGaugesPerTenant(out, sm.tsdbHeadStatisticsLastUpdate, "prometheus_tsdb_head_statistics_last_update_timestamp_seconds")
	data.SendMaxOfGaugesPerTenant(out, sm.tsdbHeadStatisticsTimeToUpdate, "prometheus_tsdb_head_statistics_time_to_update_seconds")
}

func (sm *TSDBMetrics) RegistryForTenant(userID string) *prometheus.Registry {
	return sm.regs.GetRegistryForTenant(userID)
}

func (sm *TSDBMetrics) SetRegistryForTenant(userID string, registry *prometheus.Registry) {
	sm.regs.AddTenantRegistry(userID, registry)
}

func (sm *TSDBMetrics) RemoveRegistryForTenant(userID string) {
	sm.regs.RemoveTenantRegistry(userID, false)
}
