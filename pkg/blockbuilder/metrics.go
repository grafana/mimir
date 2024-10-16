// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type blockBuilderMetrics struct {
	consumeCycleDuration     prometheus.Histogram
	processPartitionDuration *prometheus.HistogramVec
	fetchErrors              *prometheus.CounterVec
	consumerLagRecords       *prometheus.GaugeVec
}

func newBlockBuilderMetrics(reg prometheus.Registerer) blockBuilderMetrics {
	var m blockBuilderMetrics

	m.consumeCycleDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_blockbuilder_consume_cycle_duration_seconds",
		Help: "Time spent consuming a full cycle.",

		NativeHistogramBucketFactor: 1.1,
	})

	m.processPartitionDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                        "cortex_blockbuilder_process_partition_duration_seconds",
		Help:                        "Time spent processing one partition.",
		NativeHistogramBucketFactor: 1.1,
	}, []string{"partition"})

	m.fetchErrors = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_blockbuilder_fetch_errors_total",
		Help: "Total number of errors while fetching by the consumer.",
	}, []string{"partition"})

	m.consumerLagRecords = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_blockbuilder_consumer_lag_records",
		Help: "The per-topic-partition number of records, instance needs to work through each cycle.",
	}, []string{"partition"})

	return m
}

type tsdbBuilderMetrics struct {
	processSamplesDiscarded  *prometheus.CounterVec
	compactAndUploadDuration *prometheus.HistogramVec
	compactAndUploadFailed   *prometheus.CounterVec
}

func newTSDBBBuilderMetrics(reg prometheus.Registerer) tsdbBuilderMetrics {
	var m tsdbBuilderMetrics

	m.processSamplesDiscarded = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_blockbuilder_tsdb_process_samples_discarded_total",
		Help: "The total number of samples that were discarded while processing records in one partition.",
	}, []string{"partition"})

	m.compactAndUploadDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                        "cortex_blockbuilder_tsdb_compact_and_upload_duration_seconds",
		Help:                        "Time spent compacting and uploading a tsdb of one partition.",
		NativeHistogramBucketFactor: 1.1,
	}, []string{"partition"})

	m.compactAndUploadFailed = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_blockbuilder_tsdb_compact_and_upload_failed_total",
		Help: "Total number of failures compacting and uploading a tsdb of one partition.",
	}, []string{"partition"})

	return m
}
