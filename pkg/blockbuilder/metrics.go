// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/util"
)

type blockBuilderMetrics struct {
	consumeJobDuration       *prometheus.HistogramVec
	fetchErrors              *prometheus.CounterVec
	blockCounts              *prometheus.CounterVec
	invalidClusterValidation *prometheus.CounterVec
}

func newBlockBuilderMetrics(reg prometheus.Registerer) blockBuilderMetrics {
	var m blockBuilderMetrics

	m.consumeJobDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name: "cortex_blockbuilder_consume_job_duration_seconds",
		Help: "Time spent consuming a job.",

		NativeHistogramBucketFactor: 1.1,
	}, []string{"success"})

	m.fetchErrors = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_blockbuilder_fetch_errors_total",
		Help: "Total number of errors while fetching by the consumer.",
	}, []string{"partition"})

	// block_time can be "next", "current" or "previous".
	// If the block belongs to the current 2h block range, it goes in "current".
	// "next" or "previous" are used for the blocks that are not in the current 2h block range.
	m.blockCounts = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_blockbuilder_blocks_produced_total",
		Help: "Total number of blocks produced for specific block ranges (next, current, previous).",
	}, []string{"block_time"})

	m.invalidClusterValidation = util.NewRequestInvalidClusterValidationLabelsTotalCounter(reg, "block-builder", util.GRPCProtocol)
	return m
}

type tsdbBuilderMetrics struct {
	processSamplesDiscarded            *prometheus.CounterVec
	compactAndUploadDuration           *prometheus.HistogramVec
	compactAndUploadFailed             *prometheus.CounterVec
	lastSuccessfulCompactAndUploadTime *prometheus.GaugeVec
}

func newTSDBBuilderMetrics(reg prometheus.Registerer) tsdbBuilderMetrics {
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

	m.lastSuccessfulCompactAndUploadTime = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_blockbuilder_tsdb_last_successful_compact_and_upload_timestamp_seconds",
		Help: "Unix timestamp (in seconds) of the last successful tsdb block compacted and uploaded to the object storage on a partition.",
	}, []string{"partition"})

	return m
}
