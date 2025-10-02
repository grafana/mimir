// SPDX-License-Identifier: AGPL-3.0-only

package parquetconverter

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type parquetConverterMetrics struct {
	blocksConverted       *prometheus.CounterVec
	blocksConvertedFailed *prometheus.CounterVec
	conversionDuration    *prometheus.HistogramVec
	tenantsDiscovered     prometheus.Gauge
	queueSize             *prometheus.GaugeVec
	queueWaitTime         *prometheus.HistogramVec
	queueItemsEnqueued    *prometheus.CounterVec
	queueItemsProcessed   *prometheus.CounterVec
	queueItemsDropped     *prometheus.CounterVec
}

func newParquetConverterMetrics(reg prometheus.Registerer) parquetConverterMetrics {
	var m parquetConverterMetrics

	m.blocksConverted = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_parquet_converter_blocks_converted_total",
		Help: "Total number of blocks converted to parquet format",
	}, []string{"user"})

	m.blocksConvertedFailed = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_parquet_converter_blocks_converted_failed_total",
		Help: "Total number of blocks that failed to convert to parquet format",
	}, []string{"user"})

	m.conversionDuration = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "cortex_parquet_converter_conversion_duration_seconds",
		Help:                            "Time taken to convert a block to parquet format",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
	}, []string{"user"})

	m.tenantsDiscovered = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_parquet_converter_tenants_discovered",
		Help: "Number of tenants discovered by the parquet converter",
	})

	m.queueSize = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_parquet_converter_queue_size",
		Help: "Current number of blocks in the conversion queue",
	}, []string{"lb_strategy"}) // The meaning of the queue size differs depending on the load balancing strategy

	m.queueWaitTime = promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "cortex_parquet_converter_queue_wait_time_seconds",
		Help:                            "Time blocks spend waiting in the conversion queue",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
	}, []string{"user"})

	m.queueItemsEnqueued = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_parquet_converter_queue_items_enqueued_total",
		Help: "Total number of blocks enqueued for conversion",
	}, []string{"user"})

	m.queueItemsProcessed = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_parquet_converter_queue_items_processed_total",
		Help: "Total number of blocks processed from the conversion queue",
	}, []string{"user"})

	m.queueItemsDropped = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_parquet_converter_queue_items_dropped_total",
		Help: "Total number of blocks dropped from the conversion queue when full",
	}, []string{"user"})

	return m
}
