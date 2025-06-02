// SPDX-License-Identifier: AGPL-3.0-only

package parquetconverter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type parquetConverterMetrics struct {
	blocksConverted       prometheus.Counter
	blocksConvertedFailed prometheus.Counter
	conversionDuration    prometheus.Histogram
	tenantsDiscovered     prometheus.Gauge
}

func newParquetConverterMetrics(reg prometheus.Registerer) parquetConverterMetrics {
	var m parquetConverterMetrics

	m.blocksConverted = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_parquet_converter_blocks_converted_total",
		Help: "Total number of blocks converted to parquet format",
	})

	m.blocksConvertedFailed = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_parquet_converter_blocks_converted_failed_total",
		Help: "Total number of blocks that failed to convert to parquet format",
	})

	m.conversionDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_parquet_converter_conversion_duration_seconds",
		Help: "Time taken to convert a block to parquet format",
		// Buckets
		// 1 0.500s  (500ms)
		// 2 1.000s  (1000ms)
		// 3 2.000s  (2000ms)
		// 4 4.000s  (4000ms)
		// 5 8.000s  (8000ms)
		// 6 16.000s  (16000ms)
		// 7 32.000s  (32000ms)
		// 8 64.000s  (64000ms)
		// 9 128.000s  (128000ms)
		// 10 256.000s  (256000ms)
		// 11 +Inf
		Buckets: prometheus.ExponentialBuckets(0.5, 2, 10),
	})

	m.tenantsDiscovered = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_parquet_converter_tenants_discovered",
		Help: "Number of tenants discovered by the parquet converter",
	})

	return m
}
