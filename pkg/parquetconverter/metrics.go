// SPDX-License-Identifier: AGPL-3.0-only

package parquetconverter

import (
	"time"

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
		Name:                            "cortex_parquet_converter_conversion_duration_seconds",
		Help:                            "Time taken to convert a block to parquet format",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: 1 * time.Hour,
	})

	m.tenantsDiscovered = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_parquet_converter_tenants_discovered",
		Help: "Number of tenants discovered by the parquet converter",
	})

	return m
}
