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
		// 1 0.008s  (8ms)
		// 2 0.032s  (32ms)
		// 3 0.128s  (128ms)
		// 4 0.512s  (512ms)
		// 5 2.048s  (2048ms)
		// 6 8.192s  (8192ms)
		// 7 32.768s (32768ms)
		// 8 +Inf
		Buckets: prometheus.ExponentialBuckets(0.008, 4, 7),
	})

	m.tenantsDiscovered = promauto.With(reg).NewGauge(prometheus.GaugeOpts{
		Name: "cortex_parquet_converter_tenants_discovered",
		Help: "Number of tenants discovered by the parquet converter",
	})

	return m
}
