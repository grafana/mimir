package parquetconverter

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type parquetConverterMetrics struct {
	blocksConverted       prometheus.Counter
	failedBlocksConverted prometheus.Counter
	conversionDuration    prometheus.Histogram // Add this new metric
}

func newParquetConverterMetrics(reg prometheus.Registerer) parquetConverterMetrics {
	var m parquetConverterMetrics

	m.blocksConverted = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "parquet_converter_blocks_converted_total",
		Help: "Total number of blocks converted to parquet format",
	})

	m.failedBlocksConverted = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "parquet_converter_blocks_conversion_failed_total",
		Help: "Total number of blocks that failed to convert to parquet format",
	})

	m.conversionDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "parquet_converter_conversion_duration_seconds",
		Help:    "Time taken to convert a block to parquet format",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10), // From 100ms to ~102s
	})

	return m
}
