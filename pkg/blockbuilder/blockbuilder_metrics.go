package blockbuilder

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type blockBuilderMetrics struct {
	consumeCycleDuration     prometheus.Histogram
	consumeCycleFailures     prometheus.Counter
	processPartitionDuration prometheus.Histogram
	fetchRecordsTotal        prometheus.Counter
	fetchErrors              prometheus.Counter
	assignedPartitions       *prometheus.GaugeVec
	consumerLag              *prometheus.GaugeVec
}

func newBlockBuilderMetrics(reg prometheus.Registerer) blockBuilderMetrics {
	var m blockBuilderMetrics

	m.consumeCycleFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_blockbuilder_consume_cycle_failed_total",
		Help: "Total number of failed consume cycles.",
	})
	m.consumeCycleDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_blockbuilder_consume_cycle_duration_seconds",
		Help: "Time spent consuming a full cycle.",

		NativeHistogramBucketFactor: 1.1,
	})

	m.processPartitionDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:                        "cortex_blockbuilder_process_partition_duration_seconds",
		Help:                        "Time spent processing one partition.",
		NativeHistogramBucketFactor: 1.1,
	})

	m.fetchRecordsTotal = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_blockbuilder_fetch_records_total",
		Help: "Total number of records received by the consumer.",
	})
	m.fetchErrors = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_blockbuilder_fetch_errors_total",
		Help: "Total number of errors while fetching by the consumer.",
	})

	m.assignedPartitions = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_blockbuilder_assigned_partitions",
		Help: "The number of partitions currently assigned to this instance.",
	}, []string{"topic"})

	m.consumerLag = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_blockbuilder_consumer_lag",
		Help: "The per-partition lag instance needs to work through each cycle.",
	}, []string{"topic", "partition"})

	return m
}
