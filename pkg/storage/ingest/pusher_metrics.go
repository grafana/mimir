// SPDX-License-Identifier: AGPL-3.0-only

package ingest

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// pusherConsumerMetrics holds the metrics for the pusherConsumer.
type pusherConsumerMetrics struct {
	processingTimeSeconds prometheus.Observer

	storagePusherMetrics *storagePusherMetrics
}

// newPusherConsumerMetrics creates a new pusherConsumerMetrics instance.
func newPusherConsumerMetrics(reg prometheus.Registerer) *pusherConsumerMetrics {
	return &pusherConsumerMetrics{
		storagePusherMetrics: newStoragePusherMetrics(reg),
		processingTimeSeconds: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                            "cortex_ingest_storage_reader_records_processing_time_seconds",
			Help:                            "Time taken to process a batch of fetched records. Fetched records are effectively a set of WriteRequests read from Kafka.",
			NativeHistogramBucketFactor:     1.1,
			NativeHistogramMaxBucketNumber:  100,
			NativeHistogramMinResetDuration: 1 * time.Hour,
			Buckets:                         prometheus.DefBuckets,
		}),
	}
}

// storagePusherMetrics holds the metrics for both the sequentialStoragePusher and the parallelStoragePusher.
type storagePusherMetrics struct {
	// batchAge is not really important unless we're pushing many things at once, so it's only used as part of parallelStoragePusher.
	batchAge             prometheus.Histogram
	processingTime       *prometheus.HistogramVec
	timeSeriesPerFlush   prometheus.Histogram
	batchingQueueMetrics *batchingQueueMetrics
	clientErrRequests    prometheus.Counter
	serverErrRequests    prometheus.Counter
	totalRequests        prometheus.Counter
}

// newStoragePusherMetrics creates a new storagePusherMetrics instance.
func newStoragePusherMetrics(reg prometheus.Registerer) *storagePusherMetrics {
	errRequestsCounter := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_ingest_storage_reader_requests_failed_total",
		Help: "Number of write requests which caused errors while processing. Client errors are errors such as tenant limits and samples out of bounds. Server errors indicate internal recoverable errors.",
	}, []string{"cause"})

	return &storagePusherMetrics{
		batchingQueueMetrics: newBatchingQueueMetrics(reg),
		batchAge: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_ingest_storage_reader_pusher_batch_age_seconds",
			Help:                        "Age of the batch of samples that are being ingested by an ingestion shard. This is the time since adding the first sample to the batch. Higher values indicates that the batching queue is not processing fast enough or that the batches are not filling up fast enough.",
			NativeHistogramBucketFactor: 1.1,
		}),
		processingTime: promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
			Name:                        "cortex_ingest_storage_reader_pusher_processing_time_seconds",
			Help:                        "Time to ingest a batch of samples for timeseries or metadata by an ingestion shard. The 'batch_contents' label indicates the contents of the batch.",
			NativeHistogramBucketFactor: 1.1,
		}, []string{"content"}),
		timeSeriesPerFlush: promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
			Name:                        "cortex_ingest_storage_reader_pusher_timeseries_per_flush",
			Help:                        "Number of time series pushed in each batch to an ingestion shard. A lower number than -ingest-storage.kafka.ingestion-concurrency-batch-size indicates that shards are not filling up and may not be parallelizing ingestion as efficiently.",
			NativeHistogramBucketFactor: 1.1,
		}),
		clientErrRequests: errRequestsCounter.WithLabelValues("client"),
		serverErrRequests: errRequestsCounter.WithLabelValues("server"),
		totalRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_requests_total",
			Help: "Number of attempted write requests.",
		}),
	}
}

// batchingQueueMetrics holds the metrics for the batchingQueue.
type batchingQueueMetrics struct {
	flushTotal       prometheus.Counter
	flushErrorsTotal prometheus.Counter
}

// newBatchingQueueMetrics creates a new batchingQueueMetrics instance.
func newBatchingQueueMetrics(reg prometheus.Registerer) *batchingQueueMetrics {
	return &batchingQueueMetrics{
		flushTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_batching_queue_flush_total",
			Help: "Number of times a batch of samples is flushed to the storage.",
		}),
		flushErrorsTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_ingest_storage_reader_batching_queue_flush_errors_total",
			Help: "Number of errors encountered while flushing a batch of samples to the storage.",
		}),
	}
}
