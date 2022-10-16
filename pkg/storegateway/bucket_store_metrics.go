// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storegateway/bucket_store_metrics.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/grafana/mimir/pkg/storegateway/indexheader"
	"github.com/grafana/mimir/pkg/util/extprom"
)

// BucketStoreMetrics holds all the metrics tracked by BucketStore. These metrics
// MUST be monotonic (counter, summary, histogram) because a single metrics instance
// can be passed to multiple BucketStore and metrics MUST be correct even after a
// BucketStore is offloaded.
type BucketStoreMetrics struct {
	blockLoads            prometheus.Counter
	blockLoadFailures     prometheus.Counter
	blockDrops            prometheus.Counter
	blockDropFailures     prometheus.Counter
	seriesDataTouched     *prometheus.SummaryVec
	seriesDataFetched     *prometheus.SummaryVec
	seriesDataSizeTouched *prometheus.SummaryVec
	seriesDataSizeFetched *prometheus.SummaryVec
	seriesBlocksQueried   prometheus.Summary
	seriesGetAllDuration  prometheus.Histogram
	seriesMergeDuration   prometheus.Histogram
	resultSeriesCount     prometheus.Summary
	chunkSizeBytes        prometheus.Histogram
	queriesDropped        *prometheus.CounterVec
	seriesRefetches       prometheus.Counter

	cachedPostingsCompressions           *prometheus.CounterVec
	cachedPostingsCompressionErrors      *prometheus.CounterVec
	cachedPostingsCompressionTimeSeconds *prometheus.CounterVec
	cachedPostingsOriginalSizeBytes      prometheus.Counter
	cachedPostingsCompressedSizeBytes    prometheus.Counter

	seriesHashCacheRequests prometheus.Counter
	seriesHashCacheHits     prometheus.Counter

	seriesFetchDuration   prometheus.Histogram
	postingsFetchDuration prometheus.Histogram

	indexHeaderReaderMetrics *indexheader.ReaderPoolMetrics
}

func NewBucketStoreMetrics(reg prometheus.Registerer) *BucketStoreMetrics {
	var m BucketStoreMetrics

	m.blockLoads = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_block_loads_total",
		Help: "Total number of remote block loading attempts.",
	})
	m.blockLoadFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_block_load_failures_total",
		Help: "Total number of failed remote block loading attempts.",
	})
	m.blockDrops = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_block_drops_total",
		Help: "Total number of local blocks that were dropped.",
	})
	m.blockDropFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_block_drop_failures_total",
		Help: "Total number of local blocks that failed to be dropped.",
	})

	m.seriesDataTouched = promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "cortex_bucket_store_series_data_touched",
		Help: "How many items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataFetched = promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "cortex_bucket_store_series_data_fetched",
		Help: "How many items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesDataSizeTouched = promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "cortex_bucket_store_series_data_size_touched_bytes",
		Help: "Size of all items of a data type in a block were touched for a single series request.",
	}, []string{"data_type"})
	m.seriesDataSizeFetched = promauto.With(reg).NewSummaryVec(prometheus.SummaryOpts{
		Name: "cortex_bucket_store_series_data_size_fetched_bytes",
		Help: "Size of all items of a data type in a block were fetched for a single series request.",
	}, []string{"data_type"})

	m.seriesBlocksQueried = promauto.With(reg).NewSummary(prometheus.SummaryOpts{
		Name: "cortex_bucket_store_series_blocks_queried",
		Help: "Number of blocks in a bucket store that were touched to satisfy a query.",
	})
	m.seriesGetAllDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_bucket_store_series_get_all_duration_seconds",
		Help:    "Time it takes until all per-block prepares and loads for a query are finished.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.seriesMergeDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_bucket_store_series_merge_duration_seconds",
		Help:    "Time it takes to merge sub-results from all queried blocks into a single result.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.seriesRefetches = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_series_refetches_total",
		Help: "Total number of cases where the built-in max series size was not enough to fetch series from index, resulting in refetch.",
	})
	m.resultSeriesCount = promauto.With(reg).NewSummary(prometheus.SummaryOpts{
		Name: "cortex_bucket_store_series_result_series",
		Help: "Number of series observed in the final result of a query.",
	})
	m.queriesDropped = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_bucket_store_queries_dropped_total",
		Help: "Number of queries that were dropped due to the max chunks per query limit.",
	}, []string{"reason"})

	m.cachedPostingsCompressions = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_bucket_store_cached_postings_compressions_total",
		Help: "Number of postings compressions and decompressions when storing to index cache.", // TODO also decompressions?
	}, []string{"op"})
	m.cachedPostingsCompressions.WithLabelValues(labelEncode)
	m.cachedPostingsCompressions.WithLabelValues(labelDecode)

	m.cachedPostingsCompressionErrors = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_bucket_store_cached_postings_compression_errors_total",
		Help: "Number of postings compression and decompression errors.",
	}, []string{"op"})
	m.cachedPostingsCompressionErrors.WithLabelValues(labelEncode)
	m.cachedPostingsCompressionErrors.WithLabelValues(labelDecode)

	m.cachedPostingsCompressionTimeSeconds = promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
		Name: "cortex_bucket_store_cached_postings_compression_time_seconds",
		Help: "Time spent compressing and decompressing postings when storing to / reading from postings cache.",
	}, []string{"op"})
	m.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelEncode)
	m.cachedPostingsCompressionTimeSeconds.WithLabelValues(labelDecode)

	m.cachedPostingsOriginalSizeBytes = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_cached_postings_original_size_bytes_total",
		Help: "Original size of postings stored into cache.",
	})
	m.cachedPostingsCompressedSizeBytes = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_cached_postings_compressed_size_bytes_total",
		Help: "Compressed size of postings stored into cache.",
	})

	m.seriesFetchDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_bucket_store_cached_series_fetch_duration_seconds",
		Help:    "Time it takes to fetch series to respond a request sent to store-gateway. It includes both the time to fetch it from cache and from storage in case of cache misses.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})
	m.postingsFetchDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name:    "cortex_bucket_store_cached_postings_fetch_duration_seconds",
		Help:    "Time it takes to fetch postings to respond a request sent to store-gateway. It includes both the time to fetch it from cache and from storage in case of cache misses.",
		Buckets: []float64{0.001, 0.01, 0.1, 0.3, 0.6, 1, 3, 6, 9, 20, 30, 60, 90, 120},
	})

	m.seriesHashCacheRequests = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_series_hash_cache_requests_total",
		Help: "Total number of fetch attempts to the in-memory series hash cache.",
	})
	m.seriesHashCacheHits = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_bucket_store_series_hash_cache_hits_total",
		Help: "Total number of fetch hits to the in-memory series hash cache.",
	})

	m.chunkSizeBytes = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_bucket_store_sent_chunk_size_bytes",
		Help: "Size in bytes of the chunks for the single series, which is adequate to the gRPC message size sent to querier.",
		Buckets: []float64{
			32, 256, 512, 1024, 32 * 1024, 256 * 1024, 512 * 1024, 1024 * 1024, 32 * 1024 * 1024, 256 * 1024 * 1024, 512 * 1024 * 1024,
		},
	})

	m.indexHeaderReaderMetrics = indexheader.NewReaderPoolMetrics(extprom.WrapRegistererWithPrefix("cortex_bucket_store_", reg))

	return &m
}
