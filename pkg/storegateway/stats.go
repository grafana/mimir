// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/thanos-io/thanos/blob/main/pkg/store/bucket.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Thanos Authors.

package storegateway

import (
	"sync"
	"time"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

// queryStats holds query statistics. This data structure is NOT concurrency safe.
type queryStats struct {
	blocksQueried            int
	blocksQueriedByBlockMeta map[blockQueriedMeta]int

	postingsTouched          int
	postingsTouchedSizeSum   int
	postingsToFetch          int
	postingsFetched          int
	postingsFetchedSizeSum   int
	postingsFetchCount       int
	postingsFetchDurationSum time.Duration

	cachedPostingsCompressions         int
	cachedPostingsCompressionErrors    int
	cachedPostingsOriginalSizeSum      int
	cachedPostingsCompressedSizeSum    int
	cachedPostingsCompressionTimeSum   time.Duration
	cachedPostingsDecompressions       int
	cachedPostingsDecompressionErrors  int
	cachedPostingsDecompressionTimeSum time.Duration

	seriesProcessed        int
	seriesProcessedSizeSum int
	seriesOmitted          int
	seriesFetched          int
	seriesFetchedSizeSum   int
	seriesFetchDurationSum time.Duration
	seriesRefetches        int

	seriesHashCacheRequests int
	seriesHashCacheHits     int

	chunksTouched          int
	chunksTouchedSizeSum   int
	chunksFetched          int
	chunksFetchedSizeSum   int
	chunksRefetched        int
	chunksRefetchedSizeSum int
	chunksProcessed        int
	chunksProcessedSizeSum int
	chunksReturned         int
	chunksReturnedSizeSum  int

	mergedSeriesCount int
	mergedChunksCount int

	// The number of batches the Series() request has been split into.
	streamingSeriesBatchCount int

	// The total time spent loading batches.
	streamingSeriesBatchLoadDuration time.Duration

	// The total time spent waiting until the next batch is loaded, once the store-gateway was
	// ready to send it to the client.
	streamingSeriesWaitBatchLoadedDuration time.Duration

	// The Series() request timing breakdown.
	streamingSeriesExpandPostingsDuration       time.Duration
	streamingSeriesEncodeResponseDuration       time.Duration
	streamingSeriesSendResponseDuration         time.Duration
	streamingSeriesIndexHeaderLoadDuration      time.Duration
	streamingSeriesConcurrencyLimitWaitDuration time.Duration

	// streamingSeriesAmbientTime is the total wall clock time spent serving the request. It includes all other durations.
	streamingSeriesAmbientTime time.Duration
}

func newQueryStats() *queryStats {
	return &queryStats{
		blocksQueriedByBlockMeta: make(map[blockQueriedMeta]int),
	}
}

// blockQueriedMeta encapsulate a block's thanos source, compaction level, and if it
// was created from out-or-order samples
type blockQueriedMeta struct {
	source     block.SourceType
	level      int
	outOfOrder bool
}

func newBlockQueriedMeta(meta *block.Meta) blockQueriedMeta {
	return blockQueriedMeta{
		source:     meta.Thanos.Source,
		level:      meta.Compaction.Level,
		outOfOrder: meta.Compaction.FromOutOfOrder(),
	}
}

func (s queryStats) merge(o *queryStats) *queryStats {
	s.blocksQueried += o.blocksQueried
	for m, count := range o.blocksQueriedByBlockMeta {
		s.blocksQueriedByBlockMeta[m] += count
	}

	s.postingsTouched += o.postingsTouched
	s.postingsTouchedSizeSum += o.postingsTouchedSizeSum
	s.postingsToFetch += o.postingsToFetch
	s.postingsFetched += o.postingsFetched
	s.postingsFetchedSizeSum += o.postingsFetchedSizeSum
	s.postingsFetchCount += o.postingsFetchCount
	s.postingsFetchDurationSum += o.postingsFetchDurationSum

	s.cachedPostingsCompressions += o.cachedPostingsCompressions
	s.cachedPostingsCompressionErrors += o.cachedPostingsCompressionErrors
	s.cachedPostingsOriginalSizeSum += o.cachedPostingsOriginalSizeSum
	s.cachedPostingsCompressedSizeSum += o.cachedPostingsCompressedSizeSum
	s.cachedPostingsCompressionTimeSum += o.cachedPostingsCompressionTimeSum
	s.cachedPostingsDecompressions += o.cachedPostingsDecompressions
	s.cachedPostingsDecompressionErrors += o.cachedPostingsDecompressionErrors
	s.cachedPostingsDecompressionTimeSum += o.cachedPostingsDecompressionTimeSum

	s.seriesProcessed += o.seriesProcessed
	s.seriesProcessedSizeSum += o.seriesProcessedSizeSum
	s.seriesOmitted += o.seriesOmitted
	s.seriesFetched += o.seriesFetched
	s.seriesFetchedSizeSum += o.seriesFetchedSizeSum
	s.seriesFetchDurationSum += o.seriesFetchDurationSum
	s.seriesRefetches += o.seriesRefetches

	s.seriesHashCacheRequests += o.seriesHashCacheRequests
	s.seriesHashCacheHits += o.seriesHashCacheHits

	s.chunksTouched += o.chunksTouched
	s.chunksTouchedSizeSum += o.chunksTouchedSizeSum
	s.chunksFetched += o.chunksFetched
	s.chunksFetchedSizeSum += o.chunksFetchedSizeSum
	s.chunksRefetched += o.chunksRefetched
	s.chunksRefetchedSizeSum += o.chunksRefetchedSizeSum
	s.chunksProcessed += o.chunksProcessed
	s.chunksProcessedSizeSum += o.chunksProcessedSizeSum
	s.chunksReturned += o.chunksReturned
	s.chunksReturnedSizeSum += o.chunksReturnedSizeSum

	s.mergedSeriesCount += o.mergedSeriesCount
	s.mergedChunksCount += o.mergedChunksCount

	s.streamingSeriesBatchCount += o.streamingSeriesBatchCount
	s.streamingSeriesBatchLoadDuration += o.streamingSeriesBatchLoadDuration
	s.streamingSeriesWaitBatchLoadedDuration += o.streamingSeriesWaitBatchLoadedDuration
	s.streamingSeriesExpandPostingsDuration += o.streamingSeriesExpandPostingsDuration
	s.streamingSeriesEncodeResponseDuration += o.streamingSeriesEncodeResponseDuration
	s.streamingSeriesSendResponseDuration += o.streamingSeriesSendResponseDuration
	s.streamingSeriesAmbientTime += o.streamingSeriesAmbientTime

	s.streamingSeriesIndexHeaderLoadDuration += o.streamingSeriesIndexHeaderLoadDuration
	s.streamingSeriesConcurrencyLimitWaitDuration += o.streamingSeriesConcurrencyLimitWaitDuration

	return &s
}

// safeQueryStats wraps queryStats adding functions manipulate the statistics while holding a lock.
type safeQueryStats struct {
	unsafeStatsMx sync.Mutex
	unsafeStats   *queryStats
}

func newSafeQueryStats() *safeQueryStats {
	return &safeQueryStats{
		unsafeStats: newQueryStats(),
	}
}

// update the statistics while holding the lock.
func (s *safeQueryStats) update(fn func(stats *queryStats)) {
	s.unsafeStatsMx.Lock()
	defer s.unsafeStatsMx.Unlock()

	fn(s.unsafeStats)
}

// merge the statistics while holding the lock. Statistics are merged in the receiver.
func (s *safeQueryStats) merge(o *queryStats) {
	s.unsafeStatsMx.Lock()
	defer s.unsafeStatsMx.Unlock()

	s.unsafeStats = s.unsafeStats.merge(o)
}

// export returns a copy of the internal statistics.
func (s *safeQueryStats) export() *queryStats {
	s.unsafeStatsMx.Lock()
	defer s.unsafeStatsMx.Unlock()

	copied := *s.unsafeStats
	return &copied
}

// seriesAndChunksCount return the value of mergedSeriesCount and mergedChunksCount fields.
func (s *safeQueryStats) seriesAndChunksCount() (seriesCount, chunksCount int) {
	s.unsafeStatsMx.Lock()
	defer s.unsafeStatsMx.Unlock()

	return s.unsafeStats.mergedSeriesCount, s.unsafeStats.mergedChunksCount
}
