// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/limiter/query_limiter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package limiter

import (
	"context"
	"sync"

	"github.com/prometheus/prometheus/model/labels"
	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/validation"
)

type queryLimiterCtxKey struct{}

const (
	cardinalityStrategy = "Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query"
)

var (
	ctxKey = &queryLimiterCtxKey{}
)

type QueryLimiter struct {
	seriesMx        sync.Mutex
	canonicalLabels map[uint64]labels.Labels // Stores canonical labels for both deduplication and counting

	chunkBytesCount     atomic.Int64
	chunkCount          atomic.Int64
	estimatedChunkCount atomic.Int64

	maxSeriesPerQuery          int
	maxChunkBytesPerQuery      int
	maxChunksPerQuery          int
	maxEstimatedChunksPerQuery int

	queryMetrics *stats.QueryMetrics
}

// NewQueryLimiter makes a new per-query limiter. Each query limiter is configured using the
// maxSeriesPerQuery, maxChunkBytesPerQuery, maxChunksPerQuery and maxEstimatedChunksPerQuery limits.
func NewQueryLimiter(maxSeriesPerQuery, maxChunkBytesPerQuery, maxChunksPerQuery int, maxEstimatedChunksPerQuery int, queryMetrics *stats.QueryMetrics) *QueryLimiter {
	return &QueryLimiter{
		seriesMx:        sync.Mutex{},
		canonicalLabels: map[uint64]labels.Labels{},

		maxSeriesPerQuery:          maxSeriesPerQuery,
		maxChunkBytesPerQuery:      maxChunkBytesPerQuery,
		maxChunksPerQuery:          maxChunksPerQuery,
		maxEstimatedChunksPerQuery: maxEstimatedChunksPerQuery,

		queryMetrics: queryMetrics,
	}
}

func AddQueryLimiterToContext(ctx context.Context, limiter *QueryLimiter) context.Context {
	return context.WithValue(ctx, ctxKey, limiter)
}

// QueryLimiterFromContextWithFallback returns a QueryLimiter from the current context.
// If there is not a QueryLimiter on the context it will return a new no-op limiter.
func QueryLimiterFromContextWithFallback(ctx context.Context) *QueryLimiter {
	ql, ok := ctx.Value(ctxKey).(*QueryLimiter)
	if !ok {
		// If there's no limiter return a new unlimited limiter as a fallback
		ql = NewQueryLimiter(0, 0, 0, 0, nil)
	}
	return ql
}

// AddSeries adds the input series and returns the canonical labels for deduplication.
// If the series has been seen before, it returns the previously stored labels instead of the new ones,
// allowing callers to deduplicate memory consumption for series that appear multiple times.
// This is useful when the same series is returned from multiple sources (e.g., from different ingesters
// due to replication, or from different blocks in the store-gateway).
//
// Returns:
//   - canonicalLabels: the labels to use (either the input labels if new, or previously stored labels if duplicate)
//   - error: validation.LimitError if the series limit is exceeded
func (ql *QueryLimiter) AddSeries(seriesLabels labels.Labels, tracker *MemoryConsumptionTracker) (labels.Labels, error) {
	// If the max series is unlimited just return without managing map
	if ql.maxSeriesPerQuery == 0 {
		return seriesLabels, nil
	}
	fingerprint := seriesLabels.Hash()

	ql.seriesMx.Lock()
	defer ql.seriesMx.Unlock()

	// Check if we've seen this series before
	var canonicalLabels labels.Labels

	if existingLabels, exists := ql.canonicalLabels[fingerprint]; exists {
		// This is a duplicate - return the canonical labels we stored earlier
		canonicalLabels = existingLabels
	} else {
		canonicalLabels = seriesLabels
		// This is a new series - track it in the map.
		// We capture the count before and after adding to detect when we first exceed the limit.
		uniqueSeriesBefore := len(ql.canonicalLabels)
		ql.canonicalLabels[fingerprint] = seriesLabels
		uniqueSeriesAfter := len(ql.canonicalLabels)

		// Only increment the metric the first time we exceed the limit (when uniqueSeriesBefore was still within limit)
		if uniqueSeriesAfter > ql.maxSeriesPerQuery && uniqueSeriesBefore <= ql.maxSeriesPerQuery {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxSeries).Inc()
		}
	}

	// Single limit check at the end - applies to both new series and duplicates
	// This ensures that once a query exceeds the limit, all subsequent series (including duplicates) are rejected
	if len(ql.canonicalLabels) > ql.maxSeriesPerQuery {
		return canonicalLabels, NewMaxSeriesHitLimitError(uint64(ql.maxSeriesPerQuery))
	}
	if err := tracker.IncreaseMemoryConsumptionForLabels(canonicalLabels); err != nil {
		return canonicalLabels, err
	}

	return canonicalLabels, nil
}

// uniqueSeriesCount returns the count of unique series seen by this query limiter.
func (ql *QueryLimiter) uniqueSeriesCount() int {
	ql.seriesMx.Lock()
	defer ql.seriesMx.Unlock()
	return len(ql.canonicalLabels)
}

// AddChunkBytes adds the input chunk size in bytes and returns an error if the limit is reached.
func (ql *QueryLimiter) AddChunkBytes(chunkSizeInBytes int) validation.LimitError {
	if ql.maxChunkBytesPerQuery == 0 {
		return nil
	}

	totalBytes := ql.chunkBytesCount.Add(int64(chunkSizeInBytes))

	if totalBytes > int64(ql.maxChunkBytesPerQuery) {
		if totalBytes-int64(chunkSizeInBytes) <= int64(ql.maxChunkBytesPerQuery) {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxChunkBytes).Inc()
		}

		return NewMaxChunkBytesHitLimitError(uint64(ql.maxChunkBytesPerQuery))
	}
	return nil
}

func (ql *QueryLimiter) AddChunks(count int) validation.LimitError {
	if ql.maxChunksPerQuery == 0 {
		return nil
	}

	totalChunks := ql.chunkCount.Add(int64(count))

	if totalChunks > int64(ql.maxChunksPerQuery) {
		if totalChunks-int64(count) <= int64(ql.maxChunksPerQuery) {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxChunks).Inc()
		}

		return NewMaxChunksPerQueryLimitError(uint64(ql.maxChunksPerQuery))
	}
	return nil
}

func (ql *QueryLimiter) AddEstimatedChunks(count int) validation.LimitError {
	if ql.maxEstimatedChunksPerQuery == 0 {
		return nil
	}

	totalChunks := ql.estimatedChunkCount.Add(int64(count))

	if totalChunks > int64(ql.maxEstimatedChunksPerQuery) {
		if totalChunks-int64(count) <= int64(ql.maxEstimatedChunksPerQuery) {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxEstimatedChunks).Inc()
		}

		return NewMaxEstimatedChunksPerQueryLimitError(uint64(ql.maxEstimatedChunksPerQuery))
	}
	return nil
}
