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
	uniqueSeriesMx sync.Mutex
	uniqueSeries   map[uint64]labels.Labels
	conflictSeries map[uint64][]labels.Labels

	chunkBytesCount     atomic.Int64
	chunkCount          atomic.Int64
	estimatedChunkCount atomic.Int64

	maxSeriesPerQuery          int
	maxChunkBytesPerQuery      int
	maxChunksPerQuery          int
	maxEstimatedChunksPerQuery int

	queryMetrics *stats.QueryMetrics

	// hashFunc computes the hash of a labels.Labels. Defaults to labels.Labels.Hash() in production.
	// Can be overridden in tests to force hash collisions.
	hashFunc func(labels.Labels) uint64
}

// NewQueryLimiter makes a new per-query limiter. Each query limiter is configured using the
// maxSeriesPerQuery, maxChunkBytesPerQuery, maxChunksPerQuery and maxEstimatedChunksPerQuery limits.
func NewQueryLimiter(maxSeriesPerQuery, maxChunkBytesPerQuery, maxChunksPerQuery int, maxEstimatedChunksPerQuery int, queryMetrics *stats.QueryMetrics) *QueryLimiter {
	return &QueryLimiter{
		uniqueSeriesMx: sync.Mutex{},
		uniqueSeries:   map[uint64]labels.Labels{},

		maxSeriesPerQuery:          maxSeriesPerQuery,
		maxChunkBytesPerQuery:      maxChunkBytesPerQuery,
		maxChunksPerQuery:          maxChunksPerQuery,
		maxEstimatedChunksPerQuery: maxEstimatedChunksPerQuery,

		queryMetrics: queryMetrics,
		hashFunc:     func(l labels.Labels) uint64 { return l.Hash() },
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

// AddSeries adds the input series and returns an error if the limit is reached.
func (ql *QueryLimiter) AddSeries(newLabels labels.Labels, tracker *MemoryConsumptionTracker) (labels.Labels, error) {
	fingerprint := ql.hashFunc(newLabels)

	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()

	uniqueSeriesBefore := len(ql.uniqueSeries) + countConflictSeries(ql.conflictSeries)

	if existingLabels, foundDuplicate := ql.uniqueSeries[fingerprint]; !foundDuplicate {
		// newLabels is seen for the first time hence we track the series limit and its labels memory consumption.
		ql.uniqueSeries[fingerprint] = newLabels
		return ql.trackNewLabels(newLabels, uniqueSeriesBefore, tracker)
	} else if labels.Equal(existingLabels, newLabels) {
		// newLabels is seen before, deduplicate it by returning existingLabels.
		if ql.maxSeriesPerQuery != 0 && uniqueSeriesBefore > ql.maxSeriesPerQuery {
			return labels.EmptyLabels(), NewMaxSeriesHitLimitError(uint64(ql.maxSeriesPerQuery))
		}
		return existingLabels, nil
	}

	// newLabels' hash conflicted with existingLabels.
	if ql.conflictSeries == nil {
		// Note that we only track second labels' hash conflict onward in this map. The first conflict always in uniqueSeries map.
		ql.conflictSeries = make(map[uint64][]labels.Labels)
	}
	hashConflictLabels := ql.conflictSeries[fingerprint]
	for _, existingConflictedLabels := range hashConflictLabels {
		// newLabels is seen before in conflictSeries map, hence just return the existingConflictedLabels.
		if labels.Equal(existingConflictedLabels, newLabels) {
			if ql.maxSeriesPerQuery != 0 && uniqueSeriesBefore > ql.maxSeriesPerQuery {
				return labels.EmptyLabels(), NewMaxSeriesHitLimitError(uint64(ql.maxSeriesPerQuery))
			}
			return existingConflictedLabels, nil
		}
	}
	// Despite there was a hash conflict, newLabels is actually seen for the first time hence we track the series limit and its labels memory consumption.
	ql.conflictSeries[fingerprint] = append(hashConflictLabels, newLabels)
	return ql.trackNewLabels(newLabels, uniqueSeriesBefore, tracker)
}

func countConflictSeries(series map[uint64][]labels.Labels) int {
	count := 0
	for _, lbls := range series {
		count += len(lbls)
	}
	return count
}

func (ql *QueryLimiter) trackNewLabels(newLabels labels.Labels, uniqueSeriesBefore int, tracker *MemoryConsumptionTracker) (labels.Labels, error) {
	uniqueSeriesAfter := len(ql.uniqueSeries) + countConflictSeries(ql.conflictSeries)
	if ql.maxSeriesPerQuery != 0 && uniqueSeriesAfter > ql.maxSeriesPerQuery {
		if uniqueSeriesBefore <= ql.maxSeriesPerQuery {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxSeries).Inc()
		}

		return labels.EmptyLabels(), NewMaxSeriesHitLimitError(uint64(ql.maxSeriesPerQuery))
	}
	err := tracker.IncreaseMemoryConsumptionForLabels(newLabels)
	if err != nil {
		return labels.EmptyLabels(), err
	}
	return newLabels, nil
}

// uniqueSeriesCount returns the count of unique series seen by this query limiter.
func (ql *QueryLimiter) uniqueSeriesCount() int {
	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()
	return len(ql.uniqueSeries) + countConflictSeries(ql.conflictSeries)
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
