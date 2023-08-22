// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/limiter/query_limiter.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package limiter

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/atomic"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/querier/stats"
	"github.com/grafana/mimir/pkg/util/globalerror"
	"github.com/grafana/mimir/pkg/util/validation"
)

type queryLimiterCtxKey struct{}

const (
	cardinalityStrategy = "Consider reducing the time range and/or number of series selected by the query. One way to reduce the number of selected series is to add more label matchers to the query"
)

var (
	ctxKey                = &queryLimiterCtxKey{}
	MaxSeriesHitMsgFormat = globalerror.MaxSeriesPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the query exceeded the maximum number of series (limit: %d series)",
		cardinalityStrategy,
		validation.MaxSeriesPerQueryFlag,
	)
	MaxChunkBytesHitMsgFormat = globalerror.MaxChunkBytesPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the query exceeded the aggregated chunks size limit (limit: %d bytes)",
		cardinalityStrategy,
		validation.MaxChunkBytesPerQueryFlag,
	)
	MaxChunksPerQueryLimitMsgFormat = globalerror.MaxChunksPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the query exceeded the maximum number of chunks (limit: %d chunks)",
		cardinalityStrategy,
		validation.MaxChunksPerQueryFlag,
	)
	MaxEstimatedChunksPerQueryLimitMsgFormat = globalerror.MaxEstimatedChunksPerQuery.MessageWithStrategyAndPerTenantLimitConfig(
		"the estimated number of chunks for the query exceeded the maximum allowed (limit: %d chunks)",
		cardinalityStrategy,
		validation.MaxEstimatedChunksPerQueryMultiplierFlag,
	)
)

type QueryLimiter struct {
	uniqueSeriesMx sync.Mutex
	uniqueSeries   map[uint64]struct{}

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
		uniqueSeriesMx: sync.Mutex{},
		uniqueSeries:   map[uint64]struct{}{},

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

// AddSeries adds the input series and returns an error if the limit is reached.
func (ql *QueryLimiter) AddSeries(seriesLabels []mimirpb.LabelAdapter) error {
	// If the max series is unlimited just return without managing map
	if ql.maxSeriesPerQuery == 0 {
		return nil
	}
	fingerprint := mimirpb.FromLabelAdaptersToLabels(seriesLabels).Hash()

	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()

	uniqueSeriesBefore := len(ql.uniqueSeries)
	ql.uniqueSeries[fingerprint] = struct{}{}
	uniqueSeriesAfter := len(ql.uniqueSeries)

	if uniqueSeriesAfter > ql.maxSeriesPerQuery {
		if uniqueSeriesBefore <= ql.maxSeriesPerQuery {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxSeries).Inc()
		}

		return validation.LimitError(fmt.Sprintf(MaxSeriesHitMsgFormat, ql.maxSeriesPerQuery))
	}
	return nil
}

// uniqueSeriesCount returns the count of unique series seen by this query limiter.
func (ql *QueryLimiter) uniqueSeriesCount() int {
	ql.uniqueSeriesMx.Lock()
	defer ql.uniqueSeriesMx.Unlock()
	return len(ql.uniqueSeries)
}

// AddChunkBytes adds the input chunk size in bytes and returns an error if the limit is reached.
func (ql *QueryLimiter) AddChunkBytes(chunkSizeInBytes int) error {
	if ql.maxChunkBytesPerQuery == 0 {
		return nil
	}

	totalBytes := ql.chunkBytesCount.Add(int64(chunkSizeInBytes))

	if totalBytes > int64(ql.maxChunkBytesPerQuery) {
		if totalBytes-int64(chunkSizeInBytes) <= int64(ql.maxChunkBytesPerQuery) {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxChunkBytes).Inc()
		}

		return validation.LimitError(fmt.Sprintf(MaxChunkBytesHitMsgFormat, ql.maxChunkBytesPerQuery))
	}
	return nil
}

func (ql *QueryLimiter) AddChunks(count int) error {
	if ql.maxChunksPerQuery == 0 {
		return nil
	}

	totalChunks := ql.chunkCount.Add(int64(count))

	if totalChunks > int64(ql.maxChunksPerQuery) {
		if totalChunks-int64(count) <= int64(ql.maxChunksPerQuery) {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxChunks).Inc()
		}

		return validation.LimitError(fmt.Sprintf(MaxChunksPerQueryLimitMsgFormat, ql.maxChunksPerQuery))
	}
	return nil
}

func (ql *QueryLimiter) AddEstimatedChunks(count int) error {
	if ql.maxEstimatedChunksPerQuery == 0 {
		return nil
	}

	totalChunks := ql.estimatedChunkCount.Add(int64(count))

	if totalChunks > int64(ql.maxEstimatedChunksPerQuery) {
		if totalChunks-int64(count) <= int64(ql.maxEstimatedChunksPerQuery) {
			// If we've just exceeded the limit for the first time for this query, increment the failed query metric.
			ql.queryMetrics.QueriesRejectedTotal.WithLabelValues(stats.RejectReasonMaxEstimatedChunks).Inc()
		}

		return validation.LimitError(fmt.Sprintf(MaxEstimatedChunksPerQueryLimitMsgFormat, ql.maxEstimatedChunksPerQuery))
	}
	return nil
}
