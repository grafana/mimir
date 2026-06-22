// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/split_by_interval.go
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/querier/queryrange/util.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package splitandcache

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	NotCachableReasonUnalignedTimeRange   = "unaligned-time-range"
	NotCachableReasonTooNew               = "too-new"
	NotCachableReasonModifiersNotCachable = "has-modifiers"
)

type SplitAndCacheMetrics struct {
	*ResultsCacheMetrics

	SplitQueriesCount              prometheus.Counter
	QueryResultCacheAttemptedCount prometheus.Counter
	QueryResultCacheSkippedCount   *prometheus.CounterVec
}

func NewSplitAndCacheMetrics(reg prometheus.Registerer) *SplitAndCacheMetrics {
	m := &SplitAndCacheMetrics{
		ResultsCacheMetrics: NewResultsCacheMetrics("query_range", reg),
		SplitQueriesCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_split_queries_total",
			Help: "Total number of underlying query requests after the split by interval is applied.",
		}),
		QueryResultCacheAttemptedCount: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name: "cortex_frontend_query_result_cache_attempted_total",
			Help: "Total number of queries that were attempted to be fetched from cache.",
		}),
		QueryResultCacheSkippedCount: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_frontend_query_result_cache_skipped_total",
			Help: "Total number of times a query was not cacheable because of a reason. This metric is tracked for each partial query when time-splitting is enabled.",
		}, []string{"reason"}),
	}

	// Initialize known label values.
	for _, reason := range []string{NotCachableReasonUnalignedTimeRange, NotCachableReasonTooNew, NotCachableReasonModifiersNotCachable} {
		m.QueryResultCacheSkippedCount.WithLabelValues(reason)
	}

	return m
}

type ResultsCacheMetrics struct {
	CacheRequests prometheus.Counter
	CacheHits     prometheus.Counter
}

func NewResultsCacheMetrics(requestType string, reg prometheus.Registerer) *ResultsCacheMetrics {
	return &ResultsCacheMetrics{
		CacheRequests: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_frontend_query_result_cache_requests_total",
			Help:        "Total number of requests (or partial requests) looked up in the results cache.",
			ConstLabels: map[string]string{"request_type": requestType},
		}),
		CacheHits: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Name:        "cortex_frontend_query_result_cache_hits_total",
			Help:        "Total number of requests (or partial requests) fetched from the results cache.",
			ConstLabels: map[string]string{"request_type": requestType},
		}),
	}
}
