// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package stats

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// QueryMetrics collects metrics on the number of chunks used while serving queries.
type QueryMetrics struct {
	// The number of chunks received from ingesters that were exact duplicates of chunks received from other ingesters
	// responding to the same query.
	IngesterChunksDeduplicated prometheus.Counter

	// The total number of chunks received from ingesters that were used to evaluate queries, including any chunks
	// discarded due to deduplication.
	IngesterChunksTotal prometheus.Counter

	// The total number of queries that were rejected for some reason.
	QueriesRejectedTotal *prometheus.CounterVec
}

func NewQueryMetrics(reg prometheus.Registerer) *QueryMetrics {
	return &QueryMetrics{
		IngesterChunksDeduplicated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_query_ingester_chunks_deduped_total",
			Help:      "Number of chunks received from ingesters at query time but discarded as duplicates.",
		}),
		IngesterChunksTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_query_ingester_chunks_total",
			Help:      "Number of chunks received from ingesters at query time, including any chunks discarded as duplicates.",
		}),
		QueriesRejectedTotal: promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "querier_queries_rejected_total",
			Help:      "Number of queries that were rejected, for example because they exceeded a limit.",
		}, []string{"reason"}),
	}
}
