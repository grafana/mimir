// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package stats

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// QueryChunkMetrics collects metrics on the number of chunks used while serving queries.
type QueryChunkMetrics struct {
	// The number of chunks received from ingesters that were exact duplicates of chunks received from other ingesters
	// responding to the same query.
	IngesterChunksDeduplicated prometheus.Counter

	// The total number of chunks received from ingesters that were used to evaluate queries, including any chunks
	// discarded due to deduplication.
	IngesterChunksTotal prometheus.Counter
}

func NewQueryChunkMetrics(reg prometheus.Registerer) *QueryChunkMetrics {
	return &QueryChunkMetrics{
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
	}
}
