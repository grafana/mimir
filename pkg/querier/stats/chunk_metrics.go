// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/distributor/distributor.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package stats

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type QueryChunkMetrics struct {
	IngesterChunksDeduplicated prometheus.Counter
	IngesterChunksTotal        prometheus.Counter
}

func NewQueryChunkMetrics(reg prometheus.Registerer) *QueryChunkMetrics {
	return &QueryChunkMetrics{
		IngesterChunksDeduplicated: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_query_ingester_chunks_deduped_total",
			Help:      "Number of chunks deduplicated at query time from ingesters.",
		}),
		IngesterChunksTotal: promauto.With(reg).NewCounter(prometheus.CounterOpts{
			Namespace: "cortex",
			Name:      "distributor_query_ingester_chunks_total",
			Help:      "Number of chunks transferred at query time from ingesters.",
		}),
	}
}
