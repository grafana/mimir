// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type blockBuilderMetrics struct {
	consumeCycleDuration prometheus.Histogram
	consumerLagRecords   *prometheus.GaugeVec
}

func newBlockBuilderMetrics(reg prometheus.Registerer) blockBuilderMetrics {
	var m blockBuilderMetrics

	m.consumeCycleDuration = promauto.With(reg).NewHistogram(prometheus.HistogramOpts{
		Name: "cortex_blockbuilder_consume_cycle_duration_seconds",
		Help: "Time spent consuming a full cycle.",

		NativeHistogramBucketFactor: 1.1,
	})

	m.consumerLagRecords = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_blockbuilder_consumer_lag_records",
		Help: "The per-topic-partition number of records, instance needs to work through each cycle.",
	}, []string{"topic", "partition"})

	return m
}
