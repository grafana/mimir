// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/compactor/syncer_metrics.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package compactor

import (
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	dskit_metrics "github.com/grafana/dskit/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Copied from Thanos, pkg/compact/compact.go.
// Here we aggregate metrics from all finished syncers.
type aggregatedSyncerMetrics struct {
	metaSync                  prometheus.Counter
	metaSyncFailures          prometheus.Counter
	metaSyncDuration          *dskit_metrics.HistogramDataCollector // was prometheus.Histogram before
	metaBlocksSynced          *prometheus.GaugeVec
	garbageCollections        prometheus.Counter
	garbageCollectionFailures prometheus.Counter
	garbageCollectionDuration *dskit_metrics.HistogramDataCollector // was prometheus.Histogram before
}

// Copied (and modified with Mimir prefix) from Thanos, pkg/compact/compact.go
// We also ignore "group" label, since we only use a single group.
func newAggregatedSyncerMetrics(reg prometheus.Registerer) *aggregatedSyncerMetrics {
	var m aggregatedSyncerMetrics

	m.metaSync = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_compactor_meta_syncs_total",
		Help: "Total blocks metadata synchronization attempts.",
	})
	m.metaSyncFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_compactor_meta_sync_failures_total",
		Help: "Total blocks metadata synchronization failures.",
	})
	m.metaSyncDuration = dskit_metrics.NewHistogramDataCollector(prometheus.NewDesc(
		"cortex_compactor_meta_sync_duration_seconds",
		"Duration of the blocks metadata synchronization in seconds.",
		nil, nil))

	m.metaBlocksSynced = promauto.With(reg).NewGaugeVec(prometheus.GaugeOpts{
		Name: "cortex_compactor_meta_blocks_synced",
		Help: "Number of block metadata synced",
	}, []string{"state"})

	m.garbageCollections = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_compactor_garbage_collection_total",
		Help: "Total number of garbage collection operations.",
	})
	m.garbageCollectionFailures = promauto.With(reg).NewCounter(prometheus.CounterOpts{
		Name: "cortex_compactor_garbage_collection_failures_total",
		Help: "Total number of failed garbage collection operations.",
	})
	m.garbageCollectionDuration = dskit_metrics.NewHistogramDataCollector(prometheus.NewDesc(
		"cortex_compactor_garbage_collection_duration_seconds",
		"Time it took to perform garbage collection iteration.",
		nil, nil))

	if reg != nil {
		reg.MustRegister(m.metaSyncDuration, m.garbageCollectionDuration)
	}

	return &m
}

func (m *aggregatedSyncerMetrics) gatherThanosSyncerMetrics(reg *prometheus.Registry, logger log.Logger) {
	if m == nil {
		return
	}

	mf, err := reg.Gather()
	if err != nil {
		level.Warn(logger).Log("msg", "failed to gather metrics from syncer registry after compaction", "err", err)
		return
	}

	mfm, err := dskit_metrics.NewMetricFamilyMap(mf)
	if err != nil {
		level.Warn(logger).Log("msg", "failed to gather metrics from syncer registry after compaction", "err", err)
		return
	}

	m.metaSync.Add(mfm.SumCounters("blocks_meta_syncs_total"))
	m.metaSyncFailures.Add(mfm.SumCounters("blocks_meta_sync_failures_total"))
	m.metaSyncDuration.Add(mfm.SumHistograms("blocks_meta_sync_duration_seconds"))

	for _, met := range mfm["blocks_meta_synced"].GetMetric() {
		v := met.GetGauge().GetValue()
		ls := []string{}
		for _, lp := range met.GetLabel() {
			ls = append(ls, lp.GetValue())
		}
		m.metaBlocksSynced.WithLabelValues(ls...).Add(v)
	}

	m.garbageCollections.Add(mfm.SumCounters("thanos_compact_garbage_collection_total"))
	m.garbageCollectionFailures.Add(mfm.SumCounters("thanos_compact_garbage_collection_failures_total"))
	m.garbageCollectionDuration.Add(mfm.SumHistograms("thanos_compact_garbage_collection_duration_seconds"))
}
