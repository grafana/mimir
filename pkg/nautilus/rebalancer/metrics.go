// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"strconv"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

// metrics holds the Prometheus collectors for the rebalancer. Phase 1
// exposes only query-load observability gauges; the rebalancer itself
// still drives off head-series counts (L_pid).
type metrics struct {
	// Per-partition named query load (samples-per-second EWMA) summed
	// across owners reporting the same partition. Updated each
	// rebalance round from collectRates output.
	partitionQuerySamples *prometheus.GaugeVec

	// Per-ingester unnamed query load (samples-per-second EWMA).
	// Reported per ingester because the work scours all owned
	// partitions; surfaced for the unnamed/named ratio observability.
	unnamedQuerySamples *prometheus.GaugeVec

	// labelTracker remembers which (partition,) and (instance,) label
	// combinations were set in the previous round so we can delete
	// stale series when a partition becomes inactive or an ingester
	// drops out, instead of leaking flat-line zero gauges.
	mu                sync.Mutex
	lastPartitionKeys map[string]struct{}
	lastInstanceKeys  map[string]struct{}
}

func newMetrics(r prometheus.Registerer) *metrics {
	m := &metrics{
		partitionQuerySamples: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_nautilus_rebalancer_partition_query_samples_ewma",
			Help: "EWMA of samples-per-second scanned by named queries, summed across ingesters that reported the partition. Phase 1: observation-only; the rebalancer still balances on head-series count.",
		}, []string{"partition"}),
		unnamedQuerySamples: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_nautilus_rebalancer_unnamed_query_samples_ewma",
			Help: "EWMA of samples-per-second scanned by full-fanout queries on each ingester. Reported per ingester because the work scours all owned partitions.",
		}, []string{"instance"}),
		lastPartitionKeys: map[string]struct{}{},
		lastInstanceKeys:  map[string]struct{}{},
	}
	if r != nil {
		r.MustRegister(m.partitionQuerySamples, m.unnamedQuerySamples)
	}
	return m
}

// updateRound replaces the previous round's per-partition and per-
// instance query-load gauges with the values from the current round.
// Stale series (partitions no longer reporting, ingesters that left
// the ring) are deleted rather than left flat-lining at the previous
// value.
func (m *metrics) updateRound(partitionQuerySamples map[int32]float64, unnamedPerInstance map[string]float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	nextPartitionKeys := make(map[string]struct{}, len(partitionQuerySamples))
	for pid, v := range partitionQuerySamples {
		key := strconv.FormatInt(int64(pid), 10)
		nextPartitionKeys[key] = struct{}{}
		m.partitionQuerySamples.WithLabelValues(key).Set(v)
	}
	for key := range m.lastPartitionKeys {
		if _, ok := nextPartitionKeys[key]; !ok {
			m.partitionQuerySamples.DeleteLabelValues(key)
		}
	}
	m.lastPartitionKeys = nextPartitionKeys

	nextInstanceKeys := make(map[string]struct{}, len(unnamedPerInstance))
	for inst, v := range unnamedPerInstance {
		nextInstanceKeys[inst] = struct{}{}
		m.unnamedQuerySamples.WithLabelValues(inst).Set(v)
	}
	for key := range m.lastInstanceKeys {
		if _, ok := nextInstanceKeys[key]; !ok {
			m.unnamedQuerySamples.DeleteLabelValues(key)
		}
	}
	m.lastInstanceKeys = nextInstanceKeys
}
