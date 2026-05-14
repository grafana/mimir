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

	// readcacheInstanceLoad is the post-plan total load per readcache
	// instance, set by the second slicer round.
	readcacheInstanceLoad *prometheus.GaugeVec

	// readcachePartitionOwner is a (partition_id, instance_id) -> 1
	// gauge that lets dashboards visualise the latest plan as a
	// heatmap or stacked bar.
	readcachePartitionOwner *prometheus.GaugeVec

	lastReadcacheInstanceKeys  map[string]struct{}
	lastReadcachePartitionKeys map[string]struct{}
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
		readcacheInstanceLoad: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_nautilus_rebalancer_readcache_instance_load",
			Help: "Total per-instance load (alpha*active_series + beta*samples_ewma) for the most recent readcache slicer round.",
		}, []string{"instance"}),
		readcachePartitionOwner: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "cortex_nautilus_rebalancer_readcache_partition_owner",
			Help: "Per (partition, instance) gauge set to 1 for the currently-planned owner. Stale (partition, instance) pairs are deleted between rounds, so the sum over instances per partition is always 1 for active partitions.",
		}, []string{"partition", "instance"}),
		lastReadcacheInstanceKeys:  map[string]struct{}{},
		lastReadcachePartitionKeys: map[string]struct{}{},
	}
	if r != nil {
		r.MustRegister(m.partitionQuerySamples, m.unnamedQuerySamples, m.readcacheInstanceLoad, m.readcachePartitionOwner)
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

// updateReadcacheRound replaces the previous round's readcache slicer
// gauges with the values from the current round. Stale (partition,
// instance) tuples are deleted rather than left flat-lining.
func (m *metrics) updateReadcacheRound(plan readcachePlan) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	nextInstanceKeys := make(map[string]struct{}, len(plan.LoadByInstance))
	for inst, load := range plan.LoadByInstance {
		nextInstanceKeys[inst] = struct{}{}
		m.readcacheInstanceLoad.WithLabelValues(inst).Set(load)
	}
	for key := range m.lastReadcacheInstanceKeys {
		if _, ok := nextInstanceKeys[key]; !ok {
			m.readcacheInstanceLoad.DeleteLabelValues(key)
		}
	}
	m.lastReadcacheInstanceKeys = nextInstanceKeys

	nextPartitionKeys := make(map[string]struct{}, len(plan.Assignment.Entries))
	for _, e := range plan.Assignment.Entries {
		pidKey := strconv.FormatInt(int64(e.PartitionID), 10)
		key := pidKey + "|" + e.InstanceID
		nextPartitionKeys[key] = struct{}{}
		m.readcachePartitionOwner.WithLabelValues(pidKey, e.InstanceID).Set(1)
	}
	for key := range m.lastReadcachePartitionKeys {
		if _, ok := nextPartitionKeys[key]; !ok {
			pid, inst := splitMetricKey(key)
			if inst != "" {
				m.readcachePartitionOwner.DeleteLabelValues(pid, inst)
			}
		}
	}
	m.lastReadcachePartitionKeys = nextPartitionKeys
}

// splitMetricKey splits "partition|instance" back into its
// components.
func splitMetricKey(key string) (string, string) {
	for i := 0; i < len(key); i++ {
		if key[i] == '|' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}
