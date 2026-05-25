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

	// rateZeroExclusions tracks the per-round count of partitions
	// runPhase3 skipped because their reported sample rate was 0
	// despite holding L>0 series in some readcache TSDB head. A
	// persistently non-zero value points at tier-2 churn outpacing
	// the per-partition EWMA settle time; alert thresholds should
	// pair this with cortex_nautilus_rebalancer_tier2_skipped_rounds_total
	// so operators can distinguish "tier-2 is too aggressive" from
	// "readcache pods are restarting".
	rateZeroExclusions prometheus.Gauge

	// tier2RoundDecisions counts tier-2 gate decisions by reason
	// ("first_round", "interval_zero", "interval_elapsed",
	// "instances_changed" for fires; "interval_pending" for
	// skips). Lets dashboards show the breakdown of "why did tier-2
	// fire / not fire" without re-reading logs.
	tier2RoundDecisions *prometheus.CounterVec
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
		rateZeroExclusions: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "cortex_nautilus_rebalancer_phase3_excluded_partitions",
			Help: "Number of partitions runPhase3 skipped on the most recent round because their reported sample rate was 0 despite holding in-memory series (typically a partition whose readcache was just reassigned by tier-2 and whose EWMA has not yet ramped up).",
		}),
		tier2RoundDecisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "cortex_nautilus_rebalancer_tier2_round_decisions_total",
			Help: "Count of tier-2 (readcache slicer) gate decisions partitioned by outcome (fire vs skip) and reason. Lets dashboards show why tier-2 chose to fire or wait, without re-reading logs.",
		}, []string{"outcome", "reason"}),
	}
	if r != nil {
		r.MustRegister(m.partitionQuerySamples, m.unnamedQuerySamples, m.readcacheInstanceLoad, m.readcachePartitionOwner, m.rateZeroExclusions, m.tier2RoundDecisions)
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

// setRateZeroExclusions records how many partitions runPhase3
// excluded on the most recent round. Safe to call with nil
// receiver to keep unit-test wiring trivial.
func (m *metrics) setRateZeroExclusions(n int) {
	if m == nil {
		return
	}
	m.rateZeroExclusions.Set(float64(n))
}

// recordTier2FireDecision bumps the fire counter for the given
// reason ("first_round", "interval_zero", "interval_elapsed",
// "instances_changed").
func (m *metrics) recordTier2FireDecision(reason string) {
	if m == nil {
		return
	}
	m.tier2RoundDecisions.WithLabelValues("fire", reason).Inc()
}

// recordTier2SkipDecision bumps the skip counter for the given
// reason (typically "interval_pending").
func (m *metrics) recordTier2SkipDecision(reason string) {
	if m == nil {
		return
	}
	m.tier2RoundDecisions.WithLabelValues("skip", reason).Inc()
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
