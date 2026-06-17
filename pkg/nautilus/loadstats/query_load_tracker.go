// SPDX-License-Identifier: AGPL-3.0-only

package loadstats

import (
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/grafana/mimir/pkg/ingester/client"
	util_math "github.com/grafana/mimir/pkg/util/math"
)

// TickInterval is how often the per-partition and unnamed EWMA rates
// are advanced from accumulated counts to per-second rates. Aligned
// with WalkInterval so the two background signals progress on the same
// cadence.
const TickInterval = 15 * time.Second

// Alpha is the smoothing factor applied at each tick.
//
//	half_life = -tick_interval / log2(1 - alpha)
//
// At tick=15s, alpha=0.1591 yields a half-life of ~1 minute:
// 1 - 2^(-15s/60s) = 1 - 2^-0.25 ≈ 0.15910. A 1-minute half-life
// keeps the rebalancer responsive to traffic shifts — partitions
// that briefly land on a freshly-restarted readcache reflect their
// true rate within a few rebalance rounds rather than the ~25 min
// it took with the previous 5-minute half-life.
const Alpha = 0.1591

// Tracker accumulates samples-scanned per partition_id and for full-
// fanout/unnamed queries, smoothing each into a per-second EWMA. The
// rebalancer pulls the resulting rates via HashRangeStats and uses them
// as the per-partition query-load signal.
//
// Bucketing is per-partition (not per-hash-range) because the unit of
// action for query-load rebalancing is the partition: we move
// partitions between owner instances. The distributor tells the read
// recipient the partition each query was routed for via
// QueryAttributionHint; the recipient trusts it and does not re-derive
// partition ownership from matchers.
//
// Nil-vs-set is the source of truth for "did the producer make a
// routing decision?" — a nil hint bills the unnamed bucket. Partition
// 0 is a real partition and is bucketed normally when named.
type Tracker struct {
	metricPrefix string

	mu sync.Mutex
	// perPartition is keyed by partition_id (including 0). Entries are
	// created lazily on first attribution; never deleted.
	perPartition map[int32]*util_math.EwmaRate
	unnamed      *util_math.EwmaRate

	// Static descriptors shared across collected gauges.
	namedDesc   *prometheus.Desc
	unnamedDesc *prometheus.Desc
}

// NewTracker constructs a Tracker. The metricPrefix becomes the
// {prefix}_query_samples_{named,unnamed}_ewma gauge name on scrape. Use
// e.g. "cortex_readcache" to get the metrics described in the plan.
func NewTracker(metricPrefix string) *Tracker {
	return &Tracker{
		metricPrefix: metricPrefix,
		perPartition: make(map[int32]*util_math.EwmaRate),
		unnamed:      util_math.NewEWMARate(Alpha, TickInterval),
		namedDesc: prometheus.NewDesc(
			metricPrefix+"_query_samples_named_ewma",
			"EWMA of samples-per-second scanned by named queries (queries the distributor resolved to a single partition), bucketed by partition_id.",
			[]string{"partition"}, nil,
		),
		unnamedDesc: prometheus.NewDesc(
			metricPrefix+"_query_samples_unnamed_ewma",
			"EWMA of samples-per-second scanned by full-fanout queries (no resolvable __name__, complex regexes). Counted per instance because the work scours all owned partitions.",
			nil, nil,
		),
	}
}

// Describe implements prometheus.Collector.
func (t *Tracker) Describe(ch chan<- *prometheus.Desc) {
	ch <- t.namedDesc
	ch <- t.unnamedDesc
}

// Collect implements prometheus.Collector. Called by the Prometheus
// client on scrape; emits one named-EWMA gauge per partition currently
// in the tracker plus one unnamed-EWMA gauge.
func (t *Tracker) Collect(ch chan<- prometheus.Metric) {
	snap := t.Snapshot()
	for _, p := range snap.PerPartition {
		ch <- prometheus.MustNewConstMetric(
			t.namedDesc,
			prometheus.GaugeValue,
			p.SamplesEWMA,
			strconv.FormatInt(int64(p.PartitionID), 10),
		)
	}
	ch <- prometheus.MustNewConstMetric(
		t.unnamedDesc,
		prometheus.GaugeValue,
		snap.Unnamed,
	)
}

// Attribute records `samples` scanned for this query against either a
// specific partition (when hint is non-nil) or the unnamed bucket. Safe
// to call concurrently.
//
// The recipient does not check "do I own this partition?" before
// bucketing: the hint is authoritative. The rebalancer reads exactly
// the work each instance did, attributed to whichever partition the
// routing layer claimed it was for. Mis-routing during transitions is
// thus a debug-level observability concern, not a correctness one.
func (t *Tracker) Attribute(hint *client.QueryAttributionHint, samples int64) {
	if samples <= 0 {
		return
	}
	if hint == nil {
		t.unnamed.Add(samples)
		return
	}
	t.bucket(hint.PartitionId).Add(samples)
}

// bucket returns (creating if necessary) the EWMA for partition pid.
func (t *Tracker) bucket(pid int32) *util_math.EwmaRate {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r, ok := t.perPartition[pid]; ok {
		return r
	}
	r := util_math.NewEWMARate(Alpha, TickInterval)
	t.perPartition[pid] = r
	return r
}

// Tick advances every EWMA by one tick. Must be called every
// TickInterval.
func (t *Tracker) Tick() {
	t.mu.Lock()
	partitions := make([]*util_math.EwmaRate, 0, len(t.perPartition))
	for _, r := range t.perPartition {
		partitions = append(partitions, r)
	}
	t.mu.Unlock()

	for _, r := range partitions {
		r.Tick()
	}
	t.unnamed.Tick()
}

// QueryLoadSnapshot is a point-in-time view of the current per-
// partition and unnamed EWMA rates, in samples per second.
type QueryLoadSnapshot struct {
	PerPartition []PartitionQueryLoadRate
	Unnamed      float64
}

// PartitionQueryLoadRate pairs a partition ID with its current EWMA
// rate (samples/sec).
type PartitionQueryLoadRate struct {
	PartitionID int32
	SamplesEWMA float64
}

// Snapshot returns the current per-partition rates (sorted by partition
// ID, ascending) and the unnamed bucket's rate. Returned data is safe
// to retain after the call.
func (t *Tracker) Snapshot() QueryLoadSnapshot {
	t.mu.Lock()
	pids := make([]int32, 0, len(t.perPartition))
	for pid := range t.perPartition {
		pids = append(pids, pid)
	}
	rates := make(map[int32]float64, len(t.perPartition))
	for _, pid := range pids {
		rates[pid] = t.perPartition[pid].Rate()
	}
	t.mu.Unlock()

	sort.Slice(pids, func(i, j int) bool { return pids[i] < pids[j] })
	out := QueryLoadSnapshot{
		PerPartition: make([]PartitionQueryLoadRate, len(pids)),
		Unnamed:      t.unnamed.Rate(),
	}
	for i, pid := range pids {
		out.PerPartition[i] = PartitionQueryLoadRate{
			PartitionID: pid,
			SamplesEWMA: rates[pid],
		}
	}
	return out
}
