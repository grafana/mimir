// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// TestSimulation_HotMetricBandFragmentsIntoHighFanout reproduces, at
// the slicer level, the "12k readcache_query_stream_calls" phenomenon
// observed on mimir-dev-15.
//
// The chain is:
//
//  1. A single high-cardinality metric occupies one 64K locality band
//     (top 16 bits = metric name; see mimirpb.ShardByMetricNameLocality).
//  2. That band carries a large share of the write load, so the
//     slicer's Phase 4 keeps splitting it (>2x mean slice load on an
//     over-target partition) and Phase 3 keeps scattering the resulting
//     fragments to whichever partition is coldest. Phase 2 merge never
//     reclaims them because they are never cold.
//  3. The band therefore ends up owned by a large number of distinct
//     partitions. A read for that metric must fan out to all of them.
//  4. Query sharding then multiplies that fan-out: each shard
//     independently re-fans-out to every partition in the band, because
//     the query-shard hash is orthogonal to the metric-name locality
//     hash and prunes nothing.
//
// The test asserts the load-driven fragmentation (hot band spread
// across many partitions) against a same-width COLD control band that
// stays co-located on ~1 partition, then shows the implied
// QueryStream call count crosses into the thousands.
func TestSimulation_HotMetricBandFragmentsIntoHighFanout(t *testing.T) {
	const (
		numPartitions   = 300
		warmupTicks     = 30
		rebalanceRounds = 120
		ticksPerRound   = 60

		// Query-time amplifiers observed on mimir-dev-15 for the SLO
		// query: 16-way sharding, and the apiserver _bucket metric
		// appears three times in the expression (le=1/5/30).
		queryShards       = 16
		bucketOccurrences = 3
	)

	// One hot metric's 64K locality band, aligned to a metric-name
	// boundary (low 16 bits zero) — exactly what
	// mimirpb.MetricNameHashRange returns for a metric.
	const (
		hotBandLo = uint32(0x40000000)
		hotBandHi = hotBandLo | uint32(0x0000FFFF)
	)
	// A second band of identical width that stays COLD: the control.
	const (
		coldBandLo = uint32(0x80000000)
		coldBandHi = coldBandLo | uint32(0x0000FFFF)
	)

	sim := newSimulation(numPartitions, simCfg(0.09))

	// Hot band: many high-rate series spread across the bottom 16 bits
	// (distinct label sets of the SAME metric name). Their summed rate
	// makes the band a permanent split magnet.
	for k := 0; k < 256; k++ {
		hash := hotBandLo | uint32(k*256)
		sim.sources = append(sim.sources, loadSource{hash: hash, samplesPerTick: 700})
	}
	// Cold control band: a single low-rate series of a different metric.
	sim.sources = append(sim.sources, loadSource{hash: coldBandLo | uint32(0x10), samplesPerTick: 5})
	// Light background load so non-hot partitions aren't empty: the
	// slicer needs somewhere "cold" to move fragments to, and a
	// realistic non-zero mean slice load.
	hashSpacePerPartition := uint64(math.MaxUint32+1) / uint64(numPartitions)
	for pid := 0; pid < numPartitions; pid++ {
		base := uint32(uint64(pid) * hashSpacePerPartition)
		for j := 0; j < 4; j++ {
			sim.sources = append(sim.sources, loadSource{hash: base + uint32(j*4096), samplesPerTick: 60})
		}
	}

	history := sim.runScenario(warmupTicks, rebalanceRounds, ticksPerRound)

	var splits, merges, moves int
	for _, r := range history {
		for _, a := range r.Actions {
			switch a.Kind {
			case ActionSplit:
				splits++
			case ActionMerge:
				merges++
			case ActionMove:
				moves++
			}
		}
	}

	hotPartitions := distinctPartitionsOverlapping(sim.assignment, hotBandLo, hotBandHi)
	coldPartitions := distinctPartitionsOverlapping(sim.assignment, coldBandLo, coldBandHi)
	impliedCalls := hotPartitions * queryShards * bucketOccurrences

	t.Logf("slicer actions over %d rounds: splits=%d merges=%d moves=%d", rebalanceRounds, splits, merges, moves)
	t.Logf("hot 64K band fragmented across %d/%d partitions; cold control band across %d",
		hotPartitions, numPartitions, coldPartitions)
	t.Logf("implied readcache_query_stream_calls = %d partitions x %d shards x %d occurrences = %d",
		hotPartitions, queryShards, bucketOccurrences, impliedCalls)

	// Control: a cold metric of the same band width stays co-located —
	// proving the fan-out is load-driven fragmentation, not the band
	// being wide.
	assert.LessOrEqual(t, coldPartitions, 2,
		"a cold metric band should stay on ~1 partition (locality intact)")

	// Reproduction: the hot band fragments across a large number of
	// partitions purely because the slicer split it to balance write
	// load and scattered the fragments — mirroring the ~200/300 spread
	// seen on mimir-dev-15.
	assert.GreaterOrEqual(t, hotPartitions, 100,
		"hot metric band should fragment across many partitions (the fan-out root cause)")

	// Fragmentation x query sharding lands the per-query QueryStream
	// call count in the thousands — the "12k" regime seen in production.
	assert.Greater(t, impliedCalls, 5000,
		"fragmentation x sharding should reproduce the thousands-scale QueryStream fan-out")

	// The slicer really is doing "a hell of a lot" of split/merge work.
	assert.Greater(t, splits+merges, 50,
		"expected heavy split/merge churn driven by the hot band")
}

// distinctPartitionsOverlapping counts how many distinct partitions own
// any range overlapping [lo, hi] — i.e. how many partitions a read for
// that hash band must fan out to.
func distinctPartitionsOverlapping(a *assignment.Assignment, lo, hi uint32) int {
	seen := map[int32]struct{}{}
	for _, e := range a.Entries {
		if e.Range.Lo <= hi && e.Range.Hi >= lo {
			seen[e.PartitionID] = struct{}{}
		}
	}
	return len(seen)
}
