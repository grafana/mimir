// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// TestCollectRatesFromReadcaches_MirrorsAggregateWithMax pins the
// aggregation rule for a partition reported by more than one pod.
//
// Under RF>=2 (and during the dual-fleet migration, where the legacy
// single-zone pod and both zone mirrors are all in the ring at once)
// every concrete replica of a logical slot holds a full copy of the
// partition and answers HashRangeStats for it. They are copies of one
// another, not additive load, so each signal must be merged with max.
// Summing scaled the slicer's primary load signal by the number of
// live reporters.
func TestCollectRatesFromReadcaches_MirrorsAggregateWithMax(t *testing.T) {
	hrA := assignment.HashRange{Lo: 0, Hi: 999}
	hrB := assignment.HashRange{Lo: 1000, Hi: 1999}
	hrC := assignment.HashRange{Lo: 2000, Hi: 2999}

	h := newHarness(t, harnessOpts{})

	// Partition 0 is served by three concrete pods of the same
	// logical slot. Their reports differ slightly, as independent
	// EWMAs on separate pods do, so the assertions distinguish max
	// from sum, from first-wins, and from last-wins.
	type report struct {
		id         string
		rateA      float64
		seriesA    int64
		querySamps float64
	}
	for _, r := range []report{
		{id: "readcache-0", rateA: 100, seriesA: 1000, querySamps: 700},
		{id: "readcache-zone-a-0", rateA: 110, seriesA: 1100, querySamps: 720},
		{id: "readcache-zone-b-0", rateA: 90, seriesA: 900, querySamps: 680},
	} {
		rc := h.addReadcache(r.id)
		rc.owned[0] = []assignment.HashRange{hrA, hrB}
		rc.setLoad(0, hrA, r.rateA, r.seriesA)
		rc.setLoad(0, hrB, 50, 500)
		rc.pQuery[0] = r.querySamps
	}

	// Partition 1 has a single reporter, carrying exactly the load
	// that partition 0's mirrors agree on after deduplication.
	solo := h.addReadcache("readcache-1")
	solo.owned[1] = []assignment.HashRange{hrC}
	solo.setLoad(1, hrC, 160, 1600)
	solo.pQuery[1] = 720

	rates, _, partitionTotals, partitionQuerySamples, _, failed, err := h.r.collectRatesFromReadcaches(h.ctx)
	require.NoError(t, err)
	require.Empty(t, failed)

	// One entry per (partition, range), not one per reporter.
	require.Len(t, rates, 3)
	byKey := map[partitionRangeKey]rangeRate{}
	for _, rr := range rates {
		k := partitionRangeKey{partitionID: rr.partitionID, hr: rr.hr}
		_, dup := byKey[k]
		require.False(t, dup, "duplicate entry for partition %d range %v", rr.partitionID, rr.hr)
		byKey[k] = rr
	}

	assert.Equal(t, 110.0, byKey[partitionRangeKey{partitionID: 0, hr: hrA}].sampleRate)
	assert.Equal(t, int64(1100), byKey[partitionRangeKey{partitionID: 0, hr: hrA}].series)
	assert.Equal(t, 50.0, byKey[partitionRangeKey{partitionID: 0, hr: hrB}].sampleRate)

	assert.Equal(t, 720.0, partitionQuerySamples[0], "query EWMA is max across mirrors, not their sum")
	assert.Equal(t, int64(1600), partitionTotals[0], "head series is max across mirrors")

	// The regression that matters: a partition with three live
	// mirrors must not read as hotter than an identically-loaded
	// partition with one reporter. Uneven mirror counts across slots
	// are the normal state during a rollout, and inflation the slicer
	// cannot distinguish from real imbalance makes it move partitions
	// to correct load that was never there.
	load := partitionLoadFromRates(rates, []int32{0, 1})
	assert.Equal(t, 160.0, load[0])
	assert.Equal(t, load[1], load[0], "mirror count must not affect a partition's apparent load")
}

// TestCollectRatesFromReadcaches_WarmingMirrorRateIgnored covers the
// state every mirror passes through when a zonal fleet is scaled up:
// the new pod replays a Kafka backlog as fast as it can be served
// (3-4x live on mimir-dev-30), so its sample-rate EWMA reads far
// above the true rate. Taken at face value under max, that single
// replaying mirror overrides the correct readings of its warm peers
// and the slicer plans moves against a hotspot that will evaporate as
// soon as the pod catches up.
//
// Head series are a different matter: replay builds real ones, so
// they still count.
func TestCollectRatesFromReadcaches_WarmingMirrorRateIgnored(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 999}

	h := newHarness(t, harnessOpts{})

	warm := h.addReadcache("readcache-0")
	warm.owned[0] = []assignment.HashRange{hr}
	warm.setLoad(0, hr, 100, 400)
	warm.pQuery[0] = 700

	// Same logical slot, mid-replay: rate an order of magnitude
	// above live. Series are set above the warm peer's so that the
	// cardinality assertions below can only pass if the warm filter
	// left the replaying mirror's series alone — replay appends real
	// head series, and dropping them would understate the partition.
	replaying := h.addReadcache("readcache-zone-a-0")
	replaying.owned[0] = []assignment.HashRange{hr}
	replaying.setLoad(0, hr, 1500, 1000)
	replaying.pQuery[0] = 9000
	replaying.setWarming(0)

	rates, _, partitionTotals, partitionQuerySamples, _, _, err := h.r.collectRatesFromReadcaches(h.ctx)
	require.NoError(t, err)
	require.Len(t, rates, 1)

	assert.Equal(t, 100.0, rates[0].sampleRate, "replay throughput must not be read as live ingest rate")
	assert.Equal(t, int64(1000), rates[0].series, "head series survive the warm filter")
	assert.Equal(t, 700.0, partitionQuerySamples[0], "a replaying mirror serves no queries")
	assert.Equal(t, int64(1000), partitionTotals[0])
}

// TestCollectRatesFromReadcaches_AllMirrorsWarmingReadsZero pins what
// happens when no reporter has caught up yet: the partition reads as
// carrying no movable load rather than as the hottest thing in the
// fleet. It still holds head series, so it lands in the rate-zero
// exclusion set — which is what stops the slicer from reading a
// replaying partition as a cold target and piling more onto it.
func TestCollectRatesFromReadcaches_AllMirrorsWarmingReadsZero(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 999}

	h := newHarness(t, harnessOpts{})

	for _, id := range []string{"readcache-zone-a-0", "readcache-zone-b-0"} {
		rc := h.addReadcache(id)
		rc.owned[0] = []assignment.HashRange{hr}
		rc.setLoad(0, hr, 1500, 400)
		rc.setWarming(0)
	}

	rates, _, _, _, _, _, err := h.r.collectRatesFromReadcaches(h.ctx)
	require.NoError(t, err)
	require.Len(t, rates, 1)
	assert.Zero(t, rates[0].sampleRate)

	load := partitionLoadFromRates(rates, []int32{0})
	assert.Zero(t, load[0])

	excluded := computeRateZeroExclusions(load, map[int32]int64{0: 400}, []int32{0})
	assert.True(t, excluded[0], "a partition with head series but no rate must not read as an idle target")
}

// TestCollectRatesFromReadcaches_ResidueStaysSeparate guards the
// boundary of the deduplication above: reports that share a hash
// range but carry different partition IDs are distinct keys, not
// mirrors. This is how a previous owner's residue is tracked
// separately from growth on the new owner after a tier-1 move, and
// filterRatesByCurrentOwnership relies on the two staying apart.
func TestCollectRatesFromReadcaches_ResidueStaysSeparate(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 999}

	h := newHarness(t, harnessOpts{})

	oldOwner := h.addReadcache("readcache-0")
	oldOwner.owned[0] = []assignment.HashRange{hr}
	oldOwner.setLoad(0, hr, 200, 2000)

	newOwner := h.addReadcache("readcache-1")
	newOwner.owned[1] = []assignment.HashRange{hr}
	newOwner.setLoad(1, hr, 50, 500)

	rates, _, _, _, _, _, err := h.r.collectRatesFromReadcaches(h.ctx)
	require.NoError(t, err)
	require.Len(t, rates, 2)

	load := partitionLoadFromRates(rates, []int32{0, 1})
	assert.Equal(t, 200.0, load[0])
	assert.Equal(t, 50.0, load[1])
}
