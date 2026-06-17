// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// seedRateUnknownLayout pre-populates BOTH tiers so that the
// rebalancer sees a "rate-unknown" partition on the next round:
// partition `unknownPID` has L > 0 series in the readcache's
// in-memory state (so it is NOT empty), but the readcache reports
// sample_rate=0 for its ranges (so the slicer's partitionRateByPID
// sees the partition as zero-rate). This mirrors the tier-2-just-
// reassigned production scenario where the destination readcache
// owns the partition but its per-(partition, range) EWMA has not
// yet ramped up.
//
// To exercise the slicer's cold-pick path the layout deliberately
// makes one partition (`hotPID`) much hotter than the others, so
// runPhase3 has a clear "shed load to the coldest known partition"
// decision. The rate-unknown partition has the lowest L (per slice)
// of all partitions, making it the numerically coldest candidate
// in load-but-not-rate terms. Without the exclusion the slicer
// would pick it; with the exclusion it must pick a non-excluded
// cold partition instead.
//
// Returns the ranges seeded so tests can assert on which ones the
// slicer touched (or didn't touch).
func seedRateUnknownLayout(
	t *testing.T,
	h *harness,
	rc *fakeReadcache,
	partitionCount int,
	slicesPerPartition int,
	unknownPID int32,
	hotPID int32,
	hotRate float64,
	knownRate float64,
	seriesForKnown int64,
	seriesForUnknown int64,
) []assignment.HashRange {
	t.Helper()
	totalSlices := partitionCount * slicesPerPartition
	sliceWidth := (uint64(math.MaxUint32) + 1) / uint64(totalSlices)

	now := h.clock.Now()
	leaseEnd := now.Add(h.cfg.LeaseDuration)

	// Spread ranges evenly across partitions for a typical tier-1
	// shape — this gives the slicer a non-trivial layout (rather
	// than every range on one partition) so cold-pick has multiple
	// real candidates.
	entries := make([]assignment.LogEntry, 0, totalSlices)
	ranges := make([]assignment.HashRange, 0, totalSlices)
	for i := 0; i < totalSlices; i++ {
		lo := uint64(i) * sliceWidth
		hi := lo + sliceWidth - 1
		if i == totalSlices-1 {
			hi = uint64(math.MaxUint32)
		}
		hr := assignment.HashRange{Lo: uint32(lo), Hi: uint32(hi)}
		pid := int32(i % partitionCount)
		entries = append(entries, assignment.LogEntry{
			Range:       hr,
			PartitionID: pid,
			From:        now.Add(-time.Second),
			To:          leaseEnd,
		})
		ranges = append(ranges, hr)
	}
	h.r.store.seedFromEntries(entries)

	// tier-2: pin every partition to the supplied readcache so
	// stats collection has a single owner per partition.
	rcEntries := make([]readcacheassignment.LogEntry, 0, partitionCount)
	for pid := int32(0); pid < int32(partitionCount); pid++ {
		rcEntries = append(rcEntries, readcacheassignment.LogEntry{
			PartitionID: pid,
			InstanceID:  rc.id,
			From:        now.Add(-time.Second),
			To:          leaseEnd,
		})
	}
	h.r.readcacheStore.seedFromEntries(rcEntries)

	// Seed readcache owned set and per-(partition, range) state
	// to match the tier-1 assignment.
	rc.mu.Lock()
	rc.owned = make(map[int32][]assignment.HashRange)
	for _, e := range entries {
		rc.owned[e.PartitionID] = append(rc.owned[e.PartitionID], e.Range)
	}
	rc.mu.Unlock()

	for _, e := range entries {
		switch e.PartitionID {
		case unknownPID:
			// Rate-unknown: rate=0 but series > 0 (the readcache
			// owns the data, just hasn't built EWMA).
			rc.setLoad(e.PartitionID, e.Range, 0.0, seriesForUnknown/int64(slicesPerPartition))
		case hotPID:
			rc.setLoad(e.PartitionID, e.Range, hotRate, seriesForKnown/int64(slicesPerPartition))
		default:
			rc.setLoad(e.PartitionID, e.Range, knownRate, seriesForKnown/int64(slicesPerPartition))
		}
	}
	return ranges
}

// TestHarness_RateZero_ExcludedFromColdPick is the core invariant:
// a partition with reported rate=0 and L>0 must NOT receive any
// inbound move actions on this round, even if it is the
// numerically coldest candidate. Without the exclusion, the slicer
// would dump load onto it and then over-correct two rounds later
// (the production tier-2/tier-1 oscillation).
func TestHarness_RateZero_ExcludedFromColdPick(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        15 * time.Minute,
			LeaseLookahead:       5 * time.Minute,
			MinRebalanceInterval: 5 * time.Minute,
			MaxRebalanceInterval: 15 * time.Minute,
			MovementBudget:       0.5,
		},
	})
	rc0 := h.addReadcache("readcache-0")

	const unknownPID = int32(2)
	const hotPID = int32(0)
	seedRateUnknownLayout(t, h, rc0,
		/*partitionCount*/ 4,
		/*slicesPerPartition*/ 4,
		/*unknownPID*/ unknownPID,
		/*hotPID*/ hotPID,
		/*hotRate*/ 5000,
		/*knownRate*/ 1000,
		/*seriesForKnown*/ 40_000,
		/*seriesForUnknown*/ 1_000_000, // p2 has huge L, but rate=0
	)

	// Advance one minute so the seeded leases (To=now+15min) are
	// still active but rates have been "collected" once (the
	// rebalance round will collect during runRound).
	h.advance(1 * time.Minute)
	require.NoError(t, h.runRound())

	// Read the round summary off the admin to find the actions
	// taken. The harness doesn't expose round logs directly so we
	// reach into the admin trace ring.
	traces := h.r.admin.traceSnapshot()
	require.NotEmpty(t, traces, "round should have produced a trace")
	round := traces[len(traces)-1]

	// Count inbound moves per partition.
	inbound := map[int32]int{}
	for _, a := range round.Round.Actions {
		if a.Kind == ActionMove {
			inbound[a.ToPart]++
		}
	}

	// Sanity check: the slicer must have done work this round.
	// Otherwise the assertion below is vacuous.
	outboundFromHot := 0
	for _, a := range round.Round.Actions {
		if a.Kind == ActionMove && a.FromPart == hotPID {
			outboundFromHot++
		}
	}
	require.Greaterf(t, outboundFromHot, 0,
		"precondition: the slicer must shed load from the hot partition this round; got 0 outbound moves from p%d. Actions=%+v", hotPID, round.Round.Actions)

	// The rate-unknown partition must NOT have received any
	// inbound moves. The slicer cannot reason about its true
	// load, so leaving it alone for one round is the correct
	// behavior.
	assert.Equalf(t, 0, inbound[unknownPID],
		"partition %d (rate=0, L>0) must be excluded from cold-pick; got %d inbound moves. All inbound counts: %v", unknownPID, inbound[unknownPID], inbound)
}

// TestHarness_RateZero_InversionConfirmsExclusionMatters is the
// inversion test: when we DON'T exclude rate-unknown partitions
// (the pre-fix behavior, reproduced by calling runSlicer directly
// with nil exclusion), the slicer SHOULD pick the rate-unknown
// partition as the cold-pick destination. This catches the case
// where some other heuristic accidentally makes the test pass
// without the exclusion actually doing the work.
func TestHarness_RateZero_InversionConfirmsExclusionMatters(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        15 * time.Minute,
			LeaseLookahead:       5 * time.Minute,
			MinRebalanceInterval: 5 * time.Minute,
			MaxRebalanceInterval: 15 * time.Minute,
			MovementBudget:       0.5,
		},
	})
	rc0 := h.addReadcache("readcache-0")

	const unknownPID = int32(2)
	const hotPID = int32(0)
	seedRateUnknownLayout(t, h, rc0,
		/*partitionCount*/ 4,
		/*slicesPerPartition*/ 4,
		/*unknownPID*/ unknownPID,
		/*hotPID*/ hotPID,
		/*hotRate*/ 5000,
		/*knownRate*/ 1000,
		/*seriesForKnown*/ 40_000,
		/*seriesForUnknown*/ 1_000_000,
	)

	// Drive the slicer directly with nil exclusion (the
	// pre-fix behavior). The fake clock has just been seeded
	// with leases in the +15min future so calling collectRoundStats
	// implicitly is awkward; instead we hand-craft the rates the
	// same way collectRoundStats would and invoke runSlicer.
	current := h.r.store.latestActiveAssignment(h.clock.Now())
	require.NotNil(t, current, "harness should have a seeded tier-1 assignment")

	rates := make([]rangeRate, 0, len(current.Entries))
	partitionRateByPID := map[int32]float64{}
	for _, e := range current.Entries {
		var rate float64
		var series int64
		switch e.PartitionID {
		case unknownPID:
			rate, series = 0, 1_000_000/4
		case hotPID:
			rate, series = 5000, 40_000/4
		default:
			rate, series = 1000, 40_000/4
		}
		rates = append(rates, rangeRate{
			hr:          e.Range,
			series:      series,
			sampleRate:  rate,
			partitionID: e.PartitionID,
		})
		partitionRateByPID[e.PartitionID] += rate
	}

	_, actionsWithout := h.r.runSlicer(current, rates, partitionRateByPID, []int32{0, 1, 2, 3}, nil, h.clock.Now())
	inboundWithout := map[int32]int{}
	for _, a := range actionsWithout {
		if a.Kind == ActionMove {
			inboundWithout[a.ToPart]++
		}
	}
	assert.Greaterf(t, inboundWithout[unknownPID], 0,
		"inversion: WITHOUT the exclusion, the rate-unknown partition is the apparent coldest and the slicer would target it (this is the pre-fix bug). Got 0 inbound to p%d, meaning the exclusion is not what's preventing moves in the positive test.", unknownPID)
}

// TestHarness_RateZero_EmptyPartitionIsNotExcluded checks the
// negative case: a partition that is genuinely empty (rate=0 AND
// L=0) must remain in the cold-pick pool. Otherwise cold-start /
// post-merge partitions would never receive any load.
func TestHarness_RateZero_EmptyPartitionIsNotExcluded(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        15 * time.Minute,
			LeaseLookahead:       5 * time.Minute,
			MinRebalanceInterval: 5 * time.Minute,
			MaxRebalanceInterval: 15 * time.Minute,
			MovementBudget:       0.5,
		},
	})
	rc0 := h.addReadcache("readcache-0")

	const emptyPID = int32(2)
	const hotPID = int32(0)
	seedRateUnknownLayout(t, h, rc0,
		/*partitionCount*/ 4,
		/*slicesPerPartition*/ 4,
		/*unknownPID*/ emptyPID,
		/*hotPID*/ hotPID,
		/*hotRate*/ 5000,
		/*knownRate*/ 1000,
		/*seriesForKnown*/ 40_000,
		/*seriesForUnknown*/ 0, // CHANGED: L=0 makes p2 truly empty
	)

	h.advance(1 * time.Minute)
	require.NoError(t, h.runRound())

	traces := h.r.admin.traceSnapshot()
	require.NotEmpty(t, traces)
	round := traces[len(traces)-1]

	inbound := map[int32]int{}
	for _, a := range round.Round.Actions {
		if a.Kind == ActionMove {
			inbound[a.ToPart]++
		}
	}

	// Empty partition is a perfectly valid destination — the
	// slicer should still fill it from hotter neighbors.
	assert.Greater(t, inbound[emptyPID], 0,
		"partition %d (rate=0, L=0 — genuinely empty) must remain eligible as a destination", emptyPID)
}

// TestHarness_RateZero_ExclusionPersistsForOneRoundOnly is the
// "self-healing" property: once a rate-unknown partition's
// readcache reports ANY positive rate, the partition rejoins the
// slicer's pool. This bounds the duration of the workaround so
// that genuine cold partitions don't permanently leak load
// imbalance.
func TestHarness_RateZero_ExclusionPersistsForOneRoundOnly(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        15 * time.Minute,
			LeaseLookahead:       5 * time.Minute,
			MinRebalanceInterval: 5 * time.Minute,
			MaxRebalanceInterval: 15 * time.Minute,
			MovementBudget:       0.5,
		},
	})
	rc0 := h.addReadcache("readcache-0")

	const recoveringPID = int32(2)
	const hotPID = int32(0)
	seedRateUnknownLayout(t, h, rc0,
		/*partitionCount*/ 4,
		/*slicesPerPartition*/ 4,
		/*unknownPID*/ recoveringPID,
		/*hotPID*/ hotPID,
		/*hotRate*/ 5000,
		/*knownRate*/ 1000,
		/*seriesForKnown*/ 40_000,
		/*seriesForUnknown*/ 1_000_000,
	)

	// Round 1: rate=0 → excluded.
	h.advance(1 * time.Minute)
	require.NoError(t, h.runRound())

	traces := h.r.admin.traceSnapshot()
	round1 := traces[len(traces)-1]
	r1Inbound := 0
	for _, a := range round1.Round.Actions {
		if a.Kind == ActionMove && a.ToPart == recoveringPID {
			r1Inbound++
		}
	}
	require.Equal(t, 0, r1Inbound, "precondition: round 1 must exclude the rate-unknown partition")

	// Simulate the readcache's EWMA ramping up: now report a
	// realistic rate for the partition's ranges (only the ones
	// the readcache thinks pid 2 owns). The exclusion lifts the
	// moment any positive rate is seen.
	rc0.mu.Lock()
	pidRanges := append([]assignment.HashRange(nil), rc0.owned[recoveringPID]...)
	rc0.mu.Unlock()
	for _, hr := range pidRanges {
		rc0.setLoad(recoveringPID, hr, 50.0, 250_000)
	}

	// Round 2: rate is now positive (50 sps per range) so the
	// exclusion lifts. The partition becomes eligible for
	// cold-pick again. We don't assert it RECEIVES moves
	// (depends on imbalance math), only that it's no longer
	// in the exclusion set.
	h.advance(5 * time.Minute)
	require.NoError(t, h.runRound())

	excluded := computeRateZeroExclusions(
		map[int32]float64{recoveringPID: 50 * float64(len(pidRanges))},
		map[int32]int64{recoveringPID: 1_000_000},
		[]int32{recoveringPID},
	)
	assert.Nil(t, excluded,
		"once any positive rate is reported, the partition must rejoin the slicer pool")
}
