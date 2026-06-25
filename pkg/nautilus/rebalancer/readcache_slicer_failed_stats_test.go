// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// seedBalancedTier2 pins one range per partition (tier-1) and spreads
// the partitions evenly across the supplied instances (tier-2), with
// every partition reporting the SAME positive rate/series. The result
// is a perfectly balanced fleet: each instance owns numPartitions/len
// partitions and carries identical load. This is the steady state a
// tier-2 round should leave untouched.
func seedBalancedTier2(
	t *testing.T,
	h *harness,
	rcs map[string]*fakeReadcache,
	instances []string,
	numPartitions int,
	rate float64,
	series int64,
) {
	t.Helper()
	sliceWidth := (uint64(math.MaxUint32) + 1) / uint64(numPartitions)
	now := h.clock.Now()
	leaseEnd := now.Add(h.cfg.LeaseDuration)

	for _, rc := range rcs {
		rc.mu.Lock()
		rc.owned = make(map[int32][]assignment.HashRange)
		rc.mu.Unlock()
	}

	tier1 := make([]assignment.LogEntry, 0, numPartitions)
	rcEntries := make([]readcacheassignment.LogEntry, 0, numPartitions)
	for i := 0; i < numPartitions; i++ {
		lo := uint64(i) * sliceWidth
		hi := lo + sliceWidth - 1
		if i == numPartitions-1 {
			hi = uint64(math.MaxUint32)
		}
		hr := assignment.HashRange{Lo: uint32(lo), Hi: uint32(hi)}
		pid := int32(i)
		owner := instances[i%len(instances)]

		tier1 = append(tier1, assignment.LogEntry{
			Range:       hr,
			PartitionID: pid,
			From:        now.Add(-time.Second),
			To:          leaseEnd,
		})
		rcEntries = append(rcEntries, readcacheassignment.LogEntry{
			PartitionID: pid,
			InstanceID:  owner,
			From:        now.Add(-time.Second),
			To:          leaseEnd,
		})

		rc := rcs[owner]
		rc.mu.Lock()
		rc.owned[pid] = append(rc.owned[pid], hr)
		rc.mu.Unlock()
		rc.setLoad(pid, hr, rate, series)
	}
	h.r.store.seedFromEntries(tier1)
	h.r.readcacheStore.seedFromEntries(rcEntries)
}

// TestReadcacheSlicer_FailedStatsRPCInstanceIsNotPiledOn guards the
// tier-2 placement exclusion: an instance that is perfectly healthy in
// the ring but whose HashRangeStats RPC fails on a given round is
// aggregated as ZERO load (collectRatesFromReadcaches skips it). Without
// the exclusion, lightestInstance would treat the failed instance as the
// emptiest box in the fleet and the slicer would dump partitions onto it
// — exactly the instance that just demonstrated it can't even answer a
// stats call. The exclusion (readcachePlanInput.excludedTargets) keeps it
// out of the destination set so it owns only what it already held.
//
// The test is an A/B:
//   - control (all RPCs healthy): the balanced fleet is a no-op.
//   - failure (one instance's RPC fails): the failed instance must NOT
//     accrue partitions; the round stays a no-op.
func TestReadcacheSlicer_FailedStatsRPCInstanceIsNotPiledOn(t *testing.T) {
	const numPartitions = 16
	instances := []string{"readcache-0", "readcache-1", "readcache-2", "readcache-3"}
	const failInstance = "readcache-2"

	run := func(injectFailure bool) (before, after map[string]int) {
		h := newHarness(t, harnessOpts{cfg: Config{
			PartitionCount:       numPartitions,
			LeaseDuration:        15 * time.Minute,
			LeaseLookahead:       5 * time.Minute,
			MinRebalanceInterval: 5 * time.Minute,
			MaxRebalanceInterval: 15 * time.Minute,
			// Keep tier-1 quiet so the only movement we observe is the
			// tier-2 partition->instance decision under test.
			MovementBudget: 0,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 1.0,
			},
		}})

		rcs := map[string]*fakeReadcache{}
		for _, id := range instances {
			rcs[id] = h.addReadcache(id)
		}

		seedBalancedTier2(t, h, rcs, instances, numPartitions, 100.0, 10_000)
		before = h.ownersByInstance()

		if injectFailure {
			rcs[failInstance].hashRangeStatsErr = errors.New("simulated HashRangeStats RPC failure")
		}

		h.advance(1 * time.Minute)
		require.NoError(t, h.runRound())
		after = h.ownersByInstance()

		return before, after
	}

	ctrlBefore, ctrlAfter := run(false)
	failBefore, failAfter := run(true)

	// Control: a balanced fleet with all RPCs answering must be stable.
	require.Equal(t, ctrlBefore, ctrlAfter,
		"control: balanced fleet with healthy RPCs must be a no-op (no pile); before=%v after=%v", ctrlBefore, ctrlAfter)

	// Fixed behavior: the instance whose HashRangeStats RPC failed is
	// excluded as a placement target, so it must NOT accrue partitions.
	// Without the exclusion this same scenario piled it from 4 -> 7.
	require.Equalf(t, failBefore[failInstance], failAfter[failInstance],
		"instance with failed HashRangeStats RPC must NOT be a pile target; before=%d after=%d full distribution before=%v after=%v",
		failBefore[failInstance], failAfter[failInstance], failBefore, failAfter)

	// And no other instance should have absorbed a pile either: the
	// round is a clean no-op when the only signal change is a failed RPC.
	require.Equalf(t, failBefore, failAfter,
		"a failed HashRangeStats RPC alone must not move partitions; before=%v after=%v", failBefore, failAfter)
}
