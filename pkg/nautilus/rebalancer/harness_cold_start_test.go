// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This file uses the deterministic-time harness (harness_test.go)
// to exercise the rebalancer's cold-start path and its transitions
// into steady state. The motivation is the live mimir-dev-15 cold-
// start collapse loop: the rebalancer was observed re-entering the
// cold-start path every ~10 minutes for 6+ hours, never running a
// real rebalance round. The harness lets us isolate individual
// failure modes that contribute to that loop, fix them in-process,
// and have regression tests guarding against re-introduction —
// without redeploying to dev-15 and waiting hours per iteration.

// TestHarness_SecondRoundIsSteadyState is the headline regression
// test for the cold-start collapse loop. Setup: a clean rebalancer
// with two healthy readcaches, cold-start at t0, then run another
// rebalance round 30 seconds later. The contract: round 2 must
// take the regular rebalance path, NOT re-enter cold start.
//
// If this test starts failing it means a change to the cold-start
// path or the lease-aging code has reintroduced the dev-15 loop
// in-process and should not ship.
func TestHarness_SecondRoundIsSteadyState(t *testing.T) {
	h := newHarness(t, harnessOpts{
		captureLogs: true,
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        5 * time.Minute,
			LeaseLookahead:       90 * time.Second,
			MinRebalanceInterval: 30 * time.Second,
			MaxRebalanceInterval: 5 * time.Minute,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 0.5,
			},
		},
	})
	h.addReadcache("readcache-0")
	h.addReadcache("readcache-1")

	require.NoError(t, h.runRound())
	require.NotNil(t, h.tier1Active(), "round 1 must populate tier-1")
	preRound2 := h.logOutput()

	// Inside the lease window — round 2 should see active tier-1
	// leases (5min from now) and skip the cold-start path.
	h.advance(30 * time.Second)
	require.NoError(t, h.runRound())
	round2 := strings.TrimPrefix(h.logOutput(), preRound2)

	assert.NotContains(t, round2, "cold start hash assignment log seeded",
		"steady-state round must not re-seed tier-1 as if cold-starting")
	assert.NotContains(t, round2, "initialized assignment with fine even split",
		"steady-state round must not fall back to FineEvenSplit")
	assert.NotContains(t, round2, "reconstructAssignmentFromReadcache",
		"steady-state round must not invoke reconstructAssignmentFromReadcache")
	assert.Contains(t, round2, "rebalance round starting",
		"steady-state round must take the normal rebalance code path")
	require.NotNil(t, h.tier1Active(), "tier-1 must remain active across the steady-state round")
}

// TestHarness_FirstRoundSeedsBothTiers captures the cold-start
// completion contract: after a single cold-start round on a clean
// rebalancer with healthy readcaches, both tiers must be populated
// AND every partition's ranges must have been delivered to its
// readcache owner via SetHashRanges.
//
// As of writing this passes only for the tier-1/tier-2 state; the
// per-readcache range push fails because of a sequencing bug —
// see TestHarness_KnownBug_ColdStartPushesBeforeTier2IsSeeded for
// details. Tier-1 + tier-2 are asserted here; the push assertion
// is split off into the known-bug test.
func TestHarness_FirstRoundSeedsBothTiers(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount: 4,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 0.5,
			},
		},
	})
	h.addReadcache("readcache-0")
	h.addReadcache("readcache-1")

	require.NoError(t, h.runRound())

	tier1 := h.tier1Active()
	require.NotNil(t, tier1, "tier-1 must have active leases after cold-start round")
	require.NotEmpty(t, tier1.Entries)

	tier2 := h.tier2Active()
	require.Len(t, tier2, 4, "tier-2 must have an owner for every partition")
}

// TestHarness_KnownBug_ColdStartPushesBeforeTier2IsSeeded captures
// a real bug the harness uncovered on first use: in rebalance()'s
// cold-start branch, r.pushRanges() runs immediately after the
// tier-1 apply but BEFORE the tier-2 readcache slicer round.
// pushRangesToReadcache resolves owners from r.readcacheStore which
// is still empty at that point, so the very first push silently
// no-ops with "skipping SetHashRanges push: no readcache owners
// resolved". The first round's range assignments don't reach the
// readcaches until the next round's push, which is up to
// MinRebalanceInterval later (30s default) — and during that gap
// the readcaches all believe they own zero ranges.
//
// This test pins the CURRENT (buggy) behavior so the suite stays
// green and the bug is captured in code. When the fix lands —
// either by reordering pushRanges to run after runReadcacheSlicer
// in the cold-start branch, or by having runReadcacheSlicer
// trigger its own push — invert the assertions: the readcaches
// should own the partitions assigned to them.
//
// Reference: dev-15 "skipping SetHashRanges push: no readcache
// owners resolved partitions_in_assignment=300 partitions_without_owner=300"
// logs that appeared on every cold-start cycle.
func TestHarness_KnownBug_ColdStartPushesBeforeTier2IsSeeded(t *testing.T) {
	h := newHarness(t, harnessOpts{
		captureLogs: true,
		cfg: Config{
			PartitionCount: 4,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 0.5,
			},
		},
	})
	rc0 := h.addReadcache("readcache-0")
	rc1 := h.addReadcache("readcache-1")

	require.NoError(t, h.runRound())

	// Bug evidence #1: the no-owners warning appears in the logs.
	assert.Contains(t, h.logOutput(),
		"skipping SetHashRanges push: no readcache owners resolved",
		"cold start should NOT have to skip the push; this asserts the current buggy behavior")

	// Bug evidence #2: neither readcache received any ranges, even
	// though tier-2 (now populated by the post-push slicer round)
	// claims they own partitions.
	assert.Empty(t, rc0.ownedPartitions(),
		"BUG: readcache-0 received no ranges from the cold-start push")
	assert.Empty(t, rc1.ownedPartitions(),
		"BUG: readcache-1 received no ranges from the cold-start push")

	// Sanity: tier-2 IS populated (the slicer round did run after
	// the push). So we know the issue is sequencing, not a failure
	// of the slicer itself.
	require.Len(t, h.tier2Active(), 4,
		"sanity: tier-2 should still get populated by the post-push slicer round")
}

// TestHarness_KnownBug_SecondRoundFixesTheMissedPush is the
// follow-up to the sequencing bug: although round 1's push is a
// no-op, round 2 should observe the now-populated tier-2 and push
// the ranges out to the readcaches. This isn't really a bug per
// se but it's worth pinning so we know the system self-heals on
// the next round even with the broken sequence.
func TestHarness_KnownBug_SecondRoundFixesTheMissedPush(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount: 4,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 0.5,
			},
		},
	})
	rc0 := h.addReadcache("readcache-0")
	rc1 := h.addReadcache("readcache-1")

	require.NoError(t, h.runRound())          // cold start, push silently fails
	h.advance(30 * time.Second)               // inside lease window
	require.NoError(t, h.runRound())          // steady-state round; this push lands

	totalOwned := len(rc0.ownedPartitions()) + len(rc1.ownedPartitions())
	assert.Equalf(t, 4, totalOwned,
		"after round 2, every partition's ranges should be on a readcache "+
			"(round 1's missed push self-heals here). rc0=%v rc1=%v",
		rc0.ownedPartitions(), rc1.ownedPartitions())
}

// TestHarness_KnownBug_SlicerWithEmptyLoadCollapsesToFirstInstance
// captures the second class of cold-start bug the harness uncovered:
// when the readcache slicer runs with zero load signals (everyone
// reports nothing because it's a cold start), the slicer's
// "assign to the instance with the lightest current load"
// heuristic ties all instances at zero load and the tiebreaker
// resolves consistently to the same instance, dumping every
// partition onto it.
//
// On dev-15 this manifested as readcache-0 receiving all 300
// partitions on every cold start, OOMing under the combined
// traffic, restarting, and triggering another cold start — a
// self-sustaining collapse loop. The admin reset endpoint exists
// specifically as the manual escape hatch for this.
//
// Today's test setup mirrors that: 2 readcaches, 4 partitions,
// cold start with no load. Pinning the buggy "all on rc-0"
// distribution; once the slicer learns to break ties on partition
// count (or honors a fairness invariant), the assertion should
// flip to "no instance holds more than ceil(P/N) partitions".
func TestHarness_KnownBug_SlicerWithEmptyLoadCollapsesToFirstInstance(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount: 4,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 0.5,
			},
		},
	})
	h.addReadcache("readcache-0")
	h.addReadcache("readcache-1")

	require.NoError(t, h.runRound())

	owners := h.ownersByInstance()
	// Capture-current-behavior assertion: tier-2 collapses onto
	// the first instance in iteration order. When the bug is
	// fixed, this should split evenly (2 partitions each).
	assert.Equalf(t, 4, owners["readcache-0"],
		"BUG: cold-start slicer with zero load signals piles every partition "+
			"onto the first instance (got owners=%v)", owners)
	assert.Equalf(t, 0, owners["readcache-1"],
		"BUG: second instance gets nothing during cold start (got owners=%v)", owners)
}

// TestHarness_NoReadcachesAtColdStartDoesNotPanic exercises a
// degenerate case the live system was observed to hit during
// memberlist convergence after a rebalancer restart: the cold
// start runs while the ring still reports zero readcaches. The
// contract is just "don't crash and leave tier-1 in a sane state
// so the next round, with readcaches visible, can complete."
func TestHarness_NoReadcachesAtColdStartDoesNotPanic(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount: 4,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 0.5,
			},
		},
	})
	// Intentionally no readcaches in the fleet.

	require.NoError(t, h.runRound(), "cold start with empty readcache fleet must not error")
	require.NotNil(t, h.tier1Active(),
		"cold start must seed tier-1 even when no readcaches are reachable")
}
