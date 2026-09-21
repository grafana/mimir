// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sort"
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

func TestHarness_TenantlessRoundsKeepTier1Empty(t *testing.T) {
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
	assert.Nil(t, h.tier1Active())
	assert.Empty(t, h.r.store.snapshot())
	require.Len(t, h.tier2Active(), 4)

	h.advance(30 * time.Second)
	require.NoError(t, h.runRound())
	assert.Nil(t, h.tier1Active())
	assert.Empty(t, h.r.store.snapshot(), "repeated cold rounds must not invent an empty-tenant tiling")
	assert.NotContains(t, h.logOutput(), "initialized assignment with fine even split")
}

func TestHarness_FirstRoundSeedsTier2WithoutInventingTenant(t *testing.T) {
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

	assert.Nil(t, h.tier1Active())
	assert.Empty(t, h.r.store.snapshot())
	tier2 := h.tier2Active()
	require.Len(t, tier2, 4, "tier-2 must have an owner for every partition")
	assert.Contains(t, tier2, int32(0), "partition 0 must have an owner so unknown tenants can bootstrap")
}

func TestHarness_TenantlessColdStartDoesNotPushRanges(t *testing.T) {
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

	require.NoError(t, h.runRound())

	pushed := append(rc0.ownedPartitions(), rc1.ownedPartitions()...)
	sort.Slice(pushed, func(i, j int) bool { return pushed[i] < pushed[j] })
	assert.Empty(t, pushed, "there are no tenant ranges to push before a tenant is observed")

	require.Len(t, h.tier2Active(), 4,
		"tier-2 should still be populated for partition-0 bootstrap")
}

func TestHarness_TenantlessSecondRoundStillPushesNoRanges(t *testing.T) {
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

	require.NoError(t, h.runRound()) // cold start, push silently fails
	h.advance(30 * time.Second)
	require.NoError(t, h.runRound())

	totalOwned := len(rc0.ownedPartitions()) + len(rc1.ownedPartitions())
	assert.Zero(t, totalOwned)
	assert.Empty(t, h.r.store.snapshot())
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
	assert.Nil(t, h.tier1Active())
	assert.Empty(t, h.r.store.snapshot())
}
