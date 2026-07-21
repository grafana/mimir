// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestCooldownIndex_Empty(t *testing.T) {
	idx := newCooldownIndex(time.Now(), nil)
	assert.Equal(t, 0, idx.len())
	assert.False(t, idx.overlaps(assignment.HashRange{Lo: 0, Hi: 100}))
}

func TestCooldownIndex_DroppedWhenAllExpired(t *testing.T) {
	now := time.Now()
	idx := newCooldownIndex(now, map[assignment.HashRange]time.Time{
		{Lo: 0, Hi: 99}:    now.Add(-time.Second),
		{Lo: 100, Hi: 199}: now, // deadline == now is treated as expired
	})
	assert.Equal(t, 0, idx.len())
	assert.False(t, idx.overlaps(assignment.HashRange{Lo: 50, Hi: 150}))
}

func TestCooldownIndex_MergesOverlappingAndTouching(t *testing.T) {
	now := time.Now()
	future := now.Add(time.Minute)
	idx := newCooldownIndex(now, map[assignment.HashRange]time.Time{
		{Lo: 0, Hi: 99}:    future,
		{Lo: 100, Hi: 199}: future, // touching the previous interval (Hi+1 == Lo)
		{Lo: 150, Hi: 250}: future, // overlaps with the previous
		{Lo: 500, Hi: 599}: future, // disjoint
	})
	require.Equal(t, 2, idx.len(), "touching+overlapping intervals should merge into one; disjoint stays separate")
	// Merged interval covers [0..250]; second covers [500..599].
	assert.True(t, idx.overlaps(assignment.HashRange{Lo: 0, Hi: 0}))
	assert.True(t, idx.overlaps(assignment.HashRange{Lo: 250, Hi: 250}))
	assert.True(t, idx.overlaps(assignment.HashRange{Lo: 251, Hi: 499}) == false, "between merged intervals should not overlap")
	assert.True(t, idx.overlaps(assignment.HashRange{Lo: 500, Hi: 500}))
}

func TestCooldownIndex_OverlapMatchesSplitsAndMerges(t *testing.T) {
	// Mirrors TestRunSlicer_MoveCooldown_OverlapMatchesSplitsAndMerges
	// so we cover the same lineage-by-overlap semantics through the
	// new index.
	moved := assignment.HashRange{Lo: 1000, Hi: 1999}
	now := time.Now()
	idx := newCooldownIndex(now, map[assignment.HashRange]time.Time{
		moved: now.Add(time.Minute),
	})
	require.Equal(t, 1, idx.len())

	assert.True(t, idx.overlaps(moved), "exact match")
	assert.True(t, idx.overlaps(assignment.HashRange{Lo: 1000, Hi: 1499}), "sub-range (split)")
	assert.True(t, idx.overlaps(assignment.HashRange{Lo: 1500, Hi: 1999}), "sub-range (split)")
	assert.True(t, idx.overlaps(assignment.HashRange{Lo: 500, Hi: 2500}), "super-range (merge)")
	assert.False(t, idx.overlaps(assignment.HashRange{Lo: 2000, Hi: 2500}), "adjacent but disjoint")
	assert.False(t, idx.overlaps(assignment.HashRange{Lo: 0, Hi: 999}), "adjacent but disjoint")
}

func TestCooldownIndex_BoundariesMaxUint32(t *testing.T) {
	now := time.Now()
	future := now.Add(time.Minute)
	max := ^uint32(0)
	idx := newCooldownIndex(now, map[assignment.HashRange]time.Time{
		{Lo: max - 100, Hi: max}: future,
	})
	require.Equal(t, 1, idx.len())
	assert.True(t, idx.overlaps(assignment.HashRange{Lo: max, Hi: max}))
	assert.False(t, idx.overlaps(assignment.HashRange{Lo: 0, Hi: max - 101}))
}

func TestCooldownIndex_MatchesIsInMoveCooldown_RandomizedFuzz(t *testing.T) {
	// Cross-check the index against the (slow but trusted) linear
	// scan in isInMoveCooldown for a large randomized cooldown set
	// and a large set of randomized queries. This is the strongest
	// proof that the index didn't change behaviour for runPhase3.
	rng := rand.New(rand.NewSource(1))
	now := time.Now()
	cooldowns := make(map[assignment.HashRange]time.Time, 500)
	for i := 0; i < 500; i++ {
		lo := uint32(rng.Intn(1 << 20))
		hi := lo + uint32(rng.Intn(1<<10))
		// Mix expired and active so the filter step matters.
		var deadline time.Time
		if rng.Intn(4) == 0 {
			deadline = now.Add(-time.Duration(rng.Intn(60)) * time.Second)
		} else {
			deadline = now.Add(time.Duration(rng.Intn(60)+1) * time.Second)
		}
		cooldowns[assignment.HashRange{Lo: lo, Hi: hi}] = deadline
	}

	r := &Rebalancer{
		cfg:           Config{MoveCooldown: time.Minute},
		moveCooldowns: cooldowns,
	}
	idx := newCooldownIndex(now, cooldowns)

	for i := 0; i < 2000; i++ {
		lo := uint32(rng.Intn(1 << 20))
		hi := lo + uint32(rng.Intn(1<<10))
		hr := assignment.HashRange{Lo: lo, Hi: hi}
		want := r.isInMoveCooldown(now, hr)
		got := idx.overlaps(hr)
		require.Equalf(t, want, got, "mismatch on %+v", hr)
	}
}
