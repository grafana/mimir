// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// TestHarness_Heal_GappyCurrentRecoversInOneRound is the regression
// test for the dev-15 "key_not_covered" recurrence we diagnosed on
// 2026-05-24. Setup: seed the assignment log with a tiling that's
// MISSING one hash sliver in the middle, then run a rebalance round.
// Without the heal step the round would bail at
// newAssignment.Validate() and leave the gap forever (because the
// slicer only operates on ranges it can see). With the heal step,
// the round must complete, splice in a filler for the missing
// sliver, and the next round's `current` must tile the full
// keyspace.
//
// If this test starts failing it means a change to the rebalance
// path has stopped invoking healAssignmentGaps and dev-15's
// "lease horizon collapse" failure mode is back in play.
func TestHarness_Heal_GappyCurrentRecoversInOneRound(t *testing.T) {
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

	// Seed the log directly with a gappy tiling. Two entries cover
	// [0, 999] and [2000, MaxUint32]; [1000, 1999] is uncovered.
	now := h.clock.Now()
	leaseEnd := now.Add(5 * time.Minute)
	h.r.store.seedFromEntries([]assignment.LogEntry{
		{Range: assignment.HashRange{Lo: 0, Hi: 999}, PartitionID: 0, From: now.Add(-time.Second), To: leaseEnd},
		{Range: assignment.HashRange{Lo: 2000, Hi: math.MaxUint32}, PartitionID: 1, From: now.Add(-time.Second), To: leaseEnd},
	})

	// Sanity: the active assignment as-of `now` is the gappy one.
	pre := h.tier1Active()
	require.NotNil(t, pre)
	require.Error(t, pre.Validate(), "test setup must produce a gappy active assignment")

	// Run a single round. With healing, this must succeed: the
	// gap [1000, 1999] is patched by the healer before the slicer
	// runs, the slicer returns a valid output, Apply persists it,
	// and the post-round active assignment tiles the full keyspace.
	require.NoError(t, h.runRound())

	post := h.tier1Active()
	require.NotNil(t, post)
	require.NoError(t, post.Validate(), "after one healed round the active assignment must tile [0, MaxUint32]")

	logs := h.logOutput()
	assert.Contains(t, logs, "healed gappy current assignment before slicer",
		"the round must report that it healed the input")
	assert.NotContains(t, logs, "generated invalid assignment",
		"the round must NOT bail at the post-slicer validation")
}

// TestHarness_Heal_OverlapInCurrentRecovers covers the dual of the
// gap case: the input has duplicate or overlapping entries. Without
// healing, Validate fails the same way and the round bails; with
// healing, overlaps are trimmed/dropped and the round proceeds.
func TestHarness_Heal_OverlapInCurrentRecovers(t *testing.T) {
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

	now := h.clock.Now()
	leaseEnd := now.Add(5 * time.Minute)
	// Three entries: [0, 1500], [1000, 2000] (overlaps the first),
	// [2001, MaxUint32]. ActiveAt returns all three; the input
	// has both an overlap [1000,1500] and the surrounding tiling.
	h.r.store.seedFromEntries([]assignment.LogEntry{
		{Range: assignment.HashRange{Lo: 0, Hi: 1500}, PartitionID: 0, From: now.Add(-time.Second), To: leaseEnd},
		{Range: assignment.HashRange{Lo: 1000, Hi: 2000}, PartitionID: 1, From: now.Add(-time.Second), To: leaseEnd},
		{Range: assignment.HashRange{Lo: 2001, Hi: math.MaxUint32}, PartitionID: 2, From: now.Add(-time.Second), To: leaseEnd},
	})
	pre := h.tier1Active()
	require.NotNil(t, pre)
	require.Error(t, pre.Validate(), "test setup must produce an overlapping active assignment")

	require.NoError(t, h.runRound())
	post := h.tier1Active()
	require.NotNil(t, post)
	require.NoError(t, post.Validate(), "after one healed round the active assignment must tile [0, MaxUint32]")
	assert.Contains(t, h.logOutput(), "healed gappy current assignment before slicer")
}

// TestHarness_Heal_NoOpOnValidInput is the negative test: when the
// active assignment is already valid, the heal step must not run
// (no warn-level log) and the round must take the normal code path.
// Guards against the heal step firing spuriously and creating
// unnecessary churn.
func TestHarness_Heal_NoOpOnValidInput(t *testing.T) {
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

	require.NoError(t, h.runRound()) // cold-start populates tier-1 cleanly
	require.NotNil(t, h.tier1Active())

	pre := h.logOutput()
	h.advance(30 * time.Second)
	require.NoError(t, h.runRound())
	round2 := strings.TrimPrefix(h.logOutput(), pre)
	assert.NotContains(t, round2, "healed gappy current assignment",
		"steady-state round must not invoke healing on a valid input")
}
