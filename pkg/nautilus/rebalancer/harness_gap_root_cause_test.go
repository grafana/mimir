// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// seededLogEntries returns a set of log entries whose active-at-now
// projection has two gaps and one overlap. Used by the gap-recovery
// test to confirm healing copes with multiple corruption modes
// simultaneously.
func seededLogEntries(from, to time.Time) []assignment.LogEntry {
	return []assignment.LogEntry{
		// [0, 999] partition 0
		{Range: assignment.HashRange{Lo: 0, Hi: 999}, PartitionID: 0, From: from, To: to},
		// Gap: [1000, 1999] uncovered.
		// [2000, 2999] partition 1
		{Range: assignment.HashRange{Lo: 2000, Hi: 2999}, PartitionID: 1, From: from, To: to},
		// Overlap with the next entry.
		{Range: assignment.HashRange{Lo: 2500, Hi: 3500}, PartitionID: 2, From: from, To: to},
		// [3501, 9999] partition 3
		{Range: assignment.HashRange{Lo: 3501, Hi: 9999}, PartitionID: 3, From: from, To: to},
		// Gap: [10000, MaxUint32 / 2] uncovered.
		// [MaxUint32/2+1, MaxUint32] partition 0
		{Range: assignment.HashRange{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32}, PartitionID: 0, From: from, To: to},
	}
}

// TestHarness_GapsNeverAppearInNormalOperation drives the rebalancer
// through many rounds with a varying load shape and asserts that the
// active assignment tiles the full keyspace after EVERY round. This
// is the contract that, in production on mimir-dev-15, we observed
// being violated (~48% of rounds saw `slicer received invalid input`).
// If this test ever fails, we have a deterministic reproducer for
// the dev-15 "lease horizon collapse" failure mode that we can dig
// into without re-deploying.
//
// As of writing this test passes, which suggests the dev-15 gaps
// originated outside the in-process Apply/LatestActiveAssignment
// path — most likely from persisted state inherited at startup, or
// from an interaction we haven't yet reproduced (e.g. RPC failure
// patterns, very long Apply latency, scale events).
func TestHarness_GapsNeverAppearInNormalOperation(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       8,
			LeaseDuration:        5 * time.Minute,
			LeaseLookahead:       90 * time.Second,
			MinRebalanceInterval: 30 * time.Second,
			MaxRebalanceInterval: 5 * time.Minute,
			MoveCooldown:         90 * time.Second,
			MovementBudget:       0.1,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 0.5,
			},
		},
	})
	for i := 0; i < 4; i++ {
		h.addReadcache(fmt.Sprintf("readcache-%d", i))
	}

	// 60 rounds at 30s each = 30 simulated minutes, well past
	// LeaseDuration (5min) so we exercise the lease
	// expire -> pre-issue chain repeatedly. We don't inject
	// synthetic load; the fake fleet reports zero rates, so the
	// slicer's merge/move phases are mostly no-ops. The narrow
	// claim of this test is: even without slicer-driven churn,
	// the steady-state Apply -> pre-issue -> Apply loop should
	// never produce a gappy active assignment.
	require.NoError(t, h.runRound())
	for round := 1; round < 60; round++ {
		h.advance(30 * time.Second)
		require.NoErrorf(t, h.runRound(), "round %d returned an error", round)
		active := h.tier1Active()
		require.NotNilf(t, active, "round %d left the log with no active entries", round)
		assert.NoErrorf(t, active.Validate(), "round %d produced a non-tiling active assignment", round)
	}
}

// TestHarness_GapsRecoverEvenAfterSeededGap is the strongest end-to-
// end claim: take a freshly-cold rebalancer, seed it with a gappy
// persisted state (simulating what we'd inherit from a corrupt
// on-disk file), run a single round, and confirm the system is
// healed afterwards. This is the production scenario we expect
// once Fix A ships: an in-place upgrade onto a rebalancer pod with
// a bad assignment-log.json file recovers on the next round
// instead of bleeding key_not_covered until manual intervention.
func TestHarness_GapsRecoverEvenAfterSeededGap(t *testing.T) {
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

	// Seed with multiple gaps + an overlap to exercise both heal
	// branches at once.
	now := h.clock.Now()
	end := now.Add(5 * time.Minute)
	from := now.Add(-time.Second)
	h.r.store.seedFromEntries(seededLogEntries(from, end))

	// Pre-condition: the seeded active assignment fails Validate.
	pre := h.tier1Active()
	require.NotNil(t, pre)
	require.Error(t, pre.Validate(), "seeded state must be invalid for the test to be meaningful")

	require.NoError(t, h.runRound())
	post := h.tier1Active()
	require.NotNil(t, post)
	require.NoError(t, post.Validate(), "one round must produce a fully-tiled active assignment")

	// And the next round shouldn't have to heal again — the round
	// we just ran persisted a valid tiling.
	h.advance(30 * time.Second)
	preLogs := h.logOutput()
	require.NoError(t, h.runRound())
	round3 := h.logOutput()[len(preLogs):]
	assert.NotContains(t, round3, "healed gappy current assignment",
		"once healed, subsequent rounds should run unhealed")
}
