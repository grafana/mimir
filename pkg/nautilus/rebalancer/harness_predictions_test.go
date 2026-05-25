// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
	"github.com/grafana/mimir/pkg/nautilus/loadstats"
	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// seedHotPartitionLayout pre-populates BOTH tiers so the slicer's
// Phase 3 has something to move on the very first round:
//
//   - tier-1: `partitionCount` partitions, `slicesPerPartition`
//     ranges each, all stacked on partition 0 so the slicer must
//     move ranges to other partitions to balance.
//   - tier-2: every active partition pinned to the supplied
//     readcache instance.
//   - the readcache's owned state is also pre-populated so that
//     HashRangeStats actually returns rates for the seeded
//     (partition, range) pairs (the fake reports nothing for
//     unowned ranges).
//
// Returns the seeded ranges. The caller can then setLoad() on each
// to inject the per-(partition, range) sample rate.
func seedHotPartitionLayout(t *testing.T, h *harness, rc *fakeReadcache, partitionCount, slicesPerPartition int) []assignment.HashRange {
	t.Helper()
	totalSlices := partitionCount * slicesPerPartition
	sliceWidth := (uint64(math.MaxUint32) + 1) / uint64(totalSlices)
	now := h.clock.Now()
	leaseEnd := now.Add(h.cfg.LeaseDuration)
	entries := make([]assignment.LogEntry, 0, totalSlices)
	ranges := make([]assignment.HashRange, 0, totalSlices)
	for i := 0; i < totalSlices; i++ {
		lo := uint64(i) * sliceWidth
		hi := lo + sliceWidth - 1
		if i == totalSlices-1 {
			hi = uint64(math.MaxUint32)
		}
		hr := assignment.HashRange{Lo: uint32(lo), Hi: uint32(hi)}
		entries = append(entries, assignment.LogEntry{
			Range:       hr,
			PartitionID: 0, // all stacked on partition 0 to maximize imbalance
			From:        now.Add(-time.Second),
			To:          leaseEnd,
		})
		ranges = append(ranges, hr)
	}
	h.r.store.seedFromEntries(entries)

	// tier-2: pin every partition to the supplied readcache.
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

	// Make the fake readcache report on the hot ranges. The fake
	// only reports on (partition, range) pairs it considers owned.
	// We stuff partition 0 with all the ranges (matching tier-1).
	rc.mu.Lock()
	rc.owned[0] = append([]assignment.HashRange(nil), ranges...)
	rc.mu.Unlock()

	return ranges
}

// TestHarness_Predictions_RecordedAfterSlicerMoves is the wiring
// test: after a rebalance round that produced move (or reassign)
// actions, the prediction store must hold one entry per move with
// the destination partition and the source-side sample rate at
// commit time. Catches regressions where the prediction record call
// site is removed or moved before the Apply succeeds.
func TestHarness_Predictions_RecordedAfterSlicerMoves(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        5 * time.Minute,
			LeaseLookahead:       90 * time.Second,
			MinRebalanceInterval: 30 * time.Second,
			MaxRebalanceInterval: 5 * time.Minute,
			MovementBudget:       0.5,
		},
	})
	rc0 := h.addReadcache("readcache-0")

	// Stack 8 slices on partition 0 with non-trivial load so the
	// slicer's Phase 3 has cheap, fungible units to move toward
	// the other 3 partitions.
	ranges := seedHotPartitionLayout(t, h, rc0, 4, 2)
	for _, hr := range ranges {
		rc0.setLoad(0, hr, 1000, 10_000)
	}

	require.NoError(t, h.runRound())

	require.Greater(t, h.r.predictions.len(), 0,
		"a round with moves must record at least one prediction")
	for _, p := range h.r.predictions.preds {
		assert.NotEqual(t, int32(0), p.pid,
			"prediction destination must be a different partition than the hot one (0)")
		assert.Greater(t, p.rate, 0.0,
			"recorded predictions must carry a positive sample rate")
		assert.Equal(t, h.clock.Now(), p.committedAt,
			"prediction commit time must be the round's `now`")
	}
}

// TestHarness_Predictions_AppliedToSlicerView is the behavioral
// test: after a move, the partition rates the slicer sees on the
// NEXT round must include the predicted contribution at the
// destination. Verified via the admin's lastPartitionRate snapshot,
// which is populated immediately after applyTo and before runSlicer.
//
// Without this property, the slicer would see the destination as
// still-cold for ~5 minutes of EWMA settle time and keep piling
// ranges onto it.
func TestHarness_Predictions_AppliedToSlicerView(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        5 * time.Minute,
			LeaseLookahead:       90 * time.Second,
			MinRebalanceInterval: 30 * time.Second,
			MaxRebalanceInterval: 5 * time.Minute,
			MovementBudget:       0.5,
		},
	})
	rc0 := h.addReadcache("readcache-0")

	ranges := seedHotPartitionLayout(t, h, rc0, 4, 2)
	const perRangeRate = 1000.0
	for _, hr := range ranges {
		rc0.setLoad(0, hr, perRangeRate, 10_000)
	}

	// Round 1 records predictions. applyTo also runs in this
	// round but the store is empty, so it has no effect.
	require.NoError(t, h.runRound())
	require.Greater(t, h.r.predictions.len(), 0,
		"round 1 should have recorded predictions to apply later")

	// Snapshot the predictions and their commit time before
	// running round 2.
	predsByDest := map[int32]float64{}
	commitTime := h.clock.Now()
	for _, p := range h.r.predictions.preds {
		predsByDest[p.pid] += p.rate
		assert.Equal(t, commitTime, p.committedAt,
			"all predictions from round 1 should share the same commitTime")
	}

	// Round 2: applyTo now folds predictions into
	// partitionRateByPID before setLastStats. Advance one round
	// interval so decay = (1-alpha)^(30/15) ~ 0.933.
	const advance = 30 * time.Second
	h.advance(advance)
	require.NoError(t, h.runRound())

	_, _, _, lastRate := h.r.admin.snapshot()
	require.NotNil(t, lastRate)

	decayFactor := math.Pow(1.0-loadstats.Alpha, advance.Seconds()/loadstats.TickInterval.Seconds())
	for pid, predDelta := range predsByDest {
		expectedMin := predDelta * decayFactor * 0.95
		assert.GreaterOrEqualf(t, lastRate[pid], expectedMin,
			"partition %d should see at least the decayed prediction (%.2f) in its lastPartitionRate, got %.2f",
			pid, expectedMin, lastRate[pid])
	}
}

// TestHarness_Predictions_DecayOverManyRounds is the bound on the
// prediction store's memory footprint: even under sustained move
// volume, predictions older than the decay floor must be evicted.
// This protects the rebalancer process from unbounded growth on
// long-running clusters.
func TestHarness_Predictions_DecayOverManyRounds(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Seed predictions at staggered times spanning > 30 minutes.
	s := predictionStore{}
	for i := 0; i < 60; i++ {
		s.preds = append(s.preds, ratePrediction{
			pid:         int32(i % 4),
			rate:        500.0,
			committedAt: now.Add(time.Duration(i) * 30 * time.Second),
		})
	}
	require.Equal(t, 60, s.len())

	// Advance well past the floor (≥ 30 min after the FIRST
	// commit). Predictions younger than ~22min (4 half-lives at
	// alpha=0.034) survive; older ones are dropped.
	rates := map[int32]float64{0: 0, 1: 0, 2: 0, 3: 0}
	kept, dropped := s.applyTo(now.Add(45*time.Minute), rates)

	assert.Greater(t, dropped, 0, "old predictions must be evicted")
	assert.Less(t, kept, 60, "not all predictions survive")
	assert.Equal(t, kept, s.len(), "store length must match kept count after compaction")
}

// TestHarness_Predictions_NoOpRoundDoesNotRecord is the
// "record only on successful Apply" invariant: if a rebalance round
// produced no Apply-visible change (because the slicer made no
// moves, or because Apply was rejected), we must NOT record any
// predictions. Recording on a no-op would phantom-bump a partition's
// view of load with no corresponding distributor reroute, slowly
// drifting the slicer's view from reality.
func TestHarness_Predictions_NoOpRoundDoesNotRecord(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        5 * time.Minute,
			LeaseLookahead:       90 * time.Second,
			MinRebalanceInterval: 30 * time.Second,
			MaxRebalanceInterval: 5 * time.Minute,
			MovementBudget:       0.5,
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
	// Reset the prediction store to start from a known-empty state
	// (cold start may have produced reassigns).
	h.r.predictions = predictionStore{}

	// Run several rounds with NO load reported. The slicer has
	// nothing to do: rates are all zero, no imbalance, no moves.
	// Predictions must remain empty.
	for i := 0; i < 3; i++ {
		h.advance(30 * time.Second)
		require.NoError(t, h.runRound())
	}
	assert.Equal(t, 0, h.r.predictions.len(),
		"steady-state rounds with no load and no moves must not record predictions")
}
