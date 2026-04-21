// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// samplesOnlyCfg returns a Config that puts all load weight on the
// samples-per-second signal. Used by tests written before the
// active-series signal existed; they construct rangeRates with only
// samples populated.
func samplesOnlyCfg(movementBudget float64) Config {
	return Config{
		MovementBudget:    movementBudget,
		LoadWeightSeries:  0,
		LoadWeightSamples: 1,
	}
}

func TestRunSlicer_ConvergesOnSkewedLoad(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(partitions, initialSlicesPerPartition)
	require.NoError(t, initial.Validate())

	// Assign all load to partition 0's ranges, leave others cold.
	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, samples: 10000.0 / float64(initialSlicesPerPartition)})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, samples: 100.0 / float64(initialSlicesPerPartition)})
		}
	}

	r := &Rebalancer{cfg: samplesOnlyCfg(0.5)}

	result, _ := r.runSlicer(initial, rates, partitions, time.Time{})
	require.NoError(t, result.Validate())

	// After rebalancing, partition 0 should own less hash space than
	// its initial 25%, since hot ranges were moved to cold partitions.
	initialSpace := partitionHashSpace(initial, 0)
	resultSpace := partitionHashSpace(result, 0)
	assert.Less(t, resultSpace, initialSpace,
		"partition 0 should have shed hash space via moves")

	// Other partitions should have gained hash space.
	for _, pid := range partitions[1:] {
		resultOtherSpace := partitionHashSpace(result, pid)
		initialOtherSpace := partitionHashSpace(initial, pid)
		assert.GreaterOrEqual(t, resultOtherSpace, initialOtherSpace,
			"partition %d should have gained or maintained hash space", pid)
	}
}

func partitionHashSpace(a *assignment.Assignment, pid int32) uint64 {
	var total uint64
	for _, e := range a.Entries {
		if e.PartitionID == pid {
			total += e.Range.Size()
		}
	}
	return total
}

func TestRunSlicer_EvenLoadNoChange(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(partitions, initialSlicesPerPartition)

	var rates []rangeRate
	for _, e := range initial.Entries {
		rates = append(rates, rangeRate{hr: e.Range, samples: 100.0})
	}

	r := &Rebalancer{cfg: samplesOnlyCfg(0.09)}

	result, _ := r.runSlicer(initial, rates, partitions, time.Time{})
	require.NoError(t, result.Validate())

	lm := buildLoadMap(rates, 0, 1)
	partitionLoad := make(map[int32]float64)
	for _, e := range result.Entries {
		partitionLoad[e.PartitionID] += lm.load(e.Range)
	}

	totalLoad := 0.0
	for _, l := range partitionLoad {
		totalLoad += l
	}
	targetLoad := totalLoad / float64(len(partitions))

	for _, load := range partitionLoad {
		assert.InDelta(t, targetLoad, load, targetLoad*0.3, "partition load should be near target")
	}
}

func TestRunSlicer_InactivePartitionsReassigned(t *testing.T) {
	allPartitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(allPartitions, initialSlicesPerPartition)

	activePartitions := []int32{0, 1, 2}

	var rates []rangeRate
	for _, e := range initial.Entries {
		rates = append(rates, rangeRate{hr: e.Range, samples: 100.0})
	}

	r := &Rebalancer{cfg: samplesOnlyCfg(0.5)}

	result, _ := r.runSlicer(initial, rates, activePartitions, time.Time{})
	require.NoError(t, result.Validate())

	for _, e := range result.Entries {
		assert.NotEqual(t, int32(3), e.PartitionID, "inactive partition should not be assigned")
	}
}

func TestRunSlicer_SliceCountCapped(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, maxSlicesPerPartition)

	// Make one slice extremely hot to trigger splits.
	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, samples: 100000.0})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, samples: 1.0})
		}
	}

	r := &Rebalancer{cfg: samplesOnlyCfg(0.09)}

	result, _ := r.runSlicer(initial, rates, partitions, time.Time{})
	require.NoError(t, result.Validate())

	assert.LessOrEqual(t, len(result.Entries), maxSlicesPerPartition*len(partitions)+len(partitions),
		"total slices should be capped near maxSlicesPerPartition * numPartitions")
}

func TestMergeAdjacentCold(t *testing.T) {
	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: 299}, PartitionID: 1}, load: 0.1},
	}

	result, _ := mergeAdjacentCold(entries, 1.0, math.MaxFloat64, 1.0, 1)

	// All three slices are cold and adjacent; the first two merge
	// (same partition), then the third merges cross-partition onto
	// partition 0 (the larger side). Result: one entry covering 0-299.
	require.Equal(t, 1, len(result))
	assert.Equal(t, uint32(0), result[0].entry.Range.Lo)
	assert.Equal(t, uint32(299), result[0].entry.Range.Hi)
	assert.Equal(t, int32(0), result[0].entry.PartitionID)
}

// TestMergeAdjacentCold_CrossPartition verifies that the merge phase
// defragments by merging adjacent cold slices on DIFFERENT partitions
// (per the Slicer paper: "moving one onto the same task as the other").
func TestMergeAdjacentCold_CrossPartition(t *testing.T) {
	// Layout: [A:0-99, B:100-199, A:200-299] - B is sandwiched between two A slices.
	// All cold. Merge should move B onto A, producing [A:0-299].
	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 1}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: 299}, PartitionID: 0}, load: 0.1},
	}

	result, _ := mergeAdjacentCold(entries, 1.0, math.MaxFloat64, 1.0, 1)

	// Should merge into fewer entries by moving the B slice onto A's partition.
	require.Less(t, len(result), 3, "cross-partition merge should reduce entry count")
	// The result should have the B range absorbed into A's partition.
	for _, rl := range result {
		if rl.entry.Range.Lo == 100 && rl.entry.Range.Hi == 199 {
			assert.Equal(t, int32(0), rl.entry.PartitionID,
				"sandwiched B slice should be moved onto A's partition")
		}
	}
}

// TestRunSlicer_Phase1_DistributesAcrossPartitions verifies that Phase 1
// distributes slices from dead partitions across all active partitions,
// not just the first one.
func TestRunSlicer_Phase1_DistributesAcrossPartitions(t *testing.T) {
	allPartitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(allPartitions, initialSlicesPerPartition)

	// Partition 3 dies. Its 64 slices should be spread across 0, 1, 2.
	activePartitions := []int32{0, 1, 2}

	var rates []rangeRate
	for _, e := range initial.Entries {
		rates = append(rates, rangeRate{hr: e.Range, samples: 100.0})
	}

	r := &Rebalancer{cfg: samplesOnlyCfg(0.0)}

	result, _ := r.runSlicer(initial, rates, activePartitions, time.Time{})
	require.NoError(t, result.Validate())

	counts := make(map[int32]int)
	for _, e := range result.Entries {
		counts[e.PartitionID]++
	}
	// Dead partition's slices should not all go to partition 0.
	// With round-robin, each active partition should get ~85 slices (256/3).
	for _, pid := range activePartitions {
		assert.Greater(t, counts[pid], initialSlicesPerPartition,
			"partition %d should have received some of the dead partition's slices", pid)
	}
}

// TestRunSlicer_Phase4_ExhaustsBudget verifies the move phase continues
// making moves until the churn budget is exhausted, not stopping early
// on a heuristic threshold.
func TestRunSlicer_Phase4_ExhaustsBudget(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, initialSlicesPerPartition)

	// Mild imbalance: p0 gets 60% of load, p1 gets 40%.
	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, samples: 120.0})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, samples: 80.0})
		}
	}

	r := &Rebalancer{cfg: samplesOnlyCfg(0.5)}

	result, _ := r.runSlicer(initial, rates, partitions, time.Time{})
	require.NoError(t, result.Validate())

	lm := buildLoadMap(rates, 0, 1)
	loads := make(map[int32]float64)
	for _, e := range result.Entries {
		loads[e.PartitionID] += lm.load(e.Range)
	}

	total := loads[0] + loads[1]
	target := total / 2.0
	// With 50% budget and a 60/40 split, the algorithm should be able
	// to equalize almost perfectly.
	assert.InDelta(t, target, loads[0], target*0.05,
		"p0 load should be near target after exhausting budget")
	assert.InDelta(t, target, loads[1], target*0.05,
		"p1 load should be near target after exhausting budget")
}

// TestRunSlicer_Phase5_SplitsAnyHotSlice verifies that Phase 5 splits
// any slice that is ≥2x the mean slice load, regardless of whether
// its partition is overloaded. This is per the paper: splitting captures
// finer-grained load measurements for the next round.
func TestRunSlicer_Phase5_SplitsAnyHotSlice(t *testing.T) {
	partitions := []int32{0, 1}
	// 4 slices per partition = 8 total (well under 150 cap).
	initial := assignment.FineEvenSplit(partitions, 4)

	// Both partitions have the SAME total load, so neither is overloaded.
	// But each has one hot slice (500) that is well above the mean
	// slice load (1040/8 = 130, threshold = 260). The paper says these
	// should be split to gain granularity for the next round.
	var rates []rangeRate
	hotIdx := map[int]bool{0: true, 4: true}
	for i, e := range initial.Entries {
		if hotIdx[i] {
			rates = append(rates, rangeRate{hr: e.Range, samples: 500.0})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, samples: 10.0})
		}
	}

	r := &Rebalancer{cfg: samplesOnlyCfg(0.0)}

	result, _ := r.runSlicer(initial, rates, partitions, time.Time{})
	require.NoError(t, result.Validate())

	// Both hot slices should split even though neither partition is
	// overloaded. The current code only splits on overloaded partitions
	// and would leave these untouched.
	assert.Greater(t, len(result.Entries), len(initial.Entries),
		"hot slices should be split for granularity even on balanced partitions")
}

func TestAssignmentStore(t *testing.T) {
	s := &assignmentStore{}

	assert.Nil(t, s.latest())

	a1 := assignment.EvenSplit([]int32{0, 1})
	t1 := time.Now()
	s.add(t1, a1)

	assert.Equal(t, a1, s.latest())

	snap := s.snapshot()
	require.Len(t, snap.Assignments, 1)
	assert.Equal(t, t1.UnixMilli(), snap.Assignments[0].From.UnixMilli())

	a2 := assignment.EvenSplit([]int32{0, 1, 2})
	t2 := t1.Add(time.Minute)
	s.add(t2, a2)

	assert.Equal(t, a2, s.latest())

	snap = s.snapshot()
	require.Len(t, snap.Assignments, 2)
}

func TestGetAssignmentsResponse_RoundTrip(t *testing.T) {
	a := assignment.EvenSplit([]int32{0, 1, 2})
	set := &assignment.TimedAssignmentSet{}
	set.Add(time.Now(), a)

	proto := TimedAssignmentSetToProto(set)

	data, err := proto.Marshal()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	restored := &GetAssignmentsResponse{}
	err = restored.Unmarshal(data)
	require.NoError(t, err)

	restoredSet := restored.ToTimedAssignmentSet()
	require.Len(t, restoredSet.Assignments, 1)

	latest := restoredSet.Latest()
	require.NotNil(t, latest)
	require.Len(t, latest.Entries, len(a.Entries))

	for i, e := range a.Entries {
		assert.Equal(t, e.Range.Lo, latest.Entries[i].Range.Lo)
		assert.Equal(t, e.Range.Hi, latest.Entries[i].Range.Hi)
		assert.Equal(t, e.PartitionID, latest.Entries[i].PartitionID)
	}
}

func TestLoadMap_SamplesOnly(t *testing.T) {
	rates := []rangeRate{
		{hr: assignment.HashRange{Lo: 0, Hi: 999}, samples: 100.0},
		{hr: assignment.HashRange{Lo: 1000, Hi: 1999}, samples: 300.0},
	}
	lm := buildLoadMap(rates, 0, 1)

	// Each range gets samples/totalSamples * weight.
	assert.InDelta(t, 0.25, lm.load(assignment.HashRange{Lo: 0, Hi: 999}), 1e-9)
	assert.InDelta(t, 0.75, lm.load(assignment.HashRange{Lo: 1000, Hi: 1999}), 1e-9)
	assert.Equal(t, 0.0, lm.load(assignment.HashRange{Lo: 5000, Hi: 6000}))
}

func TestLoadMap_Combined(t *testing.T) {
	rates := []rangeRate{
		// hot in samples, cold in series
		{hr: assignment.HashRange{Lo: 0, Hi: 999}, samples: 900.0, series: 100},
		// cold in samples, hot in series
		{hr: assignment.HashRange{Lo: 1000, Hi: 1999}, samples: 100.0, series: 900},
	}
	// Weights: 50/50.
	lm := buildLoadMap(rates, 0.5, 0.5)

	// Each range contributes 0.5*0.9 + 0.5*0.1 = 0.5 from one signal
	// and the other way around for the other. Both should get exactly 0.5.
	assert.InDelta(t, 0.5, lm.load(rates[0].hr), 1e-9)
	assert.InDelta(t, 0.5, lm.load(rates[1].hr), 1e-9)

	// Check raw stats are exposed.
	s, n := lm.stat(rates[0].hr)
	assert.Equal(t, 900.0, s)
	assert.Equal(t, int64(100), n)
}

// TestRunSlicer_MoveCooldown_BlocksRepeatMoves verifies that, when a
// range is moved to balance load, it (and any range overlapping its
// boundaries) is excluded from being moved again until the cooldown
// expires. This protects against over-correction during the warm-up
// window where post-move stats lag reality.
func TestRunSlicer_MoveCooldown_BlocksRepeatMoves(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, 4)
	require.NoError(t, initial.Validate())

	// Strong skew: partition 0 owns all the load.
	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, samples: 1000.0})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, samples: 1.0})
		}
	}

	cfg := samplesOnlyCfg(0.5)
	cfg.MoveCooldown = 90 * time.Second
	r := &Rebalancer{cfg: cfg, moveCooldowns: make(map[assignment.HashRange]time.Time)}

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// First round: produces some moves and arms cooldowns.
	result1, actions1 := r.runSlicer(initial, rates, partitions, t0)
	require.NoError(t, result1.Validate())
	r.recordMoveCooldowns(t0, actions1)

	var movedRanges []assignment.HashRange
	for _, a := range actions1 {
		if a.Kind == ActionMove {
			movedRanges = append(movedRanges, a.Range)
		}
	}
	require.NotEmpty(t, movedRanges, "first round should produce some moves on a heavily-skewed cluster")
	require.NotEmpty(t, r.moveCooldowns, "cooldowns should be armed after moves")

	// Second round inside the cooldown window: same load profile (we
	// pretend no stats have updated yet, which is the worst case).
	t1 := t0.Add(30 * time.Second)
	r.pruneExpiredCooldowns(t1)
	_, actions2 := r.runSlicer(result1, rates, partitions, t1)
	for _, a := range actions2 {
		if a.Kind != ActionMove {
			continue
		}
		for _, cooled := range movedRanges {
			assert.Falsef(t, hashRangesOverlap(a.Range, cooled),
				"in-cooldown range %v should not be re-moved as %v", cooled, a.Range)
		}
	}

	// Third round well past the cooldown: the algorithm is free to
	// re-move overlapping ranges again.
	t2 := t0.Add(2 * time.Minute)
	r.pruneExpiredCooldowns(t2)
	assert.Empty(t, r.moveCooldowns, "cooldowns past deadline should be pruned")
}

// TestRunSlicer_MoveCooldown_DisabledByZero verifies that setting
// MoveCooldown to 0 turns off filtering entirely.
func TestRunSlicer_MoveCooldown_DisabledByZero(t *testing.T) {
	hr := assignment.HashRange{Lo: 100, Hi: 200}
	r := &Rebalancer{
		cfg:           Config{MoveCooldown: 0},
		moveCooldowns: map[assignment.HashRange]time.Time{hr: time.Now().Add(time.Hour)},
	}
	assert.False(t, r.isInMoveCooldown(time.Now(), hr),
		"cooldown should be disabled when MoveCooldown == 0")
}

// TestRunSlicer_MoveCooldown_OverlapMatchesSplitsAndMerges verifies the
// lineage-by-overlap heuristic: a range that overlaps a recently-moved
// ancestor is also considered in cooldown, even if its boundaries
// changed via split or merge.
func TestRunSlicer_MoveCooldown_OverlapMatchesSplitsAndMerges(t *testing.T) {
	moved := assignment.HashRange{Lo: 1000, Hi: 1999}
	r := &Rebalancer{
		cfg:           Config{MoveCooldown: time.Minute},
		moveCooldowns: map[assignment.HashRange]time.Time{moved: time.Now().Add(time.Minute)},
	}
	now := time.Now()

	// Exact match.
	assert.True(t, r.isInMoveCooldown(now, moved))
	// Sub-range of a recently moved range (split case).
	assert.True(t, r.isInMoveCooldown(now, assignment.HashRange{Lo: 1000, Hi: 1499}))
	assert.True(t, r.isInMoveCooldown(now, assignment.HashRange{Lo: 1500, Hi: 1999}))
	// Super-range of a recently moved range (merge case).
	assert.True(t, r.isInMoveCooldown(now, assignment.HashRange{Lo: 500, Hi: 2500}))
	// Adjacent but not overlapping.
	assert.False(t, r.isInMoveCooldown(now, assignment.HashRange{Lo: 2000, Hi: 2500}))
	assert.False(t, r.isInMoveCooldown(now, assignment.HashRange{Lo: 0, Hi: 999}))
}

func TestFineEvenSplit(t *testing.T) {
	partitions := []int32{0, 1, 2}
	a := assignment.FineEvenSplit(partitions, 4)
	require.NoError(t, a.Validate())
	assert.Equal(t, 12, len(a.Entries))

	// Each partition should own 4 entries.
	counts := make(map[int32]int)
	for _, e := range a.Entries {
		counts[e.PartitionID]++
	}
	for _, pid := range partitions {
		assert.Equal(t, 4, counts[pid])
	}
}
