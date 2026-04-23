// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// seriesCfg returns a Config with the given movement budget and a
// long-enough compaction interval that recentMoves pruning won't fire
// in a single-round test.
func seriesCfg(movementBudget float64) Config {
	return Config{
		MovementBudget:     movementBudget,
		CompactionInterval: 2 * time.Hour,
	}
}

// partitionLFromRates returns a partitionLByPID map derived by summing
// per-range series on each partition, the natural "ground-truth" L_pid
// for a test that doesn't otherwise model ingester totals.
func partitionLFromRates(current *assignment.Assignment, rates []rangeRate) map[int32]int64 {
	byRange := make(map[assignment.HashRange]int64, len(rates))
	for _, r := range rates {
		byRange[r.hr] += r.series
	}
	out := make(map[int32]int64)
	for _, e := range current.Entries {
		out[e.PartitionID] += byRange[e.Range]
	}
	return out
}

func TestRunSlicer_ConvergesOnSkewedLoad(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(partitions, initialSlicesPerPartition)
	require.NoError(t, initial.Validate())

	// Assign all load to partition 0's ranges, leave others cold.
	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, series: 10000 / int64(initialSlicesPerPartition)})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 100 / int64(initialSlicesPerPartition)})
		}
	}

	r := &Rebalancer{cfg: seriesCfg(0.5)}
	partL := partitionLFromRates(initial, rates)

	result, _ := r.runSlicer(initial, rates, partL, nil, partitions, time.Time{})
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
		rates = append(rates, rangeRate{hr: e.Range, series: 100})
	}

	r := &Rebalancer{cfg: seriesCfg(0.09)}
	partL := partitionLFromRates(initial, rates)

	result, _ := r.runSlicer(initial, rates, partL, nil, partitions, time.Time{})
	require.NoError(t, result.Validate())

	// Sum series per partition on the output assignment and check
	// they're within a reasonable band of the target. When all ranges
	// carry equal series and partitions started equal-sized, there's
	// nothing to do and every partition should stay within a small
	// delta of the mean.
	partitionSeries := make(map[int32]int64)
	for _, e := range result.Entries {
		for _, rr := range rates {
			if rr.hr == e.Range {
				partitionSeries[e.PartitionID] += rr.series
				break
			}
		}
	}
	var total int64
	for _, s := range partitionSeries {
		total += s
	}
	target := float64(total) / float64(len(partitions))
	for _, s := range partitionSeries {
		assert.InDelta(t, target, float64(s), target*0.3, "partition series should be near target")
	}
}

func TestRunSlicer_InactivePartitionsReassigned(t *testing.T) {
	allPartitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(allPartitions, initialSlicesPerPartition)

	activePartitions := []int32{0, 1, 2}

	var rates []rangeRate
	for _, e := range initial.Entries {
		rates = append(rates, rangeRate{hr: e.Range, series: 100})
	}

	r := &Rebalancer{cfg: seriesCfg(0.5)}
	partL := partitionLFromRates(initial, rates)

	result, _ := r.runSlicer(initial, rates, partL, nil, activePartitions, time.Time{})
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
			rates = append(rates, rangeRate{hr: e.Range, series: 100000})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 1})
		}
	}

	r := &Rebalancer{cfg: seriesCfg(0.09)}
	partL := partitionLFromRates(initial, rates)

	result, _ := r.runSlicer(initial, rates, partL, nil, partitions, time.Time{})
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
		rates = append(rates, rangeRate{hr: e.Range, series: 100})
	}

	r := &Rebalancer{cfg: seriesCfg(0.0)}
	partL := partitionLFromRates(initial, rates)

	result, _ := r.runSlicer(initial, rates, partL, nil, activePartitions, time.Time{})
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

// TestRunSlicer_Phase3_ExhaustsBudget verifies the move phase continues
// making moves until the churn budget is exhausted, not stopping early
// on a heuristic threshold.
func TestRunSlicer_Phase3_ExhaustsBudget(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, initialSlicesPerPartition)

	// Mild imbalance: p0 gets 60% of load, p1 gets 40%.
	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, series: 120})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 80})
		}
	}

	r := &Rebalancer{cfg: seriesCfg(0.5)}
	partL := partitionLFromRates(initial, rates)

	result, _ := r.runSlicer(initial, rates, partL, nil, partitions, time.Time{})
	require.NoError(t, result.Validate())

	// Sum per-range series per partition on the result.
	byRange := make(map[assignment.HashRange]int64, len(rates))
	for _, rr := range rates {
		byRange[rr.hr] = rr.series
	}
	loads := make(map[int32]int64)
	for _, e := range result.Entries {
		loads[e.PartitionID] += byRange[e.Range]
	}

	total := loads[0] + loads[1]
	target := float64(total) / 2.0
	// With 50% budget and a 60/40 split, the algorithm should be able
	// to equalize reasonably well.
	assert.InDelta(t, target, float64(loads[0]), target*0.15,
		"p0 series should be near target after exhausting budget")
	assert.InDelta(t, target, float64(loads[1]), target*0.15,
		"p1 series should be near target after exhausting budget")
}

// TestRunSlicer_Phase4_SplitsAnyHotSlice verifies that Phase 4 splits
// hot slices on overloaded partitions. Splitting adds fingrained
// granularity for the next round's load measurement.
func TestRunSlicer_Phase4_SplitsAnyHotSlice(t *testing.T) {
	partitions := []int32{0, 1}
	// 4 slices per partition = 8 total (well under 150 cap).
	initial := assignment.FineEvenSplit(partitions, 4)

	// P0 has high total load with one dominant hot slice; P1 cold.
	var rates []rangeRate
	hotIdx := map[int]bool{0: true}
	for i, e := range initial.Entries {
		if e.PartitionID != 0 {
			rates = append(rates, rangeRate{hr: e.Range, series: 10})
			continue
		}
		if hotIdx[i] {
			rates = append(rates, rangeRate{hr: e.Range, series: 500})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 10})
		}
	}

	r := &Rebalancer{cfg: seriesCfg(0.0)}
	partL := partitionLFromRates(initial, rates)

	result, _ := r.runSlicer(initial, rates, partL, nil, partitions, time.Time{})
	require.NoError(t, result.Validate())

	// The hot slice on the overloaded partition should have been split.
	assert.Greater(t, len(result.Entries), len(initial.Entries),
		"hot slice on overloaded partition should be split for granularity")
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

func TestLoadMap_SeriesAt(t *testing.T) {
	rates := []rangeRate{
		{hr: assignment.HashRange{Lo: 0, Hi: 999}, series: 100},
		{hr: assignment.HashRange{Lo: 1000, Hi: 1999}, series: 300},
	}
	lm := buildLoadMap(rates)

	assert.Equal(t, int64(100), lm.seriesAt(assignment.HashRange{Lo: 0, Hi: 999}))
	assert.Equal(t, int64(300), lm.seriesAt(assignment.HashRange{Lo: 1000, Hi: 1999}))
	assert.Equal(t, int64(0), lm.seriesAt(assignment.HashRange{Lo: 5000, Hi: 6000}))
}

// TestLoadMap_MaxOverReplicas verifies that buildLoadMap aggregates
// per-range series across replicas by taking the max, not the sum.
// Each healthy owner of a partition reports a near-identical count for
// the same ranges (they are mirrors), and partitionL is already on a
// max-over-owners scale. Phase 3 of the slicer mixes per-range series
// with the partition-level movable budget, so both signals must share
// the same scale; summing here would inflate the per-range load by the
// replication factor and cause systematic over-rejection in Phase 3's
// `rl.series > mov` gate.
func TestLoadMap_MaxOverReplicas(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 999}
	rates := []rangeRate{
		{hr: hr, series: 100},
		{hr: hr, series: 150},
		{hr: hr, series: 120},
	}
	lm := buildLoadMap(rates)

	assert.Equal(t, int64(150), lm.seriesAt(hr))
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
			rates = append(rates, rangeRate{hr: e.Range, series: 1000})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 1})
		}
	}

	cfg := seriesCfg(0.5)
	cfg.MoveCooldown = 90 * time.Second
	r := &Rebalancer{cfg: cfg, moveCooldowns: make(map[assignment.HashRange]time.Time)}

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// First round: produces some moves and arms cooldowns.
	partL := partitionLFromRates(initial, rates)
	result1, actions1 := r.runSlicer(initial, rates, partL, nil, partitions, t0)
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
	_, actions2 := r.runSlicer(result1, rates, partL, nil, partitions, t1)
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

// stubPartitionRing implements partitionRingView for tests that need
// to compute partition L without spinning up a real PartitionRing.
type stubPartitionRing map[int32][]string

func (s stubPartitionRing) PartitionOwnerIDs(pid int32) []string { return s[pid] }

// TestPartitionL_MaxOverOwners verifies that partitionL takes the
// worst-case owner total when a partition has multiple replicas
// (typically one per zone). Balancing on the max keeps any single
// ingester from approaching OOM.
func TestPartitionL_MaxOverOwners(t *testing.T) {
	instanceTotals := map[string]int64{
		"ingester-z1": 1000,
		"ingester-z2": 1500,
		"ingester-z3": 700,
		"ingester-s1": 400,
	}
	pRing := stubPartitionRing{
		0: {"ingester-z1", "ingester-z2", "ingester-z3"},
		1: {"ingester-s1"},
	}

	m := partitionL(instanceTotals, pRing, []int32{0, 1})
	assert.Equal(t, int64(1500), m[0], "partition 0 L = max over zone replicas")
	assert.Equal(t, int64(400), m[1])
}

// TestPartitionL_MissingOwnerIsZero verifies that a partition with no
// healthy owner in instanceTotals maps to zero (not dropped). A
// partition with L=0 simply can't be a hot source, but still shows up
// for destination selection.
func TestPartitionL_MissingOwnerIsZero(t *testing.T) {
	pRing := stubPartitionRing{
		0: {"unknown-ingester"},
	}
	m := partitionL(map[string]int64{}, pRing, []int32{0})
	assert.Equal(t, int64(0), m[0])
}

// TestRunSlicer_ExhaustedBudgetDoesNotStallOthers reproduces the bug
// (previously expressed as "orphan-locked partition"): Phase 3 of the
// slicer must not terminate its loop when the nominally-hottest
// partition has no profitable range to move. With the movable budget
// as the primary gate, a partition whose entire above-mean surplus has
// already been scheduled-out (sumRecentMoves >= L - meanL) simply
// won't be selected as hot; but even when it is selected and no move
// improves imbalance, the loop must exclude it and proceed to the
// next-hottest partition, not break.
func TestRunSlicer_ExhaustedBudgetDoesNotStallOthers(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(partitions, 8)

	// P0: per-range series zero (its owner is still draining a huge
	// backlog; current ranges are inactive).
	// P1: genuinely hot, has shedable per-range load.
	// P2, P3: cold.
	var rates []rangeRate
	for _, e := range initial.Entries {
		switch e.PartitionID {
		case 0:
			rates = append(rates, rangeRate{hr: e.Range, series: 0})
		case 1:
			rates = append(rates, rangeRate{hr: e.Range, series: 1000})
		default:
			rates = append(rates, rangeRate{hr: e.Range, series: 1})
		}
	}

	// Construct a partitionL where P0 is nominally the hottest but
	// carries no per-range series it could shed, while P1 is also
	// above mean and has real, shedable load. P2/P3 are cold. With
	// mean pulled up by P0 but not so high that P1 falls below it,
	// the slicer must:
	//   1. try P0 first, fail to find any profitable range, exclude it;
	//   2. proceed to drain P1.
	partL := map[int32]int64{
		0: 30000, // nominally hottest, no shedable ranges
		1: 20000, // above mean, has shedable ranges
		2: 100,
		3: 100,
	}
	// mean = (30000 + 20000 + 100 + 100) / 4 = 12550
	// movable(P0) = 30000 - 12550 = 17450 (but nothing to shed)
	// movable(P1) = 20000 - 12550 = 7450 (and 8*1000=8000 series of shedable load)

	cfg := seriesCfg(0.5)
	r := &Rebalancer{cfg: cfg, moveCooldowns: make(map[assignment.HashRange]time.Time)}

	_, actions := r.runSlicer(initial, rates, partL, nil, partitions, time.Time{})

	movesFromP1 := 0
	for _, a := range actions {
		if a.Kind != ActionMove {
			continue
		}
		if a.FromPart == 1 {
			movesFromP1++
		}
	}
	assert.Greaterf(t, movesFromP1, 0,
		"exhausted-budget P0 must not stall draining of hot P1; got actions=%v", actions)
}

// TestRunSlicer_MovableBudgetCapsMovesPerWindow verifies that the
// cross-round source-side budget shrinks with each move and caps total
// moves off a partition within one CompactionInterval. The window
// guards against the "keep draining what appears hot but has already
// been moved off" case: the source's reported L_pid doesn't drop until
// head compaction, so the budget must subtract in-flight moves.
func TestRunSlicer_MovableBudgetCapsMovesPerWindow(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, 16)

	// Set up a heavily-skewed starting state: P0 has a large
	// above-mean surplus. We model L_pid directly, keeping per-range
	// series constant so the slicer has ranges to move.
	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, series: 100})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 10})
		}
	}

	partL := map[int32]int64{
		0: 2000, // above mean: 2000 vs mean 1050
		1: 100,
	}
	// meanL = 1050; movable(0) initially = 2000 - 1050 = 950.

	cfg := seriesCfg(0.5)
	cfg.CompactionInterval = 2 * time.Hour
	r := &Rebalancer{
		cfg:           cfg,
		moveCooldowns: make(map[assignment.HashRange]time.Time),
		recentMoves:   make(map[int32][]moveRecord),
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Round 1: run slicer, record moves into r.recentMoves.
	result1, actions1 := r.runSlicer(initial, rates, partL, r.recentMoves, partitions, now)
	require.NoError(t, result1.Validate())
	r.recordRecentMoves(now, actions1, buildLoadMap(rates))

	var round1Moved int64
	for _, a := range actions1 {
		if a.Kind == ActionMove && a.FromPart == 0 {
			round1Moved += a.Series
		}
	}
	require.Greater(t, round1Moved, int64(0), "round 1 must move something off P0")
	// The per-source budget should cap total series moved off P0 at
	// movable(0) = 950. Some slack for float/int rounding and the
	// stride of available range sizes.
	assert.LessOrEqual(t, round1Moved, int64(950),
		"round 1 must not exceed initial movable(P0) budget of ~950 series")

	// Round 2 inside the CompactionInterval: L_pid hasn't dropped on
	// P0 because the moved series are still uncompacted. recentMoves
	// carries 950 from round 1, so movable(0) ~= 2000 - 1050 - 950 = 0.
	// The slicer must not move anything more off P0.
	now2 := now.Add(5 * time.Minute)
	r.pruneRecentMoves(now2)
	_, actions2 := r.runSlicer(result1, rates, partL, r.recentMoves, partitions, now2)

	var round2Moved int64
	for _, a := range actions2 {
		if a.Kind == ActionMove && a.FromPart == 0 {
			round2Moved += a.Series
		}
	}
	assert.Equalf(t, int64(0), round2Moved,
		"round 2 inside compaction window must not drain further; got %d", round2Moved)
}

// TestRunSlicer_BudgetResetsAfterCompactionInterval verifies that once
// a move record ages past CompactionInterval, it no longer counts
// against the source's movable budget (the moved series have
// compacted away by assumption).
func TestRunSlicer_BudgetResetsAfterCompactionInterval(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, 16)

	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, series: 100})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 10})
		}
	}

	partL := map[int32]int64{0: 2000, 1: 100}

	cfg := seriesCfg(0.5)
	cfg.CompactionInterval = time.Hour
	r := &Rebalancer{
		cfg:           cfg,
		moveCooldowns: make(map[assignment.HashRange]time.Time),
		recentMoves:   make(map[int32][]moveRecord),
	}

	// Seed recentMoves with an old record from a previous window.
	old := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.recentMoves[0] = []moveRecord{
		{hr: assignment.HashRange{Lo: 0, Hi: 999}, series: 900, at: old},
	}

	// Advance past the window. pruneRecentMoves should drop the old
	// record, restoring the full movable budget.
	fresh := old.Add(2 * time.Hour)
	r.pruneRecentMoves(fresh)
	assert.Empty(t, r.recentMoves[0], "old records past CompactionInterval should prune")

	_, actions := r.runSlicer(initial, rates, partL, r.recentMoves, partitions, fresh)
	var moved int64
	for _, a := range actions {
		if a.Kind == ActionMove && a.FromPart == 0 {
			moved += a.Series
		}
	}
	assert.Greater(t, moved, int64(0),
		"after CompactionInterval elapsed, source should be free to move again")
}

// TestRunSlicer_PlannedAdditionsPreventDestinationStuffing verifies the
// within-round destination guard: with three cold partitions of equal
// L, the slicer must spread moves across them rather than stuffing
// everything onto the (initially) coldest one. The cold partition's
// effective L inflates by plannedAdded as moves are booked.
func TestRunSlicer_PlannedAdditionsPreventDestinationStuffing(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(partitions, 16)

	var rates []rangeRate
	for _, e := range initial.Entries {
		switch e.PartitionID {
		case 0:
			rates = append(rates, rangeRate{hr: e.Range, series: 100})
		default:
			rates = append(rates, rangeRate{hr: e.Range, series: 1})
		}
	}

	partL := map[int32]int64{
		0: 4000, // hot
		1: 100,  // cold
		2: 100,  // cold
		3: 100,  // cold
	}

	cfg := seriesCfg(0.5)
	r := &Rebalancer{
		cfg:           cfg,
		moveCooldowns: make(map[assignment.HashRange]time.Time),
		recentMoves:   make(map[int32][]moveRecord),
	}

	_, actions := r.runSlicer(initial, rates, partL, r.recentMoves, partitions, time.Time{})

	destCounts := make(map[int32]int)
	for _, a := range actions {
		if a.Kind != ActionMove {
			continue
		}
		destCounts[a.ToPart]++
	}
	require.GreaterOrEqual(t, len(destCounts), 2,
		"moves should spread across multiple cold partitions, got %v", destCounts)
	// And the hot partition should not receive moves.
	assert.Equal(t, 0, destCounts[0], "hot partition should not be a destination")
}

// TestRunSlicer_PlannedAdditionsDoNotPersistAcrossRounds verifies that
// within-round plannedAdded state does NOT carry across rounds. In
// round 2 the slicer should see fresh L_pid values (the test re-
// submits the same partitionL) and be free to target the same cold
// destinations again — no "this partition received moves last round"
// penalty.
func TestRunSlicer_PlannedAdditionsDoNotPersistAcrossRounds(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, 16)

	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, series: 100})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 1})
		}
	}

	partL := map[int32]int64{0: 2000, 1: 100}
	cfg := seriesCfg(0.5)
	// Long compaction interval so recentMoves state doesn't
	// interfere with observing destination selection freedom.
	cfg.CompactionInterval = 2 * time.Hour
	r := &Rebalancer{
		cfg:           cfg,
		moveCooldowns: make(map[assignment.HashRange]time.Time),
		recentMoves:   make(map[int32][]moveRecord),
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	result1, actions1 := r.runSlicer(initial, rates, partL, r.recentMoves, partitions, now)
	r.recordRecentMoves(now, actions1, buildLoadMap(rates))

	// Round 2: we give the slicer a fresh partitionL where P1 is
	// back to being cold (pretend distributors haven't re-aimed at
	// it yet, or that a re-measurement showed it absorbed the moves
	// and then compacted). P0 is still the hottest but with the
	// budget now smaller due to recentMoves. The slicer must be
	// willing to target P1 again as the (still-only) cold destination
	// for whatever budget remains.
	now2 := now.Add(time.Minute)
	r.pruneRecentMoves(now2)
	_, actions2 := r.runSlicer(result1, rates, partL, r.recentMoves, partitions, now2)

	// If any move happens in round 2, its destination must be P1
	// (no prohibition from round 1's plannedAdded).
	for _, a := range actions2 {
		if a.Kind == ActionMove {
			assert.Equal(t, int32(1), a.ToPart,
				"round 2 moves should still target the cold P1; no cross-round destination penalty")
		}
	}
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

// TestPruneRecentMoves_DropsOldRecords verifies the pruning logic used
// between rounds: records at or older than the cutoff are dropped.
func TestPruneRecentMoves_DropsOldRecords(t *testing.T) {
	cfg := seriesCfg(0.5)
	cfg.CompactionInterval = time.Hour
	r := &Rebalancer{cfg: cfg, recentMoves: make(map[int32][]moveRecord)}

	now := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	r.recentMoves[0] = []moveRecord{
		{hr: assignment.HashRange{Lo: 0, Hi: 1}, series: 100, at: now.Add(-2 * time.Hour)},
		{hr: assignment.HashRange{Lo: 2, Hi: 3}, series: 200, at: now.Add(-30 * time.Minute)},
	}
	r.recentMoves[1] = []moveRecord{
		{hr: assignment.HashRange{Lo: 4, Hi: 5}, series: 50, at: now.Add(-90 * time.Minute)},
	}

	r.pruneRecentMoves(now)

	require.Len(t, r.recentMoves[0], 1)
	assert.Equal(t, int64(200), r.recentMoves[0][0].series)
	_, hasP1 := r.recentMoves[1]
	assert.False(t, hasP1, "P1's only record was past the cutoff; entry should be deleted")
}

// TestPruneRecentMoves_DisabledClearsAll verifies that disabling the
// window (CompactionInterval=0) clears any accumulated records, so a
// later re-enable doesn't silently gate moves against stale state.
func TestPruneRecentMoves_DisabledClearsAll(t *testing.T) {
	cfg := seriesCfg(0.5)
	cfg.CompactionInterval = 0
	r := &Rebalancer{cfg: cfg, recentMoves: make(map[int32][]moveRecord)}
	r.recentMoves[0] = []moveRecord{
		{hr: assignment.HashRange{Lo: 0, Hi: 1}, series: 100, at: time.Now()},
	}

	r.pruneRecentMoves(time.Now())
	assert.Empty(t, r.recentMoves)
}

// TestStitchReportedEntries_FullCoverageNoGaps verifies the easy case:
// reported ranges already tile [0, MaxUint32] with no gaps and no
// overlaps; stitch returns them verbatim in sorted order.
func TestStitchReportedEntries_FullCoverageNoGaps(t *testing.T) {
	reported := []reportedEntry{
		{partitionID: 1, hr: assignment.HashRange{Lo: 1000, Hi: math.MaxUint32}},
		{partitionID: 0, hr: assignment.HashRange{Lo: 0, Hi: 999}},
	}
	// Pre-sort into canonical order (stitch's precondition).
	sortReportedEntriesForTest(reported)

	out := stitchReportedEntries(reported, []int32{0, 1}, log.NewNopLogger())
	require.Len(t, out, 2)
	assert.Equal(t, int32(0), out[0].PartitionID)
	assert.Equal(t, uint32(0), out[0].Range.Lo)
	assert.Equal(t, uint32(999), out[0].Range.Hi)
	assert.Equal(t, int32(1), out[1].PartitionID)
	assert.Equal(t, uint32(1000), out[1].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), out[1].Range.Hi)

	a := &assignment.Assignment{Entries: out}
	require.NoError(t, a.Validate())
}

// TestStitchReportedEntries_FillsGaps verifies that holes — leading,
// middle, and trailing — are all filled by single-entry fillers.
func TestStitchReportedEntries_FillsGaps(t *testing.T) {
	// Reported coverage: [100..200], [500..600]. Expect 5 entries:
	// leading [0..99], [100..200], middle [201..499], [500..600],
	// trailing [601..MaxUint32].
	reported := []reportedEntry{
		{partitionID: 7, hr: assignment.HashRange{Lo: 100, Hi: 200}},
		{partitionID: 9, hr: assignment.HashRange{Lo: 500, Hi: 600}},
	}
	sortReportedEntriesForTest(reported)

	out := stitchReportedEntries(reported, []int32{3, 4, 5}, log.NewNopLogger())
	require.Len(t, out, 5)

	assert.Equal(t, uint32(0), out[0].Range.Lo)
	assert.Equal(t, uint32(99), out[0].Range.Hi)
	assert.Equal(t, int32(3), out[0].PartitionID, "first filler rotates to activePartitions[0]")

	assert.Equal(t, uint32(100), out[1].Range.Lo)
	assert.Equal(t, uint32(200), out[1].Range.Hi)
	assert.Equal(t, int32(7), out[1].PartitionID)

	assert.Equal(t, uint32(201), out[2].Range.Lo)
	assert.Equal(t, uint32(499), out[2].Range.Hi)
	assert.Equal(t, int32(4), out[2].PartitionID, "second filler rotates to activePartitions[1]")

	assert.Equal(t, uint32(500), out[3].Range.Lo)
	assert.Equal(t, uint32(600), out[3].Range.Hi)
	assert.Equal(t, int32(9), out[3].PartitionID)

	assert.Equal(t, uint32(601), out[4].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), out[4].Range.Hi)
	assert.Equal(t, int32(5), out[4].PartitionID, "third filler rotates to activePartitions[2]")

	a := &assignment.Assignment{Entries: out}
	require.NoError(t, a.Validate())
}

// TestStitchReportedEntries_FirstReplicaWinsOnOverlap verifies that
// when two different partitions claim overlapping hash space, the
// earlier (lower-Lo) entry keeps its coverage and the later entry is
// truncated to the uncovered tail.
func TestStitchReportedEntries_FirstReplicaWinsOnOverlap(t *testing.T) {
	reported := []reportedEntry{
		{partitionID: 1, hr: assignment.HashRange{Lo: 0, Hi: 1000}},
		{partitionID: 2, hr: assignment.HashRange{Lo: 500, Hi: 2000}}, // overlaps with 1 on [500..1000]
	}
	sortReportedEntriesForTest(reported)

	out := stitchReportedEntries(reported, []int32{1, 2}, log.NewNopLogger())
	require.Len(t, out, 3)

	assert.Equal(t, int32(1), out[0].PartitionID)
	assert.Equal(t, uint32(0), out[0].Range.Lo)
	assert.Equal(t, uint32(1000), out[0].Range.Hi)

	assert.Equal(t, int32(2), out[1].PartitionID)
	assert.Equal(t, uint32(1001), out[1].Range.Lo, "overlap with partition 1 truncated off the front")
	assert.Equal(t, uint32(2000), out[1].Range.Hi)

	// Trailing gap from 2001 filled round-robin (next filler slot → activePartitions[0] = 1).
	assert.Equal(t, int32(1), out[2].PartitionID)
	assert.Equal(t, uint32(2001), out[2].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), out[2].Range.Hi)

	a := &assignment.Assignment{Entries: out}
	require.NoError(t, a.Validate())
}

// TestStitchReportedEntries_FullyCoveredRangeIsDropped verifies that a
// late-arriving entry entirely contained within already-covered space
// is dropped (not emitted as a zero-width entry).
func TestStitchReportedEntries_FullyCoveredRangeIsDropped(t *testing.T) {
	reported := []reportedEntry{
		{partitionID: 1, hr: assignment.HashRange{Lo: 0, Hi: 1000}},
		{partitionID: 2, hr: assignment.HashRange{Lo: 200, Hi: 500}}, // fully inside [0..1000]
	}
	sortReportedEntriesForTest(reported)

	out := stitchReportedEntries(reported, []int32{1, 2}, log.NewNopLogger())
	require.Len(t, out, 2, "should have only partition 1's range plus one trailing filler")
	assert.Equal(t, int32(1), out[0].PartitionID)
	assert.Equal(t, uint32(0), out[0].Range.Lo)
	assert.Equal(t, uint32(1000), out[0].Range.Hi)
	assert.Equal(t, uint32(1001), out[1].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), out[1].Range.Hi)

	a := &assignment.Assignment{Entries: out}
	require.NoError(t, a.Validate())
}

// TestStitchReportedEntries_EmptyInputReturnsFullRoundRobin verifies
// that with no reported entries the stitch emits a single filler
// covering [0, MaxUint32]. In production this path is unused because
// reconstructAssignment short-circuits to FineEvenSplit when no ranges
// are reported; this test exists to document stitch's behavior in
// isolation.
func TestStitchReportedEntries_EmptyInputReturnsFullRoundRobin(t *testing.T) {
	out := stitchReportedEntries(nil, []int32{5, 6}, log.NewNopLogger())
	require.Len(t, out, 1)
	assert.Equal(t, int32(5), out[0].PartitionID)
	assert.Equal(t, uint32(0), out[0].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), out[0].Range.Hi)
}

// TestStitchReportedEntries_BoundaryHiMaxUint32 verifies the edge case
// where a reported range ends exactly at math.MaxUint32: no trailing
// filler should be emitted (avoid a zero-width entry or overflow).
func TestStitchReportedEntries_BoundaryHiMaxUint32(t *testing.T) {
	reported := []reportedEntry{
		{partitionID: 1, hr: assignment.HashRange{Lo: 100, Hi: math.MaxUint32}},
	}
	sortReportedEntriesForTest(reported)

	out := stitchReportedEntries(reported, []int32{1, 2}, log.NewNopLogger())
	// Expect: leading filler [0..99] via rr=0 → partition 1, then the reported range.
	require.Len(t, out, 2)
	assert.Equal(t, uint32(0), out[0].Range.Lo)
	assert.Equal(t, uint32(99), out[0].Range.Hi)
	assert.Equal(t, uint32(100), out[1].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), out[1].Range.Hi)

	a := &assignment.Assignment{Entries: out}
	require.NoError(t, a.Validate())
}

// sortReportedEntriesForTest is a thin alias over sortReportedEntries
// for test readability.
func sortReportedEntriesForTest(r []reportedEntry) {
	sortReportedEntries(r)
}
