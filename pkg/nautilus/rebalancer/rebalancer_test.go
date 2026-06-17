// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// seriesCfg returns a Config with the given movement budget. Tests
// model load by populating rangeRate.series; withSampleRateFromSeries
// then copies the integer count into the float sampleRate field so
// the slicer (which balances on sampleRate) treats one series as one
// sample-per-second of load.
func seriesCfg(movementBudget float64) Config {
	return Config{
		MovementBudget: movementBudget,
	}
}

// withSampleRateFromSeries fills in rangeRate.sampleRate for every
// entry that has a non-zero series count but no sample rate. Tests
// have historically driven the slicer with raw series counts (the
// pre-v5 implicit fallback in buildLoadMap); now that the fallback
// is gone, tests opt in to the same effective behavior by calling
// this helper on the rates they build. Production code does NOT use
// this — the readcache reports sampleRate directly.
func withSampleRateFromSeries(rates []rangeRate) []rangeRate {
	for i := range rates {
		if rates[i].sampleRate == 0 && rates[i].series > 0 {
			rates[i].sampleRate = float64(rates[i].series)
		}
	}
	return rates
}

// partitionLFromRates returns a per-partition sum of rangeRate.series.
// Kept as a test helper so existing fixtures that build a "ground-truth
// L_pid" for assertion narratives still compile, even though the
// slicer no longer takes such a map as input.
func partitionLFromRates(_ *assignment.Assignment, rates []rangeRate) map[int32]int64 {
	out := make(map[int32]int64)
	for _, r := range rates {
		out[r.partitionID] += r.series
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
			rates = append(rates, rangeRate{hr: e.Range, series: 10000 / int64(initialSlicesPerPartition), partitionID: e.PartitionID})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 100 / int64(initialSlicesPerPartition), partitionID: e.PartitionID})
		}
	}

	r := &Rebalancer{cfg: seriesCfg(0.5)}
	partL := partitionLFromRates(initial, rates)

	rates = withSampleRateFromSeries(rates)
	_ = partL
	result, _ := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})
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
		rates = append(rates, rangeRate{hr: e.Range, series: 100, partitionID: e.PartitionID})
	}

	r := &Rebalancer{cfg: seriesCfg(0.09)}
	partL := partitionLFromRates(initial, rates)

	rates = withSampleRateFromSeries(rates)
	_ = partL
	result, _ := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})
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
		rates = append(rates, rangeRate{hr: e.Range, series: 100, partitionID: e.PartitionID})
	}

	r := &Rebalancer{cfg: seriesCfg(0.5)}
	partL := partitionLFromRates(initial, rates)

	rates = withSampleRateFromSeries(rates)
	_ = partL
	result, _ := r.runSlicer(initial, rates, nil, activePartitions, nil, time.Time{})
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
			rates = append(rates, rangeRate{hr: e.Range, series: 100000, partitionID: e.PartitionID})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 1, partitionID: e.PartitionID})
		}
	}

	r := &Rebalancer{cfg: seriesCfg(0.09)}
	partL := partitionLFromRates(initial, rates)

	rates = withSampleRateFromSeries(rates)
	_ = partL
	result, _ := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})
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

	result, _ := mergeAdjacentCold(entries, 1.0, math.MaxFloat64, 1.0, 1, 0)

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

	result, _ := mergeAdjacentCold(entries, 1.0, math.MaxFloat64, 1.0, 1, 0)

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

// TestMergeAdjacentCold_PerPartitionFloor verifies that
// cross-partition merges stop draining a partition once it reaches
// the per-partition floor. Without this guard, a lightly-loaded
// partition can be fully absorbed by neighbours, leaving its owner
// ingesters with zero ranges until Phase 3 floods them back.
func TestMergeAdjacentCold_PerPartitionFloor(t *testing.T) {
	// 6 alternating cold ranges: A B A B A B. Without a floor,
	// every B would merge into A, leaving B with 0 entries. With a
	// floor of 2, the merge must leave B with at least 2 entries.
	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 1}, load: 0.05},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: 299}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 300, Hi: 399}, PartitionID: 1}, load: 0.05},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 400, Hi: 499}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 500, Hi: 599}, PartitionID: 1}, load: 0.05},
	}

	result, _ := mergeAdjacentCold(entries, 1.0, math.MaxFloat64, 1.0, 1, 2)

	count := map[int32]int{}
	for _, rl := range result {
		count[rl.entry.PartitionID]++
	}
	assert.GreaterOrEqual(t, count[1], 2,
		"per-partition floor should keep partition 1 above %d entries; got %d",
		2, count[1])
}

// TestMergeAdjacentCold_FloorPreventsCompleteDrain reproduces the
// production symptom: a partition whose ranges are all cold-adjacent
// to neighbours gets drained to zero in a single round. With the
// floor in place, at least one entry survives.
func TestMergeAdjacentCold_FloorPreventsCompleteDrain(t *testing.T) {
	// One donor partition (P=1) with 3 cold ranges, all adjacent to
	// hotter (still cold relative to mean) ranges on P=0 and P=2.
	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 0.2},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 1}, load: 0.05},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: 299}, PartitionID: 2}, load: 0.2},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 300, Hi: 399}, PartitionID: 1}, load: 0.05},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 400, Hi: 499}, PartitionID: 0}, load: 0.2},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 500, Hi: 599}, PartitionID: 1}, load: 0.05},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 600, Hi: 699}, PartitionID: 2}, load: 0.2},
	}

	// Without floor (==0): partition 1 is fully drained.
	resultNoFloor, _ := mergeAdjacentCold(entries, 1.0, math.MaxFloat64, 1.0, 1, 0)
	noFloorCount := 0
	for _, rl := range resultNoFloor {
		if rl.entry.PartitionID == 1 {
			noFloorCount++
		}
	}
	require.Equal(t, 0, noFloorCount, "without floor, partition 1 should be fully drained (this is the bug)")

	// With floor=1: partition 1 retains at least one entry.
	resultFloor, _ := mergeAdjacentCold(entries, 1.0, math.MaxFloat64, 1.0, 1, 1)
	floorCount := 0
	for _, rl := range resultFloor {
		if rl.entry.PartitionID == 1 {
			floorCount++
		}
	}
	assert.GreaterOrEqual(t, floorCount, 1, "with floor=1, partition 1 should retain at least one entry")
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
		rates = append(rates, rangeRate{hr: e.Range, series: 100, partitionID: e.PartitionID})
	}

	r := &Rebalancer{cfg: seriesCfg(0.0)}
	partL := partitionLFromRates(initial, rates)

	rates = withSampleRateFromSeries(rates)
	_ = partL
	result, _ := r.runSlicer(initial, rates, nil, activePartitions, nil, time.Time{})
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
			rates = append(rates, rangeRate{hr: e.Range, series: 120, partitionID: e.PartitionID})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 80, partitionID: e.PartitionID})
		}
	}

	r := &Rebalancer{cfg: seriesCfg(0.5)}
	partL := partitionLFromRates(initial, rates)

	rates = withSampleRateFromSeries(rates)
	_ = partL
	result, _ := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})
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

// TestValidateSlicerPhaseEntries covers the helper that runSlicer uses
// to detect a phase that has produced gaps, overlaps, or a missing
// prefix/suffix in the tiling. The phase's per-round revert depends on
// this helper recognising every shape of invalidity Validate flags.
func TestValidateSlicerPhaseEntries(t *testing.T) {
	t.Run("valid full tiling returns nil", func(t *testing.T) {
		entries := []rangeLoad{
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1}},
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2}},
		}
		assert.NoError(t, validateSlicerPhaseEntries(entries))
	})

	t.Run("empty entries returns error", func(t *testing.T) {
		assert.Error(t, validateSlicerPhaseEntries(nil))
	})

	t.Run("first entry not starting at 0 returns error", func(t *testing.T) {
		entries := []rangeLoad{
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 10, Hi: math.MaxUint32}, PartitionID: 1}},
		}
		err := validateSlicerPhaseEntries(entries)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "first entry must start at 0")
	})

	t.Run("gap between adjacent entries returns error", func(t *testing.T) {
		entries := []rangeLoad{
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1}},
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 2}},
		}
		err := validateSlicerPhaseEntries(entries)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "gap or overlap")
	})

	t.Run("overlap between adjacent entries returns error", func(t *testing.T) {
		entries := []rangeLoad{
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 150}, PartitionID: 1}},
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2}},
		}
		err := validateSlicerPhaseEntries(entries)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "gap or overlap")
	})

	t.Run("last entry not ending at MaxUint32 returns error", func(t *testing.T) {
		entries := []rangeLoad{
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1}},
		}
		err := validateSlicerPhaseEntries(entries)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "last entry must end")
	})

	t.Run("unsorted but otherwise-valid entries are sorted before validation", func(t *testing.T) {
		entries := []rangeLoad{
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2}},
			{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1}},
		}
		assert.NoError(t, validateSlicerPhaseEntries(entries))
	})
}

// TestSnapshotRangeLoads covers the deep-copy helper used to enable
// per-phase revert in runSlicer. Mutating the snapshot or the source
// must not bleed into the other.
func TestSnapshotRangeLoads(t *testing.T) {
	src := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1}, load: 10, series: 5},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2}, load: 20, series: 10},
	}
	snap := snapshotRangeLoads(src)

	require.Len(t, snap, 2)
	assert.Equal(t, src[0], snap[0])
	assert.Equal(t, src[1], snap[1])

	// Mutating src must not affect snap.
	src[0].entry.PartitionID = 99
	src[0].load = 0
	assert.EqualValues(t, 1, snap[0].entry.PartitionID, "snapshot must not see src mutation")
	assert.EqualValues(t, 10, snap[0].load, "snapshot must not see src mutation")

	// Mutating snap must not affect src.
	snap[1].entry.PartitionID = 50
	assert.EqualValues(t, 2, src[1].entry.PartitionID, "src must not see snapshot mutation")
}

// TestSlicerInvalidBoundaryWindow covers the helper that picks a small
// context window around the first invalid boundary, so log lines pin
// the bug down without exploding in size.
func TestSlicerInvalidBoundaryWindow(t *testing.T) {
	t.Run("missing prefix windows from index 0", func(t *testing.T) {
		entries := []assignment.Entry{
			{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 1},
			{Range: assignment.HashRange{Lo: 200, Hi: 299}, PartitionID: 2},
			{Range: assignment.HashRange{Lo: 300, Hi: math.MaxUint32}, PartitionID: 3},
		}
		lo, hi := slicerInvalidBoundaryWindow(entries)
		assert.Equal(t, 0, lo)
		assert.Equal(t, 2, hi) // capped at len-1
	})

	t.Run("interior gap windows around the boundary", func(t *testing.T) {
		entries := []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1},
			{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 2},
			{Range: assignment.HashRange{Lo: 200, Hi: 299}, PartitionID: 3},
			{Range: assignment.HashRange{Lo: 500, Hi: 599}, PartitionID: 4}, // gap before this
			{Range: assignment.HashRange{Lo: 600, Hi: math.MaxUint32}, PartitionID: 5},
		}
		lo, hi := slicerInvalidBoundaryWindow(entries)
		assert.Equal(t, 1, lo) // i-2
		assert.Equal(t, 4, hi) // i+2 capped at len-1
	})

	t.Run("missing suffix windows the last few", func(t *testing.T) {
		entries := []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1},
			{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 2},
		}
		lo, hi := slicerInvalidBoundaryWindow(entries)
		assert.Equal(t, 0, lo)
		assert.Equal(t, 1, hi)
	})

	t.Run("valid tiling returns sentinel", func(t *testing.T) {
		entries := []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1},
		}
		lo, hi := slicerInvalidBoundaryWindow(entries)
		assert.Equal(t, -1, lo)
		assert.Equal(t, -1, hi)
	})
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
			rates = append(rates, rangeRate{hr: e.Range, series: 10, partitionID: e.PartitionID})
			continue
		}
		if hotIdx[i] {
			rates = append(rates, rangeRate{hr: e.Range, series: 500, partitionID: e.PartitionID})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 10, partitionID: e.PartitionID})
		}
	}

	r := &Rebalancer{cfg: seriesCfg(0.0)}
	partL := partitionLFromRates(initial, rates)

	rates = withSampleRateFromSeries(rates)
	_ = partL
	result, _ := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})
	require.NoError(t, result.Validate())

	// The hot slice on the overloaded partition should have been split.
	assert.Greater(t, len(result.Entries), len(initial.Entries),
		"hot slice on overloaded partition should be split for granularity")
}

const (
	testStoreLease     = time.Hour
	testStoreLookahead = 5 * time.Minute
)

func TestLogStore(t *testing.T) {
	s := newLogStore()

	assert.Nil(t, s.latestActiveAssignment(time.Now()))
	assert.Empty(t, s.snapshot())

	a1 := assignment.EvenSplit([]int32{0, 1})
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.True(t, s.apply(t1, a1, testStoreLease, testStoreLookahead, time.Hour))

	got := s.latestActiveAssignment(t1)
	require.NotNil(t, got)
	require.Equal(t, len(a1.Entries), len(got.Entries))

	a2 := assignment.EvenSplit([]int32{0, 1, 2})
	t2 := t1.Add(time.Minute)
	require.True(t, s.apply(t2, a2, testStoreLease, testStoreLookahead, time.Hour))

	got = s.latestActiveAssignment(t2)
	require.NotNil(t, got)
	require.Equal(t, len(a2.Entries), len(got.Entries))

	// Preempted entries from a1 plus active entries from a2 should
	// be reflected in the snapshot. Exact count depends on partition
	// boundary alignment.
	snap := s.snapshot()
	require.NotEmpty(t, snap)
}

func TestLogStore_SubscribeReceivesUpdates(t *testing.T) {
	s := newLogStore()

	a1 := assignment.EvenSplit([]int32{0, 1})
	require.True(t, s.apply(time.Now(), a1, testStoreLease, testStoreLookahead, time.Hour))

	initial, updates, unsubscribe := s.subscribe(false)
	defer unsubscribe()

	require.NotNil(t, initial)
	require.NotEmpty(t, initial.entries)

	a2 := assignment.EvenSplit([]int32{0, 1, 2})
	require.True(t, s.apply(time.Now().Add(time.Minute), a2, testStoreLease, testStoreLookahead, time.Hour))

	select {
	case u := <-updates:
		require.NotEmpty(t, u.entries)
	case <-time.After(time.Second):
		t.Fatal("did not receive update on subscription channel")
	}
}

func TestLogStore_SubscribeConflatesSlowConsumer(t *testing.T) {
	s := newLogStore()

	_, updates, unsubscribe := s.subscribe(false)
	defer unsubscribe()

	// Three back-to-back applies; a slow consumer should see exactly
	// one (the last) snapshot.
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.True(t, s.apply(t0, assignment.EvenSplit([]int32{0}), testStoreLease, testStoreLookahead, time.Hour))
	require.True(t, s.apply(t0.Add(time.Second), assignment.EvenSplit([]int32{0, 1}), testStoreLease, testStoreLookahead, time.Hour))
	require.True(t, s.apply(t0.Add(2*time.Second), assignment.EvenSplit([]int32{0, 1, 2}), testStoreLease, testStoreLookahead, time.Hour))

	// Drain — channel must hold exactly one buffered value (the
	// latest), and after reading nothing else should be queued.
	select {
	case u := <-updates:
		require.NotEmpty(t, u.entries)
	case <-time.After(time.Second):
		t.Fatal("did not receive any update")
	}
	select {
	case <-updates:
		t.Fatal("conflation failed: expected only one update buffered")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestLogStore_UnsubscribeReleasesSubscriber(t *testing.T) {
	s := newLogStore()
	_, _, unsubscribe := s.subscribe(false)
	require.Equal(t, 1, s.numSubscribers())
	unsubscribe()
	require.Equal(t, 0, s.numSubscribers())
}

// TestLogStore_SubscribeBeforeFirstApplyReturnsNilInitial is the
// regression test for the empty-snapshot-before-ready bug: a
// rebalancer that has just restarted loads its persisted log from
// disk; those entries may all be expired by the time a subscriber
// connects. Returning the (empty) LiveEntries as the "initial"
// snapshot would tell readcaches/distributors "you own nothing" and
// trigger a fleet-wide drop. subscribe must return nil until
// apply() has run, signalling the gRPC handler to skip its initial
// Send and wait for the first real broadcast instead.
func TestLogStore_SubscribeBeforeFirstApplyReturnsNilInitial(t *testing.T) {
	s := newLogStore()

	// Seed the in-memory log directly to mimic a startup that loaded
	// persisted entries off disk. We do NOT call apply(), mirroring
	// the production cold-start order: seedFromEntries runs in
	// starting() before the first rebalance round.
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	seeded := []assignment.LogEntry{
		{Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 0, From: t0, To: t0.Add(time.Hour)},
	}
	s.seedFromEntries(seeded)

	initial, _, unsubscribe := s.subscribe(false)
	defer unsubscribe()
	assert.Nil(t, initial,
		"subscribe must return nil initial before the first apply, even when the log has live entries, to prevent a freshly-restarted rebalancer from broadcasting stale state as authoritative")
}

// TestLogStore_FirstApplyPrimesSubscribersAttachedEarly verifies
// that a subscriber attached before any apply still receives the
// first snapshot once apply runs. The previous design also delivered
// the broadcast here because Apply naturally returns changed=true on
// the first call to a fresh store; this test pins the contract.
func TestLogStore_FirstApplyPrimesSubscribersAttachedEarly(t *testing.T) {
	s := newLogStore()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	initial, updates, unsubscribe := s.subscribe(false)
	defer unsubscribe()
	require.Nil(t, initial, "subscribe before ready returns nil initial")

	a := assignment.EvenSplit([]int32{0, 1})
	require.True(t, s.apply(t0, a, testStoreLease, testStoreLookahead, time.Hour))

	select {
	case u := <-updates:
		assert.NotEmpty(t, u.entries,
			"first apply after subscribe must prime the subscriber with the current live state")
		assert.True(t, u.reset, "priming broadcast must be a snapshot")
	case <-time.After(time.Second):
		t.Fatal("subscriber attached before first apply did not receive a priming broadcast")
	}
}

// TestLogStore_NoOpApplyStillPrimesEarlySubscriber covers the more
// subtle case: subscriber connects before any apply, then an apply
// runs but produces no log change (e.g. the log was seeded from
// disk and the slicer round produced the same assignment). Without
// the !changed && becameReady carve-out in apply, the subscriber
// would be stuck waiting indefinitely. With it, the !ready -> ready
// transition forces one broadcast so the subscriber learns the
// current live state.
func TestLogStore_NoOpApplyStillPrimesEarlySubscriber(t *testing.T) {
	s := newLogStore()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Seed the in-memory log to simulate a startup that loaded
	// persisted state. Note: seedFromEntries does NOT flip ready.
	a := assignment.EvenSplit([]int32{0, 1})
	seedEntries := make([]assignment.LogEntry, 0, len(a.Entries))
	for _, e := range a.Entries {
		seedEntries = append(seedEntries, assignment.LogEntry{
			Range:       e.Range,
			PartitionID: e.PartitionID,
			From:        t0,
			To:          t0.Add(testStoreLease),
		})
	}
	s.seedFromEntries(seedEntries)

	// Subscriber connects before any apply.
	initial, updates, unsubscribe := s.subscribe(false)
	defer unsubscribe()
	require.Nil(t, initial, "subscribe before ready returns nil initial even when log is non-empty")

	// Apply the same assignment we seeded with at a time that
	// keeps the existing entries comfortably outside the lookahead
	// window, so Apply produces no successor and returns
	// changed=false. The rebalancer must still broadcast on the
	// !ready -> ready edge.
	changed := s.apply(t0.Add(time.Second), a, testStoreLease, testStoreLookahead, time.Hour)
	assert.False(t, changed, "fixture must reproduce the no-op apply case")

	select {
	case u := <-updates:
		assert.NotEmpty(t, u.entries,
			"a no-op apply on the !ready -> ready edge must still prime the subscriber")
		assert.True(t, u.reset, "priming broadcast must be a snapshot")
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive a priming broadcast on the no-op-but-becameReady apply")
	}
}

// TestLogStore_SubscribeAfterApplyReturnsLiveEntries restates the
// happy-path contract: once apply has run, subscribe returns the
// current live entries as initial, matching the pre-fix behavior
// for established rebalancers.
func TestLogStore_SubscribeAfterApplyReturnsLiveEntries(t *testing.T) {
	s := newLogStore()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a := assignment.EvenSplit([]int32{0, 1})
	require.True(t, s.apply(t0, a, testStoreLease, testStoreLookahead, time.Hour))

	initial, _, unsubscribe := s.subscribe(false)
	defer unsubscribe()
	require.NotNil(t, initial)
	assert.Len(t, initial.entries, len(a.Entries),
		"after first apply, subscribe must return the current live snapshot as initial")
	assert.True(t, initial.reset, "initial must be a snapshot")
}

// TestLogStore_SubscribeIncludesRetainedHistory asserts that
// subscribers receive the full retention-bounded log, history
// included. The distributor's read path resolves partition ownership
// over the query's wall-clock window, so expired leases are
// load-bearing: omitting them makes any query window bound that lands
// before the latest rotation resolve no partitions (or no readcache
// owners). Only entries beyond EntryRetention may be dropped.
func TestLogStore_SubscribeIncludesRetainedHistory(t *testing.T) {
	s := newLogStore()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Round 1: install a tiling. Use a short lease so the entries
	// are expired (but retained) by the time we subscribe.
	a := assignment.EvenSplit([]int32{0, 1})
	require.True(t, s.apply(t1, a, time.Minute, 10*time.Second, time.Hour))
	require.Len(t, s.snapshot(), len(a.Entries))

	// Round 2 well past the lease horizon but within retention: the
	// round-1 entries (To = t1+1m) are expired at t2 yet must still
	// be handed to subscribers.
	t2 := t1.Add(30 * time.Minute)
	require.True(t, s.apply(t2, a, time.Minute, 10*time.Second, time.Hour))
	full := s.snapshot()
	require.Greater(t, len(full), len(a.Entries),
		"unfiltered snapshot must include both rounds")

	initial, _, unsubscribe := s.subscribe(false)
	defer unsubscribe()

	require.NotNil(t, initial)
	require.Len(t, initial.entries, len(full),
		"subscribe must return the full retained log, expired entries included")
}

// TestLogStore_BroadcastIncludesRetainedHistoryAndHonoursRetention
// asserts the broadcast counterpart of the above, and that pruning by
// EntryRetention still bounds what subscribers receive.
func TestLogStore_BroadcastIncludesRetainedHistoryAndHonoursRetention(t *testing.T) {
	s := newLogStore()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Subscribe before any apply so we can observe the broadcasts.
	_, updates, unsubscribe := s.subscribe(false)
	defer unsubscribe()

	// Round 1.
	a := assignment.EvenSplit([]int32{0, 1})
	require.True(t, s.apply(t1, a, time.Minute, 10*time.Second, time.Hour))
	select {
	case <-updates:
	case <-time.After(time.Second):
		t.Fatal("did not receive round-1 broadcast")
	}

	// Round 2 past the round-1 lease horizon but within retention:
	// the broadcast must still carry the expired round-1 entries.
	t2 := t1.Add(30 * time.Minute)
	require.True(t, s.apply(t2, a, time.Minute, 10*time.Second, time.Hour))
	expired := 0
	select {
	case u := <-updates:
		require.NotEmpty(t, u.entries)
		for _, e := range u.entries {
			if !e.To.After(t2) {
				expired++
			}
		}
		require.NotZero(t, expired,
			"round-2 broadcast must include the expired-but-retained round-1 entries")
	case <-time.After(time.Second):
		t.Fatal("did not receive round-2 broadcast")
	}

	// Round 3 beyond round 1's retention horizon: Prune drops the
	// round-1 entries (To = t1+1m < t3-1h), so the broadcast may not
	// grow without bound.
	t3 := t1.Add(2 * time.Hour)
	require.True(t, s.apply(t3, a, time.Minute, 10*time.Second, time.Hour))
	select {
	case u := <-updates:
		require.NotEmpty(t, u.entries)
		for _, e := range u.entries {
			assert.False(t, e.To.Before(t3.Add(-time.Hour)),
				"broadcast included an entry past the retention horizon: %+v", e)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive round-3 broadcast")
	}
}

// TestLogStore_DeltaSubscriberReceivesOnlyMutations pins the delta
// protocol on the hash store: a delta subscriber is primed with a
// reset snapshot, then each mutating apply delivers only the entries
// it created or rewrote, and replaying those deltas client-side
// reproduces the store's snapshot exactly.
func TestLogStore_DeltaSubscriberReceivesOnlyMutations(t *testing.T) {
	s := newLogStore()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a2 := assignment.EvenSplit([]int32{0, 1})
	require.True(t, s.apply(t0, a2, testStoreLease, testStoreLookahead, time.Hour))

	initial, updates, unsubscribe := s.subscribe(true)
	defer unsubscribe()
	require.NotNil(t, initial)
	require.True(t, initial.reset, "delta subscriber must be primed with a snapshot")
	client := assignment.NewLogFromEntries(initial.entries)

	// A steady-state extension round: the lookahead window catches
	// up, so apply pre-issues one successor per chain and touches
	// nothing else. The delta must carry exactly the successors,
	// not the untouched history.
	require.True(t, s.apply(t0.Add(testStoreLease-testStoreLookahead), a2, testStoreLease, testStoreLookahead, time.Hour))

	select {
	case u := <-updates:
		require.False(t, u.reset, "primed delta subscriber must receive a delta, not a snapshot")
		require.Len(t, u.entries, len(a2.Entries),
			"extension round delta must carry only the pre-issued successors")
		require.Less(t, len(u.entries), len(s.snapshot()),
			"delta must be smaller than the full log")
		client = client.MergedWithEntries(u.entries)
		if !u.pruneBefore.IsZero() {
			client.Prune(u.pruneBefore)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive delta broadcast")
	}

	// A reassignment round: partition 2 joins, preempting the active
	// chains. The delta must carry the rewritten (preempted) entries
	// too, or replay would leave stale active leases client-side.
	a3 := assignment.EvenSplit([]int32{0, 1, 2})
	require.True(t, s.apply(t0.Add(testStoreLease), a3, testStoreLease, testStoreLookahead, time.Hour))

	select {
	case u := <-updates:
		require.False(t, u.reset)
		client = client.MergedWithEntries(u.entries)
		if !u.pruneBefore.IsZero() {
			client.Prune(u.pruneBefore)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive reassignment delta broadcast")
	}

	assert.Equal(t, s.snapshot(), client.Entries(),
		"snapshot + delta replay must reproduce the server log exactly")
}

// TestLogStore_DeltaCoalescingLosesNothing covers the slow-subscriber
// path: multiple mutating applies land while the subscriber isn't
// consuming. The pending deltas must coalesce (not drop), so a single
// read plus replay still reproduces the final server log.
func TestLogStore_DeltaCoalescingLosesNothing(t *testing.T) {
	s := newLogStore()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	require.True(t, s.apply(t0, assignment.EvenSplit([]int32{0}), testStoreLease, testStoreLookahead, time.Hour))

	initial, updates, unsubscribe := s.subscribe(true)
	defer unsubscribe()
	require.NotNil(t, initial)
	client := assignment.NewLogFromEntries(initial.entries)

	// Three mutating applies without a consume in between.
	require.True(t, s.apply(t0.Add(time.Second), assignment.EvenSplit([]int32{0, 1}), testStoreLease, testStoreLookahead, time.Hour))
	require.True(t, s.apply(t0.Add(2*time.Second), assignment.EvenSplit([]int32{0, 1, 2}), testStoreLease, testStoreLookahead, time.Hour))
	require.True(t, s.apply(t0.Add(3*time.Second), assignment.EvenSplit([]int32{2, 3}), testStoreLease, testStoreLookahead, time.Hour))

	// The channel holds exactly one coalesced delta.
	select {
	case u := <-updates:
		require.False(t, u.reset)
		client = client.MergedWithEntries(u.entries)
		if !u.pruneBefore.IsZero() {
			client.Prune(u.pruneBefore)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive coalesced delta")
	}
	select {
	case <-updates:
		t.Fatal("expected exactly one coalesced update buffered")
	case <-time.After(50 * time.Millisecond):
	}

	assert.Equal(t, s.snapshot(), client.Entries(),
		"coalesced delta replay must reproduce the server log exactly")
}

// TestLogStore_LegacySubscriberStillGetsSnapshots pins backwards
// compatibility: a subscriber that did not opt into deltas receives
// a full snapshot on every mutating apply.
func TestLogStore_LegacySubscriberStillGetsSnapshots(t *testing.T) {
	s := newLogStore()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	require.True(t, s.apply(t0, assignment.EvenSplit([]int32{0, 1}), testStoreLease, testStoreLookahead, time.Hour))

	_, updates, unsubscribe := s.subscribe(false)
	defer unsubscribe()

	require.True(t, s.apply(t0.Add(time.Minute), assignment.EvenSplit([]int32{0, 1, 2}), testStoreLease, testStoreLookahead, time.Hour))

	select {
	case u := <-updates:
		assert.True(t, u.reset, "legacy subscriber broadcasts must be snapshots")
		assert.Equal(t, s.snapshot(), u.entries,
			"legacy subscriber must receive the full log on every broadcast")
	case <-time.After(time.Second):
		t.Fatal("did not receive broadcast")
	}
}

func TestLogStore_ApplyOutsideLookaheadIsNoOp(t *testing.T) {
	s := newLogStore()
	a := assignment.EvenSplit([]int32{0, 1})
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	require.True(t, s.apply(t0, a, testStoreLease, testStoreLookahead, time.Hour))
	// Re-applying soon after the first round: latest lease's To
	// is far outside lookahead, so this must NOT mutate the log
	// and must NOT broadcast.
	require.False(t, s.apply(t0.Add(time.Second), a, testStoreLease, testStoreLookahead, time.Hour))
}

func TestLogStore_ApplyWithinLookaheadEmitsSuccessor(t *testing.T) {
	s := newLogStore()
	a := assignment.EvenSplit([]int32{0, 1})
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	require.True(t, s.apply(t0, a, testStoreLease, testStoreLookahead, time.Hour))
	// Step forward to (lease - lookahead): now+lookahead == lease
	// horizon, so apply must queue successor leases for every pair.
	t1 := t0.Add(testStoreLease - testStoreLookahead)
	require.True(t, s.apply(t1, a, testStoreLease, testStoreLookahead, time.Hour))

	require.Equal(t, 2*len(a.Entries), len(s.snapshot()),
		"each (Range, PID) now has two entries: active + pre-issued successor")
}

func TestLogStore_ApplyPrunesExpiredEntries(t *testing.T) {
	s := newLogStore()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	const lease = time.Minute
	const lookahead = time.Second
	const retention = time.Minute

	a1 := &assignment.Assignment{Entries: []assignment.Entry{
		{Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1},
	}}
	require.True(t, s.apply(t0, a1, lease, lookahead, retention))

	// Preempt P1 by giving ownership to P2 one second later.
	t1 := t0.Add(time.Second)
	a2 := &assignment.Assignment{Entries: []assignment.Entry{
		{Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 2},
	}}
	require.True(t, s.apply(t1, a2, lease, lookahead, retention))

	// Both entries are still present: P1's lease ended at t1, P2's
	// is active.
	require.Len(t, s.snapshot(), 2)

	// Apply far in the future. By now retention drops anything with
	// To < threshold = (t_now - retention).
	tFuture := t1.Add(10 * time.Minute)
	a3 := &assignment.Assignment{Entries: []assignment.Entry{
		{Range: assignment.HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 3},
	}}
	require.True(t, s.apply(tFuture, a3, lease, lookahead, retention))

	// P1's preempted entry (To=t1) and P2's expired entry
	// (To=t1+lease) both fall well before (tFuture - retention),
	// so both are pruned. Only the freshly-created P3 entry
	// survives.
	snap := s.snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, int32(3), snap[0].PartitionID)
}

func TestLogStore_LeaseHorizon(t *testing.T) {
	s := newLogStore()
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Empty store: zero time.
	assert.True(t, s.leaseHorizon(t0).IsZero())

	a := assignment.EvenSplit([]int32{0, 1})
	require.True(t, s.apply(t0, a, testStoreLease, testStoreLookahead, time.Hour))
	assert.Equal(t, t0.Add(testStoreLease), s.leaseHorizon(t0))
}

func TestEntriesProtoRoundTrip(t *testing.T) {
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	domain := []assignment.LogEntry{
		{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: t0, To: t0.Add(time.Hour)},
		{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 2, From: t0, To: t0.Add(30 * time.Minute)},
		{Range: assignment.HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 3, From: t0, To: t0.Add(2 * time.Hour)},
	}

	wire := EntriesToProto(domain)
	require.Len(t, wire, len(domain))

	round := EntriesFromProto(wire)
	require.Len(t, round, len(domain))
	for i, want := range domain {
		require.Equal(t, want.Range, round[i].Range)
		require.Equal(t, want.PartitionID, round[i].PartitionID)
		require.True(t, want.From.Equal(round[i].From))
		require.True(t, want.To.Equal(round[i].To))
	}
}

// fakeWatchAssignmentsStream implements
// NautilusRebalancer_WatchAssignmentsServer for in-process tests of
// the WatchAssignments handler. Sent messages are appended to a
// channel; ctx and Send errors are configurable.
type fakeWatchAssignmentsStream struct {
	grpc.ServerStream
	ctx     context.Context
	sent    chan *WatchAssignmentsResponse
	sendErr error
}

func newFakeStream(ctx context.Context, buf int) *fakeWatchAssignmentsStream {
	return &fakeWatchAssignmentsStream{
		ctx:  ctx,
		sent: make(chan *WatchAssignmentsResponse, buf),
	}
}

func (s *fakeWatchAssignmentsStream) Send(m *WatchAssignmentsResponse) error {
	if s.sendErr != nil {
		return s.sendErr
	}
	s.sent <- m
	return nil
}

func (s *fakeWatchAssignmentsStream) Context() context.Context       { return s.ctx }
func (s *fakeWatchAssignmentsStream) SetHeader(_ metadata.MD) error  { return nil }
func (s *fakeWatchAssignmentsStream) SendHeader(_ metadata.MD) error { return nil }
func (s *fakeWatchAssignmentsStream) SetTrailer(_ metadata.MD)       {}
func (s *fakeWatchAssignmentsStream) SendMsg(_ interface{}) error    { return nil }
func (s *fakeWatchAssignmentsStream) RecvMsg(_ interface{}) error    { return nil }

func TestRebalancer_NextRoundDelay(t *testing.T) {
	cfg := Config{
		MinRebalanceInterval: 30 * time.Second,
		MaxRebalanceInterval: 5 * time.Minute,
		LeaseDuration:        5 * time.Minute,
		LeaseLookahead:       90 * time.Second,
	}
	r := &Rebalancer{cfg: cfg, store: newLogStore()}
	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Empty log: floor at MinRebalanceInterval.
	assert.Equal(t, cfg.MinRebalanceInterval, r.nextRoundDelay(t0))

	// Seed a tiling. Lease horizon = t0 + 5m.
	require.True(t, r.store.apply(t0, assignment.EvenSplit([]int32{0, 1}), cfg.LeaseDuration, cfg.LeaseLookahead, time.Hour))

	// Just after the round: horizon - lookahead = 5m - 90s = 3m30s.
	assert.Equal(t, 3*time.Minute+30*time.Second, r.nextRoundDelay(t0))

	// After we've consumed all but lookahead's worth of runway:
	// floor kicks in. (We haven't queued a successor here; the
	// chain-end is the original lease's To.)
	assert.Equal(t, cfg.MinRebalanceInterval, r.nextRoundDelay(t0.Add(cfg.LeaseDuration-10*time.Second)))

	// After a successor has been queued at the lookahead edge, the
	// chain extends to (original.To + leaseDuration). The next round
	// must be scheduled relative to the chain end, not to the
	// soon-to-expire active lease — otherwise the rebalancer would
	// thrash at MinRebalanceInterval for the entire active-lease tail.
	tEdge := t0.Add(cfg.LeaseDuration - cfg.LeaseLookahead)
	require.True(t, r.store.apply(tEdge, assignment.EvenSplit([]int32{0, 1}), cfg.LeaseDuration, cfg.LeaseLookahead, time.Hour))
	// Chain-end = t0 + 2*lease. delay at tEdge = (t0+2*lease) - tEdge - lookahead = lease.
	// That equals MaxRebalanceInterval here, so the ceiling clamps it.
	assert.Equal(t, cfg.MaxRebalanceInterval, r.nextRoundDelay(tEdge))
	// And one minute later we should still be ~lease - 1m away from
	// needing to act (modulo the ceiling), well above the floor.
	assert.Greater(t, r.nextRoundDelay(tEdge.Add(time.Minute)), cfg.MinRebalanceInterval)

	// Hypothetical horizon very far in the future: ceiling kicks in.
	rFar := &Rebalancer{cfg: cfg, store: newLogStore()}
	require.True(t, rFar.store.apply(t0, assignment.EvenSplit([]int32{0, 1}), 24*time.Hour, cfg.LeaseLookahead, time.Hour))
	assert.Equal(t, cfg.MaxRebalanceInterval, rFar.nextRoundDelay(t0))
}

// TestRebalancer_WatchAssignments_SkipsInitialBeforeFirstApply
// pins the wire-level contract that complements the
// subscribe-returns-nil-initial fix: the gRPC handler must NOT send
// an empty Entries response before the rebalancer's first apply.
// Sending one would tell every connected distributor / readcache
// "the assignment is empty" — which is exactly the misread that
// triggered the readcache fleet drop after a rebalancer restart.
func TestRebalancer_WatchAssignments_SkipsInitialBeforeFirstApply(t *testing.T) {
	r := &Rebalancer{store: newLogStore()}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newFakeStream(ctx, 4)

	done := make(chan error, 1)
	go func() {
		done <- r.WatchAssignments(&WatchAssignmentsRequest{}, stream)
	}()

	// No apply has run; handler must not Send anything yet.
	select {
	case msg := <-stream.sent:
		t.Fatalf("WatchAssignments must not send before first apply, got %+v", msg)
	case <-time.After(100 * time.Millisecond):
	}

	// First apply primes the subscriber via the broadcast path.
	require.True(t, r.store.apply(time.Now(), assignment.EvenSplit([]int32{0, 1}), testStoreLease, testStoreLookahead, time.Hour))
	select {
	case msg := <-stream.sent:
		require.NotEmpty(t, msg.Entries,
			"first apply must broadcast the primed snapshot to subscribers attached pre-ready")
	case <-time.After(time.Second):
		t.Fatal("did not receive priming broadcast after first apply")
	}

	cancel()
	<-done
}

func TestRebalancer_WatchAssignments_SendsInitialAndUpdates(t *testing.T) {
	r := &Rebalancer{store: newLogStore()}

	// Seed the log near wall-clock now so the entries are still
	// live when WatchAssignments calls subscribe(time.Now()).
	// testStoreLease is long (1h) so a small skew between this
	// timestamp and the handler's time.Now() is irrelevant.
	t0 := time.Now()
	r.store.apply(t0, assignment.EvenSplit([]int32{0, 1}), testStoreLease, testStoreLookahead, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newFakeStream(ctx, 4)

	done := make(chan error, 1)
	go func() {
		done <- r.WatchAssignments(&WatchAssignmentsRequest{}, stream)
	}()

	// Initial snapshot.
	select {
	case msg := <-stream.sent:
		require.NotEmpty(t, msg.Entries)
	case <-time.After(time.Second):
		t.Fatal("did not receive initial snapshot")
	}

	// Apply a change; expect a fresh snapshot.
	r.store.apply(t0.Add(time.Minute), assignment.EvenSplit([]int32{0, 1, 2}), testStoreLease, testStoreLookahead, time.Hour)
	select {
	case msg := <-stream.sent:
		require.NotEmpty(t, msg.Entries)
	case <-time.After(time.Second):
		t.Fatal("did not receive update snapshot")
	}

	// Cancel the stream context; handler should exit.
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("WatchAssignments did not exit on context cancel")
	}
}

// TestRebalancer_WatchAssignments_StreamObservability pins the
// connect/send/disconnect accounting added to diagnose rebalancer
// TX: a delta subscriber must register exactly one started stream,
// one snapshot send (the initial full log), and per-round delta
// sends, with the active gauge returning to zero on disconnect.
func TestRebalancer_WatchAssignments_StreamObservability(t *testing.T) {
	reg := prometheus.NewPedanticRegistry()
	r := &Rebalancer{store: newLogStore(), metrics: newMetrics(reg)}

	t0 := time.Now()
	r.store.apply(t0, assignment.EvenSplit([]int32{0, 1}), testStoreLease, testStoreLookahead, time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := newFakeStream(ctx, 4)

	done := make(chan error, 1)
	go func() {
		done <- r.WatchAssignments(&WatchAssignmentsRequest{SupportsDeltas: true}, stream)
	}()

	// Initial snapshot.
	select {
	case msg := <-stream.sent:
		require.True(t, msg.Reset_, "first message must be the snapshot")
	case <-time.After(time.Second):
		t.Fatal("did not receive initial snapshot")
	}

	// A mutating round produces a delta for the primed subscriber.
	r.store.apply(t0.Add(time.Minute), assignment.EvenSplit([]int32{0, 1, 2}), testStoreLease, testStoreLookahead, time.Hour)
	select {
	case msg := <-stream.sent:
		require.False(t, msg.Reset_, "primed delta subscriber must receive a delta")
	case <-time.After(time.Second):
		t.Fatal("did not receive delta")
	}

	assert.Equal(t, float64(1), testutil.ToFloat64(r.metrics.watchStreamsStarted.WithLabelValues("hash", "delta")))
	assert.Equal(t, float64(1), testutil.ToFloat64(r.metrics.watchStreamsActive.WithLabelValues("hash")))
	assert.Equal(t, float64(1), testutil.ToFloat64(r.metrics.watchSentMessages.WithLabelValues("hash", "snapshot")))
	assert.Equal(t, float64(1), testutil.ToFloat64(r.metrics.watchSentMessages.WithLabelValues("hash", "delta")))
	assert.Greater(t, testutil.ToFloat64(r.metrics.watchSentBytes.WithLabelValues("hash", "snapshot")), float64(0))
	assert.Greater(t, testutil.ToFloat64(r.metrics.watchSentEntries.WithLabelValues("hash", "snapshot")), float64(0))

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("WatchAssignments did not exit on context cancel")
	}
	assert.Equal(t, float64(0), testutil.ToFloat64(r.metrics.watchStreamsActive.WithLabelValues("hash")),
		"active gauge must return to zero on disconnect")
}

func TestLoadMap_SeriesAt(t *testing.T) {
	rates := []rangeRate{
		{hr: assignment.HashRange{Lo: 0, Hi: 999}, series: 100, partitionID: 0},
		{hr: assignment.HashRange{Lo: 1000, Hi: 1999}, series: 300, partitionID: 1},
	}
	lm := buildLoadMap(rates)

	assert.Equal(t, int64(100), lm.seriesAt(0, assignment.HashRange{Lo: 0, Hi: 999}))
	assert.Equal(t, int64(300), lm.seriesAt(1, assignment.HashRange{Lo: 1000, Hi: 1999}))
	assert.Equal(t, int64(0), lm.seriesAt(0, assignment.HashRange{Lo: 5000, Hi: 6000}))
	// Same range under a different partition is a different key.
	assert.Equal(t, int64(0), lm.seriesAt(1, assignment.HashRange{Lo: 0, Hi: 999}))
}

// TestLoadMap_MaxOverReplicas verifies that buildLoadMap aggregates
// per-(partition, range) series across replicas by taking the max,
// not the sum. Each healthy mirror of a partition reports a near-
// identical count for the same ranges, and partitionL is already on
// a max-over-owners scale. Phase 3 of the slicer mixes per-range
// series with the partition-level movable budget, so both signals
// must share the same scale; summing here would inflate the
// per-range load by the replication factor and cause systematic
// over-rejection in Phase 3's `rl.series > mov` gate.
func TestLoadMap_MaxOverReplicas(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 999}
	rates := []rangeRate{
		{hr: hr, series: 100, partitionID: 0},
		{hr: hr, series: 150, partitionID: 0},
		{hr: hr, series: 120, partitionID: 0},
	}
	lm := buildLoadMap(rates)

	assert.Equal(t, int64(150), lm.seriesAt(0, hr))
}

// TestLoadMap_PartitionResidueSeparate verifies that reports with the
// same hash range but different partition IDs (e.g. residue on a
// previous owner alongside growth on the new owner) are tracked as
// distinct entries rather than collapsed.
func TestLoadMap_PartitionResidueSeparate(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 999}
	rates := []rangeRate{
		// New owner: growth.
		{hr: hr, series: 50, partitionID: 7},
		// Previous owner: residue larger than current growth.
		{hr: hr, series: 200, partitionID: 3},
	}
	lm := buildLoadMap(rates)

	assert.Equal(t, int64(50), lm.seriesAt(7, hr), "new owner only sees its growth")
	assert.Equal(t, int64(200), lm.seriesAt(3, hr), "previous owner reports its residue")
}

// TestLoadMap_SampleRateAt verifies that buildLoadMap propagates the
// new per-(partition, range) sample-rate signal from rates onto the
// loadMap. Phase 3's hot/cold scoring and movable-budget math both
// read via sampleRateAt; if the population step drops the field the
// slicer silently sees zero load and never schedules a move.
func TestLoadMap_SampleRateAt(t *testing.T) {
	rates := []rangeRate{
		{hr: assignment.HashRange{Lo: 0, Hi: 999}, series: 100, sampleRate: 12345.5, partitionID: 0},
		{hr: assignment.HashRange{Lo: 1000, Hi: 1999}, series: 0, sampleRate: 6789.25, partitionID: 1},
	}
	lm := buildLoadMap(rates)

	assert.Equal(t, 12345.5, lm.sampleRateAt(0, assignment.HashRange{Lo: 0, Hi: 999}))
	assert.Equal(t, 6789.25, lm.sampleRateAt(1, assignment.HashRange{Lo: 1000, Hi: 1999}))
	assert.Equal(t, 0.0, lm.sampleRateAt(0, assignment.HashRange{Lo: 5000, Hi: 6000}))
	// Different partition is a different key.
	assert.Equal(t, 0.0, lm.sampleRateAt(1, assignment.HashRange{Lo: 0, Hi: 999}))
}

// TestRunSlicer_SeriesOnlyReporterProducesNoMoves verifies that with
// the v5 fallback removed, a reporter that populated only series and
// no sampleRate produces no Phase 3 moves: the slicer sees zero load
// everywhere and has nothing to balance. Tests that want raw series
// counts to drive Phase 3 must opt in via withSampleRateFromSeries.
func TestRunSlicer_SeriesOnlyReporterProducesNoMoves(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, 4)

	// Heavily skewed series counts; deliberately no sampleRate set.
	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, series: 10000, partitionID: e.PartitionID})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 1, partitionID: e.PartitionID})
		}
	}

	r := &Rebalancer{
		cfg:           seriesCfg(0.5),
		moveCooldowns: make(map[assignment.HashRange]time.Time),
	}
	_, actions := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})

	var moves int
	for _, a := range actions {
		if a.Kind == ActionMove {
			moves++
		}
	}
	assert.Equalf(t, 0, moves,
		"expected zero Phase 3 moves when reporters only populate series "+
			"(no sample-rate signal means no load gradient for the slicer to balance); "+
			"got %d moves", moves)
}

// TestLoadMap_SeriesOnlyReporterContributesZeroLoad verifies that a
// reporter populating series but no sampleRate (legacy ingester
// reporters during the readcache rollout, or a faulty new reporter)
// contributes ZERO load: from v5 onward sampleRate is the sole load
// signal and there is no implicit fallback to head-series count.
// Tests that want raw series counts to drive the slicer can opt in
// via withSampleRateFromSeries.
func TestLoadMap_SeriesOnlyReporterContributesZeroLoad(t *testing.T) {
	hr := assignment.HashRange{Lo: 0, Hi: 999}
	rates := []rangeRate{
		{hr: hr, series: 42, partitionID: 0}, // no sampleRate set
	}
	lm := buildLoadMap(rates)

	assert.Equal(t, int64(42), lm.seriesAt(0, hr),
		"series count is still surfaced for observability")
	assert.Equal(t, 0.0, lm.sampleRateAt(0, hr),
		"reporters with no sampleRate contribute zero to the slicer's load signal")
}

// TestPartitionLoadFromRates verifies that the per-partition load
// signal Phase 3 consumes is the sum of per-range sample rates,
// restricted to the active partition set. Reporters that didn't
// populate sampleRate add nothing — series counts are not a fallback.
func TestPartitionLoadFromRates(t *testing.T) {
	rates := []rangeRate{
		{hr: assignment.HashRange{Lo: 0, Hi: 99}, sampleRate: 100, partitionID: 0},
		{hr: assignment.HashRange{Lo: 100, Hi: 199}, sampleRate: 200, partitionID: 0},
		{hr: assignment.HashRange{Lo: 200, Hi: 299}, sampleRate: 50, partitionID: 1},
		// Inactive partition: still aggregated (so a partition that
		// just went inactive can be observed as draining), but the
		// caller decides whether to consume it.
		{hr: assignment.HashRange{Lo: 300, Hi: 399}, sampleRate: 999, partitionID: 99},
		// Series-only reporter contributes zero — no fallback.
		{hr: assignment.HashRange{Lo: 400, Hi: 499}, series: 7, partitionID: 1},
	}
	out := partitionLoadFromRates(rates, []int32{0, 1})

	assert.Equal(t, 300.0, out[0])
	assert.Equal(t, 50.0, out[1],
		"series-only reporters contribute zero; only sampleRate is summed")
	// active set entries always present (seeded).
	_, ok0 := out[0]
	_, ok1 := out[1]
	assert.True(t, ok0 && ok1)
}

// TestCurrentOwnershipSet verifies that we build a (partition, range)
// set from the live assignment that filterRatesByCurrentOwnership
// can probe in O(1) per rate report. Empty/nil assignments collapse
// to a nil set so callers fall through to "no filter" semantics
// during cold start.
func TestCurrentOwnershipSet(t *testing.T) {
	t.Run("nil assignment yields nil set", func(t *testing.T) {
		assert.Nil(t, currentOwnershipSet(nil))
	})

	t.Run("empty assignment yields nil set", func(t *testing.T) {
		assert.Nil(t, currentOwnershipSet(&assignment.Assignment{}))
	})

	t.Run("populated assignment yields one key per entry", func(t *testing.T) {
		a := &assignment.Assignment{Entries: []assignment.Entry{
			{PartitionID: 0, Range: assignment.HashRange{Lo: 0, Hi: 99}},
			{PartitionID: 1, Range: assignment.HashRange{Lo: 100, Hi: 199}},
			{PartitionID: 0, Range: assignment.HashRange{Lo: 200, Hi: 299}},
		}}
		owned := currentOwnershipSet(a)
		require.NotNil(t, owned)
		require.Len(t, owned, 3)

		_, ok := owned[partitionRangeKey{partitionID: 0, hr: assignment.HashRange{Lo: 0, Hi: 99}}]
		assert.True(t, ok)
		_, ok = owned[partitionRangeKey{partitionID: 1, hr: assignment.HashRange{Lo: 100, Hi: 199}}]
		assert.True(t, ok)

		// Same range under a different partition is a distinct key:
		// that's exactly the residue case we're trying to detect.
		_, ok = owned[partitionRangeKey{partitionID: 9, hr: assignment.HashRange{Lo: 0, Hi: 99}}]
		assert.False(t, ok)
	})
}

// TestFilterRatesByCurrentOwnership covers the residue-suppression
// helper. The key behaviour: a rate is kept iff its (partitionID,
// range) tuple matches an entry in the current assignment. Reports
// from previous owners (residue) drop out. Reports from current
// owners survive even when the range has zero sampleRate (newly
// seeded EWMAs).
func TestFilterRatesByCurrentOwnership(t *testing.T) {
	hrA := assignment.HashRange{Lo: 0, Hi: 99}
	hrB := assignment.HashRange{Lo: 100, Hi: 199}

	t.Run("nil owned set is a passthrough", func(t *testing.T) {
		rates := []rangeRate{
			{hr: hrA, sampleRate: 1, partitionID: 0},
			{hr: hrB, sampleRate: 2, partitionID: 1},
		}
		kept, dropped := filterRatesByCurrentOwnership(rates, nil)
		assert.Equal(t, 0, dropped)
		assert.Len(t, kept, 2)
	})

	t.Run("drops residue from previous owner", func(t *testing.T) {
		owned := currentOwnershipSet(&assignment.Assignment{Entries: []assignment.Entry{
			{PartitionID: 7, Range: hrA},
		}})
		rates := []rangeRate{
			// Current owner reports growth on (7, hrA): keep.
			{hr: hrA, sampleRate: 100, partitionID: 7},
			// Previous owner still EWMA-decaying on (3, hrA): drop.
			{hr: hrA, sampleRate: 250, partitionID: 3},
		}
		kept, dropped := filterRatesByCurrentOwnership(rates, owned)
		require.Equal(t, 1, dropped)
		require.Len(t, kept, 1)
		assert.Equal(t, int32(7), kept[0].partitionID)
		assert.Equal(t, 100.0, kept[0].sampleRate)
	})

	t.Run("drops rates whose range does not exactly match a current entry", func(t *testing.T) {
		// A split moved (0, [0,199]) into (0, [0,99]) and (0, [100,199]).
		// The readcache may still report the parent range one tick.
		owned := currentOwnershipSet(&assignment.Assignment{Entries: []assignment.Entry{
			{PartitionID: 0, Range: hrA},
			{PartitionID: 0, Range: hrB},
		}})
		rates := []rangeRate{
			{hr: assignment.HashRange{Lo: 0, Hi: 199}, sampleRate: 9999, partitionID: 0}, // pre-split: drop
			{hr: hrA, sampleRate: 50, partitionID: 0},                                    // post-split child: keep
			{hr: hrB, sampleRate: 50, partitionID: 0},                                    // post-split child: keep
		}
		kept, dropped := filterRatesByCurrentOwnership(rates, owned)
		assert.Equal(t, 1, dropped)
		assert.Len(t, kept, 2)
	})

	t.Run("returns empty slice not nil when everything is residue", func(t *testing.T) {
		owned := currentOwnershipSet(&assignment.Assignment{Entries: []assignment.Entry{
			{PartitionID: 0, Range: hrA},
		}})
		rates := []rangeRate{
			{hr: hrA, sampleRate: 1, partitionID: 99},
			{hr: hrB, sampleRate: 1, partitionID: 0},
		}
		kept, dropped := filterRatesByCurrentOwnership(rates, owned)
		assert.Equal(t, 2, dropped)
		assert.NotNil(t, kept) // empty slice, not nil
		assert.Len(t, kept, 0)
	})
}

// TestPartitionLoadFromRates_AfterResidueFilter verifies the
// downstream effect of the filter on the per-partition load signal
// runPhase3 consumes: with residue suppressed, the "phantom" load
// that used to keep a former owner looking hot collapses to zero.
// This is the headline change for the residue-aware aggregation.
func TestPartitionLoadFromRates_AfterResidueFilter(t *testing.T) {
	hrA := assignment.HashRange{Lo: 0, Hi: 99}

	// Current state: range hrA owned by partition 7. Partition 3 is
	// still around but no longer owns hrA. Without the filter,
	// partition 3 would look hot because its readcache still
	// reports a non-zero EWMA for (3, hrA).
	current := &assignment.Assignment{Entries: []assignment.Entry{
		{PartitionID: 7, Range: hrA},
	}}
	rates := []rangeRate{
		{hr: hrA, sampleRate: 100, partitionID: 7},  // current owner's signal
		{hr: hrA, sampleRate: 5000, partitionID: 3}, // residue we want to suppress
	}

	// Without filter: residue inflates partition 3's apparent load.
	unfiltered := partitionLoadFromRates(rates, []int32{3, 7})
	assert.Equal(t, 5000.0, unfiltered[3], "without filter, residue makes ex-owner look hot")
	assert.Equal(t, 100.0, unfiltered[7])

	// With filter: residue removed.
	filtered, dropped := filterRatesByCurrentOwnership(rates, currentOwnershipSet(current))
	require.Equal(t, 1, dropped)
	out := partitionLoadFromRates(filtered, []int32{3, 7})
	assert.Equal(t, 0.0, out[3], "after residue filter, ex-owner contributes zero load")
	assert.Equal(t, 100.0, out[7])
}

// TestRunSlicer_ResidueDoesNotDriveMoves is the integration check
// for the residue filter. We start from a balanced assignment with
// uniform real load and add a large phantom load on a partition that
// no longer owns that range. Pre-filter, the slicer treats the
// ex-owner as the hottest partition and shuffles unrelated ranges
// off it. Post-filter (production code path: rebalance() filters
// rates against the current assignment before passing them into
// runSlicer), residue contributes nothing and the slicer is quiet.
func TestRunSlicer_ResidueDoesNotDriveMoves(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(partitions, 8)
	require.NoError(t, initial.Validate())

	// Real signal: identical sample rate per range so the cluster is
	// balanced and the slicer has nothing to do.
	var rates []rangeRate
	for _, e := range initial.Entries {
		rates = append(rates, rangeRate{
			hr:          e.Range,
			sampleRate:  100,
			partitionID: e.PartitionID,
		})
	}

	// Residue: a previous-owner phantom report for ranges that
	// partition 0 used to own but now lives on partitions 1..3.
	// Use a huge sample rate so it would dominate per-partition L
	// if it leaked into the aggregation.
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			continue
		}
		rates = append(rates, rangeRate{
			hr:          e.Range,
			sampleRate:  100000,
			partitionID: 0, // claims to be from the (now-gone) previous owner
		})
	}

	r := &Rebalancer{
		cfg:           seriesCfg(0.5),
		moveCooldowns: make(map[assignment.HashRange]time.Time),
	}

	// The production code path: filter residue against the current
	// assignment, then pass the filtered slice into runSlicer.
	filtered, dropped := filterRatesByCurrentOwnership(rates, currentOwnershipSet(initial))
	require.Positive(t, dropped, "test setup: residue rates should have been filtered")
	partitionRateByPID := partitionLoadFromRates(filtered, partitions)

	_, actions := r.runSlicer(initial, filtered, partitionRateByPID, partitions, nil, time.Time{})

	var moves int
	for _, a := range actions {
		if a.Kind == ActionMove {
			moves++
		}
	}
	assert.Equalf(t, 0, moves,
		"residue from a non-owner must not drive Phase 3 moves on an otherwise balanced cluster (got %d moves)",
		moves)
}

// TestRunSlicer_MovesBySampleRate verifies that, with sampleRate set
// and series counts deliberately UNINFORMATIVE (all equal), the
// slicer still detects skew and moves load from the hot partition
// to a cold one. This is the headline behaviour change for v4: the
// rebalancer now responds to ingest throughput, not just head
// cardinality.
func TestRunSlicer_MovesBySampleRate(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.FineEvenSplit(partitions, 4)
	require.NoError(t, initial.Validate())

	var rates []rangeRate
	for _, e := range initial.Entries {
		// Series counts: identical across partitions. If the slicer
		// were still balancing on series it would see no skew at
		// all and produce zero moves.
		rr := rangeRate{hr: e.Range, series: 100, partitionID: e.PartitionID}
		if e.PartitionID == 0 {
			rr.sampleRate = 10000
		} else {
			rr.sampleRate = 100
		}
		rates = append(rates, rr)
	}

	r := &Rebalancer{cfg: seriesCfg(0.5)}
	partL := partitionLFromRates(initial, rates) // unused by Phase 3 now
	rates = withSampleRateFromSeries(rates)
	_ = partL
	result, actions := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})
	require.NoError(t, result.Validate())

	var movesOffHot int
	for _, a := range actions {
		if a.Kind == ActionMove && a.FromPart == 0 {
			movesOffHot++
		}
	}
	assert.Greater(t, movesOffHot, 0,
		"slicer must move ranges off the partition with the higher sample rate "+
			"even when head-series counts are identical across partitions")
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
			rates = append(rates, rangeRate{hr: e.Range, series: 1000, partitionID: e.PartitionID})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 1, partitionID: e.PartitionID})
		}
	}

	cfg := seriesCfg(0.5)
	cfg.MoveCooldown = 90 * time.Second
	r := &Rebalancer{cfg: cfg, moveCooldowns: make(map[assignment.HashRange]time.Time)}

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// First round: produces some moves and arms cooldowns.
	rates = withSampleRateFromSeries(rates)
	result1, actions1 := r.runSlicer(initial, rates, nil, partitions, nil, t0)
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
	_, actions2 := r.runSlicer(result1, rates, nil, partitions, nil, t1)
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

// TestRunSlicer_ExhaustedBudgetDoesNotStallOthers reproduces the bug
// (previously expressed as "orphan-locked partition"): Phase 3 of the
// slicer must not terminate its loop when the nominally-hottest
// partition has no profitable range to move. With the within-round
// movable budget as the primary gate, a partition whose entire
// above-mean surplus has already been scheduled-out this round simply
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
			rates = append(rates, rangeRate{hr: e.Range, series: 0, partitionID: e.PartitionID})
		case 1:
			rates = append(rates, rangeRate{hr: e.Range, series: 1000, partitionID: e.PartitionID})
		default:
			rates = append(rates, rangeRate{hr: e.Range, series: 1, partitionID: e.PartitionID})
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

	rates = withSampleRateFromSeries(rates)
	_ = partL
	_, actions := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})

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
			rates = append(rates, rangeRate{hr: e.Range, series: 100, partitionID: e.PartitionID})
		default:
			rates = append(rates, rangeRate{hr: e.Range, series: 1, partitionID: e.PartitionID})
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
	}

	rates = withSampleRateFromSeries(rates)
	// partL is left in the fixture for narrative clarity but is no
	// longer passed to the slicer — Phase 3 reads sampleRate via
	// rates. We still validate it as a sanity check.
	_ = partL
	_, actions := r.runSlicer(initial, rates, nil, partitions, nil, time.Time{})

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
// round 2 the slicer should see fresh load values (the test re-
// submits the same rates) and be free to target the same cold
// destinations again — no "this partition received moves last round"
// penalty.
func TestRunSlicer_PlannedAdditionsDoNotPersistAcrossRounds(t *testing.T) {
	partitions := []int32{0, 1}
	initial := assignment.FineEvenSplit(partitions, 16)

	var rates []rangeRate
	for _, e := range initial.Entries {
		if e.PartitionID == 0 {
			rates = append(rates, rangeRate{hr: e.Range, series: 100, partitionID: e.PartitionID})
		} else {
			rates = append(rates, rangeRate{hr: e.Range, series: 1, partitionID: e.PartitionID})
		}
	}
	rates = withSampleRateFromSeries(rates)

	cfg := seriesCfg(0.5)
	r := &Rebalancer{
		cfg:           cfg,
		moveCooldowns: make(map[assignment.HashRange]time.Time),
	}

	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	result1, _ := r.runSlicer(initial, rates, nil, partitions, nil, now)

	// Round 2: pretend distributors haven't re-aimed at P1 yet — the
	// rates fixture is unchanged so P0 is still hotter. The slicer
	// must be willing to target P1 again as the (still-only) cold
	// destination; there is no cross-round destination penalty in
	// the new design (and never was anything that survived the round
	// once recentMoves was removed).
	now2 := now.Add(time.Minute)
	_, actions2 := r.runSlicer(result1, rates, nil, partitions, nil, now2)

	// If any move happens in round 2, its destination must be P1
	// (no prohibition from round 1's plannedAdded).
	for _, a := range actions2 {
		if a.Kind == ActionMove {
			assert.Equal(t, int32(1), a.ToPart,
				"round 2 moves should still target the cold P1; no cross-round destination penalty")
		}
	}
}

// TestRunPhase3_AntiOvershootCap verifies the "move at most half the
// hot/cold gap" rule: a candidate range that would leave the recipient
// hotter than the donor (newCold > newHot) must be rejected, even when
// it fits the source's movable budget and would reduce total imbalance.
//
// Layout chosen so the overshoot range slips past the *source* budget
// (so the cap is the only thing that can block it): one hot partition
// holds two ranges (loads 12 and 8) while four other partitions are
// empty. mean = 20/5 = 4, so the hot partition's surplus is 16 — large
// enough that moving the load-12 range alone (12 < 16) clears the
// source check. But moving it onto an empty partition would make that
// recipient (0+12=12) hotter than the donor would be left (20-12=8):
// the classic overshoot. The load-8 range, by contrast, fits under
// half the gap (8 ≤ (20-0)/2) and is the move the slicer should pick.
func TestRunPhase3_AntiOvershootCap(t *testing.T) {
	rangeA := assignment.HashRange{Lo: 0, Hi: 99}    // load 12: would overshoot
	rangeB := assignment.HashRange{Lo: 100, Hi: 199} // load 8: fits half the gap

	entries := []rangeLoad{
		{entry: assignment.Entry{Range: rangeA, PartitionID: 0}, load: 12, series: 120},
		{entry: assignment.Entry{Range: rangeB, PartitionID: 0}, load: 8, series: 80},
	}
	partitionLoadByPID := map[int32]float64{0: 20, 1: 0, 2: 0, 3: 0, 4: 0}
	activePartitions := []int32{0, 1, 2, 3, 4}

	r := &Rebalancer{
		cfg:           seriesCfg(0.5),
		moveCooldowns: make(map[assignment.HashRange]time.Time),
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	actions := r.runPhase3(entries, partitionLoadByPID, activePartitions, nil, now)

	var moves []Action
	for _, a := range actions {
		if a.Kind == ActionMove {
			moves = append(moves, a)
		}
	}

	// The overshoot range (A) must never move; only the fitting range
	// (B) should, and onto the coldest partition.
	require.Len(t, moves, 1, "exactly one move: the load-8 range that fits half the gap")
	assert.Equal(t, rangeB, moves[0].Range, "the moved range must be the one that fits under half the gap")
	assert.Equal(t, int32(0), moves[0].FromPart)

	for _, m := range moves {
		assert.NotEqual(t, rangeA, m.Range,
			"the load-12 range overshoots the recipient (newCold>newHot) and must be left in place")
	}

	// Post-state: A still on P0, B reassigned off P0.
	var aPID, bPID int32 = -1, -1
	for _, e := range entries {
		switch e.entry.Range {
		case rangeA:
			aPID = e.entry.PartitionID
		case rangeB:
			bPID = e.entry.PartitionID
		}
	}
	assert.Equal(t, int32(0), aPID, "overshoot range A stays on the hot partition")
	assert.NotEqual(t, int32(0), bPID, "fitting range B is moved off the hot partition")
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

// TestActivePartitionsForRound_UsesPartitionCount verifies that when
// ActivePartitionCount is unset, the rebalancer uses PartitionCount.
func TestActivePartitionsForRound_UsesPartitionCount(t *testing.T) {
	r := &Rebalancer{cfg: Config{PartitionCount: 3}}
	got := r.activePartitionsForRound()
	assert.Equal(t, []int32{0, 1, 2}, got)
}

// TestActivePartitionsForRound_CapSynthesises ensures that when the
// cap is set, the rebalancer slices over [0, K).
func TestActivePartitionsForRound_CapSynthesises(t *testing.T) {
	r := &Rebalancer{cfg: Config{ActivePartitionCount: 4, PartitionCount: 8}}
	got := r.activePartitionsForRound()
	assert.Equal(t, []int32{0, 1, 2, 3}, got)
}

// TestActivePartitionsForRound_CapSmallerThanTopic covers the
// "scale down" path: cap below partition_count still produces [0, K).
func TestActivePartitionsForRound_CapSmallerThanTopic(t *testing.T) {
	r := &Rebalancer{cfg: Config{ActivePartitionCount: 2, PartitionCount: 4}}
	got := r.activePartitionsForRound()
	assert.Equal(t, []int32{0, 1}, got)
}

// TestConfig_Validate exercises the Validate() rules for the new
// ActivePartitionCount knob. Negative values and values exceeding
// PartitionCount must be rejected at parse time; zero (default) and
// any value within the topic's provisioned partition count are OK.
func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
	}{
		{name: "partition count only", cfg: Config{PartitionCount: 100}},
		{name: "cap within partition count", cfg: Config{PartitionCount: 100, ActivePartitionCount: 50}},
		{name: "cap equal to partition count", cfg: Config{PartitionCount: 100, ActivePartitionCount: 100}},
		{name: "negative cap rejected", cfg: Config{ActivePartitionCount: -1}, wantErr: true},
		{name: "cap above partition count rejected", cfg: Config{PartitionCount: 100, ActivePartitionCount: 101}, wantErr: true},
		{name: "neither partition count nor cap rejected", cfg: Config{}, wantErr: true},
		{name: "cap alone allowed", cfg: Config{ActivePartitionCount: 320}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
