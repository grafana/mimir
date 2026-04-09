// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestLoadForRange(t *testing.T) {
	var rates [hashBucketCount]float64
	rates[0] = 100.0
	rates[1] = 200.0

	bucketSize := (uint64(math.MaxUint32) + 1) / hashBucketCount

	// Full first bucket.
	load := loadForRange(assignment.HashRange{Lo: 0, Hi: uint32(bucketSize - 1)}, rates)
	assert.InDelta(t, 100.0, load, 0.01)

	// Full second bucket.
	load = loadForRange(assignment.HashRange{Lo: uint32(bucketSize), Hi: uint32(2*bucketSize - 1)}, rates)
	assert.InDelta(t, 200.0, load, 0.01)

	// Half of first bucket.
	load = loadForRange(assignment.HashRange{Lo: 0, Hi: uint32(bucketSize/2 - 1)}, rates)
	assert.InDelta(t, 50.0, load, 0.01)

	// Spans both buckets.
	load = loadForRange(assignment.HashRange{Lo: 0, Hi: uint32(2*bucketSize - 1)}, rates)
	assert.InDelta(t, 300.0, load, 0.01)
}

func TestRunSlicer_ConvergesOnSkewedLoad(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.EvenSplit(partitions)
	require.NoError(t, initial.Validate())

	// Skewed load: most samples in bucket 0, none elsewhere.
	var rates [hashBucketCount]float64
	rates[0] = 10000.0

	cfg := Config{
		MovementBudget: 0.5,
	}
	r := &Rebalancer{cfg: cfg}

	result := r.runSlicer(initial, rates, partitions)
	require.NoError(t, result.Validate())

	// The hot partition (partition 0, which has bucket 0) should have lost
	// some of its load to other partitions. Verify we have more entries than
	// the initial 4 (splits should have occurred).
	assert.Greater(t, len(result.Entries), len(initial.Entries))
}

func TestRunSlicer_EvenLoadNoChange(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.EvenSplit(partitions)

	// Even load across all buckets.
	var rates [hashBucketCount]float64
	for i := range rates {
		rates[i] = 100.0
	}

	cfg := Config{
		MovementBudget: 0.09,
	}
	r := &Rebalancer{cfg: cfg}

	result := r.runSlicer(initial, rates, partitions)
	require.NoError(t, result.Validate())

	// With even load, partition assignments should remain balanced.
	partitionLoad := make(map[int32]float64)
	for _, e := range result.Entries {
		partitionLoad[e.PartitionID] += loadForRange(e.Range, rates)
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
	// Start with 4 partitions, but only 3 are active.
	allPartitions := []int32{0, 1, 2, 3}
	initial := assignment.EvenSplit(allPartitions)

	activePartitions := []int32{0, 1, 2} // partition 3 is inactive

	var rates [hashBucketCount]float64
	for i := range rates {
		rates[i] = 100.0
	}

	cfg := Config{
		MovementBudget: 0.5,
	}
	r := &Rebalancer{cfg: cfg}

	result := r.runSlicer(initial, rates, activePartitions)
	require.NoError(t, result.Validate())

	// No entry should be assigned to partition 3.
	for _, e := range result.Entries {
		assert.NotEqual(t, int32(3), e.PartitionID, "inactive partition should not be assigned")
	}
}

func TestMergeAdjacentUnderloaded(t *testing.T) {
	var rates [hashBucketCount]float64
	for i := range rates {
		rates[i] = 1.0
	}

	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: 299}, PartitionID: 1}, load: 0.1},
	}

	result := mergeAdjacentUnderloaded(entries, 1.0, rates)

	// First two entries should be merged (same partition, adjacent, underloaded).
	assert.Equal(t, 2, len(result))
	assert.Equal(t, uint32(0), result[0].entry.Range.Lo)
	assert.Equal(t, uint32(199), result[0].entry.Range.Hi)
	assert.Equal(t, int32(0), result[0].entry.PartitionID)
}
