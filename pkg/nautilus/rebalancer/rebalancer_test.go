// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestRunSlicer_ConvergesOnSkewedLoad(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.EvenSplit(partitions)
	require.NoError(t, initial.Validate())

	rates := []rangeRate{
		{hr: initial.Entries[0].Range, rate: 10000.0},
		{hr: initial.Entries[1].Range, rate: 100.0},
		{hr: initial.Entries[2].Range, rate: 100.0},
		{hr: initial.Entries[3].Range, rate: 100.0},
	}

	cfg := Config{
		MovementBudget: 0.5,
	}
	r := &Rebalancer{cfg: cfg}

	result := r.runSlicer(initial, rates, partitions)
	require.NoError(t, result.Validate())

	assert.Greater(t, len(result.Entries), len(initial.Entries),
		"splits should have occurred on the hot range")
}

func TestRunSlicer_EvenLoadNoChange(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	initial := assignment.EvenSplit(partitions)

	rates := make([]rangeRate, len(initial.Entries))
	for i, e := range initial.Entries {
		rates[i] = rangeRate{hr: e.Range, rate: 100.0}
	}

	cfg := Config{
		MovementBudget: 0.09,
	}
	r := &Rebalancer{cfg: cfg}

	result := r.runSlicer(initial, rates, partitions)
	require.NoError(t, result.Validate())

	partitionLoad := make(map[int32]float64)
	rateMap := buildRateMap(rates)
	for _, e := range result.Entries {
		partitionLoad[e.PartitionID] += lookupRate(e.Range, rateMap)
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
	initial := assignment.EvenSplit(allPartitions)

	activePartitions := []int32{0, 1, 2}

	rates := make([]rangeRate, len(initial.Entries))
	for i, e := range initial.Entries {
		rates[i] = rangeRate{hr: e.Range, rate: 100.0}
	}

	cfg := Config{
		MovementBudget: 0.5,
	}
	r := &Rebalancer{cfg: cfg}

	result := r.runSlicer(initial, rates, activePartitions)
	require.NoError(t, result.Validate())

	for _, e := range result.Entries {
		assert.NotEqual(t, int32(3), e.PartitionID, "inactive partition should not be assigned")
	}
}

func TestMergeAdjacentUnderloaded(t *testing.T) {
	rateMap := map[assignment.HashRange]float64{
		{Lo: 0, Hi: 99}:   0.1,
		{Lo: 100, Hi: 199}: 0.1,
		{Lo: 200, Hi: 299}: 0.1,
	}

	entries := []rangeLoad{
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 100, Hi: 199}, PartitionID: 0}, load: 0.1},
		{entry: assignment.Entry{Range: assignment.HashRange{Lo: 200, Hi: 299}, PartitionID: 1}, load: 0.1},
	}

	result := mergeAdjacentUnderloaded(entries, 1.0, rateMap)

	assert.Equal(t, 2, len(result))
	assert.Equal(t, uint32(0), result[0].entry.Range.Lo)
	assert.Equal(t, uint32(199), result[0].entry.Range.Hi)
	assert.Equal(t, int32(0), result[0].entry.PartitionID)
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

func TestLookupRate(t *testing.T) {
	rateMap := map[assignment.HashRange]float64{
		{Lo: 0, Hi: 999}:    100.0,
		{Lo: 1000, Hi: 1999}: 200.0,
	}

	assert.Equal(t, 100.0, lookupRate(assignment.HashRange{Lo: 0, Hi: 999}, rateMap))
	assert.Equal(t, 200.0, lookupRate(assignment.HashRange{Lo: 1000, Hi: 1999}, rateMap))
	assert.Equal(t, 0.0, lookupRate(assignment.HashRange{Lo: 5000, Hi: 6000}, rateMap))
}
