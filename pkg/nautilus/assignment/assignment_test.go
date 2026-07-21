// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
	"bytes"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHashRange_Contains(t *testing.T) {
	r := HashRange{Lo: 100, Hi: 200}
	assert.True(t, r.Contains(100))
	assert.True(t, r.Contains(150))
	assert.True(t, r.Contains(200))
	assert.False(t, r.Contains(99))
	assert.False(t, r.Contains(201))
}

func TestHashRange_Size(t *testing.T) {
	assert.Equal(t, uint64(1), HashRange{Lo: 5, Hi: 5}.Size())
	assert.Equal(t, uint64(101), HashRange{Lo: 100, Hi: 200}.Size())
	assert.Equal(t, uint64(math.MaxUint32)+1, HashRange{Lo: 0, Hi: math.MaxUint32}.Size())
}

func TestLookup_EvenSplit(t *testing.T) {
	partitions := []int32{0, 1, 2, 3}
	a := EvenSplit(partitions)
	require.NoError(t, a.Validate())

	// Each partition should own ~25% of the hash space.
	for key := uint32(0); key < 1000; key++ {
		pid, ok := a.Lookup(key)
		require.True(t, ok, "key %d should be found", key)
		assert.Equal(t, int32(0), pid, "low keys should map to partition 0")
	}

	// The last key should map to the last partition.
	pid, ok := a.Lookup(math.MaxUint32)
	require.True(t, ok)
	assert.Equal(t, int32(3), pid)

	// A key near the middle should map to partition 1 or 2.
	pid, ok = a.Lookup(math.MaxUint32 / 2)
	require.True(t, ok)
	assert.True(t, pid == 1 || pid == 2)
}

func TestLookup_SinglePartition(t *testing.T) {
	a := EvenSplit([]int32{42})
	require.NoError(t, a.Validate())

	for _, key := range []uint32{0, 1, math.MaxUint32 / 2, math.MaxUint32} {
		pid, ok := a.Lookup(key)
		require.True(t, ok)
		assert.Equal(t, int32(42), pid)
	}
}

func TestLookup_EmptyAssignment(t *testing.T) {
	a := &Assignment{}
	_, ok := a.Lookup(123)
	assert.False(t, ok)
}

func TestLookup_BoundaryKeys(t *testing.T) {
	a := &Assignment{
		Entries: []Entry{
			{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 0},
			{Range: HashRange{Lo: 100, Hi: 199}, PartitionID: 1},
			{Range: HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 2},
		},
	}
	require.NoError(t, a.Validate())

	tests := []struct {
		key         uint32
		expectedPID int32
	}{
		{0, 0},
		{99, 0},
		{100, 1},
		{199, 1},
		{200, 2},
		{math.MaxUint32, 2},
	}

	for _, tc := range tests {
		pid, ok := a.Lookup(tc.key)
		require.True(t, ok, "key %d", tc.key)
		assert.Equal(t, tc.expectedPID, pid, "key %d", tc.key)
	}
}

func TestValidate_Gap(t *testing.T) {
	a := &Assignment{
		Entries: []Entry{
			{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 0},
			{Range: HashRange{Lo: 101, Hi: math.MaxUint32}, PartitionID: 1}, // gap at 100
		},
	}
	assert.Error(t, a.Validate())
}

func TestValidate_DoesNotStartAtZero(t *testing.T) {
	a := &Assignment{
		Entries: []Entry{
			{Range: HashRange{Lo: 1, Hi: math.MaxUint32}, PartitionID: 0},
		},
	}
	assert.Error(t, a.Validate())
}

func TestValidate_DoesNotEndAtMax(t *testing.T) {
	a := &Assignment{
		Entries: []Entry{
			{Range: HashRange{Lo: 0, Hi: math.MaxUint32 - 1}, PartitionID: 0},
		},
	}
	assert.Error(t, a.Validate())
}

func TestJSONRoundTrip(t *testing.T) {
	original := EvenSplit([]int32{10, 20, 30})
	require.NoError(t, original.Validate())

	var buf bytes.Buffer
	require.NoError(t, original.WriteJSON(&buf))

	loaded, err := Load(&buf)
	require.NoError(t, err)

	restored := loaded.(*Assignment)
	require.Equal(t, len(original.Entries), len(restored.Entries))

	for i := range original.Entries {
		assert.Equal(t, original.Entries[i], restored.Entries[i])
	}
}

func TestEvenSplit_Empty(t *testing.T) {
	a := EvenSplit([]int32{})
	assert.Empty(t, a.Entries)
}

func TestEvenSplit_ManyPartitions(t *testing.T) {
	ids := make([]int32, 1000)
	for i := range ids {
		ids[i] = int32(i)
	}
	a := EvenSplit(ids)
	require.NoError(t, a.Validate())
	assert.Len(t, a.Entries, 1000)

	// Every key should resolve.
	for _, key := range []uint32{0, 1, math.MaxUint32 / 2, math.MaxUint32 - 1, math.MaxUint32} {
		_, ok := a.Lookup(key)
		assert.True(t, ok, "key %d should be found", key)
	}
}
