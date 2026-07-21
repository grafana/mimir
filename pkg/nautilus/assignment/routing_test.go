// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLookup_AllKeysResolve(t *testing.T) {
	partitions := []int32{0, 1, 2, 3, 4, 5, 6, 7}
	a := EvenSplit(partitions)
	require.NoError(t, a.Validate())

	for key := uint32(0); key < 10000; key++ {
		pid, ok := a.Lookup(key)
		require.True(t, ok, "key %d should resolve", key)
		assert.Contains(t, partitions, pid)
	}

	// Test high end of hash space.
	for key := uint32(math.MaxUint32 - 10000); key < math.MaxUint32; key++ {
		pid, ok := a.Lookup(key)
		require.True(t, ok, "key %d should resolve", key)
		assert.Contains(t, partitions, pid)
	}
	pid, ok := a.Lookup(math.MaxUint32)
	require.True(t, ok)
	assert.Contains(t, partitions, pid)
}

func TestLookup_CustomAssignment(t *testing.T) {
	a := &Assignment{
		Entries: []Entry{
			{Range: HashRange{Lo: 0, Hi: 1000}, PartitionID: 10},
			{Range: HashRange{Lo: 1001, Hi: 2000}, PartitionID: 20},
			{Range: HashRange{Lo: 2001, Hi: math.MaxUint32}, PartitionID: 30},
		},
	}
	require.NoError(t, a.Validate())

	tests := []struct {
		key         uint32
		expectedPID int32
	}{
		{0, 10},
		{500, 10},
		{1000, 10},
		{1001, 20},
		{1500, 20},
		{2000, 20},
		{2001, 30},
		{math.MaxUint32, 30},
	}

	for _, tc := range tests {
		pid, ok := a.Lookup(tc.key)
		require.True(t, ok)
		assert.Equal(t, tc.expectedPID, pid, "key %d", tc.key)
	}
}

func TestEvenSplit_CoverageVerification(t *testing.T) {
	for numPartitions := 1; numPartitions <= 100; numPartitions++ {
		ids := make([]int32, numPartitions)
		for i := range ids {
			ids[i] = int32(i)
		}
		a := EvenSplit(ids)
		require.NoError(t, a.Validate(), "numPartitions=%d", numPartitions)

		// Every entry should map to a partition in ids.
		for _, e := range a.Entries {
			assert.Contains(t, ids, e.PartitionID, "numPartitions=%d", numPartitions)
		}
	}
}
