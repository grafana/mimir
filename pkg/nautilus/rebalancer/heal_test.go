// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestHeal_AlreadyValid_NoOp(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1},
			{Range: assignment.HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2},
		},
	}
	out, rep := healAssignmentGaps(in, []int32{10, 11, 12})
	require.NoError(t, out.Validate())
	assert.Equal(t, 0, rep.gapsFilled)
	assert.Equal(t, 0, rep.overlapsResolved)
	assert.Equal(t, "no-op", rep.String())
	assert.Equal(t, in.Entries, out.Entries, "valid input should round-trip unchanged")
}

func TestHeal_NilInput_NoOp(t *testing.T) {
	out, rep := healAssignmentGaps(nil, []int32{1, 2})
	assert.Nil(t, out)
	assert.Equal(t, "no-op", rep.String())
}

func TestHeal_EmptyActivePartitions_BailsWithoutHealing(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1},
		},
	}
	out, rep := healAssignmentGaps(in, nil)
	assert.Same(t, in, out, "no activePartitions means we have no filler PIDs to pick from")
	assert.Equal(t, "no-op", rep.String())
}

func TestHeal_MissingPrefix(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 1},
		},
	}
	out, rep := healAssignmentGaps(in, []int32{42, 43})
	require.NoError(t, out.Validate())
	assert.Equal(t, 1, rep.gapsFilled)
	assert.Equal(t, uint64(100), rep.gapHashesFilled)
	assert.True(t, rep.hasFirstGap)
	assert.Equal(t, assignment.HashRange{Lo: 0, Hi: 99}, rep.firstGap)
	require.Len(t, out.Entries, 2)
	assert.Equal(t, assignment.HashRange{Lo: 0, Hi: 99}, out.Entries[0].Range)
	assert.Equal(t, int32(42), out.Entries[0].PartitionID, "first round-robin pick should be the first activePartition")
}

func TestHeal_MissingSuffix(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1},
		},
	}
	out, rep := healAssignmentGaps(in, []int32{42, 43})
	require.NoError(t, out.Validate())
	assert.Equal(t, 1, rep.gapsFilled)
	assert.Equal(t, uint64(math.MaxUint32)-99, rep.gapHashesFilled)
	require.Len(t, out.Entries, 2)
	assert.Equal(t, uint32(100), out.Entries[1].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), out.Entries[1].Range.Hi)
}

func TestHeal_InternalGap(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1},
			{Range: assignment.HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 2},
		},
	}
	out, rep := healAssignmentGaps(in, []int32{42})
	require.NoError(t, out.Validate())
	assert.Equal(t, 1, rep.gapsFilled)
	assert.Equal(t, uint64(100), rep.gapHashesFilled)
	assert.Equal(t, assignment.HashRange{Lo: 100, Hi: 199}, rep.firstGap)
	require.Len(t, out.Entries, 3)
	assert.Equal(t, assignment.HashRange{Lo: 100, Hi: 199}, out.Entries[1].Range)
	assert.Equal(t, int32(42), out.Entries[1].PartitionID)
}

func TestHeal_MultipleInternalGaps_RoundRobinsPartitions(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 9}, PartitionID: 1},
			{Range: assignment.HashRange{Lo: 20, Hi: 29}, PartitionID: 2},
			{Range: assignment.HashRange{Lo: 40, Hi: math.MaxUint32}, PartitionID: 3},
		},
	}
	out, rep := healAssignmentGaps(in, []int32{100, 101, 102})
	require.NoError(t, out.Validate())
	assert.Equal(t, 2, rep.gapsFilled)
	require.Len(t, out.Entries, 5)
	// First filler [10,19] gets activePartitions[0]=100; second [30,39] gets activePartitions[1]=101.
	assert.Equal(t, int32(100), out.Entries[1].PartitionID)
	assert.Equal(t, int32(101), out.Entries[3].PartitionID)
}

func TestHeal_FullOverlap_DropsShadowedEntry(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 100}, PartitionID: 1},
			{Range: assignment.HashRange{Lo: 50, Hi: 80}, PartitionID: 2},
			{Range: assignment.HashRange{Lo: 101, Hi: math.MaxUint32}, PartitionID: 3},
		},
	}
	out, rep := healAssignmentGaps(in, []int32{42})
	require.NoError(t, out.Validate())
	assert.Equal(t, 1, rep.overlapsResolved)
	assert.Equal(t, uint64(31), rep.overlapHashesResolved, "shadowed range [50,80] is 31 hashes")
	require.Len(t, out.Entries, 2)
	assert.Equal(t, []assignment.Entry{
		{Range: assignment.HashRange{Lo: 0, Hi: 100}, PartitionID: 1},
		{Range: assignment.HashRange{Lo: 101, Hi: math.MaxUint32}, PartitionID: 3},
	}, out.Entries)
}

func TestHeal_PartialOverlap_TrimsEntry(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 0, Hi: 100}, PartitionID: 1},
			{Range: assignment.HashRange{Lo: 50, Hi: 200}, PartitionID: 2},
			{Range: assignment.HashRange{Lo: 201, Hi: math.MaxUint32}, PartitionID: 3},
		},
	}
	out, rep := healAssignmentGaps(in, []int32{42})
	require.NoError(t, out.Validate())
	assert.Equal(t, 1, rep.overlapsResolved)
	assert.Equal(t, uint64(51), rep.overlapHashesResolved, "[50..100] = 51 hashes double-claimed")
	require.Len(t, out.Entries, 3)
	assert.Equal(t, assignment.HashRange{Lo: 101, Hi: 200}, out.Entries[1].Range,
		"overlapping entry should be trimmed to start just after the previous one")
	assert.Equal(t, int32(2), out.Entries[1].PartitionID, "partition ID is preserved on trim")
}

func TestHeal_UnsortedInput_SortsAndHeals(t *testing.T) {
	in := &assignment.Assignment{
		Entries: []assignment.Entry{
			{Range: assignment.HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 2},
			{Range: assignment.HashRange{Lo: 0, Hi: 99}, PartitionID: 1},
		},
	}
	out, rep := healAssignmentGaps(in, []int32{42})
	require.NoError(t, out.Validate())
	assert.Equal(t, 1, rep.gapsFilled, "reordering then walking should detect the [100,199] gap")
	require.Len(t, out.Entries, 3)
	assert.Equal(t, uint32(0), out.Entries[0].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), out.Entries[2].Range.Hi)
}

func TestHeal_DevSizedGappyTracePattern(t *testing.T) {
	// Synthesises the gap shape we observed in mimir-dev-15 at
	// 21:14:51: the input has ~17k entries that should tile [0,
	// MaxUint32] but is missing a sliver of ~12k hashes in the
	// middle. The healer must fill the sliver and return a strict
	// tiling without dropping any of the surrounding entries.
	const numEntries = 17000
	const gapStart = 1729373989
	const gapEnd = 1729385973
	rangePerEntry := (uint64(math.MaxUint32) + 1) / numEntries
	entries := make([]assignment.Entry, 0, numEntries)
	for i := uint64(0); i < numEntries; i++ {
		lo := i * rangePerEntry
		hi := lo + rangePerEntry - 1
		if i == numEntries-1 {
			hi = math.MaxUint32
		}
		// Drop any entry that would land inside the gap; this is
		// the symptom — a chain of entries went missing for that
		// hash range.
		if uint64(lo) > gapEnd || uint64(hi) < gapStart {
			entries = append(entries, assignment.Entry{
				Range:       assignment.HashRange{Lo: uint32(lo), Hi: uint32(hi)},
				PartitionID: int32(i % 300),
			})
		}
	}
	// Make sure we actually produced a gap; otherwise the test is
	// vacuous.
	in := &assignment.Assignment{Entries: entries}
	require.Error(t, in.Validate(), "test setup must produce an invalid input")

	out, rep := healAssignmentGaps(in, []int32{1, 2, 3, 4, 5})
	require.NoError(t, out.Validate())
	assert.GreaterOrEqual(t, rep.gapsFilled, 1)
	assert.GreaterOrEqual(t, rep.gapHashesFilled, uint64(gapEnd-gapStart+1))
}

func TestHeal_FuzzReachesValid(t *testing.T) {
	// Property test: for any randomly-corrupted assignment, the
	// healer must return one that satisfies Validate(). This is
	// the strongest single guarantee the production wire-in
	// depends on.
	rng := rand.New(rand.NewSource(42))
	activePartitions := []int32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for iter := 0; iter < 200; iter++ {
		// Build a random set of intervals — may have gaps,
		// overlaps, duplicates, unsorted order, missing prefix or
		// suffix.
		numEntries := rng.Intn(50) + 1
		entries := make([]assignment.Entry, numEntries)
		for i := range entries {
			lo := uint32(rng.Int63n(int64(^uint32(0))))
			hi := lo + uint32(rng.Intn(1<<20))
			if hi < lo {
				hi = lo // overflow guard
			}
			entries[i] = assignment.Entry{
				Range:       assignment.HashRange{Lo: lo, Hi: hi},
				PartitionID: int32(rng.Intn(300)),
			}
		}
		in := &assignment.Assignment{Entries: entries}
		out, _ := healAssignmentGaps(in, activePartitions)
		require.NoError(t, out.Validate(), "iter %d: heal output failed Validate(); input=%+v", iter, in.Entries)
		// The input must never be mutated by the healer.
		require.Equal(t, numEntries, len(in.Entries), "iter %d: input mutated", iter)
	}
}
