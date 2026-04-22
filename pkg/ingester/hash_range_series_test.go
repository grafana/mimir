// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"math"
	"sync"
	"testing"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

func TestHashRangeSeries_RangesReturnsCopy(t *testing.T) {
	h := newHashRangeSeries()

	assert.Empty(t, h.Ranges())

	orig := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(orig)

	got := h.Ranges()
	require.Len(t, got, 2)
	assert.Equal(t, uint32(0), got[0].Lo)
	assert.Equal(t, uint32(999), got[0].Hi)
	assert.Equal(t, uint32(1000), got[1].Lo)
	assert.Equal(t, uint32(math.MaxUint32), got[1].Hi)

	got[0] = assignment.HashRange{Lo: 42, Hi: 42}
	again := h.Ranges()
	assert.Equal(t, uint32(0), again[0].Lo, "mutating the returned slice leaked back into hashRangeSeries")
}

func TestHashRangeSeries_SetRangesSortsAndZerosCounts(t *testing.T) {
	h := newHashRangeSeries()
	h.SetRanges([]assignment.HashRange{
		{Lo: 2000, Hi: math.MaxUint32},
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: 1999},
	})

	snap := h.Snapshot()
	require.Len(t, snap.Ranges, 3)
	assert.Equal(t, uint32(0), snap.Ranges[0].Lo)
	assert.Equal(t, uint32(1000), snap.Ranges[1].Lo)
	assert.Equal(t, uint32(2000), snap.Ranges[2].Lo)

	require.Len(t, snap.Counts, 3)
	for _, c := range snap.Counts {
		assert.Equal(t, int64(0), c)
	}
}

func TestHashRangeSeries_SetCountsFor(t *testing.T) {
	h := newHashRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)

	applied := h.SetCountsFor(ranges, []int64{42, 1337})
	assert.True(t, applied)

	snap := h.Snapshot()
	require.Len(t, snap.Counts, 2)
	assert.Equal(t, int64(42), snap.Counts[0])
	assert.Equal(t, int64(1337), snap.Counts[1])
}

func TestHashRangeSeries_SetCountsForDiscardedOnMismatch(t *testing.T) {
	h := newHashRangeSeries()
	oldRanges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(oldRanges)

	// Walk captures oldRanges; meanwhile a SetHashRanges swaps in a
	// different set. Stale walk results must not overwrite.
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 499},
		{Lo: 500, Hi: 1999},
		{Lo: 2000, Hi: math.MaxUint32},
	})

	applied := h.SetCountsFor(oldRanges, []int64{42, 1337})
	assert.False(t, applied, "counts should be discarded when ranges changed mid-walk")

	snap := h.Snapshot()
	require.Len(t, snap.Counts, 3)
	for _, c := range snap.Counts {
		assert.Equal(t, int64(0), c, "counts must remain zero after discarded update")
	}
}

func TestHashRangeSeries_SetCountsForLengthMismatch(t *testing.T) {
	h := newHashRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)

	applied := h.SetCountsFor(ranges, []int64{42, 1337, 99})
	assert.False(t, applied)

	snap := h.Snapshot()
	require.Len(t, snap.Counts, 2)
}

func TestHashRangeSeries_SetRangesZerosCountsOnResize(t *testing.T) {
	h := newHashRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)
	require.True(t, h.SetCountsFor(ranges, []int64{10, 20}))

	// Changing ranges zeros counts, even if one range coincidentally
	// has the same bounds as before. (No stale state leaks across.)
	h.SetRanges([]assignment.HashRange{
		{Lo: 0, Hi: 999},
		{Lo: 1000, Hi: 1999},
		{Lo: 2000, Hi: math.MaxUint32},
	})

	snap := h.Snapshot()
	require.Len(t, snap.Counts, 3)
	for _, c := range snap.Counts {
		assert.Equal(t, int64(0), c)
	}
}

func TestHashRangeSeries_HasRanges(t *testing.T) {
	h := newHashRangeSeries()
	assert.False(t, h.HasRanges())

	h.SetRanges([]assignment.HashRange{{Lo: 0, Hi: math.MaxUint32}})
	assert.True(t, h.HasRanges())

	h.SetRanges(nil)
	assert.False(t, h.HasRanges())
}

func TestHashRangeSeries_ConcurrentAccess(t *testing.T) {
	h := newHashRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32 / 2},
		{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)

	var wg sync.WaitGroup
	wg.Add(3)

	// Writer: keeps pushing new count snapshots.
	go func() {
		defer wg.Done()
		for i := int64(0); i < 1000; i++ {
			h.SetCountsFor(ranges, []int64{i, i * 2})
		}
	}()

	// Reader: keeps snapshotting.
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			snap := h.Snapshot()
			require.Len(t, snap.Ranges, 2)
		}
	}()

	// Ranges churner: periodically replaces ranges, which will cause
	// some SetCountsFor calls from the writer to be discarded.
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			h.SetRanges(ranges)
		}
	}()

	wg.Wait()
}

func TestHashRangeSeries_LogSummary(t *testing.T) {
	h := newHashRangeSeries()
	ranges := []assignment.HashRange{
		{Lo: 0, Hi: math.MaxUint32 / 2},
		{Lo: math.MaxUint32/2 + 1, Hi: math.MaxUint32},
	}
	h.SetRanges(ranges)
	require.True(t, h.SetCountsFor(ranges, []int64{100, 200}))

	// Should not panic with a real logger.
	h.LogSummary(log.NewNopLogger())
}
