// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActiveTable_EmptyLog(t *testing.T) {
	l := NewLog()
	assert.Nil(t, l.ActiveTable(time.Now()))
}

func TestActiveTable_LookupCoversFullSpace(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := EvenSplit([]int32{0, 1, 2, 3})
	require.True(t, l.Apply(at, a, testLease, testLookahead))

	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)
	assert.Equal(t, len(a.Entries), tbl.Len())
	assert.Equal(t, at.Add(testLease), tbl.ValidUntil())
	assert.Equal(t, at, tbl.BuiltAt())

	// Lookup covers every entry's boundaries plus interior.
	for _, e := range a.Entries {
		mid := uint32((uint64(e.Range.Lo) + uint64(e.Range.Hi)) / 2) //nolint:gosec // wide-on-purpose to avoid uint32 overflow.
		for _, key := range []uint32{e.Range.Lo, e.Range.Hi, mid} {
			pid, ok := tbl.Lookup(key)
			require.Truef(t, ok, "key %d in range [%d, %d]", key, e.Range.Lo, e.Range.Hi)
			assert.Equal(t, e.PartitionID, pid, "key %d", key)
		}
	}
}

func TestActiveTable_LookupAgreesWithLogLookup(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Build a non-uniform tiling.
	a := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: 999}, PartitionID: 1},
		{Range: HashRange{Lo: 1000, Hi: 99999}, PartitionID: 2},
		{Range: HashRange{Lo: 100000, Hi: 99_999_999}, PartitionID: 3},
		{Range: HashRange{Lo: 100_000_000, Hi: math.MaxUint32}, PartitionID: 4},
	}}
	require.True(t, l.Apply(at, a, testLease, testLookahead))
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)

	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		key := rng.Uint32()
		wantPID, wantOK := l.Lookup(at, key)
		gotPID, gotOK := tbl.Lookup(key)
		require.Equalf(t, wantOK, gotOK, "key %d", key)
		require.Equalf(t, wantPID, gotPID, "key %d", key)
	}
}

func TestActiveTable_ExcludesPreIssuedFutureLeases(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := EvenSplit([]int32{0, 1})
	require.True(t, l.Apply(t1, a, testLease, testLookahead))

	// Force successor pre-issue.
	tEdge := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(tEdge, a, testLease, testLookahead))
	require.Equal(t, 2*len(a.Entries), l.Len())

	// At tEdge, only the active leases are in the table — the
	// pre-issued successors have From == t1+lease > tEdge so they're
	// not yet active.
	tbl := l.ActiveTable(tEdge)
	require.NotNil(t, tbl)
	assert.Equal(t, len(a.Entries), tbl.Len())
	// Each entry in the table must have From <= tEdge < To.
	for i := 0; i < tbl.Len(); i++ {
		assert.True(t, tbl.entries[i].ActiveAt(tEdge))
	}
	assert.Equal(t, t1.Add(testLease), tbl.ValidUntil())
}

func TestActiveTable_ExcludesExpiredLeases(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.True(t, l.Apply(t1, EvenSplit([]int32{0, 1}), time.Minute, time.Second))

	// Far past the lease horizon.
	assert.Nil(t, l.ActiveTable(t1.Add(time.Hour)))
}

func TestActiveTable_ValidUntilIsMinimumTo(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Hand-build entries with different To values.
	l.entries = []LogEntry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: at, To: at.Add(10 * time.Minute)},
		{Range: HashRange{Lo: 100, Hi: 199}, PartitionID: 2, From: at, To: at.Add(3 * time.Minute)},
		{Range: HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 3, From: at, To: at.Add(7 * time.Minute)},
	}
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)
	assert.Equal(t, at.Add(3*time.Minute), tbl.ValidUntil())
}

func TestActiveTable_CoversAt(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.True(t, l.Apply(at, EvenSplit([]int32{0, 1}), testLease, testLookahead))
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)

	assert.False(t, tbl.CoversAt(at.Add(-time.Second)), "before built-at: not covered")
	assert.True(t, tbl.CoversAt(at), "at built-at: covered")
	assert.True(t, tbl.CoversAt(at.Add(testLease-time.Nanosecond)), "just before validUntil: covered")
	assert.False(t, tbl.CoversAt(at.Add(testLease)), "at validUntil: not covered (To is exclusive)")
	assert.False(t, tbl.CoversAt(at.Add(testLease+time.Second)), "after validUntil: not covered")
}

func TestActiveTable_Lookup_HashGap(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Hand-build a gap.
	l.entries = []LogEntry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 2, From: at, To: at.Add(testLease)},
	}
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)

	pid, ok := tbl.Lookup(150)
	assert.False(t, ok, "key in gap returns false")
	assert.Zero(t, pid)

	// Boundaries still resolve.
	pid, ok = tbl.Lookup(99)
	require.True(t, ok)
	assert.Equal(t, int32(1), pid)
	pid, ok = tbl.Lookup(200)
	require.True(t, ok)
	assert.Equal(t, int32(2), pid)
}

func TestActiveTable_PartitionsOverlapping(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// A non-uniform tiling where partition 2 owns two disjoint
	// tiles, so a wide query range can resolve to it once.
	l.entries = []LogEntry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 100, Hi: 199}, PartitionID: 2, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 200, Hi: 299}, PartitionID: 3, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 300, Hi: 399}, PartitionID: 2, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 400, Hi: math.MaxUint32}, PartitionID: 4, From: at, To: at.Add(testLease)},
	}
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)

	t.Run("range inside a single tile", func(t *testing.T) {
		assert.Equal(t, []int32{1}, tbl.PartitionsOverlapping(10, 50))
	})

	t.Run("range spanning several tiles dedupes partitions", func(t *testing.T) {
		// [50, 350] touches tiles 1,2,3,2 -> {1,2,3} in tile order.
		assert.Equal(t, []int32{1, 2, 3}, tbl.PartitionsOverlapping(50, 350))
	})

	t.Run("exact tile boundaries are inclusive", func(t *testing.T) {
		assert.Equal(t, []int32{2}, tbl.PartitionsOverlapping(100, 199))
		assert.Equal(t, []int32{2, 3}, tbl.PartitionsOverlapping(199, 200))
	})

	t.Run("range covering the whole space returns all distinct partitions", func(t *testing.T) {
		assert.Equal(t, []int32{1, 2, 3, 4}, tbl.PartitionsOverlapping(0, math.MaxUint32))
	})

	t.Run("inverted range returns nil", func(t *testing.T) {
		assert.Nil(t, tbl.PartitionsOverlapping(300, 100))
	})
}

func TestActiveTable_PartitionsOverlapping_HashGap(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l.entries = []LogEntry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 2, From: at, To: at.Add(testLease)},
	}
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)

	// A query range that falls entirely within the gap matches no tile.
	assert.Nil(t, tbl.PartitionsOverlapping(120, 150))
	// A range straddling the gap matches the tiles on both sides.
	assert.Equal(t, []int32{1, 2}, tbl.PartitionsOverlapping(50, 250))
}

func TestActiveTable_AllPartitions(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l.entries = []LogEntry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 5, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 100, Hi: 199}, PartitionID: 7, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 5, From: at, To: at.Add(testLease)},
	}
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)

	// Partition 5 owns two tiles but appears once, in tile order.
	assert.Equal(t, []int32{5, 7}, tbl.AllPartitions())
}

func TestActiveTable_AllPartitions_EvenSplit(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	require.True(t, l.Apply(at, EvenSplit([]int32{0, 1, 2, 3}), testLease, testLookahead))
	tbl := l.ActiveTable(at)
	require.NotNil(t, tbl)
	assert.Equal(t, []int32{0, 1, 2, 3}, tbl.AllPartitions())
}

// BenchmarkActiveTable_Lookup measures the per-key lookup cost on an
// active table sized like a typical fleet (96 partitions × 1 tile
// each = 96 entries).
func BenchmarkActiveTable_Lookup(b *testing.B) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pids := make([]int32, 96)
	for i := range pids {
		pids[i] = int32(i)
	}
	require.True(b, l.Apply(at, EvenSplit(pids), testLease, testLookahead))
	tbl := l.ActiveTable(at)
	require.NotNil(b, tbl)

	rng := rand.New(rand.NewSource(1))
	keys := make([]uint32, 1024)
	for i := range keys {
		keys[i] = rng.Uint32()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = tbl.Lookup(keys[i&1023])
	}
}

// BenchmarkLog_Lookup measures the per-key lookup cost on a Log
// with the same active set plus 24h of expired history (the cost
// the distributor was actually paying before ActiveTable existed).
func BenchmarkLog_Lookup(b *testing.B) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	pids := make([]int32, 96)
	for i := range pids {
		pids[i] = int32(i)
	}

	// Simulate ~24h of rebalance history at one round per 5m: ~288
	// rounds, each adding successor leases for 96 pairs. Round it
	// to ~5000 expired entries plus the active set.
	const historyEntries = 5000
	for i := 0; i < historyEntries/len(pids); i++ {
		past := at.Add(time.Duration(i-1000) * 5 * time.Minute)
		_ = l.Apply(past, EvenSplit(pids), testLease, testLookahead)
	}
	require.True(b, l.Apply(at, EvenSplit(pids), testLease, testLookahead))

	rng := rand.New(rand.NewSource(1))
	keys := make([]uint32, 1024)
	for i := range keys {
		keys[i] = rng.Uint32()
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = l.Lookup(at, keys[i&1023])
	}
}
