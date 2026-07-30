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

func TestHashRangeIndex_AgreesWithLinearScan(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	epoch := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	entries := make([]LogEntry, 5000)
	for i := range entries {
		lo := rng.Uint32()
		width := rng.Uint32() % (1 << 20)
		hi := uint32(min(uint64(lo)+uint64(width), uint64(math.MaxUint32)))
		from := epoch.Add(time.Duration(rng.Intn(200)-100) * time.Minute)
		entries[i] = LogEntry{
			Range:       HashRange{Lo: lo, Hi: hi},
			PartitionID: int32(rng.Intn(300)),
			From:        from,
			To:          from.Add(time.Duration(rng.Intn(30)+1) * time.Minute),
		}
	}
	l := NewLogFromEntries(entries)
	require.NotNil(t, l.rangeIndex)

	for range 1000 {
		lo, hi := rng.Uint32(), rng.Uint32()
		if lo > hi {
			lo, hi = hi, lo
		}
		w0 := epoch.Add(time.Duration(rng.Intn(200)-100) * time.Minute)
		w1 := w0.Add(time.Duration(rng.Intn(60)+1) * time.Minute)

		assert.Equal(t,
			linearPartitionsOverlappingInterval(l.entries, w0, w1, lo, hi),
			l.PartitionsOverlappingInterval(w0, w1, lo, hi),
		)
	}
}

func TestHashRangeIndex_RebuiltAfterMutation(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := NewLog()
	require.True(t, l.Apply(at, EvenSplit([]int32{1, 2}), 5*time.Minute, time.Minute))
	assertIndexMatchesLinear(t, l, at, at.Add(time.Minute), 0, math.MaxUint32)

	l = l.MergedWithEntries([]LogEntry{{
		Range:       HashRange{Lo: 0, Hi: 99},
		PartitionID: 3,
		From:        at.Add(-time.Hour),
		To:          at.Add(-30 * time.Minute),
	}})
	assertIndexMatchesLinear(t, l, at.Add(-50*time.Minute), at.Add(-40*time.Minute), 0, 99)

	l.Prune(at.Add(-time.Minute))
	assertIndexMatchesLinear(t, l, at.Add(-50*time.Minute), at.Add(-40*time.Minute), 0, 99)
}

func TestHashRangeIndex_ExactMetricBandBoundaries(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	const lo, hi = uint32(0x80000000), uint32(0x8000ffff)
	activeFrom, activeTo := at.Add(-time.Minute), at.Add(time.Minute)
	l := NewLogFromEntries([]LogEntry{
		{Range: HashRange{Lo: lo, Hi: hi}, PartitionID: 1, From: activeFrom, To: activeTo},
		{Range: HashRange{Lo: lo + 10, Hi: hi - 10}, PartitionID: 2, From: activeFrom, To: activeTo},
		{Range: HashRange{Lo: lo - 100, Hi: lo}, PartitionID: 3, From: activeFrom, To: activeTo},
		{Range: HashRange{Lo: hi, Hi: hi + 100}, PartitionID: 4, From: activeFrom, To: activeTo},
		{Range: HashRange{Lo: lo - 100, Hi: lo - 1}, PartitionID: 5, From: activeFrom, To: activeTo},
		{Range: HashRange{Lo: hi + 1, Hi: hi + 100}, PartitionID: 6, From: activeFrom, To: activeTo},
		{Range: HashRange{Lo: lo, Hi: hi}, PartitionID: 7, From: at.Add(-time.Minute), To: at},
		{Range: HashRange{Lo: lo, Hi: hi}, PartitionID: 8, From: activeTo, To: activeTo.Add(time.Minute)},
		{Range: HashRange{Lo: math.MaxUint32, Hi: math.MaxUint32}, PartitionID: 9, From: activeFrom, To: activeTo},
	})

	assert.Equal(t, []int32{1, 2, 3, 4}, l.PartitionsOverlappingInterval(at, activeTo, lo, hi))
	assert.Equal(t, []int32{9}, l.PartitionsOverlappingInterval(at, activeTo, math.MaxUint32, math.MaxUint32))
}

func linearPartitionsOverlappingInterval(entries []LogEntry, w0, w1 time.Time, lo, hi uint32) []int32 {
	seen := make(map[int32]struct{})
	for _, e := range entries {
		if e.From.Before(w1) && e.To.After(w0) && e.Range.Overlaps(lo, hi) {
			seen[e.PartitionID] = struct{}{}
		}
	}
	return sortedDistinctPartitions(seen)
}

func assertIndexMatchesLinear(t *testing.T, l *Log, w0, w1 time.Time, lo, hi uint32) {
	t.Helper()
	require.NotNil(t, l.rangeIndex)
	assert.Equal(t,
		linearPartitionsOverlappingInterval(l.entries, w0, w1, lo, hi),
		l.PartitionsOverlappingInterval(w0, w1, lo, hi),
	)
}
