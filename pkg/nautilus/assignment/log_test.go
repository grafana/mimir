// SPDX-License-Identifier: AGPL-3.0-only

package assignment

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testLease     = 5 * time.Minute
	testLookahead = 90 * time.Second
)

func TestLog_ApplyFromEmpty(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a := EvenSplit([]int32{0, 1, 2, 3})
	require.True(t, l.Apply(at, a, testLease, testLookahead))

	require.NoError(t, l.ActiveTilesFullSpace(at))
	require.Equal(t, len(a.Entries), l.Len(), "cold start emits exactly the desired tiling")

	for _, e := range l.Entries() {
		assert.Equal(t, at, e.From)
		assert.Equal(t, at.Add(testLease), e.To)
		assert.True(t, e.ActiveAt(at))
	}
}

func TestLog_ApplyNoOpOutsideLookahead(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := EvenSplit([]int32{0, 1, 2, 3})

	require.True(t, l.Apply(t1, a, testLease, testLookahead))
	beforeLen := l.Len()

	// One minute later, the active leases (To = t1+5m) still have
	// 4 minutes of runway, well outside the 90s lookahead. Apply
	// must be a true no-op: no successor queued, no broadcast.
	t2 := t1.Add(time.Minute)
	require.False(t, l.Apply(t2, a, testLease, testLookahead))
	assert.Equal(t, beforeLen, l.Len())
}

func TestLog_ApplyEmitsSuccessorWithinLookahead(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := EvenSplit([]int32{0, 1, 2, 3})

	require.True(t, l.Apply(t1, a, testLease, testLookahead))
	require.Equal(t, len(a.Entries), l.Len())

	// At t1 + (lease - lookahead), every active lease's To is exactly
	// at + lookahead. Apply must queue successors for every pair.
	t2 := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(t2, a, testLease, testLookahead))
	require.Equal(t, 2*len(a.Entries), l.Len(), "every entry got a queued successor")

	for _, e := range a.Entries {
		// Find the successor for this (Range, PID).
		var successors int
		for _, le := range l.Entries() {
			if le.Range == e.Range && le.PartitionID == e.PartitionID && le.From.Equal(t1.Add(testLease)) {
				successors++
				assert.Equal(t, t1.Add(2*testLease), le.To)
			}
		}
		assert.Equal(t, 1, successors)
	}

	// The active entries themselves must be untouched (immutable in
	// the routine path).
	for _, le := range l.Entries() {
		if le.From.Equal(t1) {
			assert.Equal(t, t1.Add(testLease), le.To, "original entries are immutable in the routine path")
		}
	}
}

func TestLog_ApplyDoesNotDoubleEmitSuccessor(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := EvenSplit([]int32{0, 1, 2, 3})

	require.True(t, l.Apply(t1, a, testLease, testLookahead))

	t2 := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(t2, a, testLease, testLookahead))
	twoEntriesPerPair := l.Len()

	// A second apply at the same instant must be a no-op: the
	// successor is already queued and extends past at+lookahead.
	require.False(t, l.Apply(t2, a, testLease, testLookahead))
	assert.Equal(t, twoEntriesPerPair, l.Len())
}

func TestLog_ApplyPartitionChangePreemptsAndCreates(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a1 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1},
		{Range: HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2},
	}}
	require.True(t, l.Apply(t1, a1, testLease, testLookahead))

	t2 := t1.Add(time.Minute)
	a2 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 3}, // P1 -> P3
		{Range: HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2},
	}}
	require.True(t, l.Apply(t2, a2, testLease, testLookahead))

	entries := l.Entries()
	require.Len(t, entries, 3, "preempted P1 + created P3 + unchanged P2")

	var foundPreempted, foundNewP3, foundUnchangedP2 bool
	for _, e := range entries {
		switch {
		case e.PartitionID == 1 && e.Range.Lo == 0:
			assert.Equal(t, t1, e.From)
			assert.Equal(t, t2, e.To, "preempted lease's To is truncated to t2")
			foundPreempted = true
		case e.PartitionID == 3 && e.Range.Lo == 0:
			assert.Equal(t, t2, e.From)
			assert.Equal(t, t2.Add(testLease), e.To, "new owner gets a fresh full-duration lease starting at t2")
			foundNewP3 = true
		case e.PartitionID == 2:
			assert.Equal(t, t1, e.From, "P2 lease must be unchanged: still well outside lookahead")
			assert.Equal(t, t1.Add(testLease), e.To)
			foundUnchangedP2 = true
		}
	}
	assert.True(t, foundPreempted)
	assert.True(t, foundNewP3)
	assert.True(t, foundUnchangedP2)

	require.NoError(t, l.ActiveTilesFullSpace(t2))
}

func TestLog_ApplyRangeSplit(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a1 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1},
	}}
	require.True(t, l.Apply(t1, a1, testLease, testLookahead))

	t2 := t1.Add(time.Minute)
	a2 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: 1000}, PartitionID: 1},
		{Range: HashRange{Lo: 1001, Hi: math.MaxUint32}, PartitionID: 2},
	}}
	require.True(t, l.Apply(t2, a2, testLease, testLookahead))

	require.NoError(t, l.ActiveTilesFullSpace(t2))

	active := l.ActiveAt(t2)
	require.Len(t, active, 2)

	preemptedCount := 0
	for _, e := range l.Entries() {
		if !e.ActiveAt(t2) {
			preemptedCount++
			assert.Equal(t, uint32(0), e.Range.Lo)
			assert.Equal(t, uint32(math.MaxUint32), e.Range.Hi)
			assert.Equal(t, int32(1), e.PartitionID)
			assert.Equal(t, t2, e.To)
		}
	}
	assert.Equal(t, 1, preemptedCount)
}

func TestLog_ApplyRangeMerge(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a1 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: 1000}, PartitionID: 1},
		{Range: HashRange{Lo: 1001, Hi: math.MaxUint32}, PartitionID: 1},
	}}
	require.True(t, l.Apply(t1, a1, testLease, testLookahead))

	t2 := t1.Add(time.Minute)
	a2 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1},
	}}
	require.True(t, l.Apply(t2, a2, testLease, testLookahead))

	active := l.ActiveAt(t2)
	require.Len(t, active, 1)
	assert.Equal(t, uint32(0), active[0].Range.Lo)
	assert.Equal(t, uint32(math.MaxUint32), active[0].Range.Hi)

	preemptedCount := 0
	for _, e := range l.Entries() {
		if !e.ActiveAt(t2) {
			preemptedCount++
			assert.Equal(t, t2, e.To)
		}
	}
	assert.Equal(t, 2, preemptedCount)
}

func TestLog_ApplyAfterFullExpiryRestartsCleanly(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := EvenSplit([]int32{0, 1})

	require.True(t, l.Apply(t1, a, testLease, testLookahead))

	// Skip well past the lease's expiry without renewing — simulates
	// the rebalancer being down for longer than a lease duration.
	tFar := t1.Add(2 * testLease)
	require.True(t, l.Apply(tFar, a, testLease, testLookahead))

	// All four entries are present: two expired ([t1, t1+lease)),
	// two fresh ([tFar, tFar+lease)).
	assert.Equal(t, 2*len(a.Entries), l.Len())

	require.NoError(t, l.ActiveTilesFullSpace(tFar))
}

func TestLog_LookupActive(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 10},
		{Range: HashRange{Lo: 100, Hi: 199}, PartitionID: 20},
		{Range: HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 30},
	}}
	l.Apply(at, a, testLease, testLookahead)

	tests := []struct {
		key  uint32
		want int32
	}{
		{0, 10}, {99, 10},
		{100, 20}, {199, 20},
		{200, 30}, {math.MaxUint32, 30},
	}
	for _, tc := range tests {
		pid, ok := l.Lookup(at, tc.key)
		require.True(t, ok, "key %d", tc.key)
		assert.Equal(t, tc.want, pid)
	}
}

func TestLog_LookupExpiredLeaseNotReturned(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Single short lease that's never refreshed.
	l.Apply(t1, &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1},
	}}, time.Minute, time.Second)

	_, ok := l.Lookup(t1.Add(2*time.Minute), 1234)
	assert.False(t, ok, "expired lease must not be returned")
}

func TestLog_LookupPrefersActiveOverPreIssued(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := EvenSplit([]int32{0, 1})

	require.True(t, l.Apply(t1, a, testLease, testLookahead))
	// Trigger successor queuing.
	tEdge := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(tEdge, a, testLease, testLookahead))

	// Lookup at tEdge: only the active (original) entry covers it;
	// the pre-issued successor's From == t1+lease > tEdge, so it
	// must be skipped.
	for _, e := range a.Entries {
		pid, ok := l.Lookup(tEdge, e.Range.Lo)
		require.True(t, ok)
		assert.Equal(t, e.PartitionID, pid)
	}

	// At t1 + lease, the original has expired; the successor takes
	// over.
	tCutover := t1.Add(testLease)
	for _, e := range a.Entries {
		pid, ok := l.Lookup(tCutover, e.Range.Lo)
		require.True(t, ok)
		assert.Equal(t, e.PartitionID, pid)
	}
}

func TestLog_LookupBeforeLeaseStarts(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	l.Apply(at, EvenSplit([]int32{0, 1}), testLease, testLookahead)

	_, ok := l.Lookup(at.Add(-time.Second), 100)
	assert.False(t, ok)
}

func TestLog_LookupEmpty(t *testing.T) {
	l := NewLog()
	_, ok := l.Lookup(time.Now(), 0)
	assert.False(t, ok)
}

func TestLog_Prune(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	l.Apply(t1, &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1},
	}}, time.Minute, time.Second)

	t2 := t1.Add(time.Minute)
	// Preempt P1 by giving ownership to P2.
	l.Apply(t2, &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 2},
	}}, time.Minute, time.Second)
	require.Equal(t, 2, l.Len())

	// Prune entries whose lease ended before t2 + 1ns: drops the
	// P1 entry (To=t2) but keeps the P2 entry (To=t2+1m).
	l.Prune(t2.Add(time.Nanosecond))
	require.Equal(t, 1, l.Len())
	assert.Equal(t, int32(2), l.Entries()[0].PartitionID)
}

func TestLog_PruneRetainsActiveAndPreIssuedLeases(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	a := EvenSplit([]int32{0, 1, 2})

	l.Apply(at, a, testLease, testLookahead)
	// Trigger successor queuing.
	tEdge := at.Add(testLease - testLookahead)
	l.Apply(tEdge, a, testLease, testLookahead)
	require.Equal(t, 2*len(a.Entries), l.Len())

	// Prune at the lease horizon — leases ending exactly at To are
	// retained (Prune drops only those strictly before).
	l.Prune(at.Add(testLease))
	assert.Equal(t, 2*len(a.Entries), l.Len())
}

func TestLog_PruneDropsExpiredLeases(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	l.Apply(at, EvenSplit([]int32{0, 1, 2}), time.Minute, time.Second)
	require.Equal(t, 3, l.Len())

	l.Prune(at.Add(time.Hour))
	assert.Equal(t, 0, l.Len())
}

func TestLog_LeaseHorizon(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Empty log: zero time.
	assert.True(t, l.LeaseHorizon(at).IsZero())

	a := EvenSplit([]int32{0, 1, 2, 3})
	l.Apply(at, a, testLease, testLookahead)

	// All initial leases share the same To, so horizon == To.
	assert.Equal(t, at.Add(testLease), l.LeaseHorizon(at))

	// Pre-issue successors. Each (Range, PID) chain now extends to
	// at + 2*testLease (active.To = at+lease, successor.To = at+2*lease;
	// chain-end is the latter). Horizon must reflect the chain-end,
	// not the soon-to-expire active lease — otherwise the rebalancer
	// would re-fire immediately after queuing the successor.
	tEdge := at.Add(testLease - testLookahead)
	l.Apply(tEdge, a, testLease, testLookahead)
	assert.Equal(t, at.Add(2*testLease), l.LeaseHorizon(tEdge))

	// At the cutover instant, the originals have just expired but
	// the successors are still in flight; horizon still points to
	// the successor's To = at + 2*lease.
	tCutover := at.Add(testLease)
	assert.Equal(t, at.Add(2*testLease), l.LeaseHorizon(tCutover))

	// Long after everything has expired: zero time.
	assert.True(t, l.LeaseHorizon(at.Add(10*testLease)).IsZero())
}

func TestLog_LeaseHorizon_TakesMinAcrossChains(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Hand-build entries: two chains with different lengths.
	// Chain A: just an active lease, ends at at+5m.
	// Chain B: active + queued successor, ends at at+10m.
	// Horizon should be the soonest chain-end = at+5m.
	l.entries = []LogEntry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: at, To: at.Add(5 * time.Minute)},
		{Range: HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2, From: at, To: at.Add(5 * time.Minute)},
		{Range: HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2, From: at.Add(5 * time.Minute), To: at.Add(10 * time.Minute)},
	}
	assert.Equal(t, at.Add(5*time.Minute), l.LeaseHorizon(at))
}

func TestLog_ActiveTilesFullSpace_DetectsGap(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l.entries = []LogEntry{
		{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: at, To: at.Add(testLease)},
		{Range: HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 2, From: at, To: at.Add(testLease)},
	}
	assert.Error(t, l.ActiveTilesFullSpace(at))
}

func TestLog_ActiveTilesFullSpace_DetectsExpiredLeases(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l.Apply(at, EvenSplit([]int32{0, 1}), time.Minute, time.Second)

	assert.Error(t, l.ActiveTilesFullSpace(at.Add(time.Hour)))
}

func TestLog_LatestActiveAssignment(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a := EvenSplit([]int32{0, 1, 2, 3})
	l.Apply(at, a, testLease, testLookahead)

	got := l.LatestActiveAssignment(at)
	require.NotNil(t, got)
	require.NoError(t, got.Validate())
	require.Len(t, got.Entries, len(a.Entries))
	for i, e := range a.Entries {
		assert.Equal(t, e, got.Entries[i])
	}
}

func TestLog_LatestActiveAssignment_ExpiredReturnsNil(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	l.Apply(at, EvenSplit([]int32{0, 1}), time.Minute, time.Second)
	assert.Nil(t, l.LatestActiveAssignment(at.Add(time.Hour)))
}

func TestLog_LatestActiveAssignment_Empty(t *testing.T) {
	l := NewLog()
	assert.Nil(t, l.LatestActiveAssignment(time.Now()))
}

func TestLog_LiveEntries_FiltersExpired(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Hand-build entries: one expired, one active, one pre-issued
	// future. LiveEntries(at) must drop only the expired one.
	expired := LogEntry{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: at.Add(-2 * testLease), To: at.Add(-testLease)}
	active := LogEntry{Range: HashRange{Lo: 100, Hi: 199}, PartitionID: 2, From: at.Add(-time.Minute), To: at.Add(testLease)}
	future := LogEntry{Range: HashRange{Lo: 200, Hi: math.MaxUint32}, PartitionID: 3, From: at.Add(testLease), To: at.Add(2 * testLease)}
	l.entries = []LogEntry{expired, active, future}

	got := l.LiveEntries(at)
	require.Len(t, got, 2)
	assert.Contains(t, got, active)
	assert.Contains(t, got, future)
	assert.NotContains(t, got, expired)
}

func TestLog_LiveEntries_BoundaryIsExclusive(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// To == at must be treated as expired (matches Lookup, which
	// uses at < To); To == at+1ns must be live.
	atBoundary := LogEntry{Range: HashRange{Lo: 0, Hi: 99}, PartitionID: 1, From: at.Add(-testLease), To: at}
	justAfter := LogEntry{Range: HashRange{Lo: 100, Hi: math.MaxUint32}, PartitionID: 2, From: at.Add(-testLease), To: at.Add(time.Nanosecond)}
	l.entries = []LogEntry{atBoundary, justAfter}

	got := l.LiveEntries(at)
	require.Len(t, got, 1)
	assert.Equal(t, justAfter, got[0])
}

func TestLog_LiveEntries_DefensiveCopy(t *testing.T) {
	l := NewLog()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l.Apply(at, EvenSplit([]int32{0, 1}), testLease, testLookahead)

	got := l.LiveEntries(at)
	require.NotEmpty(t, got)
	got[0].PartitionID = 99
	// Mutating the returned slice must not affect the log's internal
	// state — the snapshot is a copy.
	assert.NotEqual(t, int32(99), l.Entries()[0].PartitionID)
}

func TestLog_LiveEntries_EmptyLog(t *testing.T) {
	l := NewLog()
	got := l.LiveEntries(time.Now())
	assert.Empty(t, got)
}

// TestLog_ApplySplitAfterSuccessorPreIssued is a regression test
// for a production bug where Apply skipped pre-issued future
// entries during preemption. If a range was split, merged, or
// reassigned after its successor had been queued, the orphaned
// successor would remain in the log and activate on schedule,
// producing two overlapping active leases for the same partition.
func TestLog_ApplySplitAfterSuccessorPreIssued(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Round 1: cold start with one big range.
	a1 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1},
	}}
	require.True(t, l.Apply(t1, a1, testLease, testLookahead))

	// Round 2: still well inside the lookahead window but close
	// enough that the successor for the full-space lease gets
	// pre-issued. After this, the log has both an active lease
	// (To = t1+5m) and a future successor (From = t1+5m, To =
	// t1+10m) for ([0, MaxUint32], P=1).
	t2 := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(t2, a1, testLease, testLookahead))
	hasFutureSuccessor := false
	for _, e := range l.Entries() {
		if e.From.Equal(t1.Add(testLease)) && e.PartitionID == 1 {
			hasFutureSuccessor = true
			break
		}
	}
	require.True(t, hasFutureSuccessor, "round 2 should pre-issue the successor")

	// Round 3: split the full-space range into two pieces. The
	// pre-issued successor for the full-space range is no longer
	// wanted and must be invalidated; otherwise it will activate
	// at t1+5m and overlap with the new sub-range leases.
	t3 := t2.Add(time.Second)
	a3 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: 2_000_000}, PartitionID: 1},
		{Range: HashRange{Lo: 2_000_001, Hi: math.MaxUint32}, PartitionID: 2},
	}}
	require.True(t, l.Apply(t3, a3, testLease, testLookahead))

	// At any point past the original successor's From, the active
	// set must remain a clean tiling with no overlap. Sample at
	// the boundary where the bug used to surface (t1+5m), and
	// also a bit later for good measure.
	for _, sample := range []time.Time{t1.Add(testLease), t1.Add(testLease).Add(time.Second)} {
		require.NoErrorf(t, l.ActiveTilesFullSpace(sample),
			"active tiling must be a clean cover at %s", sample)
		assn := l.LatestActiveAssignment(sample)
		require.NotNil(t, assn)
		require.NoErrorf(t, assn.Validate(),
			"latest active assignment must validate at %s", sample)
	}
}

// TestLog_ApplyMergeAfterSuccessorPreIssued mirrors the split
// regression but for merges: two adjacent ranges with pre-issued
// successors get merged into one. Both old successors must be
// invalidated; otherwise they'll activate and overlap with the
// merged lease.
func TestLog_ApplyMergeAfterSuccessorPreIssued(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	a1 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: 2_000_000}, PartitionID: 1},
		{Range: HashRange{Lo: 2_000_001, Hi: math.MaxUint32}, PartitionID: 2},
	}}
	require.True(t, l.Apply(t1, a1, testLease, testLookahead))

	t2 := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(t2, a1, testLease, testLookahead))

	t3 := t2.Add(time.Second)
	a3 := &Assignment{Entries: []Entry{
		{Range: HashRange{Lo: 0, Hi: math.MaxUint32}, PartitionID: 1},
	}}
	require.True(t, l.Apply(t3, a3, testLease, testLookahead))

	for _, sample := range []time.Time{t1.Add(testLease), t1.Add(testLease).Add(time.Second)} {
		assn := l.LatestActiveAssignment(sample)
		require.NotNil(t, assn)
		require.NoErrorf(t, assn.Validate(),
			"latest active assignment must validate at %s", sample)
		require.Lenf(t, assn.Entries, 1,
			"exactly one merged tile expected at %s, got %v", sample, assn.Entries)
	}
}

// TestLog_ApplyReaddPreemptsAliveFuture is a white-box test on
// Fix B's preemption side: when a chain is re-added while a
// previously-pre-issued future entry for the same (Range, PID)
// is still alive (not preempted), Apply must preempt the future
// so that wall-clock crossing its From doesn't surface two
// active entries for the same range.
//
// Construct the state by reaching into the Log: a normal Apply
// sequence can produce a dead future (To = From) but never a
// live future + no active. We seed that combination directly so
// the test pins the exact branch.
func TestLog_ApplyReaddPreemptsAliveFuture(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rng := HashRange{Lo: 0, Hi: math.MaxUint32}

	// Seed: a live future entry [t1+1min, t1+6min] for (rng, P=1),
	// no active entry. This represents the corner case Fix B
	// guarantees (correctly handles even if a buggy prior version
	// of Apply left the log in this state).
	live := NewLogFromEntries([]LogEntry{
		{Range: rng, PartitionID: 1, From: t1.Add(time.Minute), To: t1.Add(6 * time.Minute)},
	})

	// Apply at t1 with (rng, P=1) in `next`. Pre-Fix-B: the
	// edge-case branch sets from = latestTo = t1+6min, creating
	// a new entry [t1+6min, t1+11min]. Then wall-clock crossing
	// t1+1min would surface the seeded live future (which the
	// first pass left alone because it's in wanted), creating
	// two active entries for the same range from t1+1min to
	// t1+6min. With Fix B: the seeded future gets preempted
	// (To := From) and a fresh entry [t1, t1+5min] is created.
	require.True(t, live.Apply(t1, &Assignment{Entries: []Entry{
		{Range: rng, PartitionID: 1},
	}}, testLease, testLookahead))

	// Sample across the danger zone. With Fix B, every sample
	// returns exactly one active entry for the range, no overlap.
	for _, sample := range []time.Time{
		t1,
		t1.Add(30 * time.Second),
		t1.Add(time.Minute),         // the seeded future's old From
		t1.Add(2 * time.Minute),
		t1.Add(testLease - time.Second),
	} {
		assn := live.LatestActiveAssignment(sample)
		require.NotNilf(t, assn, "no active at %s", sample)
		require.NoErrorf(t, assn.Validate(),
			"active assignment must be a clean tiling at %s, got %d entries: %+v",
			sample, len(assn.Entries), assn.Entries)
		require.Lenf(t, assn.Entries, 1,
			"exactly one active tile expected at %s, got %d: %+v",
			sample, len(assn.Entries), assn.Entries)
	}
}

// TestLog_ApplyReaddDeadFutureIsIdempotent confirms that the
// future-preemption loop in Fix B's !isActive branch is a no-op
// when the future is already dead (To = From). Guards against a
// regression where re-preempting flips `changed` true on a
// no-actual-change Apply.
func TestLog_ApplyReaddDeadFutureIsIdempotent(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rng := HashRange{Lo: 0, Hi: math.MaxUint32}

	require.True(t, l.Apply(t1, &Assignment{Entries: []Entry{{Range: rng, PartitionID: 1}}}, testLease, testLookahead))
	t2 := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(t2, &Assignment{Entries: []Entry{{Range: rng, PartitionID: 1}}}, testLease, testLookahead))

	// Drop the chain. After this, the pre-issued future for P=1
	// is dead (To = From = t1+lease).
	t3 := t2.Add(time.Second)
	require.True(t, l.Apply(t3, &Assignment{Entries: []Entry{{Range: rng, PartitionID: 2}}}, testLease, testLookahead))

	// Snapshot the entries so we can detect any mutation by
	// the next Apply other than the expected new fresh lease.
	before := l.Entries()

	t4 := t3.Add(time.Second)
	require.True(t, l.Apply(t4, &Assignment{Entries: []Entry{{Range: rng, PartitionID: 1}}}, testLease, testLookahead))

	after := l.Entries()
	// The Apply at t4 should have appended exactly one fresh
	// lease [t4, t4+lease] for (rng, P=1) and preempted the now-
	// stale P=2 active. The dead P=1 future from step 2 should
	// be untouched (To already == From, no-op preemption).
	require.Equal(t, len(before)+1, len(after))
	deadFutureFound := false
	for _, e := range after {
		if e.PartitionID == 1 && e.From.Equal(t1.Add(testLease)) {
			deadFutureFound = true
			assert.Equal(t, e.From, e.To, "dead future must still be zero-duration")
		}
	}
	assert.True(t, deadFutureFound, "dead future entry should still be present in the log")
}

// TestLog_ApplyDropThenReaddWithinLeaseDoesNotLeaveGap is the
// regression test for the dev-15 "slicer received invalid input
// assignment" pattern observed on 2026-05-24.
//
// Scenario: a (Range, PID) chain has its active lease + a
// pre-issued successor. The slicer drops the chain (e.g., moves
// the range to a different partition) at round R. At round R+1
// — still within leaseDuration of the pre-issued successor's From —
// the slicer puts the same (Range, PID) chain back. The original
// edge-case branch (from = latestTo) seeded the new lease at the
// dead successor's From, leaving the window [at, future.From)
// uncovered for (Range, PID). Because the slicer's `next` is a
// strict tiling, no other entry covers that hash range either,
// and LatestActiveAssignment(at) developed a gap.
//
// Fix B (always start fresh leases at `at`, preempt in-wanted
// futures) closes the gap.
func TestLog_ApplyDropThenReaddWithinLeaseDoesNotLeaveGap(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rng := HashRange{Lo: 0, Hi: math.MaxUint32}

	a1 := &Assignment{Entries: []Entry{{Range: rng, PartitionID: 1}}}
	require.True(t, l.Apply(t1, a1, testLease, testLookahead))

	// Step 2: pre-issue the successor so the chain has [active,
	// future] entries for (rng, P=1). Both end of life is past
	// the round in step 4 below.
	t2 := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(t2, a1, testLease, testLookahead))

	// Step 3: drop (rng, P=1) in favor of (rng, P=2). The first
	// pass preempts the active (To=at) and the still-future
	// successor (To=From=t1+lease, zero-duration).
	t3 := t2.Add(time.Second)
	a3 := &Assignment{Entries: []Entry{{Range: rng, PartitionID: 2}}}
	require.True(t, l.Apply(t3, a3, testLease, testLookahead))

	// Step 4: re-add (rng, P=1) before the dead successor's From
	// is reached (still within testLease of t1). This is the
	// critical step: with the pre-Fix-B code, Apply's !isActive
	// branch sees latestTo == old successor's From > at and seeds
	// the new lease at latestTo, leaving [t4, t1+lease) uncovered
	// for (rng, P=1). With Fix B, the new lease starts at t4.
	t4 := t3.Add(time.Second)
	require.True(t, l.Apply(t4, a1, testLease, testLookahead))

	// The active assignment at every point from t4 onward (up to
	// well past the original successor's From) must validate as
	// a strict tiling. Without Fix B, the window
	// [t4, t1+lease) is uncovered and Validate fails. We sample
	// densely across the danger zone to catch any sub-window
	// regression.
	dangerZone := []time.Time{
		t4,                          // moment of re-add
		t4.Add(time.Second),         // immediately after
		t4.Add(time.Minute),         // mid-window
		t1.Add(testLease - time.Second), // just before old successor.From
		t1.Add(testLease),           // at old successor.From
		t1.Add(testLease).Add(time.Second), // just past
		t1.Add(testLease).Add(time.Minute), // well past
	}
	for _, sample := range dangerZone {
		assn := l.LatestActiveAssignment(sample)
		require.NotNilf(t, assn, "no active assignment at %s — full coverage required", sample)
		require.NoErrorf(t, assn.Validate(),
			"active assignment must tile [0, MaxUint32] at %s, got %d entries: %+v",
			sample, len(assn.Entries), assn.Entries)
	}

	// And the active partition for any sample in [t4, ...] must
	// be P=1 (the re-added owner), not P=2 (the previous round's
	// owner whose lease was preempted at t4).
	for _, sample := range []time.Time{t4, t4.Add(time.Minute), t1.Add(testLease).Add(time.Minute)} {
		pid, ok := l.Lookup(sample, 12345)
		require.Truef(t, ok, "lookup must succeed at %s", sample)
		assert.Equalf(t, int32(1), pid,
			"re-added partition must own the range at %s", sample)
	}
}

// TestLog_ApplyReassignAfterSuccessorPreIssued covers the third
// shape: the same range stays but moves to a different partition
// after a successor has been pre-issued. The old partition's
// successor must not activate and overlap with the new
// partition's lease.
func TestLog_ApplyReassignAfterSuccessorPreIssued(t *testing.T) {
	l := NewLog()
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	rng := HashRange{Lo: 0, Hi: math.MaxUint32}
	a1 := &Assignment{Entries: []Entry{{Range: rng, PartitionID: 1}}}
	require.True(t, l.Apply(t1, a1, testLease, testLookahead))

	t2 := t1.Add(testLease - testLookahead)
	require.True(t, l.Apply(t2, a1, testLease, testLookahead))

	t3 := t2.Add(time.Second)
	a3 := &Assignment{Entries: []Entry{{Range: rng, PartitionID: 9}}}
	require.True(t, l.Apply(t3, a3, testLease, testLookahead))

	for _, sample := range []time.Time{t1.Add(testLease), t1.Add(testLease).Add(time.Second)} {
		assn := l.LatestActiveAssignment(sample)
		require.NotNil(t, assn)
		require.NoErrorf(t, assn.Validate(),
			"latest active assignment must validate at %s", sample)
		// Exactly one entry — the new partition — should cover
		// the full range. The old partition's pre-issued
		// successor must not activate.
		require.Len(t, assn.Entries, 1, "exactly one active tile expected at %s", sample)
		assert.Equal(t, int32(9), assn.Entries[0].PartitionID,
			"only the new partition should be active at %s", sample)
	}
}
