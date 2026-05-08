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
