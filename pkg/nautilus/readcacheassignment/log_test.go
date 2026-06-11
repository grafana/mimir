// SPDX-License-Identifier: AGPL-3.0-only

package readcacheassignment

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testLease     = 30 * time.Second
	testLookahead = 10 * time.Second
	// testSafety = 0 reproduces legacy immediate-handoff preemption,
	// so these existing cases keep asserting To = move-time.
	testSafety = time.Duration(0)
)

func TestLog_ApplyColdStart(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)

	want := &Assignment{
		Entries: []AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}
	changed := l.Apply(at, want, testLease, testLookahead, testSafety)
	assert.True(t, changed)

	owners := l.Lookup(at, 0)
	require.Len(t, owners, 1)
	assert.Equal(t, "rc-a", owners[0])

	owners = l.Lookup(at, 1)
	require.Len(t, owners, 1)
	assert.Equal(t, "rc-b", owners[0])

	// Second apply with same desired state is a no-op (steady).
	changed = l.Apply(at, want, testLease, testLookahead, testSafety)
	assert.False(t, changed, "steady-state apply should not mutate")
}

func TestLog_ApplyPreemption(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)

	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead, testSafety)

	// Reassign partition 0 to rc-b later.
	at2 := at.Add(5 * time.Second)
	l.Apply(at2, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-b"}},
	}, testLease, testLookahead, testSafety)

	// At the reassignment moment rc-a's lease should be preempted (To
	// clamped to at2) and rc-b's fresh lease should be active.
	owners := l.Lookup(at2, 0)
	require.Len(t, owners, 1, "exactly one owner should be active at the reassignment moment")
	assert.Equal(t, "rc-b", owners[0])
}

// TestLog_ApplyPreemptionWithSafetyWindow verifies the overlapping
// move scheme: when a partition moves rc-a -> rc-b with a positive
// safety window, rc-a's lease is kept alive until move_time+safety so
// both instances own the partition during the overlap (no gap), then
// rc-a is dropped. This is what lets the distributor's OwnersDuring
// fan-out reach both the frozen previous slice and the live new slice.
func TestLog_ApplyPreemptionWithSafetyWindow(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)
	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead, 0)

	const safety = 2 * time.Second
	at2 := at.Add(5 * time.Second)
	l.Apply(at2, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-b"}},
	}, testLease, testLookahead, safety)

	// During the overlap both owners are active.
	owners := l.Lookup(at2, 0)
	sort.Strings(owners)
	assert.Equal(t, []string{"rc-a", "rc-b"}, owners, "both owners active at the move instant")

	owners = l.Lookup(at2.Add(safety-time.Millisecond), 0)
	sort.Strings(owners)
	assert.Equal(t, []string{"rc-a", "rc-b"}, owners, "previous owner stays alive through the safety window")

	// At the end of the safety window the previous owner is gone.
	owners = l.Lookup(at2.Add(safety), 0)
	assert.Equal(t, []string{"rc-b"}, owners, "previous owner frozen after the safety window")

	// OwnersDuring over the whole move interval reports both, which is
	// what the distributor fans out to.
	during := l.OwnersDuring(0, at, at2.Add(safety+time.Second))
	sort.Strings(during)
	assert.Equal(t, []string{"rc-a", "rc-b"}, during)
}

func TestLog_PreissueSuccessor(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)

	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead, testSafety)

	// Advance to within the lookahead window; the rebalancer queues
	// the successor.
	at2 := at.Add(testLease - testLookahead/2)
	changed := l.Apply(at2, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead, testSafety)
	assert.True(t, changed, "successor should be queued when lease is within lookahead")

	// Both the active and the pre-issued successor cover their own
	// windows.
	entries := l.Entries()
	require.GreaterOrEqual(t, len(entries), 2, "should have queued a successor lease")
}

func TestLog_MultiOwner(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)

	// Two readcache instances co-own partition 0 (e.g. during a
	// failover hand-off).
	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 0, InstanceID: "rc-b"},
		},
	}, testLease, testLookahead, testSafety)

	owners := l.Lookup(at, 0)
	sort.Strings(owners)
	assert.Equal(t, []string{"rc-a", "rc-b"}, owners)
}

func TestLog_LeaseHorizon(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)

	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, testLease, testLookahead, testSafety)

	horizon := l.LeaseHorizon(at)
	assert.True(t, horizon.Equal(at.Add(testLease)), "horizon should match the lease duration on cold start")
}

func TestLog_Prune(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)
	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead, testSafety)

	preReassign := at.Add(5 * time.Second)
	l.Apply(preReassign, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-b"}},
	}, testLease, testLookahead, testSafety)

	pruneCutoff := preReassign.Add(time.Second)
	l.Prune(pruneCutoff)

	for _, e := range l.Entries() {
		assert.True(t, !e.To.Before(pruneCutoff), "Prune left an expired entry")
	}
}

// TestLog_MergedWithEntries_ReplayEquivalence drives a Log through a
// partition move with a safety window — the readcache log's richest
// mutation pattern (the old owner's To is clamped to move+safety,
// the new owner gets a fresh lease) — and verifies that replaying
// per-round diffs onto an initial snapshot reproduces the final log,
// the invariant the delta wire protocol rests on.
func TestLog_MergedWithEntries_ReplayEquivalence(t *testing.T) {
	t0 := time.Unix(1000, 0)
	lease, lookahead, safety := 5*time.Minute, 90*time.Second, 2*time.Minute

	server := NewLog()
	server.Apply(t0, &Assignment{
		Entries: []AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, lease, lookahead, safety)

	client := NewLogFromEntries(server.Entries())

	steps := []*Assignment{
		// Partition 0 moves rc-a -> rc-b (preemption with safety
		// window on rc-a's active lease).
		{Entries: []AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-b"},
			{PartitionID: 1, InstanceID: "rc-b"},
		}},
		// And back again.
		{Entries: []AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		}},
	}
	at := t0
	for _, next := range steps {
		at = at.Add(lease - lookahead)
		before := server.Entries()
		server.Apply(at, next, lease, lookahead, safety)
		after := server.Entries()

		type key struct {
			pid      int32
			instance string
			fromMs   int64
		}
		prevTo := make(map[key]time.Time, len(before))
		for _, e := range before {
			prevTo[key{e.PartitionID, e.InstanceID, e.From.UnixMilli()}] = e.To
		}
		var delta []LogEntry
		for _, e := range after {
			if to, ok := prevTo[key{e.PartitionID, e.InstanceID, e.From.UnixMilli()}]; !ok || !to.Equal(e.To) {
				delta = append(delta, e)
			}
		}

		client = client.MergedWithEntries(delta)
	}

	assert.Equal(t, server.Entries(), client.Entries(),
		"replaying per-round diffs onto the initial snapshot must reproduce the server log exactly")
}

func TestLog_LiveEntries(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)
	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead, testSafety)

	// All entries are live (the lease just started).
	live := l.LiveEntries(at)
	assert.Len(t, live, 1)

	// Past the lease window, no entries are live.
	future := at.Add(testLease + time.Second)
	live = l.LiveEntries(future)
	assert.Empty(t, live)
}

// TestLog_EntriesDuring pins the diagnostic accessor behind the
// distributor's sampled routing-decision log: it must return exactly
// the leases OwnersDuring would have matched (same half-open window
// predicate), preserving each lease's bounds so the log can explain
// WHY an instance was selected.
func TestLog_EntriesDuring(t *testing.T) {
	at := time.Unix(1000, 0)
	l := NewLogFromEntries([]LogEntry{
		// Two consecutive leases on partition 0: a move from rc-a to rc-b.
		{PartitionID: 0, InstanceID: "rc-a", From: at, To: at.Add(10 * time.Minute)},
		{PartitionID: 0, InstanceID: "rc-b", From: at.Add(10 * time.Minute), To: at.Add(20 * time.Minute)},
		// Other partition, must never leak into partition 0 results.
		{PartitionID: 1, InstanceID: "rc-c", From: at, To: at.Add(20 * time.Minute)},
	})

	// Window spanning the move returns both leases with their bounds.
	got := l.EntriesDuring(0, at.Add(5*time.Minute), at.Add(15*time.Minute))
	require.Len(t, got, 2)
	assert.Equal(t, "rc-a", got[0].InstanceID)
	assert.True(t, got[0].From.Equal(at))
	assert.True(t, got[0].To.Equal(at.Add(10*time.Minute)))
	assert.Equal(t, "rc-b", got[1].InstanceID)

	// Window entirely inside the first lease returns only it.
	got = l.EntriesDuring(0, at.Add(time.Minute), at.Add(2*time.Minute))
	require.Len(t, got, 1)
	assert.Equal(t, "rc-a", got[0].InstanceID)

	// Half-open semantics match OwnersDuring: a window starting
	// exactly at a lease's To excludes it, and a window ending
	// exactly at a lease's From excludes it.
	got = l.EntriesDuring(0, at.Add(10*time.Minute), at.Add(11*time.Minute))
	require.Len(t, got, 1)
	assert.Equal(t, "rc-b", got[0].InstanceID)
	got = l.EntriesDuring(0, at.Add(-time.Minute), at)
	assert.Empty(t, got)

	// The set of instances always agrees with OwnersDuring over the
	// same window.
	w0, w1 := at.Add(5*time.Minute), at.Add(15*time.Minute)
	owners := l.OwnersDuring(0, w0, w1)
	fromEntries := make(map[string]struct{})
	for _, e := range l.EntriesDuring(0, w0, w1) {
		fromEntries[e.InstanceID] = struct{}{}
	}
	require.Len(t, fromEntries, len(owners))
	for _, o := range owners {
		assert.Contains(t, fromEntries, o)
	}
}

func TestNewLogFromEntries(t *testing.T) {
	at := time.Unix(1000, 0)
	seed := []LogEntry{
		{PartitionID: 2, InstanceID: "rc-x", From: at, To: at.Add(testLease)},
		{PartitionID: 0, InstanceID: "rc-a", From: at, To: at.Add(testLease)},
		{PartitionID: 1, InstanceID: "rc-b", From: at, To: at.Add(testLease)},
	}
	l := NewLogFromEntries(seed)
	require.Equal(t, 3, l.Len())

	entries := l.Entries()
	for i := 1; i < len(entries); i++ {
		assert.LessOrEqual(t, entries[i-1].PartitionID, entries[i].PartitionID, "entries should be sorted by partition")
	}
}
