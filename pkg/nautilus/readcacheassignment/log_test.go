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
	changed := l.Apply(at, want, testLease, testLookahead)
	assert.True(t, changed)

	owners := l.Lookup(at, 0)
	require.Len(t, owners, 1)
	assert.Equal(t, "rc-a", owners[0])

	owners = l.Lookup(at, 1)
	require.Len(t, owners, 1)
	assert.Equal(t, "rc-b", owners[0])

	// Second apply with same desired state is a no-op (steady).
	changed = l.Apply(at, want, testLease, testLookahead)
	assert.False(t, changed, "steady-state apply should not mutate")
}

func TestLog_ApplyPreemption(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)

	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead)

	// Reassign partition 0 to rc-b later.
	at2 := at.Add(5 * time.Second)
	l.Apply(at2, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-b"}},
	}, testLease, testLookahead)

	// At the reassignment moment rc-a's lease should be preempted (To
	// clamped to at2) and rc-b's fresh lease should be active.
	owners := l.Lookup(at2, 0)
	require.Len(t, owners, 1, "exactly one owner should be active at the reassignment moment")
	assert.Equal(t, "rc-b", owners[0])
}

func TestLog_PreissueSuccessor(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)

	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead)

	// Advance to within the lookahead window; the rebalancer queues
	// the successor.
	at2 := at.Add(testLease - testLookahead/2)
	changed := l.Apply(at2, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead)
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
	}, testLease, testLookahead)

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
	}, testLease, testLookahead)

	horizon := l.LeaseHorizon(at)
	assert.True(t, horizon.Equal(at.Add(testLease)), "horizon should match the lease duration on cold start")
}

func TestLog_Prune(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)
	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead)

	preReassign := at.Add(5 * time.Second)
	l.Apply(preReassign, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-b"}},
	}, testLease, testLookahead)

	pruneCutoff := preReassign.Add(time.Second)
	l.Prune(pruneCutoff)

	for _, e := range l.Entries() {
		assert.True(t, !e.To.Before(pruneCutoff), "Prune left an expired entry")
	}
}

func TestLog_LiveEntries(t *testing.T) {
	l := NewLog()
	at := time.Unix(1000, 0)
	l.Apply(at, &Assignment{
		Entries: []AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, testLease, testLookahead)

	// All entries are live (the lease just started).
	live := l.LiveEntries(at)
	assert.Len(t, live, 1)

	// Past the lease window, no entries are live.
	future := at.Add(testLease + time.Second)
	live = l.LiveEntries(future)
	assert.Empty(t, live)
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
