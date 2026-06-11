// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// TestReadcacheLogStore_SubscribeBeforeFirstApplyReturnsNilInitial
// is the readcache-store mirror of the empty-snapshot-before-ready
// regression: a rebalancer that restarts loads its persisted
// readcache log from disk; those entries may all be expired by the
// time a readcache pod reconnects. Returning that empty
// LiveEntries() result as the "initial" snapshot tells every
// readcache "you own nothing" and triggers a fleet-wide partition
// drop. subscribe must return nil until apply() has run.
func TestReadcacheLogStore_SubscribeBeforeFirstApplyReturnsNilInitial(t *testing.T) {
	s := newReadcacheLogStore()

	// Seed the in-memory log directly to mimic a startup that loaded
	// persisted entries off disk. We do NOT call apply().
	t0 := time.Unix(1000, 0)
	s.seedFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "rc-a", From: t0, To: t0.Add(time.Hour)},
	})

	initial, _, unsubscribe := s.subscribe(false)
	defer unsubscribe()
	assert.Nil(t, initial,
		"subscribe must return nil initial before the first apply, even when seeded entries are live, so a rebalancer restart never broadcasts stale persisted state as authoritative")
}

// TestReadcacheLogStore_FirstApplyPrimesSubscriber confirms a
// subscriber attached before any apply receives the priming
// broadcast as soon as apply runs.
func TestReadcacheLogStore_FirstApplyPrimesSubscriber(t *testing.T) {
	s := newReadcacheLogStore()
	t0 := time.Unix(1000, 0)

	initial, updates, unsubscribe := s.subscribe(false)
	defer unsubscribe()
	require.Nil(t, initial)

	require.True(t, s.apply(t0, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, time.Minute, 10*time.Second, time.Hour, 0))

	select {
	case u := <-updates:
		assert.Len(t, u.entries, 2,
			"first apply must prime subscribers attached before ready with the full live snapshot")
		assert.True(t, u.reset, "priming broadcast must be a snapshot")
	case <-time.After(time.Second):
		t.Fatal("subscriber attached before first apply did not receive a priming broadcast")
	}
}

// TestReadcacheLogStore_NoOpApplyStillPrimesEarlySubscriber covers
// the case where the rebalancer's first round (or admin reset)
// produces no log change but is still the first apply call —
// becameReady must trigger a broadcast so subscribers attached
// before ready learn the current state.
func TestReadcacheLogStore_NoOpApplyStillPrimesEarlySubscriber(t *testing.T) {
	s := newReadcacheLogStore()
	t0 := time.Unix(1000, 0)

	// Seed with entries equivalent to what apply will produce.
	leaseDur := time.Minute
	lookahead := 10 * time.Second
	seed := []readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "rc-a", From: t0, To: t0.Add(leaseDur)},
		{PartitionID: 1, InstanceID: "rc-b", From: t0, To: t0.Add(leaseDur)},
	}
	s.seedFromEntries(seed)

	initial, updates, unsubscribe := s.subscribe(false)
	defer unsubscribe()
	require.Nil(t, initial)

	changed := s.apply(t0.Add(time.Second), &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, leaseDur, lookahead, time.Hour, 0)
	assert.False(t, changed, "fixture must reproduce the no-op apply case")

	select {
	case u := <-updates:
		assert.NotEmpty(t, u.entries,
			"a no-op apply on the !ready -> ready edge must still prime the subscriber")
	case <-time.After(time.Second):
		t.Fatal("subscriber did not receive a priming broadcast on the no-op-but-becameReady apply")
	}
}

// TestReadcacheLogStore_SubscribeAfterApplyReturnsLiveEntries is
// the happy-path counter-example: once apply has flipped ready,
// subscribers immediately get the current live snapshot as initial.
func TestReadcacheLogStore_SubscribeAfterApplyReturnsLiveEntries(t *testing.T) {
	s := newReadcacheLogStore()
	t0 := time.Unix(1000, 0)

	require.True(t, s.apply(t0, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
		},
	}, time.Minute, 10*time.Second, time.Hour, 0))

	initial, _, unsubscribe := s.subscribe(false)
	defer unsubscribe()
	require.NotNil(t, initial)
	assert.Len(t, initial.entries, 1,
		"after first apply, subscribe must return the current live snapshot as initial")
}

// TestReadcacheLogStore_DeltaSubscriberSurvivesPartitionMove drives
// the readcache store through a safety-windowed partition move and
// verifies a delta subscriber's replayed log matches the server
// exactly — including the previous owner's clamped lease, which the
// distributor needs to find the frozen slice.
func TestReadcacheLogStore_DeltaSubscriberSurvivesPartitionMove(t *testing.T) {
	s := newReadcacheLogStore()
	t0 := time.Unix(1000, 0)
	lease, lookahead, safety := 5*time.Minute, 90*time.Second, 2*time.Minute

	require.True(t, s.apply(t0, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, lease, lookahead, time.Hour, safety))

	initial, updates, unsubscribe := s.subscribe(true)
	defer unsubscribe()
	require.NotNil(t, initial)
	require.True(t, initial.reset)
	client := readcacheassignment.NewLogFromEntries(initial.entries)

	// Partition 0 moves rc-a -> rc-b. rc-a's active lease is clamped
	// to move+safety; rc-b gets a fresh lease. Both mutations must
	// ride the delta.
	t1 := t0.Add(time.Minute)
	require.True(t, s.apply(t1, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-b"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, lease, lookahead, time.Hour, safety))

	select {
	case u := <-updates:
		require.False(t, u.reset, "primed delta subscriber must receive a delta")
		client = client.MergedWithEntries(u.entries)
		if !u.pruneBefore.IsZero() {
			client.Prune(u.pruneBefore)
		}
	case <-time.After(time.Second):
		t.Fatal("did not receive move delta")
	}

	require.Equal(t, s.snapshot(), client.Entries(),
		"snapshot + delta replay must reproduce the server log exactly")

	// The replayed log must still answer the interval-ownership
	// question that motivated all of this: who owned partition 0
	// across the move window?
	owners := client.OwnersDuring(0, t0, t1.Add(safety))
	assert.Equal(t, []string{"rc-a", "rc-b"}, owners,
		"replayed log must expose both the previous and new owner across the move")
}

// TestReadcacheLogStore_SubscribeIncludesRetainedHistory asserts that
// expired-but-retained leases are handed to subscribers. The
// distributor resolves "which readcache owned partition P during the
// query's wall-clock window", so the previous owner's expired lease is
// load-bearing: without it, queries spanning a partition move can't
// find the readcache holding the frozen slice, and any window bound
// that lands before the latest rotation reports "partition P had no
// readcache owner during the query window".
func TestReadcacheLogStore_SubscribeIncludesRetainedHistory(t *testing.T) {
	s := newReadcacheLogStore()
	t0 := time.Unix(1000, 0)

	// rc-a owns partition 0; its lease expires after one minute.
	require.True(t, s.apply(t0, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
		},
	}, time.Minute, 10*time.Second, time.Hour, 0))

	// Thirty minutes later (well past rc-a's lease, well within the
	// 1h retention) the partition moves to rc-b.
	t1 := t0.Add(30 * time.Minute)
	require.True(t, s.apply(t1, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-b"},
		},
	}, time.Minute, 10*time.Second, time.Hour, 0))

	initial, _, unsubscribe := s.subscribe(false)
	defer unsubscribe()

	require.NotNil(t, initial)
	var sawExpired bool
	for _, e := range initial.entries {
		if e.InstanceID == "rc-a" && !e.To.After(t1) {
			sawExpired = true
		}
	}
	assert.True(t, sawExpired,
		"subscribe must include the previous owner's expired-but-retained lease")
}
