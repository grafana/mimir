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

	initial, _, unsubscribe := s.subscribe(t0)
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

	initial, updates, unsubscribe := s.subscribe(t0)
	defer unsubscribe()
	require.Nil(t, initial)

	require.True(t, s.apply(t0, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, time.Minute, 10*time.Second, time.Hour))

	select {
	case snap := <-updates:
		assert.Len(t, snap, 2,
			"first apply must prime subscribers attached before ready with the full live snapshot")
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

	initial, updates, unsubscribe := s.subscribe(t0)
	defer unsubscribe()
	require.Nil(t, initial)

	changed := s.apply(t0.Add(time.Second), &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{
			{PartitionID: 0, InstanceID: "rc-a"},
			{PartitionID: 1, InstanceID: "rc-b"},
		},
	}, leaseDur, lookahead, time.Hour)
	assert.False(t, changed, "fixture must reproduce the no-op apply case")

	select {
	case snap := <-updates:
		assert.NotEmpty(t, snap,
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
	}, time.Minute, 10*time.Second, time.Hour))

	initial, _, unsubscribe := s.subscribe(t0)
	defer unsubscribe()
	assert.Len(t, initial, 1,
		"after first apply, subscribe must return the current live snapshot as initial")
}
