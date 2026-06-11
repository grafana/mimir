// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// newTestRebalancerForRefresh returns a Rebalancer wired with an
// empty in-memory readcache log store and the lease parameters used
// in production-typical configs. logger is captured via the optional
// logBuf so tests can assert on the warning the no-active-leases path
// emits.
func newTestRebalancerForRefresh(t *testing.T, logBuf *bytes.Buffer) *Rebalancer {
	t.Helper()
	cfg := Config{
		LeaseDuration:  5 * time.Minute,
		LeaseLookahead: 4 * time.Minute,
		EntryRetention: time.Hour,
	}
	var logger log.Logger = log.NewNopLogger()
	if logBuf != nil {
		logger = log.NewLogfmtLogger(logBuf)
	}
	return &Rebalancer{
		cfg:            cfg,
		logger:         logger,
		readcacheStore: newReadcacheLogStore(),
	}
}

// TestRefreshReadcacheLeases_ExtendsLeaseHorizonForActiveOwners
// reproduces the mimir-dev-15 failure mode that motivated this
// helper: a rebalancer with the readcache slicer disabled never
// reapplies the readcache assignment log, so leases age out roughly
// one LeaseDuration after the persisted state was last touched.
// Refresh must pre-issue successors for every currently-active
// owner so the lease horizon advances by LeaseDuration on each call.
func TestRefreshReadcacheLeases_ExtendsLeaseHorizonForActiveOwners(t *testing.T) {
	r := newTestRebalancerForRefresh(t, nil)

	// Seed the log with two owners whose leases will expire within
	// the lookahead window. seedFromEntries deliberately bypasses
	// apply so r.readcacheStore.ready stays false at this point —
	// the refresh must flip ready and pre-issue successors.
	t0 := time.Unix(10_000, 0)
	originalTo := t0.Add(time.Minute) // < t0 + LeaseLookahead, so a successor is due.
	r.readcacheStore.seedFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "rc-a", From: t0.Add(-time.Minute), To: originalTo},
		{PartitionID: 1, InstanceID: "rc-b", From: t0.Add(-time.Minute), To: originalTo},
	})

	require.True(t, r.refreshReadcacheLeases(t0),
		"refresh must mutate the log when leases are within the lookahead window")

	// After refresh, both (partition, instance) pairs should have
	// a successor lease with To = originalTo + LeaseDuration.
	all := r.readcacheStore.snapshot()
	type key struct {
		pid      int32
		instance string
	}
	latest := make(map[key]time.Time)
	for _, e := range all {
		k := key{pid: e.PartitionID, instance: e.InstanceID}
		if l, ok := latest[k]; !ok || e.To.After(l) {
			latest[k] = e.To
		}
	}
	require.Len(t, latest, 2, "both owners must still be present")
	for k, latestTo := range latest {
		assert.True(t, latestTo.After(originalTo),
			"refresh must advance latest To beyond the original lease end for %v (got %v, original %v)", k, latestTo, originalTo)
		assert.Equal(t, originalTo.Add(r.cfg.LeaseDuration), latestTo,
			"successor's To should be originalTo + LeaseDuration for %v", k)
	}
}

// TestRefreshReadcacheLeases_PreservesOwnership verifies the
// refresh does NOT reassign any partition — that's the slicer's
// job. Refresh is supposed to extend leases on the EXISTING
// (partition -> readcache) mapping, full stop. A bug here would
// silently move partitions between readcaches every round, which is
// what the operator who disabled the slicer is explicitly trying to
// avoid.
func TestRefreshReadcacheLeases_PreservesOwnership(t *testing.T) {
	r := newTestRebalancerForRefresh(t, nil)

	t0 := time.Unix(10_000, 0)
	r.readcacheStore.seedFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "rc-a", From: t0.Add(-time.Minute), To: t0.Add(time.Minute)},
		{PartitionID: 1, InstanceID: "rc-b", From: t0.Add(-time.Minute), To: t0.Add(time.Minute)},
		{PartitionID: 2, InstanceID: "rc-a", From: t0.Add(-time.Minute), To: t0.Add(time.Minute)},
	})

	r.refreshReadcacheLeases(t0)

	// Ownership at the moment of refresh — and at the lease horizon
	// — must still be exactly what was seeded. Any successor entry
	// must carry the same InstanceID as its predecessor.
	owner := func(at time.Time) map[int32]string {
		out := make(map[int32]string)
		for _, e := range r.readcacheStore.snapshot() {
			if e.ActiveAt(at) {
				out[e.PartitionID] = e.InstanceID
			}
		}
		return out
	}
	assert.Equal(t,
		map[int32]string{0: "rc-a", 1: "rc-b", 2: "rc-a"},
		owner(t0),
		"refresh must not change ownership at the moment it runs")
	// Pick a time inside the just-pre-issued successor window.
	later := t0.Add(time.Minute).Add(time.Second)
	assert.Equal(t,
		map[int32]string{0: "rc-a", 1: "rc-b", 2: "rc-a"},
		owner(later),
		"refresh must not change ownership in the pre-issued successor window")
}

// TestRefreshReadcacheLeases_NoActiveLeasesEmitsWarning covers
// the stuck-cold-start state: rebalancer restarted, the persisted
// readcache log entries are all in the past, and no operator has
// triggered an admin reset to bootstrap fresh (partition ->
// readcache) ownership. Refresh has nothing to extend and must say
// so loudly so operators can find the gap without having to read
// the source.
func TestRefreshReadcacheLeases_NoActiveLeasesEmitsWarning(t *testing.T) {
	var buf bytes.Buffer
	r := newTestRebalancerForRefresh(t, &buf)

	// All entries expired before t0.
	r.readcacheStore.seedFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "rc-a", From: time.Unix(0, 0), To: time.Unix(100, 0)},
		{PartitionID: 1, InstanceID: "rc-b", From: time.Unix(0, 0), To: time.Unix(100, 0)},
	})

	t0 := time.Unix(10_000, 0)
	assert.False(t, r.refreshReadcacheLeases(t0),
		"refresh must report no change when there are no active leases")

	logged := buf.String()
	assert.Contains(t, logged, "skipped readcache lease refresh: no active leases to extend",
		"refresh must warn loudly when it cannot extend anything; got: %s", logged)
	assert.Contains(t, logged, "admin reset",
		"warning must hint at the operator action that unsticks this state; got: %s", logged)
}

// TestRefreshReadcacheLeases_EmptyStoreIsSafeNoOp verifies the
// helper is harmless on a freshly-constructed store that has never
// been seeded, the state every rebalancer starts in before persisted
// entries (if any) are loaded.
func TestRefreshReadcacheLeases_EmptyStoreIsSafeNoOp(t *testing.T) {
	var buf bytes.Buffer
	r := newTestRebalancerForRefresh(t, &buf)

	t0 := time.Unix(10_000, 0)
	assert.False(t, r.refreshReadcacheLeases(t0))

	// Same expected warning as the all-expired case: from the
	// helper's POV both states look identical (no ActiveAt(now)
	// entries).
	assert.Contains(t, buf.String(), "skipped readcache lease refresh")
}

// TestRefreshReadcacheLeases_NilStoreReturnsFalse defends the
// pre-init code path: rebalance() can in principle be invoked before
// readcacheStore is wired (defensively, even though production wiring
// always installs one). The helper must not panic.
func TestRefreshReadcacheLeases_NilStoreReturnsFalse(t *testing.T) {
	r := newTestRebalancerForRefresh(t, nil)
	r.readcacheStore = nil
	assert.False(t, r.refreshReadcacheLeases(time.Unix(10_000, 0)))
}

// TestRefreshReadcacheLeases_DedupesMultiOwnerKeys covers the
// (currently dormant) multi-owner mode where the same partition
// appears with multiple InstanceIDs. The helper must collapse
// duplicates by (PartitionID, InstanceID) before handing the
// assignment to Log.Apply: feeding duplicates would not be
// semantically wrong (Apply de-dupes too) but it pollutes the
// AssignmentEntry slice and grows the log churn for no reason.
func TestRefreshReadcacheLeases_DedupesMultiOwnerKeys(t *testing.T) {
	r := newTestRebalancerForRefresh(t, nil)

	t0 := time.Unix(10_000, 0)
	r.readcacheStore.seedFromEntries([]readcacheassignment.LogEntry{
		// P0 has two active owners (multi-owner mode).
		{PartitionID: 0, InstanceID: "rc-a", From: t0.Add(-time.Minute), To: t0.Add(time.Minute)},
		{PartitionID: 0, InstanceID: "rc-b", From: t0.Add(-time.Minute), To: t0.Add(time.Minute)},
		// Plus another pre-issued successor for the same pair that
		// is also still active (overlapping leases).
		{PartitionID: 0, InstanceID: "rc-a", From: t0.Add(-30 * time.Second), To: t0.Add(2 * time.Minute)},
	})

	require.True(t, r.refreshReadcacheLeases(t0),
		"with two (P0, *) pairs needing successors, refresh must mutate the log")

	// Count distinct (P, I) pairs in the post-refresh snapshot
	// whose latest To advanced past the original 2-minute horizon.
	type key struct {
		pid      int32
		instance string
	}
	advancedPairs := make(map[key]bool)
	originalHorizon := t0.Add(2 * time.Minute)
	for _, e := range r.readcacheStore.snapshot() {
		if e.To.After(originalHorizon) {
			advancedPairs[key{pid: e.PartitionID, instance: e.InstanceID}] = true
		}
	}
	assert.Len(t, advancedPairs, 2,
		"refresh must issue exactly one successor per distinct (P, I), not one per active log entry")
}

// TestRefreshReadcacheLeases_PrimesEarlySubscriber confirms that
// running the refresh on a !ready store flips ready and broadcasts
// to subscribers attached before the refresh — same primer-on-edge
// behavior as runReadcacheSlicer's apply. Without this, a readcache
// pod that subscribes during the cold-start window before the first
// rebalance round would receive no initial snapshot until the next
// round.
func TestRefreshReadcacheLeases_PrimesEarlySubscriber(t *testing.T) {
	r := newTestRebalancerForRefresh(t, nil)

	t0 := time.Unix(10_000, 0)
	r.readcacheStore.seedFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "rc-a", From: t0.Add(-time.Minute), To: t0.Add(time.Minute)},
	})

	initial, updates, unsubscribe := r.readcacheStore.subscribe(false)
	defer unsubscribe()
	require.Nil(t, initial,
		"sanity: subscribe before any apply must return nil initial")

	r.refreshReadcacheLeases(t0)

	select {
	case u := <-updates:
		assert.NotEmpty(t, u.entries,
			"refresh must prime early subscribers by flipping !ready -> ready inside apply")
	case <-time.After(time.Second):
		t.Fatal("subscriber attached before refresh did not receive a priming broadcast")
	}
}

// TestRefreshReadcacheLeases_NoOpWhenHorizonAlreadyBeyondLookahead
// is the steady-state case: a recent slicer round (or admin reset)
// already pre-issued successors past the lookahead window, so this
// round's refresh has nothing to do. Refresh must return false and
// must not log a misleading "extended" success line.
func TestRefreshReadcacheLeases_NoOpWhenHorizonAlreadyBeyondLookahead(t *testing.T) {
	var buf bytes.Buffer
	r := newTestRebalancerForRefresh(t, &buf)

	t0 := time.Unix(10_000, 0)
	// Lease horizon = t0 + 10min > t0 + LeaseLookahead (4min), so
	// Log.Apply finds the "successor already pre-issued" branch and
	// leaves the log alone.
	r.readcacheStore.seedFromEntries([]readcacheassignment.LogEntry{
		{PartitionID: 0, InstanceID: "rc-a", From: t0.Add(-time.Minute), To: t0.Add(10 * time.Minute)},
	})
	// Flip ready via an apply so the no-op-but-becameReady code
	// path inside readcacheStore.apply doesn't fire on the refresh
	// call below — that path correctly returns changed=true the
	// first time it runs and would mask the steady-state behavior
	// we're trying to assert. The apply itself happens to be a no-op
	// (latestTo already past lookahead) but ready still flips.
	r.readcacheStore.apply(t0, &readcacheassignment.Assignment{
		Entries: []readcacheassignment.AssignmentEntry{{PartitionID: 0, InstanceID: "rc-a"}},
	}, r.cfg.LeaseDuration, r.cfg.LeaseLookahead, r.cfg.EntryRetention, r.cfg.ReadcacheMoveSafetyWindow)

	buf.Reset()
	assert.False(t, r.refreshReadcacheLeases(t0),
		"with horizon already past lookahead, refresh must be a true no-op")

	assert.NotContains(t, buf.String(), "readcache leases refreshed",
		"refresh must not log an extended-success line when nothing actually changed; got: %s", buf.String())
	assert.NotContains(t, strings.ToLower(buf.String()), "skipped",
		"steady-state refresh must not emit the no-active-leases warning either; got: %s", buf.String())
}
