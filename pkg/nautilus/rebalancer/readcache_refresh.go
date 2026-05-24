// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"time"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// refreshReadcacheLeases re-applies the currently-active (partition ->
// readcache) ownership to the readcache assignment log, which extends
// successor leases via the usual Log.Apply pre-issue path without
// changing any partition's owner.
//
// Why this exists: the readcache slicer (runReadcacheSlicer) is the
// only round-loop path that calls readcacheStore.apply, and it is
// gated on cfg.ReadcacheSlicer.Enabled. When the slicer is disabled
// (the Phase 2A bring-up default) the readcache log is never
// reapplied, so the leases seeded from disk at startup (or installed
// by a one-off admin reset) age out at most one LeaseDuration later.
// Once the leases expire LookupReadcache returns nothing, every round
// logs "skipping SetHashRanges push: no readcache owners resolved",
// the readcaches stop receiving fresh range pushes, and Tier-1 writes
// land in Kafka partitions that no readcache is currently consuming —
// queries return empty even though the data is intact in Kafka.
//
// The refresh closes that gap by extending leases for the existing
// assignment every round. It deliberately does NOT compute or apply
// new moves: that decision is the slicer's job, and the operator
// turning the slicer off has explicitly opted out of move planning.
//
// The refresh is a no-op (with a warning) when no active leases exist.
// This is the "stuck cold-start" state: a fresh rebalancer whose
// persisted readcache log loaded only expired entries and whose
// operator has not yet triggered an admin reset to bootstrap a fresh
// (partition -> readcache) assignment. The warning surfaces the gap
// so the operator knows to either click "Reset to even split" on the
// admin page or enable the slicer.
//
// Returns true if Log.Apply mutated the log (i.e., a successor was
// pre-issued or a stale entry preempted), matching the bool semantics
// of runReadcacheSlicer.
func (r *Rebalancer) refreshReadcacheLeases(now time.Time) bool {
	if r.readcacheStore == nil {
		return false
	}

	// Build the next-assignment from currently-active log entries.
	// readcacheStore.snapshot() returns every entry (including
	// expired ones retained for ActiveAt-at-past-T queries) so we
	// filter to ActiveAt(now) here; only those represent the
	// authoritative current ownership the refresh wants to preserve.
	//
	// Dedupe by (PartitionID, InstanceID) so multi-owner mode (each
	// partition replicated to N readcaches) doesn't synthesize a
	// duplicate AssignmentEntry that Log.Apply would just collapse
	// again. Single-owner mode (the Phase 2A default) sees at most
	// one entry per partition, so the dedupe is a cheap no-op there.
	snapshot := r.readcacheStore.snapshot()
	type ownerKey struct {
		pid      int32
		instance string
	}
	seen := make(map[ownerKey]struct{}, len(snapshot))
	entries := make([]readcacheassignment.AssignmentEntry, 0, len(snapshot))
	for _, e := range snapshot {
		if !e.ActiveAt(now) {
			continue
		}
		k := ownerKey{pid: e.PartitionID, instance: e.InstanceID}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		entries = append(entries, readcacheassignment.AssignmentEntry{
			PartitionID: e.PartitionID,
			InstanceID:  e.InstanceID,
		})
	}

	if len(entries) == 0 {
		// Surface the stuck-cold-start state. Logging at warn here is
		// deliberate: with the slicer disabled there is no other
		// signal in the rebalancer logs that the readcache ownership
		// has collapsed, and the downstream "skipping SetHashRanges
		// push: no readcache owners resolved" warning fires under
		// many other conditions too (Tier-2 ring not converged, every
		// readcache temporarily unreachable, etc).
		level.Warn(r.logger).Log(
			"msg", "skipped readcache lease refresh: no active leases to extend",
			"hint", "either enable nautilus_rebalancer.readcache_slicer.enabled or trigger an admin reset to seed (partition -> readcache) ownership",
		)
		return false
	}

	next := &readcacheassignment.Assignment{Entries: entries}
	changed := r.readcacheStore.apply(now, next, r.cfg.LeaseDuration, r.cfg.LeaseLookahead, r.cfg.EntryRetention)
	if changed {
		level.Info(r.logger).Log(
			"msg", "readcache leases refreshed",
			"entries", len(entries),
			"lease_horizon", r.readcacheStore.leaseHorizon(now).Format(time.RFC3339),
			"subscribers", r.readcacheStore.numSubscribers(),
		)
	}
	return changed
}
