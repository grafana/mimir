// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"errors"
	"sort"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// ResetReadcacheResult describes the outcome of a manual
// (partition -> readcache instance) reset triggered from the admin UI
// or any future scripted path.
type ResetReadcacheResult struct {
	// At is the wall-clock time the new leases start at.
	At time.Time `json:"at"`
	// NumPartitions is the count of partitions in the new assignment.
	NumPartitions int `json:"num_partitions"`
	// NumInstances is the count of distinct readcache instances the
	// partitions were spread across.
	NumInstances int `json:"num_instances"`
	// PerInstance is the count of partitions assigned to each
	// readcache instance.
	PerInstance map[string]int `json:"per_instance"`
}

// ResetReadcacheAssignment forces an immediate round-robin
// (partition -> readcache) assignment, preempting any existing
// leases that don't match. It exists as an operational escape hatch
// for stuck states the slicer can't unstick on its own — for example
// when every reporter is returning zero load and Pass 2's
// "lightest instance" heuristic collapses onto a single pod, OOMing
// it before any load signal can break the symmetry.
//
// Semantics:
//
//  1. Active partition IDs are [0, K) from config (ActivePartitionCount
//     when set, otherwise PartitionCount).
//  2. Active readcache instance IDs are resolved via
//     activeReadcacheInstances (static list first, then ring).
//  3. Partition i is assigned to instance i % len(instances),
//     with both lists sorted for determinism.
//  4. The mapping is applied to the readcache log via the usual
//     apply() path, which preempts any conflicting active or
//     pre-issued leases, persists the new state to disk, and
//     broadcasts to all WatchReadcacheAssignments subscribers.
//
// Once apply returns, every readcache subscriber will receive a
// fresh snapshot within one tick and reconcile its owned partitions
// to match. The next scheduled rebalance round (within
// MinRebalanceInterval) will re-push hash ranges to the new
// owners, and any load-based refinement can resume from this
// known-balanced starting point.
func (r *Rebalancer) ResetReadcacheAssignment(now time.Time) (ResetReadcacheResult, error) {
	if r.readcacheStore == nil {
		return ResetReadcacheResult{}, errors.New("readcache assignment store is not initialized")
	}
	instances := r.activeReadcacheInstances()
	if len(instances) == 0 {
		return ResetReadcacheResult{}, errors.New("no active readcache instances available (check the ring and -nautilus-rebalancer.readcache-slicer.instances)")
	}
	active := r.activePartitionsForRound()
	if len(active) == 0 {
		return ResetReadcacheResult{}, errors.New("no active partitions to assign")
	}

	// Both lists sorted so the round-robin is stable regardless of
	// the upstream iteration order. activeReadcacheInstances already
	// sorts its output, but be defensive.
	sort.Strings(instances)
	sort.Slice(active, func(i, j int) bool { return active[i] < active[j] })

	entries := make([]readcacheassignment.AssignmentEntry, len(active))
	perInstance := make(map[string]int, len(instances))
	for _, inst := range instances {
		perInstance[inst] = 0
	}
	for i, pid := range active {
		inst := instances[i%len(instances)]
		entries[i] = readcacheassignment.AssignmentEntry{PartitionID: pid, InstanceID: inst}
		perInstance[inst]++
	}

	next := &readcacheassignment.Assignment{Entries: entries}
	r.readcacheStore.apply(now, next, r.cfg.LeaseDuration, r.cfg.LeaseLookahead, r.cfg.EntryRetention)

	// Reflect the reset in the slicer's cooldown bookkeeping so the
	// next slicer round (if enabled) does not immediately try to
	// undo what an operator just did. We record every partition as
	// "recently moved" for the configured move cooldown; if the
	// readcache slicer isn't enabled this is a no-op since the map
	// is otherwise unused.
	if r.cfg.ReadcacheSlicer.Enabled {
		fakeMoves := make([]readcacheMove, 0, len(entries))
		for _, e := range entries {
			fakeMoves = append(fakeMoves, readcacheMove{PartitionID: e.PartitionID})
		}
		r.readcacheCooldowns.extendForMoves(now, r.cfg.ReadcacheSlicer.MoveCooldown, fakeMoves)
	}

	return ResetReadcacheResult{
		At:            now,
		NumPartitions: len(active),
		NumInstances:  len(instances),
		PerInstance:   perInstance,
	}, nil
}
