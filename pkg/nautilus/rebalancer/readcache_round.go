// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"time"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// runReadcacheSlicer executes the second slicer round: balancing the
// (partition -> readcache instance) mapping using the per-partition
// load signal collected by collectRates().
//
// activeInstances is the set of readcache instance IDs the slicer
// may assign to this round. The caller resolves it (preferring the
// ring when wired, falling back to ReadcacheSlicerConfig.Instances)
// so the slicer itself is agnostic to discovery mechanism.
//
// This is a no-op when ReadcacheSlicer.Enabled is false or
// activeInstances is empty (those gates are checked by the caller).
// When it runs, it:
//
//  1. Reads the current (partition -> instance) mapping from
//     readcacheStore.
//  2. Builds the per-partition load vector as
//     alpha*active_series + beta*samples_ewma.
//  3. Calls planReadcacheAssignment to produce the next mapping.
//  4. Records the moves into the cooldown map.
//  5. Applies the plan to readcacheStore (which broadcasts to
//     subscribers and persists the new state if a data-dir is
//     configured).
func (r *Rebalancer) runReadcacheSlicer(
	now time.Time,
	activePartitions []int32,
	partitionLByPID map[int32]int64,
	partitionQuerySamples map[int32]float64,
	activeInstances []string,
) {
	cfg := r.cfg.ReadcacheSlicer

	// Build the load function: alpha * active_series + beta * EWMA.
	loadByPartition := make(map[int32]float64, len(activePartitions))
	for _, pid := range activePartitions {
		loadByPartition[pid] = cfg.Alpha*float64(partitionLByPID[pid]) + cfg.Beta*partitionQuerySamples[pid]
	}

	// Build the current owner map from the live readcache log.
	currentOwner := make(map[int32]string, len(activePartitions))
	for _, entry := range r.readcacheStore.snapshot() {
		if entry.ActiveAt(now) {
			// Single-owner mode: each partition has at most one
			// active entry; if multi-owner gets enabled later this
			// will need a deterministic tie-break.
			currentOwner[entry.PartitionID] = entry.InstanceID
		}
	}

	r.readcacheCooldowns.prune(now)
	plan := planReadcacheAssignment(cfg, readcachePlanInput{
		partitions:      activePartitions,
		loadByPartition: loadByPartition,
		instances:       activeInstances,
		currentOwner:    currentOwner,
		recentlyMoved:   r.readcacheCooldowns.stillCooling(now),
	})

	r.readcacheCooldowns.extendForMoves(now, cfg.MoveCooldown, plan.Moves)

	if changed := r.readcacheStore.apply(now, plan.Assignment, r.cfg.LeaseDuration, r.cfg.LeaseLookahead, r.cfg.EntryRetention); changed {
		level.Info(r.logger).Log(
			"msg", "readcache slicer round produced changes",
			"moves", len(plan.Moves),
			"partitions", len(activePartitions),
			"instances", len(activeInstances),
		)
	}
	r.metrics.updateReadcacheRound(plan)

	// Admin trace plumbing: record planned-owner-by-partition and
	// per-instance load. The trace serializer reads
	// r.admin.lastReadcachePlan so the in-process /rounds.json reflects
	// the most recent round.
	r.admin.setLastReadcachePlan(now, plan, currentOwner)

	// Update load gauges. These are last-write-wins per partition /
	// per instance, exactly as the partitionLByPID gauges in the
	// existing slicer.
	_ = readcacheassignment.LogEntry{} // keep package import warm if no flag branch uses it directly
}
