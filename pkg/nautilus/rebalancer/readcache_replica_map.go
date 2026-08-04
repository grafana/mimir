// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sort"

	"github.com/go-kit/log/level"

	"github.com/grafana/mimir/pkg/nautilus/readcacheassignment"
)

// placementReadcacheInstances returns the logical slot IDs the
// tier-2 slicer may assign partitions to this round.
//
// When DesiredReplicas > 0 the placement set is sticky desired
// slots (independent of ring liveness). Otherwise this falls back to
// stabilized ring/static membership (legacy RF=1).
func (r *Rebalancer) placementReadcacheInstances() []string {
	if n := r.cfg.ReadcacheSlicer.DesiredReplicas; n > 0 {
		return readcacheassignment.DesiredLogicalSlots(r.cfg.ReadcacheSlicer.LogicalIDPrefix, n)
	}
	return r.stabilizedReadcacheInstances()
}

// placementReadcacheInstancesFrom is placementReadcacheInstances for
// callers that have already computed the stabilized membership set.
// stabilizedReadcacheInstances advances the membership tracker's
// hysteresis counters as a side effect, so a round must call it
// exactly once.
func (r *Rebalancer) placementReadcacheInstancesFrom(stabilized []string) []string {
	if n := r.cfg.ReadcacheSlicer.DesiredReplicas; n > 0 {
		return readcacheassignment.DesiredLogicalSlots(r.cfg.ReadcacheSlicer.LogicalIDPrefix, n)
	}
	return stabilized
}

// refreshReplicaMap rebuilds the logical→concrete replica map from
// the readcache ring (or static Instances list) and publishes it on
// the watch stream. Under DesiredReplicas > 0 the map is what lets
// readcaches and queriers expand logical lease IDs to zone pods
// without doubling the lease log.
//
// When DesiredReplicas is 0 the map is cleared (identity / RF=1).
func (r *Rebalancer) refreshReplicaMap() readcacheassignment.ReplicaMap {
	if r.cfg.ReadcacheSlicer.DesiredReplicas <= 0 {
		r.readcacheStore.setReplicaMap(nil)
		return nil
	}

	concrete := r.concreteReadcacheReplicas()
	m := readcacheassignment.BuildReplicaMap(concrete)
	// Ensure every desired logical slot has an entry (possibly empty
	// replicas if both zone mirrors are down — queriers fail that
	// slot; slicer still keeps the slot in the placement set).
	for _, logical := range readcacheassignment.DesiredLogicalSlots(r.cfg.ReadcacheSlicer.LogicalIDPrefix, r.cfg.ReadcacheSlicer.DesiredReplicas) {
		if _, ok := m[logical]; !ok {
			if m == nil {
				m = readcacheassignment.ReplicaMap{}
			}
			m[logical] = nil
		}
	}
	r.readcacheStore.setReplicaMap(m)
	return m
}

// concreteReadcacheReplicas enumerates concrete pods from the static
// allow-list (if set) or the ring, including zone labels.
func (r *Rebalancer) concreteReadcacheReplicas() []readcacheassignment.Replica {
	if len(r.cfg.ReadcacheSlicer.Instances) > 0 {
		out := make([]readcacheassignment.Replica, 0, len(r.cfg.ReadcacheSlicer.Instances))
		for _, id := range r.cfg.ReadcacheSlicer.Instances {
			zone := ""
			if ident, ok := readcacheassignment.ParseInstanceIdentity(id); ok {
				zone = ident.Zone
			}
			out = append(out, readcacheassignment.Replica{InstanceID: id, Zone: zone})
		}
		return out
	}
	if r.readcacheRing == nil {
		return nil
	}
	set, err := r.readcacheRing.GetAllHealthy(readcacheRingOp)
	if err != nil {
		level.Warn(r.logger).Log("msg", "readcache ring lookup failed while building replica map", "err", err)
		return nil
	}
	out := make([]readcacheassignment.Replica, 0, len(set.Instances))
	for _, inst := range set.Instances {
		zone := inst.Zone
		if zone == "" {
			if ident, ok := readcacheassignment.ParseInstanceIdentity(inst.Id); ok {
				zone = ident.Zone
			}
		}
		out = append(out, readcacheassignment.Replica{InstanceID: inst.Id, Zone: zone})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out
}

// excludeLogicalTargetsFromConcreteFailures converts concrete
// instance IDs that failed HashRangeStats into logical slot IDs that
// should not receive new partitions. A logical slot is excluded only
// when it has no healthy concrete replica left in the ring (single
// zone failure must not block the slot under RF=2).
func excludeLogicalTargetsFromConcreteFailures(failedConcrete map[string]struct{}, replicaMap readcacheassignment.ReplicaMap, healthyConcrete map[string]struct{}) map[string]struct{} {
	if len(failedConcrete) == 0 {
		return nil
	}
	if len(replicaMap) == 0 {
		// RF=1 / identity: concrete IDs are logical IDs.
		return failedConcrete
	}
	out := map[string]struct{}{}
	for logical, reps := range replicaMap {
		if len(reps) == 0 {
			out[logical] = struct{}{}
			continue
		}
		anyHealthy := false
		for _, rep := range reps {
			if _, failed := failedConcrete[rep.InstanceID]; failed {
				continue
			}
			if healthyConcrete != nil {
				if _, ok := healthyConcrete[rep.InstanceID]; !ok {
					continue
				}
			}
			anyHealthy = true
			break
		}
		if !anyHealthy {
			out[logical] = struct{}{}
		}
	}
	return out
}

// healthyConcreteSet returns the set of concrete instance IDs currently
// in the ring (or static list).
func (r *Rebalancer) healthyConcreteSet() map[string]struct{} {
	out := map[string]struct{}{}
	for _, rep := range r.concreteReadcacheReplicas() {
		out[rep.InstanceID] = struct{}{}
	}
	return out
}
