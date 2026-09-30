// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"errors"
	"sort"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"

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

// refreshSlotView rebuilds the logical→concrete grouping from the
// readcache ring (or the static Instances list). A timestamp-only ring
// update is a no-op. The result is not published on the assignment
// stream; queriers keep their own copy from the ring they already watch.
//
// A failed ring read marks the view unavailable. An empty ring is a
// successful read of no healthy pods.
func (r *Rebalancer) refreshSlotView() bool {
	cache := r.slotCache()
	pods, err := r.healthyReadcachePods()
	if err != nil {
		if errors.Is(err, ring.ErrEmptyRing) {
			return cache.Observe(nil)
		}
		level.Warn(r.logger).Log("msg", "readcache ring lookup failed while refreshing slot view", "err", err)
		cache.MarkUnavailable()
		return false
	}
	return cache.Observe(pods)
}

func (r *Rebalancer) slotCache() *readcacheassignment.SlotViewCache {
	if r.readcacheSlots == nil {
		r.readcacheSlots = readcacheassignment.NewSlotViewCache()
	}
	return r.readcacheSlots
}

func (r *Rebalancer) currentSlotView() (readcacheassignment.SlotView, bool) {
	if r.readcacheSlots == nil {
		return readcacheassignment.SlotView{}, false
	}
	return r.readcacheSlots.Current()
}

// concreteIDsForSlot returns the healthy pods for a logical slot.
// A missing slot or an unavailable view returns nil. It does not fall
// back to dialing the logical id.
func (r *Rebalancer) concreteIDsForSlot(logicalID string) []string {
	view, ok := r.currentSlotView()
	if !ok {
		return nil
	}
	pods, ok := view.Pods(logicalID)
	if !ok {
		return nil
	}
	out := make([]string, len(pods))
	for i, pod := range pods {
		out[i] = pod.InstanceID
	}
	return out
}

// slotReplicaMap is the view as a ReplicaMap, with an empty entry for
// every desired slot that has no healthy pod. ok is false when the
// ring could not be read.
func (r *Rebalancer) slotReplicaMap() (readcacheassignment.ReplicaMap, bool) {
	view, ok := r.currentSlotView()
	if !ok {
		return nil, false
	}
	if n := r.cfg.ReadcacheSlicer.DesiredReplicas; n > 0 {
		return view.ReplicaMapIncluding(readcacheassignment.DesiredLogicalSlots(r.cfg.ReadcacheSlicer.LogicalIDPrefix, n)), true
	}
	return view.ReplicaMap(), true
}

func (r *Rebalancer) healthyReadcachePods() ([]readcacheassignment.HealthyPod, error) {
	if len(r.cfg.ReadcacheSlicer.Instances) > 0 {
		out := make([]readcacheassignment.HealthyPod, 0, len(r.cfg.ReadcacheSlicer.Instances))
		for _, id := range r.cfg.ReadcacheSlicer.Instances {
			out = append(out, readcacheassignment.HealthyPod{InstanceID: id, Addr: id})
		}
		return out, nil
	}
	if r.readcacheRing == nil {
		return nil, errors.New("readcache ring is not configured")
	}
	set, err := r.readcacheRing.GetAllHealthy(readcacheRingOp)
	if err != nil {
		return nil, err
	}
	out := make([]readcacheassignment.HealthyPod, 0, len(set.Instances))
	for _, inst := range set.Instances {
		out = append(out, readcacheassignment.HealthyPod{
			InstanceID: inst.Id,
			Zone:       inst.Zone,
			Addr:       inst.Addr,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, nil
}

// healthyConcreteSet returns the set of concrete instance IDs in the
// current slot view.
func (r *Rebalancer) healthyConcreteSet() map[string]struct{} {
	view, ok := r.currentSlotView()
	if !ok {
		return nil
	}
	return view.InstanceIDs()
}

// excludeLogicalTargetsFromConcreteFailures converts concrete
// instance IDs that failed HashRangeStats into logical slot IDs that
// should not receive new partitions. A logical slot is excluded only
// when it has no healthy concrete replica left in the ring (single
// zone failure must not block the slot under RF=2).
func excludeLogicalTargetsFromConcreteFailures(failedConcrete map[string]struct{}, replicaMap readcacheassignment.ReplicaMap, healthyConcrete map[string]struct{}) map[string]struct{} {
	if len(replicaMap) == 0 {
		// RF=1 / identity: concrete IDs are logical IDs.
		if len(failedConcrete) == 0 {
			return nil
		}
		return failedConcrete
	}
	out := map[string]struct{}{}
	for logical, reps := range replicaMap {
		if len(reps) == 0 {
			// No healthy pod. Keep the slot out of new placement even
			// when every stats call succeeded.
			out[logical] = struct{}{}
			continue
		}
		if len(failedConcrete) == 0 {
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
