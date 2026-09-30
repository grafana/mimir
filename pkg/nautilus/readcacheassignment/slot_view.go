// SPDX-License-Identifier: AGPL-3.0-only

package readcacheassignment

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// SlotViewRefreshInterval is how often a process re-reads the readcache
// ring into its slot view. GetAllHealthy is a local read; Observe drops
// the result when the healthy set is unchanged, so heartbeat timestamps
// alone do not rebuild the grouping. The query path never calls it.
const SlotViewRefreshInterval = 5 * time.Second

// HealthyPod is one readcache the ring currently considers healthy.
// Addr is the dial target. Zone may be empty when the ring omits it;
// the name is parsed in that case.
type HealthyPod struct {
	InstanceID string
	Zone       string
	Addr       string
}

// SlotPod is one concrete pod serving a logical slot.
type SlotPod struct {
	InstanceID string
	Zone       string
	Addr       string
}

// SlotView groups healthy readcache pods by the slot parsed from the
// hostname. A missing slot means no healthy pod. Callers must not treat
// that as "dial the logical id".
type SlotView struct {
	bySlot map[string][]SlotPod
	byID   map[string]SlotPod
}

// Pods returns the healthy pods for a logical slot. ok is false when
// the slot has none.
func (v SlotView) Pods(logicalID string) ([]SlotPod, bool) {
	pods, ok := v.bySlot[logicalID]
	if !ok || len(pods) == 0 {
		return nil, false
	}
	out := make([]SlotPod, len(pods))
	copy(out, pods)
	return out, true
}

// Addr returns the dial target cached for a concrete instance.
func (v SlotView) Addr(instanceID string) (string, bool) {
	pod, ok := v.byID[instanceID]
	if !ok || pod.Addr == "" {
		return "", false
	}
	return pod.Addr, true
}

// InstanceIDs is the set of concrete instance ids in the view.
func (v SlotView) InstanceIDs() map[string]struct{} {
	if len(v.byID) == 0 {
		return map[string]struct{}{}
	}
	out := make(map[string]struct{}, len(v.byID))
	for id := range v.byID {
		out[id] = struct{}{}
	}
	return out
}

// ReplicaMap converts the view into the logical→concrete shape the
// rebalancer's exclusion helper already understands. Slots with no
// healthy pod are absent; use ReplicaMapIncluding to record those.
func (v SlotView) ReplicaMap() ReplicaMap {
	if len(v.bySlot) == 0 {
		return nil
	}
	out := make(ReplicaMap, len(v.bySlot))
	for logical, pods := range v.bySlot {
		reps := make([]Replica, len(pods))
		for i, pod := range pods {
			reps[i] = Replica{InstanceID: pod.InstanceID, Zone: pod.Zone}
		}
		out[logical] = reps
	}
	return out
}

// ReplicaMapIncluding is ReplicaMap plus an empty entry for every
// logical id that has no healthy pod. The rebalancer uses the empty
// entry to keep a fully-down slot from receiving new partitions.
func (v SlotView) ReplicaMapIncluding(logicalIDs []string) ReplicaMap {
	out := v.ReplicaMap()
	for _, id := range logicalIDs {
		if id == "" {
			continue
		}
		if _, ok := out[id]; ok {
			continue
		}
		if out == nil {
			out = ReplicaMap{}
		}
		out[id] = nil
	}
	return out
}

// GroupHealthyPods groups pods by parsed slot. Members are ordered by
// zone, then instance id. A pod whose ring entry has no zone keeps the
// zone parsed from its name.
func GroupHealthyPods(pods []HealthyPod) SlotView {
	normalized := normalizeHealthyPods(pods)
	if len(normalized) == 0 {
		return SlotView{}
	}
	replicas := make([]Replica, len(normalized))
	addrs := make(map[string]string, len(normalized))
	for i, pod := range normalized {
		replicas[i] = Replica{InstanceID: pod.InstanceID, Zone: pod.Zone}
		addrs[pod.InstanceID] = pod.Addr
	}
	grouped := BuildReplicaMap(replicas)
	view := SlotView{
		bySlot: make(map[string][]SlotPod, len(grouped)),
		byID:   make(map[string]SlotPod, len(normalized)),
	}
	for logical, reps := range grouped {
		slotPods := make([]SlotPod, len(reps))
		for i, rep := range reps {
			slotPods[i] = SlotPod{InstanceID: rep.InstanceID, Zone: rep.Zone, Addr: addrs[rep.InstanceID]}
			view.byID[rep.InstanceID] = slotPods[i]
		}
		view.bySlot[logical] = slotPods
	}
	return view
}

// ObserveReplicaMap installs an explicit logical→concrete expansion.
// Tests use it for names that are not readcache slot hostnames. The
// next Observe from a ring read replaces it.
func (c *SlotViewCache) ObserveReplicaMap(m ReplicaMap) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.view = viewFromReplicaMap(m)
	c.available = true
	// Not a ring fingerprint. The next Observe, including an empty
	// ring read, replaces this installation.
	c.fp = "replica-map"
}

func viewFromReplicaMap(m ReplicaMap) SlotView {
	if len(m) == 0 {
		return SlotView{}
	}
	view := SlotView{
		bySlot: make(map[string][]SlotPod, len(m)),
		byID:   make(map[string]SlotPod, len(m)),
	}
	for logical, reps := range m {
		pods := make([]SlotPod, 0, len(reps))
		for _, rep := range reps {
			if rep.InstanceID == "" {
				continue
			}
			pod := SlotPod{InstanceID: rep.InstanceID, Zone: rep.Zone, Addr: rep.InstanceID}
			pods = append(pods, pod)
			view.byID[rep.InstanceID] = pod
		}
		if len(pods) == 0 {
			continue
		}
		view.bySlot[logical] = pods
	}
	return view
}

func normalizeHealthyPods(pods []HealthyPod) []HealthyPod {
	if len(pods) == 0 {
		return nil
	}
	out := make([]HealthyPod, 0, len(pods))
	for _, pod := range pods {
		if pod.InstanceID == "" {
			continue
		}
		if pod.Zone == "" {
			if id, ok := ParseInstanceIdentity(pod.InstanceID); ok {
				pod.Zone = id.Zone
			}
		}
		out = append(out, pod)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].InstanceID != out[j].InstanceID {
			return out[i].InstanceID < out[j].InstanceID
		}
		if out[i].Zone != out[j].Zone {
			return out[i].Zone < out[j].Zone
		}
		return out[i].Addr < out[j].Addr
	})
	return out
}

func healthyPodFingerprint(pods []HealthyPod) string {
	normalized := normalizeHealthyPods(pods)
	if len(normalized) == 0 {
		return ""
	}
	var b strings.Builder
	for _, pod := range normalized {
		b.WriteString(pod.InstanceID)
		b.WriteByte('|')
		b.WriteString(pod.Zone)
		b.WriteByte('|')
		b.WriteString(pod.Addr)
		b.WriteByte('\n')
	}
	return b.String()
}

// SlotViewCache is the querier and rebalancer copy of the ring grouping.
// It is unavailable until a ring read succeeds, and again after a read
// fails. A later read that sees the same pods, addresses, and zones is
// a no-op.
type SlotViewCache struct {
	mu        sync.RWMutex
	available bool
	fp        string
	view      SlotView
}

// NewSlotViewCache returns a cache that reports unavailable until Observe.
func NewSlotViewCache() *SlotViewCache {
	return &SlotViewCache{}
}

// Observe records a successful ring read. changed is false when the
// healthy set matches the cached one. An empty list is a successful
// read of no healthy pods, which replaces any previous view.
func (c *SlotViewCache) Observe(pods []HealthyPod) (changed bool) {
	fp := healthyPodFingerprint(pods)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.available && c.fp == fp {
		return false
	}
	c.view = GroupHealthyPods(pods)
	c.available = true
	c.fp = fp
	return true
}

// MarkUnavailable drops the cached grouping. The next successful
// Observe rebuilds it even if the membership matches the old one.
func (c *SlotViewCache) MarkUnavailable() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.available = false
	c.view = SlotView{}
	c.fp = ""
}

// Current returns the cached grouping. ok is false when the ring could
// not be read.
func (c *SlotViewCache) Current() (SlotView, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.available {
		return SlotView{}, false
	}
	return c.view, true
}
