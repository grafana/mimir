// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sort"

	"github.com/go-kit/log/level"
)

const readcacheMembershipStableRounds = 2

// readcacheMembershipTracker applies symmetric hysteresis to the healthy
// readcache set. The initial set is admitted immediately so cold start can
// assign partitions. Afterwards an instance must be present or absent for
// readcacheMembershipStableRounds consecutive observations before joining or
// leaving the placement set.
type readcacheMembershipTracker struct {
	initialized bool
	stable      map[string]struct{}
	present     map[string]int
	missing     map[string]int
}

func newReadcacheMembershipTracker() readcacheMembershipTracker {
	return readcacheMembershipTracker{
		stable:  map[string]struct{}{},
		present: map[string]int{},
		missing: map[string]int{},
	}
}

func (t *readcacheMembershipTracker) seed(instances []string) {
	t.ensureMaps()
	if t.initialized {
		return
	}
	for _, id := range instances {
		if id != "" {
			t.stable[id] = struct{}{}
		}
	}
	t.initialized = true
}

func (t *readcacheMembershipTracker) observe(observed []string) []string {
	t.ensureMaps()

	seen := make(map[string]struct{}, len(observed))
	for _, id := range observed {
		seen[id] = struct{}{}
	}

	if !t.initialized {
		for id := range seen {
			t.stable[id] = struct{}{}
		}
		t.initialized = true
		return t.current()
	}

	for id := range t.stable {
		if _, ok := seen[id]; ok {
			delete(t.missing, id)
			continue
		}
		t.missing[id]++
		if t.missing[id] >= readcacheMembershipStableRounds {
			delete(t.stable, id)
			delete(t.missing, id)
		}
	}

	for id := range t.present {
		if _, ok := seen[id]; !ok {
			delete(t.present, id)
		}
	}
	for id := range seen {
		if _, ok := t.stable[id]; ok {
			continue
		}
		t.present[id]++
		if t.present[id] >= readcacheMembershipStableRounds {
			t.stable[id] = struct{}{}
			delete(t.present, id)
			delete(t.missing, id)
		}
	}

	return t.current()
}

func (t *readcacheMembershipTracker) current() []string {
	t.ensureMaps()
	out := make([]string, 0, len(t.stable))
	for id := range t.stable {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// unavailable returns stable members currently inside their removal grace
// period. They retain existing ownership but must not receive new partitions.
func (t *readcacheMembershipTracker) unavailable() map[string]struct{} {
	out := make(map[string]struct{}, len(t.missing))
	for id := range t.missing {
		if _, stable := t.stable[id]; stable {
			out[id] = struct{}{}
		}
	}
	return out
}

func (t *readcacheMembershipTracker) ensureMaps() {
	if t.stable == nil {
		t.stable = map[string]struct{}{}
	}
	if t.present == nil {
		t.present = map[string]int{}
	}
	if t.missing == nil {
		t.missing = map[string]int{}
	}
}

// stabilizedReadcacheInstances returns the placement set for this rebalance
// round. Static operator pins are authoritative and bypass hysteresis. A ring
// lookup failure does not count as an absence observation: the last stable set
// is retained and the next successful round resumes the streak calculation.
func (r *Rebalancer) stabilizedReadcacheInstances() []string {
	if len(r.cfg.ReadcacheSlicer.Instances) > 0 {
		out := append([]string(nil), r.cfg.ReadcacheSlicer.Instances...)
		sort.Strings(out)
		return out
	}
	if r.readcacheRing == nil {
		return nil
	}

	set, err := r.readcacheRing.GetAllHealthy(readcacheRingOp)
	if err != nil {
		level.Warn(r.logger).Log("msg", "readcache ring lookup failed; retaining stable slicer membership", "err", err)
		return r.readcacheMembership.current()
	}

	observed := make([]string, 0, len(set.Instances))
	for _, inst := range set.Instances {
		observed = append(observed, inst.Id)
	}
	if !r.readcacheMembership.initialized && r.readcacheStore != nil {
		owners := make([]string, 0)
		for _, entry := range r.readcacheStore.snapshot() {
			if entry.ActiveAt(r.now()) {
				owners = append(owners, entry.InstanceID)
			}
		}
		if len(owners) > 0 {
			r.readcacheMembership.seed(owners)
		}
	}
	return r.readcacheMembership.observe(observed)
}
