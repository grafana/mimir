// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"sort"

	"github.com/go-kit/log/level"
	"github.com/grafana/dskit/ring"
)

// activeReadcacheInstances returns the readcache instance IDs the
// slicer may assign partitions to this round. Precedence:
//
//  1. Ring (when wired) — enumerates healthy ACTIVE members of the
//     readcache ring. Picks up scale-up/scale-down at the next round.
//  2. Static ReadcacheSlicer.Instances — fallback for tests and
//     degraded-mode bring-ups where no ring KV is available.
//
// Returns nil when neither source yields an instance set; the
// caller treats that as "skip the readcache slicer round".
//
// Even when the ring is consulted, ReadcacheSlicer.Instances acts as
// an allow-list override when non-empty: an operator who wants to
// pin the slicer to a known subset of pods (e.g. to drain one) can
// just set the static list. This is the same escape hatch the
// ingester ring offers via -ingester.ring.instances-block-list.
func (r *Rebalancer) activeReadcacheInstances() []string {
	// Static list takes priority when present: it's the operator's
	// explicit allow-list. Without it, fall back to the ring.
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
		level.Warn(r.logger).Log("msg", "readcache ring lookup failed; skipping slicer round", "err", err)
		return nil
	}
	out := make([]string, 0, len(set.Instances))
	for _, inst := range set.Instances {
		out = append(out, inst.Id)
	}
	sort.Strings(out)
	return out
}

// readcacheRingOp is the dskit ring operation used to enumerate the
// readcache fleet. Since the readcache ring is service-discovery
// only (the rebalancer's log is the authority on partition
// ownership), any healthy ACTIVE member is eligible.
var readcacheRingOp = ring.NewOp([]ring.InstanceState{ring.ACTIVE}, nil)
