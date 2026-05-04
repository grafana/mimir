// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"sync"
	"sync/atomic"
	"time"
)

// CachedAgentStatsTracker wraps an AgentStatsTracker and caches the output
// of ClusterStats for cacheTTL. TrackAgentRequest and AgentStats forward
// to the inner tracker live; only ClusterStats benefits from the cache,
// because aggregating snapshots from every agent on every call would be
// wasteful while the cluster view barely changes within a short window.
//
// The cache is keyed by (slowMultiplier, faultyThreshold). When a caller
// varies them between calls, the cache simply refreshes on the next call;
// callers that pass the same parameters repeatedly hit the cache.
type CachedAgentStatsTracker struct {
	inner    AgentStatsTracker
	cacheTTL time.Duration
	cache    atomic.Pointer[clusterStatsCache]

	// refreshMu serializes the recompute path: only one goroutine populates
	// the cache at a time. Readers go through the atomic pointer and never
	// take this mutex on the fast path.
	refreshMu sync.Mutex
}

type clusterStatsCache struct {
	slowMultiplier  float64
	faultyThreshold float64
	stats           ClusterStats
	// ok captures the inner tracker's quorum verdict. A cache entry with
	// ok=false serves as a "no-quorum sentinel" that suppresses gather
	// calls until cacheTTL elapses.
	ok         bool
	computedAt time.Time
}

// NewCachedAgentStatsTracker returns a wrapper around inner that caches
// ClusterStats for at most ttl.
func NewCachedAgentStatsTracker(inner AgentStatsTracker, ttl time.Duration) *CachedAgentStatsTracker {
	return &CachedAgentStatsTracker{
		inner:    inner,
		cacheTTL: ttl,
	}
}

func (w *CachedAgentStatsTracker) TrackAgentRequest(now time.Time, nodeID int32, latency time.Duration, err error) {
	w.inner.TrackAgentRequest(now, nodeID, latency, err)
}

func (w *CachedAgentStatsTracker) AgentStats(now time.Time, nodeID int32) (AgentStats, bool) {
	return w.inner.AgentStats(now, nodeID)
}

// PurgeAgents forwards to the inner tracker and clears the cache so a
// removed agent does not linger in the cached stats for up to cacheTTL.
func (w *CachedAgentStatsTracker) PurgeAgents(nodeIDs []int32) {
	w.inner.PurgeAgents(nodeIDs)
	w.cache.Store(nil)
}

func (w *CachedAgentStatsTracker) ClusterStats(now time.Time, slowMultiplier, faultyThreshold float64) (ClusterStats, bool) {
	if c := w.getCachedClusterStats(now, slowMultiplier, faultyThreshold); c != nil {
		return c.stats, c.ok
	}
	w.refreshMu.Lock()
	defer w.refreshMu.Unlock()
	// Double-check: another goroutine may have refreshed while we waited.
	if c := w.getCachedClusterStats(now, slowMultiplier, faultyThreshold); c != nil {
		return c.stats, c.ok
	}
	stats, ok := w.inner.ClusterStats(now, slowMultiplier, faultyThreshold)
	w.cache.Store(&clusterStatsCache{
		slowMultiplier:  slowMultiplier,
		faultyThreshold: faultyThreshold,
		stats:           stats,
		ok:              ok,
		computedAt:      now,
	})
	return stats, ok
}

// getCachedClusterStats returns the cached entry if it is still within cacheTTL and
// matches the requested parameters; nil otherwise. A cache miss covers
// any of: no entry, expired, or different parameters.
func (w *CachedAgentStatsTracker) getCachedClusterStats(now time.Time, slowMultiplier, faultyThreshold float64) *clusterStatsCache {
	c := w.cache.Load()
	if c == nil || now.Sub(c.computedAt) >= w.cacheTTL {
		return nil
	}
	if c.slowMultiplier != slowMultiplier || c.faultyThreshold != faultyThreshold {
		return nil
	}
	return c
}
