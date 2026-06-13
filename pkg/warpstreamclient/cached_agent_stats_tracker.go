// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"sync"
	"time"
)

// CachedAgentStatsTracker wraps an AgentStatsTracker and caches the output
// of ClusterStats for cacheTTL. TrackAgentRequest and AgentStats forward
// to the inner tracker live; only ClusterStats benefits from the cache,
// because aggregating snapshots from every agent on every call would be
// wasteful while the cluster view barely changes within a short window.
//
// The cache holds one entry per (slowMultiplier, faultyThreshold) input
// pair. Multiple consumers — e.g. Hedger and Demoter — call ClusterStats
// with different parameters; without per-key caching they would
// invalidate each other on every call. Each cached entry has its own
// TTL window, so a stale entry for one key does not force a recompute
// for another.
type CachedAgentStatsTracker struct {
	inner    AgentStatsTracker
	cacheTTL time.Duration

	// refreshMu serializes the recompute path so only one goroutine
	// populates the cache for any key at a time. Readers go through
	// cacheMu (a short critical section) and never take refreshMu.
	refreshMu sync.Mutex

	cacheMu sync.RWMutex
	cache   map[clusterStatsCacheKey]clusterStatsCacheEntry
}

type clusterStatsCacheKey struct {
	slowMultiplier  float64
	faultyThreshold float64
}

type clusterStatsCacheEntry struct {
	stats ClusterStats
	// ok captures the inner tracker's quorum verdict. A cache entry with
	// ok=false serves as a "no-quorum sentinel" that suppresses gather
	// calls until cacheTTL elapses.
	ok         bool
	computedAt time.Time
}

// NewCachedAgentStatsTracker returns a wrapper around inner that caches
// ClusterStats for at most ttl per (slowMultiplier, faultyThreshold) key.
func NewCachedAgentStatsTracker(inner AgentStatsTracker, ttl time.Duration) *CachedAgentStatsTracker {
	return &CachedAgentStatsTracker{
		inner:    inner,
		cacheTTL: ttl,
		cache:    make(map[clusterStatsCacheKey]clusterStatsCacheEntry),
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
	w.cacheMu.Lock()
	clear(w.cache)
	w.cacheMu.Unlock()
}

func (w *CachedAgentStatsTracker) ClusterStats(now time.Time, slowMultiplier, faultyThreshold float64) (ClusterStats, bool) {
	key := clusterStatsCacheKey{slowMultiplier: slowMultiplier, faultyThreshold: faultyThreshold}
	if entry, hit := w.lookup(key, now); hit {
		return entry.stats, entry.ok
	}
	w.refreshMu.Lock()
	defer w.refreshMu.Unlock()
	// Double-check: another goroutine may have refreshed while we waited.
	if entry, hit := w.lookup(key, now); hit {
		return entry.stats, entry.ok
	}
	stats, ok := w.inner.ClusterStats(now, slowMultiplier, faultyThreshold)
	w.cacheMu.Lock()
	w.cache[key] = clusterStatsCacheEntry{
		stats:      stats,
		ok:         ok,
		computedAt: now,
	}
	w.cacheMu.Unlock()
	return stats, ok
}

// lookup returns the cached entry for key if it is still within cacheTTL.
func (w *CachedAgentStatsTracker) lookup(key clusterStatsCacheKey, now time.Time) (clusterStatsCacheEntry, bool) {
	w.cacheMu.RLock()
	entry, ok := w.cache[key]
	w.cacheMu.RUnlock()
	if !ok {
		return clusterStatsCacheEntry{}, false
	}
	if now.Sub(entry.computedAt) >= w.cacheTTL {
		return clusterStatsCacheEntry{}, false
	}
	return entry, true
}
