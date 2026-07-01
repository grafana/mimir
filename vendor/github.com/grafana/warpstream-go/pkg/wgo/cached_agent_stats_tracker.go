package wgo

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
// pair. Each cached entry has its own TTL window, so a stale entry for one
// key does not force a recompute for another.
type CachedAgentStatsTracker struct {
	inner    AgentStatsTracker
	cacheTTL time.Duration

	// refreshMu serializes the recompute path so only one goroutine
	// recomputes the cache at a time (a single mutex across all keys).
	refreshMu sync.Mutex

	cacheMu sync.RWMutex
	cache   map[clusterStatsCacheKey]clusterStatsCacheEntry
	// cachePurgeGen increments on every PurgeAgents (under cacheMu). ClusterStats
	// snapshots it before computing and skips the cache write if it changed,
	// so a snapshot computed before a purge can't repopulate the cache with
	// agents the purge just removed.
	cachePurgeGen uint64
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
	w.cachePurgeGen++
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

	// Fetch the cache purge generation before recomputing the cluster stats.
	w.cacheMu.RLock()
	gen := w.cachePurgeGen
	w.cacheMu.RUnlock()

	stats, ok := w.inner.ClusterStats(now, slowMultiplier, faultyThreshold)

	// Drop the result if a purge raced the compute: the snapshot may
	// reflect agents the purge removed, so caching it would resurrect them
	// for up to cacheTTL. The caller still gets this (transient) view; the
	// next call recomputes fresh.
	w.cacheMu.Lock()
	if w.cachePurgeGen == gen {
		w.cache[key] = clusterStatsCacheEntry{
			stats:      stats,
			ok:         ok,
			computedAt: now,
		}
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
