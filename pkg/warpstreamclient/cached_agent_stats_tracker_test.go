// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCachedAgentStatsTracker_AgentStats(t *testing.T) {
	t.Run("is not cached", func(t *testing.T) {
		now := time.Unix(3600, 0)
		inner := NewAverageAgentStatsTracker()
		// Long TTL — if AgentStats were cached, the second call below
		// would return the old value rather than reflecting the mutation.
		w := NewCachedAgentStatsTracker(inner, time.Hour)
		nowNs := now.UnixNano()

		seedFullWindow(inner, 1, nowNs, 20, 10, 0)
		stats, ok := w.AgentStats(now, 1)
		require.True(t, ok)
		assert.Equal(t, 10*time.Millisecond, stats.Latency)

		// Mutate the inner state without advancing the clock.
		seedFullWindow(inner, 1, nowNs, 20, 50, 0)
		stats, ok = w.AgentStats(now, 1)
		require.True(t, ok)
		assert.Equal(t, 50*time.Millisecond, stats.Latency)
	})
}

func TestCachedAgentStatsTracker_ClusterStats(t *testing.T) {
	t.Run("caches the gather within TTL and refreshes after it elapses", func(t *testing.T) {
		now := time.Unix(3600, 0)
		inner := NewAverageAgentStatsTracker()
		w := NewCachedAgentStatsTracker(inner, time.Second)
		nowNs := now.UnixNano()

		seedFullWindow(inner, 1, nowNs, 20, 10, 0)
		seedFullWindow(inner, 2, nowNs, 20, 10, 0)

		c1, ok := w.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)

		// Mutate the inner state in a way that *would* affect the gather, but
		// does NOT advance the clock: the cache should still serve the previous
		// snapshot until the TTL elapses.
		seedFullWindow(inner, 3, nowNs, 20, 100, 0)
		c2, ok := w.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		assert.Equal(t, c1.BaselineLatency, c2.BaselineLatency)

		// Advance past the TTL — the cache must refresh and pick up agent 3.
		now = now.Add(2 * time.Second)
		c3, ok := w.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		assert.NotEqual(t, c1.BaselineLatency, c3.BaselineLatency)
	})

	t.Run("no-quorum sentinel keeps returning no stats until TTL elapses", func(t *testing.T) {
		now := time.Unix(3600, 0)
		inner := NewAverageAgentStatsTracker()
		w := NewCachedAgentStatsTracker(inner, time.Second)

		// First call: no qualifying agents yet, returns no stats and
		// sets the no-quorum sentinel.
		_, ok := w.ClusterStats(now, 2.0, 0.05)
		assert.False(t, ok)

		// Now there's data — but within cacheTTL the no-quorum sentinel
		// keeps suppressing the gather, so we still get no stats.
		seedFullWindow(inner, 1, now.UnixNano(), 20, 10, 0)
		seedFullWindow(inner, 2, now.UnixNano(), 20, 10, 0)
		_, ok = w.ClusterStats(now, 2.0, 0.05)
		assert.False(t, ok)

		// Once cacheTTL has elapsed the sentinel expires, the gather runs,
		// and the cached cluster view becomes available.
		now = now.Add(2 * time.Second)
		stats, ok := w.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		assert.Equal(t, 10*time.Millisecond, stats.BaselineLatency)
	})

	t.Run("distinct parameter pairs are cached independently", func(t *testing.T) {
		// Two consumers (e.g. Hedger + Demoter) calling ClusterStats with
		// different (slowMultiplier, faultyThreshold) pairs must each
		// have their own cached entry. Otherwise every Hedger call would
		// invalidate the Demoter's entry and vice versa, defeating the
		// cache.
		now := time.Unix(3600, 0)
		raw := NewAverageAgentStatsTracker()
		seedFullWindow(raw, 1, now.UnixNano(), 20, 10, 0)
		seedFullWindow(raw, 2, now.UnixNano(), 20, 10, 0)
		inner := &countingTracker{inner: raw}
		w := NewCachedAgentStatsTracker(inner, time.Second)

		// First call for params A → recompute. Recompute counter = 1.
		_, ok := w.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		require.Equal(t, 1, inner.clusterStatsCalls())

		// First call for params B → recompute (different cache key).
		// Recompute counter = 2.
		_, ok = w.ClusterStats(now, 3.0, 0.10)
		require.True(t, ok)
		require.Equal(t, 2, inner.clusterStatsCalls())

		// Repeat calls for both keys within the TTL: both should hit.
		_, _ = w.ClusterStats(now, 2.0, 0.05)
		_, _ = w.ClusterStats(now, 3.0, 0.10)
		_, _ = w.ClusterStats(now, 2.0, 0.05)
		_, _ = w.ClusterStats(now, 3.0, 0.10)
		assert.Equal(t, 2, inner.clusterStatsCalls())
	})
}

// countingTracker wraps an AgentStatsTracker and counts ClusterStats calls
// for assertions about cache hit/miss behaviour.
type countingTracker struct {
	inner AgentStatsTracker
	mu    sync.Mutex
	calls int
}

func (c *countingTracker) TrackAgentRequest(now time.Time, nodeID int32, latency time.Duration, err error) {
	c.inner.TrackAgentRequest(now, nodeID, latency, err)
}
func (c *countingTracker) AgentStats(now time.Time, nodeID int32) (AgentStats, bool) {
	return c.inner.AgentStats(now, nodeID)
}
func (c *countingTracker) ClusterStats(now time.Time, slowMultiplier, faultyThreshold float64) (ClusterStats, bool) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.inner.ClusterStats(now, slowMultiplier, faultyThreshold)
}
func (c *countingTracker) PurgeAgents(nodeIDs []int32) { c.inner.PurgeAgents(nodeIDs) }
func (c *countingTracker) clusterStatsCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func TestCachedAgentStatsTracker_PurgeAgents(t *testing.T) {
	t.Run("should invalidate cache", func(t *testing.T) {
		now := time.Unix(3600, 0)
		inner := NewAverageAgentStatsTracker()
		w := NewCachedAgentStatsTracker(inner, time.Hour) // long TTL — must rely on Purge invalidation
		nowNs := now.UnixNano()

		seedFullWindow(inner, 1, nowNs, 20, 10, 0)
		seedFullWindow(inner, 2, nowNs, 20, 10, 0)
		seedFullWindow(inner, 3, nowNs, 20, 100, 0)

		c1, ok := w.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		// Baseline = (10 + 10 + 100) / 3 = 40
		assert.Equal(t, 40*time.Millisecond, c1.BaselineLatency)

		// Purge agent 3. The cache must be invalidated so the next call recomputes.
		w.PurgeAgents([]int32{3})

		c2, ok := w.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		// Without agent 3, baseline = (10 + 10) / 2 = 10
		assert.Equal(t, 10*time.Millisecond, c2.BaselineLatency)
	})
}

func BenchmarkCachedAgentStatsTracker_ClusterStats(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(b.Name()+"/agents="+strconv.Itoa(n), func(b *testing.B) {
			now := time.Unix(0, 0).Add(time.Duration(numStatsBuckets-1) * bucketDuration)
			inner := NewAverageAgentStatsTracker()
			w := NewCachedAgentStatsTracker(inner, time.Second)
			nowNs := now.UnixNano()

			for i := range n {
				seedFullWindow(inner, int32(i), nowNs, 20, 1, 0)
			}
			// Warm the cache so the benchmark measures the steady-state path.
			_, _ = w.ClusterStats(now, 2.0, 0.05)
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				_, _ = w.ClusterStats(now, 2.0, 0.05)
			}
		})
	}
}
