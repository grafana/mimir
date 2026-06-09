// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seedFullWindow fills all numStatsBuckets of an agent ending at nowNs.
// Each bucket holds successPerBucket successful requests at avgMs latency,
// plus errorsPerBucket errored requests.
func seedFullWindow(tr *AverageAgentStatsTracker, nodeID int32, nowNs, successPerBucket, avgMs, errorsPerBucket int64) {
	s := tr.getOrCreate(nodeID)
	s.bucketsMu.Lock()
	defer s.bucketsMu.Unlock()
	currentEpoch := (nowNs / bucketDurationNs) * bucketDurationNs
	for i := 0; i < numStatsBuckets; i++ {
		epochStart := currentEpoch - int64(numStatsBuckets-1-i)*bucketDurationNs
		idx := bucketIndex(epochStart)
		s.buckets[idx] = averageAgentStatsBucket{
			epochStart:             epochStart,
			successfulLatencySumMs: successPerBucket * avgMs,
			successfulLatencyCount: successPerBucket,
			faultyCount:            errorsPerBucket,
		}
	}
}

// seedSingleBucket fills the most recent bucket of an agent at nowNs with
// the given mix of successful and errored requests.
func seedSingleBucket(tr *AverageAgentStatsTracker, nodeID int32, nowNs, success, avgMs, errors int64) {
	s := tr.getOrCreate(nodeID)
	s.bucketsMu.Lock()
	defer s.bucketsMu.Unlock()
	currentEpoch := (nowNs / bucketDurationNs) * bucketDurationNs
	idx := bucketIndex(currentEpoch)
	s.buckets[idx] = averageAgentStatsBucket{
		epochStart:             currentEpoch,
		successfulLatencySumMs: success * avgMs,
		successfulLatencyCount: success,
		faultyCount:            errors,
	}
}

func TestAverageAgentStatsTracker_TrackAgentRequest(t *testing.T) {
	t.Run("first successful request lands in the current bucket", func(t *testing.T) {
		now := time.Unix(123, 0)
		tr := NewAverageAgentStatsTracker()

		tr.TrackAgentRequest(now, 1, 5*time.Millisecond, nil)

		snap := tr.stats[1].snapshot(now.UnixNano())
		assert.Equal(t, int64(1), snap.totalRequestsCount())
		assert.Equal(t, int64(1), snap.successfulRequestsLatencyCount)
		assert.Equal(t, int64(5), snap.successfulRequestsLatencySumMs)
		assert.Equal(t, int64(0), snap.faultyRequestsCount)
		assert.Equal(t, int64(1), snap.filledBuckets)
	})

	t.Run("successful requests in the same bucket accumulate", func(t *testing.T) {
		now := time.Unix(123, 0)
		tr := NewAverageAgentStatsTracker()

		for range 10 {
			tr.TrackAgentRequest(now, 1, time.Millisecond, nil)
		}
		snap := tr.stats[1].snapshot(now.UnixNano())
		assert.Equal(t, int64(10), snap.totalRequestsCount())
		assert.Equal(t, int64(10), snap.successfulRequestsLatencyCount)
		assert.Equal(t, int64(10), snap.successfulRequestsLatencySumMs)
		assert.Equal(t, int64(0), snap.faultyRequestsCount)
		assert.Equal(t, int64(1), snap.filledBuckets)
	})

	t.Run("requests distributed across buckets fill multiple slots", func(t *testing.T) {
		now := time.Unix(0, 0)
		tr := NewAverageAgentStatsTracker()

		for range numStatsBuckets {
			tr.TrackAgentRequest(now, 1, time.Millisecond, nil)
			now = now.Add(bucketDuration)
		}
		now = now.Add(-time.Nanosecond)
		snap := tr.stats[1].snapshot(now.UnixNano())
		assert.Equal(t, int64(numStatsBuckets), snap.totalRequestsCount())
		assert.Equal(t, int64(numStatsBuckets), snap.successfulRequestsLatencyCount)
		assert.Equal(t, int64(numStatsBuckets), snap.successfulRequestsLatencySumMs)
		assert.Equal(t, int64(0), snap.faultyRequestsCount)
		assert.Equal(t, int64(numStatsBuckets), snap.filledBuckets)
	})

	t.Run("buckets older than the window are ignored", func(t *testing.T) {
		now := time.Unix(0, 0)
		tr := NewAverageAgentStatsTracker()

		tr.TrackAgentRequest(now, 1, time.Millisecond, nil)
		now = now.Add(time.Hour)
		snap := tr.stats[1].snapshot(now.UnixNano())
		assert.Equal(t, int64(0), snap.totalRequestsCount())
		assert.Equal(t, int64(0), snap.successfulRequestsLatencyCount)
		assert.Equal(t, int64(0), snap.successfulRequestsLatencySumMs)
		assert.Equal(t, int64(0), snap.faultyRequestsCount)
		assert.Equal(t, int64(0), snap.filledBuckets)
	})

	t.Run("errored requests count toward totalRequestsCount only, not toward latency", func(t *testing.T) {
		now := time.Unix(0, 0)
		tr := NewAverageAgentStatsTracker()

		// 3 successful requests at 10ms, then 2 errors.
		for range 3 {
			tr.TrackAgentRequest(now, 1, 10*time.Millisecond, nil)
		}
		for range 2 {
			tr.TrackAgentRequest(now, 1, time.Millisecond, errors.New("fail"))
		}
		snap := tr.stats[1].snapshot(now.UnixNano())
		assert.Equal(t, int64(5), snap.totalRequestsCount())
		assert.Equal(t, int64(3), snap.successfulRequestsLatencyCount)
		assert.Equal(t, int64(30), snap.successfulRequestsLatencySumMs, "errored requests must not contribute to the latency sum")
		assert.Equal(t, int64(2), snap.faultyRequestsCount)
		assert.Equal(t, int64(1), snap.filledBuckets)
	})

	t.Run("context-cancelled calls are not recorded", func(t *testing.T) {
		now := time.Unix(0, 0)
		tr := NewAverageAgentStatsTracker()

		// One real success to make the bucket non-empty.
		tr.TrackAgentRequest(now, 1, time.Millisecond, nil)
		// Cancellations must not be recorded as either a success or a fault.
		tr.TrackAgentRequest(now, 1, time.Millisecond, context.Canceled)
		// Wrapped sentinel must also be skipped.
		tr.TrackAgentRequest(now, 1, time.Millisecond, fmt.Errorf("wrapped: %w", context.Canceled))

		snap := tr.stats[1].snapshot(now.UnixNano())
		assert.Equal(t, int64(1), snap.totalRequestsCount(), "only the real success should count")
		assert.Equal(t, int64(1), snap.successfulRequestsLatencyCount)
		assert.Equal(t, int64(1), snap.successfulRequestsLatencySumMs)
		assert.Equal(t, int64(0), snap.faultyRequestsCount)
		assert.Equal(t, int64(1), snap.filledBuckets)
	})

	t.Run("deadline-exceeded calls count as faulty", func(t *testing.T) {
		now := time.Unix(0, 0)
		tr := NewAverageAgentStatsTracker()

		// Two successes plus three deadline-exceeded calls (one wrapped).
		// Deadlines reflect real agent/network slowness and must show up
		// in faultyRequestsCount.
		for range 2 {
			tr.TrackAgentRequest(now, 1, time.Millisecond, nil)
		}
		tr.TrackAgentRequest(now, 1, time.Millisecond, context.DeadlineExceeded)
		tr.TrackAgentRequest(now, 1, time.Millisecond, context.DeadlineExceeded)
		tr.TrackAgentRequest(now, 1, time.Millisecond, fmt.Errorf("wrapped: %w", context.DeadlineExceeded))

		snap := tr.stats[1].snapshot(now.UnixNano())
		assert.Equal(t, int64(5), snap.totalRequestsCount())
		assert.Equal(t, int64(2), snap.successfulRequestsLatencyCount)
		assert.Equal(t, int64(2), snap.successfulRequestsLatencySumMs)
		assert.Equal(t, int64(3), snap.faultyRequestsCount, "deadline-exceeded calls must be tallied as faulty")
		assert.Equal(t, int64(1), snap.filledBuckets)
	})

	t.Run("rotation overwrites stale ring slots without leaking old data", func(t *testing.T) {
		now := time.Unix(0, 0)
		tr := NewAverageAgentStatsTracker()

		for range 10 {
			tr.TrackAgentRequest(now, 1, time.Millisecond, nil)
		}
		now = now.Add(time.Duration(numStatsBuckets) * bucketDuration)
		tr.TrackAgentRequest(now, 1, 9*time.Millisecond, nil)

		snap := tr.stats[1].snapshot(now.UnixNano())
		assert.Equal(t, int64(1), snap.totalRequestsCount(), "old requests must be evicted on rotation")
		assert.Equal(t, int64(1), snap.successfulRequestsLatencyCount)
		assert.Equal(t, int64(9), snap.successfulRequestsLatencySumMs)
		assert.Equal(t, int64(0), snap.faultyRequestsCount)
		assert.Equal(t, int64(1), snap.filledBuckets)
	})
}

func TestAverageAgentStatsTracker_PurgeAgents(t *testing.T) {
	tests := map[string]struct {
		recordNodeIDs []int32
		purgeNodeIDs  []int32
		wantPresent   []int32
	}{
		"purge single agent": {
			recordNodeIDs: []int32{1, 2, 3}, purgeNodeIDs: []int32{2}, wantPresent: []int32{1, 3},
		},
		"purge multiple agents": {
			recordNodeIDs: []int32{1, 2, 3, 4}, purgeNodeIDs: []int32{1, 3}, wantPresent: []int32{2, 4},
		},
		"purge unknown nodeID is a no-op": {
			recordNodeIDs: []int32{1}, purgeNodeIDs: []int32{99}, wantPresent: []int32{1},
		},
		"purge all agents": {
			recordNodeIDs: []int32{1, 2}, purgeNodeIDs: []int32{1, 2}, wantPresent: []int32{},
		},
		"purge empty list is a no-op": {
			recordNodeIDs: []int32{1}, purgeNodeIDs: nil, wantPresent: []int32{1},
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			tr := NewAverageAgentStatsTracker()
			now := time.Now()
			for _, id := range tc.recordNodeIDs {
				tr.TrackAgentRequest(now, id, time.Millisecond, nil)
			}
			tr.PurgeAgents(tc.purgeNodeIDs)
			tr.statsMu.RLock()
			defer tr.statsMu.RUnlock()
			assert.Len(t, tr.stats, len(tc.wantPresent))
			for _, id := range tc.wantPresent {
				assert.Contains(t, tr.stats, id)
			}
		})
	}
}

func TestAverageAgentStatsTracker_AgentStats(t *testing.T) {
	now := time.Unix(3600, 0)
	tr := NewAverageAgentStatsTracker()
	nowNs := now.UnixNano()

	t.Run("requests concentrated in too few buckets return no stats", func(t *testing.T) {
		seedSingleBucket(tr, 2, nowNs, 100, 50, 0)
		_, ok := tr.AgentStats(now, 2)
		assert.False(t, ok)
	})

	t.Run("fully populated window returns latency and a zero error rate", func(t *testing.T) {
		seedFullWindow(tr, 3, nowNs, 5, 50, 0)
		stats, ok := tr.AgentStats(now, 3)
		require.True(t, ok)
		assert.Equal(t, 50*time.Millisecond, stats.Latency)
		assert.Equal(t, 0.0, stats.ErrorRate)
	})

	t.Run("agent with errors reports error rate proportional to totalCount", func(t *testing.T) {
		// Per bucket: 3 success + 1 error → ErrorRate = 6/24 = 0.25, latency over successes only.
		seedFullWindow(tr, 4, nowNs, 3, 100, 1)
		stats, ok := tr.AgentStats(now, 4)
		require.True(t, ok)
		assert.Equal(t, 100*time.Millisecond, stats.Latency)
		assert.InDelta(t, 0.25, stats.ErrorRate, 1e-9)
	})

	t.Run("all-error agent reports zero latency and 100 percent error rate", func(t *testing.T) {
		seedFullWindow(tr, 5, nowNs, 0, 0, 5)
		stats, ok := tr.AgentStats(now, 5)
		require.True(t, ok)
		assert.Equal(t, time.Duration(0), stats.Latency)
		assert.Equal(t, 1.0, stats.ErrorRate)
	})

	t.Run("unknown agent returns no stats", func(t *testing.T) {
		_, ok := tr.AgentStats(now, 999)
		assert.False(t, ok)
	})

	t.Run("ErrorRate is the raw faulty/total ratio with RequestCount exposed", func(t *testing.T) {
		// 1 error per bucket × 6 buckets = 6 errors / 6 requests = 100 %.
		// AgentStats reports the raw rate; callers gate small-sample noise
		// against RequestCount themselves.
		seedFullWindow(tr, 6, nowNs, 0, 0, 1)
		stats, ok := tr.AgentStats(now, 6)
		require.True(t, ok)
		assert.Equal(t, 1.0, stats.ErrorRate)
		assert.Equal(t, int64(6), stats.RequestCount)
	})
}

func TestAverageAgentStatsTracker_ClusterStats(t *testing.T) {
	now := time.Unix(3600, 0)
	tr := NewAverageAgentStatsTracker()
	nowNs := now.UnixNano()

	t.Run("no agents returns no stats", func(t *testing.T) {
		_, ok := tr.ClusterStats(now, 2.0, 0.05)
		assert.False(t, ok)
	})

	t.Run("computes baselines, threshold, slow fraction, faulty fraction", func(t *testing.T) {
		// Latency: 50, 1, 1 → BaselineLatency = 17 ms; SlowThreshold = 34 ms; SlowFraction = 1/3 (id=1).
		// Error rates: all zero → BaselineErrorRate = 0, FaultyFraction = 0.
		seedFullWindow(tr, 1, nowNs, 20, 50, 0)
		seedFullWindow(tr, 2, nowNs, 20, 1, 0)
		seedFullWindow(tr, 3, nowNs, 20, 1, 0)

		cluster, ok := tr.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		assert.Equal(t, 17*time.Millisecond, cluster.BaselineLatency)
		assert.Equal(t, 34*time.Millisecond, cluster.SlowThreshold)
		assert.InDelta(t, 1.0/3.0, cluster.SlowFraction, 1e-9)
		assert.Equal(t, 0.0, cluster.BaselineErrorRate)
		assert.Equal(t, 0.05, cluster.FaultyThreshold)
		assert.Equal(t, 0.0, cluster.FaultyFraction)
	})

	t.Run("an outlier shows up in SlowFraction", func(t *testing.T) {
		tr := NewAverageAgentStatsTracker()
		// 6 healthy + 1 outlier on latency.
		for id := int32(1); id <= 6; id++ {
			seedFullWindow(tr, id, nowNs, 20, 10, 0)
		}
		seedFullWindow(tr, 7, nowNs, 20, 100, 0)

		cluster, ok := tr.ClusterStats(now, 1.5, 0.05)
		require.True(t, ok)
		// Baseline = (10*6 + 100) / 7 ≈ 22 ms; threshold ≈ 33 ms.
		assert.InDelta(t, 1.0/7.0, cluster.SlowFraction, 1e-9)
	})

	t.Run("FaultyFraction reflects agents above the absolute threshold", func(t *testing.T) {
		tr := NewAverageAgentStatsTracker()
		// 6 agents at 0 % errors, 1 agent at 50 % errors.
		for id := int32(1); id <= 6; id++ {
			seedFullWindow(tr, id, nowNs, 20, 10, 0)
		}
		// 10 success + 10 errors per bucket → ErrorRate = 0.5 in window.
		seedFullWindow(tr, 7, nowNs, 10, 10, 10)

		cluster, ok := tr.ClusterStats(now, 1.5, 0.05)
		require.True(t, ok)
		assert.InDelta(t, 1.0/7.0, cluster.FaultyFraction, 1e-9)
		// BaselineErrorRate = (0*6 + 0.5)/7 ≈ 0.0714.
		assert.InDelta(t, 0.5/7.0, cluster.BaselineErrorRate, 1e-9)
	})

	t.Run("all-error agent contributes to FaultyFraction but not to BaselineLatency", func(t *testing.T) {
		tr := NewAverageAgentStatsTracker()
		seedFullWindow(tr, 1, nowNs, 20, 10, 0)
		seedFullWindow(tr, 2, nowNs, 20, 10, 0)
		seedFullWindow(tr, 3, nowNs, 0, 0, 20) // all errors

		cluster, ok := tr.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		// BaselineLatency averages over agents with successful requests only:
		// (10 + 10) / 2 = 10 ms.
		assert.Equal(t, 10*time.Millisecond, cluster.BaselineLatency)
		// FaultyFraction over all qualifying agents: 1/3.
		assert.InDelta(t, 1.0/3.0, cluster.FaultyFraction, 1e-9)
	})

	t.Run("low cluster volume: a low-request faulty agent is still counted", func(t *testing.T) {
		// Backpressure: every agent is under base, so the average drops below
		// base and the gate relaxes — the faulty agent is counted even though
		// it has only 6 requests, the case the fixed base=20 gate would miss.
		// 5 agents total: 1 faulty (all errors, no successes) + 4 healthy. The
		// all-errors agent still counts as observed (totalRequestsCount sums
		// successes and failures), so the denominator is 5, not 4.
		tr := NewAverageAgentStatsTracker()
		seedFullWindow(tr, 1, nowNs, 0, 0, 1) // faulty: 6 errored requests, 0 successful
		for id := int32(2); id <= 5; id++ {
			seedFullWindow(tr, id, nowNs, 1, 10, 0) // healthy, low volume (6 requests)
		}

		cluster, ok := tr.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		assert.Equal(t, int64(6), cluster.AvgRequestsPerAgent)
		assert.InDelta(t, 1.0/5.0, cluster.FaultyFraction, 1e-9)
		assert.Equal(t, int64(5), cluster.FaultyContributorsCount)
	})

	t.Run("high cluster volume: a low-request faulty agent is gated out", func(t *testing.T) {
		// Busy cluster: the average is well above base, so the gate holds at
		// base and the low-request faulty agent is NOT counted — its sparseness
		// is the anomaly, not a fault.
		tr := NewAverageAgentStatsTracker()
		seedFullWindow(tr, 1, nowNs, 0, 0, 1) // faulty but low volume (6 requests)
		for id := int32(2); id <= 5; id++ {
			seedFullWindow(tr, id, nowNs, 20, 10, 0) // healthy, high volume (120 requests)
		}

		cluster, ok := tr.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		assert.GreaterOrEqual(t, cluster.AvgRequestsPerAgent, int64(20))
		assert.Equal(t, 0.0, cluster.FaultyFraction)
		assert.Equal(t, int64(5), cluster.FaultyContributorsCount)
	})

	t.Run("idle agents are excluded from the denominator (observed, not known)", func(t *testing.T) {
		// A known but idle agent (window aged out) must not inflate the
		// FaultyFraction denominator: the fraction is over agents we currently
		// have a signal for, not every agent the tracker has ever seen.
		tr := NewAverageAgentStatsTracker()
		past := now.Add(-30 * time.Minute)
		seedFullWindow(tr, 99, past.UnixNano(), 20, 10, 0) // known, but aged out of the window
		seedFullWindow(tr, 1, nowNs, 0, 0, 1)              // faulty, low volume
		seedFullWindow(tr, 2, nowNs, 1, 10, 0)             // healthy
		seedFullWindow(tr, 3, nowNs, 1, 10, 0)             // healthy

		cluster, ok := tr.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		// 3 observed agents (id 99 contributes nothing) → 1/3, not 1/4.
		assert.Equal(t, int64(3), cluster.FaultyContributorsCount)
		assert.InDelta(t, 1.0/3.0, cluster.FaultyFraction, 1e-9)
	})

	t.Run("cluster-wide failure: most observed agents faulty drives the fraction high", func(t *testing.T) {
		// When the failure is cluster-wide (2 of 3 observed agents at 100%
		// errors under low volume), FaultyFraction must climb above the
		// suppression floor so the demoter/hedger stand down rather than
		// demote most of the pool onto the survivors.
		tr := NewAverageAgentStatsTracker()
		seedFullWindow(tr, 1, nowNs, 0, 0, 1)  // faulty
		seedFullWindow(tr, 2, nowNs, 0, 0, 1)  // faulty
		seedFullWindow(tr, 3, nowNs, 1, 10, 0) // healthy

		cluster, ok := tr.ClusterStats(now, 2.0, 0.05)
		require.True(t, ok)
		assert.InDelta(t, 2.0/3.0, cluster.FaultyFraction, 1e-9)
	})
}

// TestAverageAgentStatsTracker_Scenarios collects specific scenarios for
// which we want to verify the tracker's behaviour end-to-end. Each
// subtest captures one situation that motivated a design decision (for
// example, the failure modes of a plain EMA that the bucketed window is
// designed to handle).
func TestAverageAgentStatsTracker_Scenarios(t *testing.T) {
	const (
		primaryID       = int32(1)
		slowMultiplier  = 2.0
		faultyThreshold = 0.05
	)

	now := time.Unix(3600, 0)
	nowNs := now.UnixNano()

	t.Run("idle then burst: stale data ages out, new burst is not enough to mark slow", func(t *testing.T) {
		past := now.Add(-30 * time.Minute)
		tr := NewAverageAgentStatsTracker()

		seedFullWindow(tr, primaryID, past.UnixNano(), 20, 1, 0)
		seedFullWindow(tr, 2, nowNs, 20, 1, 0)
		seedFullWindow(tr, 3, nowNs, 20, 1, 0)

		for range 100 {
			tr.TrackAgentRequest(now, primaryID, 50*time.Millisecond, nil)
		}

		_, ok := tr.AgentStats(now, primaryID)
		assert.False(t, ok)
	})

	t.Run("high-throughput burst masquerading as long observation: spread gate blocks", func(t *testing.T) {
		tr := NewAverageAgentStatsTracker()

		seedFullWindow(tr, 2, nowNs, 20, 1, 0)
		seedFullWindow(tr, 3, nowNs, 20, 1, 0)
		seedSingleBucket(tr, primaryID, nowNs, 1000, 50, 0)

		_, ok := tr.AgentStats(now, primaryID)
		assert.False(t, ok, "1000 requests in one bucket fail the spread gate regardless of count")
	})

	t.Run("sustained slowness across the window: ClusterStats reports primary as slow", func(t *testing.T) {
		tr := NewAverageAgentStatsTracker()

		seedFullWindow(tr, 2, nowNs, 20, 1, 0)
		seedFullWindow(tr, 3, nowNs, 20, 1, 0)
		seedFullWindow(tr, primaryID, nowNs, 1000/numStatsBuckets, 50, 0)

		primary, ok := tr.AgentStats(now, primaryID)
		require.True(t, ok)
		cluster, ok := tr.ClusterStats(now, slowMultiplier, faultyThreshold)
		require.True(t, ok)
		assert.Greater(t, primary.Latency, cluster.SlowThreshold)
	})
}

func TestAverageAgentStatsTracker_PurgeAgentsConcurrentWithTrackAgentRequest(t *testing.T) {
	tr := NewAverageAgentStatsTracker()
	now := time.Now()
	for id := int32(0); id < 8; id++ {
		tr.TrackAgentRequest(now, id, time.Millisecond, nil)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int32(0); ; i++ {
				select {
				case <-stop:
					return
				default:
					tr.TrackAgentRequest(time.Now(), i%8, time.Millisecond, nil)
				}
			}
		}()
	}
	for range 100 {
		tr.PurgeAgents([]int32{int32(time.Now().UnixNano() % 8)})
	}
	close(stop)
	wg.Wait()
}

// TestAverageAgentStatsTracker_ConcurrentRotation drives multiple writers
// across bucket boundaries while a Purge runs, exercising the rotation
// path under -race. An atomic-int64 clock is used here so writers and the
// time-advancer can race on a single shared "now" without locking.
func TestAverageAgentStatsTracker_ConcurrentRotation(t *testing.T) {
	tr := NewAverageAgentStatsTracker()
	var nowNs atomic.Int64
	nowNs.Store(time.Unix(3600, 0).UnixNano())
	now := func() time.Time { return time.Unix(0, nowNs.Load()) }

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
					tr.TrackAgentRequest(now(), int32(i%8), time.Millisecond, nil)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			select {
			case <-stop:
				return
			default:
				nowNs.Add((bucketDuration / 4).Nanoseconds())
			}
		}
	}()
	for range 50 {
		tr.PurgeAgents([]int32{int32(time.Now().UnixNano() % 8)})
	}
	close(stop)
	wg.Wait()
}

func BenchmarkAverageAgentStatsTracker_TrackAgentRequest(b *testing.B) {
	tr := NewAverageAgentStatsTracker()
	b.ReportAllocs()
	for i := range b.N {
		tr.TrackAgentRequest(time.Now(), int32(i%8), time.Millisecond, nil)
	}
}

func BenchmarkAverageAgentStatsTracker_ClusterStats(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run(b.Name()+"/agents="+strconv.Itoa(n), func(b *testing.B) {
			now := time.Unix(0, 0).Add(time.Duration(numStatsBuckets-1) * bucketDuration)
			tr := NewAverageAgentStatsTracker()
			nowNs := now.UnixNano()

			for i := range n {
				seedFullWindow(tr, int32(i), nowNs, 20, 1, 0)
			}
			b.ResetTimer()
			b.ReportAllocs()
			for range b.N {
				_, _ = tr.ClusterStats(now, 2.0, 0.05)
			}
		})
	}
}

func TestErrorRateMinRequests(t *testing.T) {
	cases := map[string]struct {
		faultyThreshold     float64
		avgRequestsPerAgent int64
		expected            int64
	}{
		"disabled when threshold is zero":               {0, 100, 0},
		"busy cluster holds at base":                    {0.2, 10, 5},               // avg >= base(5)
		"average exactly at base holds":                 {0.2, 5, 5},                // avg == base(5)
		"starved cluster relaxes to minFilledBuckets":   {0.2, 3, minFilledBuckets}, // avg < base(5)
		"starved cluster with zero average":             {0.2, 0, minFilledBuckets}, //
		"relaxed floor never exceeds base":              {1.0, 0, 1},                // base(1) < minFilledBuckets
		"busy cluster holds at base for high threshold": {0.5, 5, 2},                // avg >= base(2)
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := errorRateMinRequests(ClusterStats{FaultyThreshold: tc.faultyThreshold, AvgRequestsPerAgent: tc.avgRequestsPerAgent})
			assert.Equal(t, tc.expected, got)
		})
	}
}
