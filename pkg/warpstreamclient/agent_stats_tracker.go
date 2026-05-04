// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// Per-agent observations accumulate into a sliding window of fixed-duration
// buckets. The window covers the last numStatsBuckets * bucketDuration of
// activity; requests outside the window are discarded by bucket rotation.
//
// Bucketing — rather than an EMA — gives the latency / error-rate queries
// time-aware statistics: a burst of slow or errored requests concentrated
// in one bucket cannot trigger hedging because the bucket-spread gate
// requires sustained activity across multiple buckets. The window also
// handles long quiet periods correctly: after silence longer than the
// window, all buckets age out, so the next burst of activity restarts
// observation from scratch.
//
// The constants below define "is the data reliable" — the tracker's domain.
// They are deliberately not configurable: callers describe their *policy*
// via SlowMultiplier / MaxSlowFraction / FaultyThreshold / MaxFaultyFraction
// in HedgerConfig, while the tracker owns its own reliability gates.
//
// There is no fixed minimum request count: low-throughput agents are
// admitted as long as their requests are spread across enough buckets.
// For the cluster error-rate signal a request-count floor is derived
// per-call from the FaultyThreshold passed to ClusterStats (see
// errorRateMinRequests): the smallest non-zero observable error rate is
// 1/N, so we suppress noise from low-N agents only when it could pretend
// to be above the configured threshold. AgentStats itself reports the
// raw ErrorRate plus a RequestCount so the caller can apply the same
// gate against its own threshold.
const (
	numStatsBuckets  = 6
	bucketDuration   = 10 * time.Second
	bucketDurationNs = int64(bucketDuration)

	// minFilledBuckets is the number of distinct time buckets that must
	// hold at least one request before an agent's stats are considered
	// representative. With 6 buckets of 10s, requiring 3 means requests
	// must cover at least 30 seconds of wall-clock activity.
	//
	// The same gate is used for AgentStats (per-agent decisions) and for
	// admitting an agent to the cluster baseline computation: an agent
	// that contributes to the baseline is one whose own stats are also
	// available.
	minFilledBuckets = 3
)

// AgentStatsTracker is implemented by components that track per-agent and
// cluster-wide request stats (latency and error rate).
type AgentStatsTracker interface {
	// TrackAgentRequest records the outcome of one request for nodeID: the
	// observed latency and whether the call errored. Errored requests
	// contribute only to the error rate; the latency stat is computed
	// from successful requests only. Calls cancelled by their context
	// (err is context.Canceled) are not recorded — that outcome reflects
	// the caller's intent, not the agent's health. context.DeadlineExceeded
	// is treated as a real failure: a timeout is an indication the agent
	// (or the network path to it) was too slow, so it counts toward the
	// error rate.
	TrackAgentRequest(now time.Time, nodeID int32, latency time.Duration, err error)

	// PurgeAgents removes all tracked state for the given NodeIDs.
	PurgeAgents(nodeIDs []int32)

	// AgentStats returns the agent's stats over the current observation
	// window. Returns false when the agent has not yet collected requests
	// spread across enough time buckets to be representative.
	AgentStats(now time.Time, nodeID int32) (AgentStats, bool)

	// ClusterStats returns the cluster-wide stats view. slowMultiplier scales
	// the baseline latency into the slow threshold; faultyThreshold is the
	// absolute error-rate threshold above which an agent is considered faulty.
	// Returns false when fewer than a quorum of qualifying agents have
	// reported data — without enough agents the cluster view would not be
	// statistically meaningful.
	ClusterStats(now time.Time, slowMultiplier float64, faultyThreshold float64) (ClusterStats, bool)
}

// AgentStats summarises one agent's observations over the current window.
type AgentStats struct {
	// Latency summarises the latency observed across successful requests
	// in the window. Zero if no successful request is present in the
	// window — in that case the caller should rely on ErrorRate to
	// interpret the agent's state.
	Latency time.Duration

	// ErrorRate is the fraction of requests within the window that errored.
	// Reported as a raw ratio: callers comparing it against a threshold
	// should also consult RequestCount to decide whether the sample is
	// large enough to be trustworthy (the smallest non-zero rate is
	// 1/RequestCount).
	ErrorRate float64

	// RequestCount is the total number of requests observed for the agent
	// in the window (successful + faulty). Lets the caller gate decisions
	// on whether the sample is large enough to be statistically meaningful.
	RequestCount int64
}

// ClusterStats summarises cluster-wide stats.
type ClusterStats struct {
	// BaselineLatency is the typical latency observed across the cluster's
	// agents. Zero when no agent has any successful request in the window;
	// the rest of the cluster view (BaselineErrorRate, FaultyFraction)
	// remains meaningful in that case, since they are computed from total
	// request counts rather than from successful-only ones.
	BaselineLatency time.Duration

	// SlowThreshold is the latency above which an agent is considered slow.
	SlowThreshold time.Duration

	// SlowFraction is the fraction of agents whose latency exceeds SlowThreshold.
	SlowFraction float64

	// SlowContributorsCount is the number of agents that contributed to
	// SlowFraction (i.e. agents with at least one successful request).
	// Lets the caller scale fraction-based decisions to cluster size.
	SlowContributorsCount int64

	// BaselineErrorRate is the typical error rate observed across the cluster's agents.
	BaselineErrorRate float64

	// FaultyThreshold is the error rate above which an agent is considered faulty.
	FaultyThreshold float64

	// FaultyFraction is the fraction of agents whose error rate exceeds FaultyThreshold.
	FaultyFraction float64

	// FaultyContributorsCount is the number of agents that contributed to
	// FaultyFraction (i.e. agents whose request count is large enough for
	// the error rate to be statistically meaningful). Lets the caller scale
	// fraction-based decisions to cluster size.
	FaultyContributorsCount int64
}

// AverageAgentStatsTracker is an AgentStatsTracker that averages observations
// over a sliding window of fixed-duration buckets. The reliability gates
// that define when AgentStats / ClusterStats are willing to return a value
// (request count and bucket spread) are package constants, not configurable.
type AverageAgentStatsTracker struct {
	statsMu sync.RWMutex
	stats   map[int32]*averageAgentStats // nodeID → stats; entries added lazily on first TrackAgentRequest
}

// newAverageAgentStatsTracker returns a tracker with no recorded stats.
func newAverageAgentStatsTracker() *AverageAgentStatsTracker {
	return &AverageAgentStatsTracker{
		stats: make(map[int32]*averageAgentStats),
	}
}

func (t *AverageAgentStatsTracker) TrackAgentRequest(now time.Time, nodeID int32, latency time.Duration, err error) {
	if errors.Is(err, context.Canceled) {
		// Caller cut the call short; the outcome reflects caller intent,
		// not agent health. context.DeadlineExceeded is intentionally not
		// skipped — a timeout is real evidence the agent or the network
		// path to it was too slow, and counts as a faulty request.
		return
	}
	s := t.getOrCreate(nodeID)
	nowNs := now.UnixNano()
	epochStart := (nowNs / bucketDurationNs) * bucketDurationNs
	idx := bucketIndex(nowNs)

	s.bucketsMu.Lock()
	defer s.bucketsMu.Unlock()
	b := &s.buckets[idx]
	if b.epochStart != epochStart {
		b.epochStart = epochStart
		b.successfulLatencySumMs = 0
		b.successfulLatencyCount = 0
		b.faultyCount = 0
	}
	if err == nil {
		b.successfulLatencySumMs += latency.Milliseconds()
		b.successfulLatencyCount++
	} else {
		b.faultyCount++
	}
}

func (t *AverageAgentStatsTracker) PurgeAgents(nodeIDs []int32) {
	if len(nodeIDs) == 0 {
		return
	}
	t.statsMu.Lock()
	defer t.statsMu.Unlock()
	for _, id := range nodeIDs {
		delete(t.stats, id)
	}
}

func (t *AverageAgentStatsTracker) AgentStats(now time.Time, nodeID int32) (AgentStats, bool) {
	t.statsMu.RLock()
	s, ok := t.stats[nodeID]
	t.statsMu.RUnlock()
	if !ok {
		return AgentStats{}, false
	}
	snap := s.snapshot(now.UnixNano())
	if snap.filledBuckets < minFilledBuckets {
		return AgentStats{}, false
	}
	return snap.toAgentStats(), true
}

// ClusterStats walks every agent and aggregates the cluster-wide view in
// two passes: the first computes the baseline latency and the (request-
// weighted) baseline error rate; the second counts agents above the slow
// and faulty thresholds. Returns false when there is no quorum of
// qualifying agents.
//
// SlowFraction is the fraction of agents (with at least one successful
// request, since latency is undefined otherwise) whose latency exceeds
// the slow threshold. FaultyFraction excludes agents whose request count
// is too small to distinguish their error rate from 1/N quantisation
// (totalCount < ceil(1/faultyThreshold)) from both numerator and
// denominator, so a cluster of low-throughput agents reports zero
// (no signal) instead of a noisy ratio.
func (t *AverageAgentStatsTracker) ClusterStats(now time.Time, slowMultiplier, faultyThreshold float64) (ClusterStats, bool) {
	t.statsMu.RLock()
	defer t.statsMu.RUnlock()

	// Agents below this request count have an error rate too coarse to
	// reliably distinguish from the threshold (smallest non-zero rate
	// would already exceed it). They are excluded from the FaultyFraction
	// numerator AND denominator so a cluster of low-throughput agents
	// reports 0 (no signal) rather than a noisy ratio.
	minErrorRateRequests := errorRateMinRequests(faultyThreshold)

	var (
		nowNs = now.UnixNano()

		// Per-agent latencies for agents with at least one successful
		// request — the only data the second pass needs to count slow
		// agents. Agents without a successful request are simply not
		// appended.
		agentAverageLatenciesMs = make([]int64, 0, len(t.stats))

		// Agents count.
		observedAgentsCount  int64 // agents with any request in the window
		qualifiedAgentsCount int64 // agents that also pass the filled-buckets gate

		// Latency aggregation.
		qualifiedAgentsLatencySumMs int64 // sum of per-agent average latencies

		// Error-rate aggregation.
		qualifiedAgentsTotalRequestsCount       int64
		qualifiedAgentsTotalFaultyRequestsCount int64

		// Faulty-fraction counters.
		faultyAgentsCount       int64
		faultyContributorsCount int64
	)

	for _, s := range t.stats {
		snap := s.snapshot(nowNs)
		agentRequestsCount := snap.totalRequestsCount()
		if agentRequestsCount == 0 {
			// No requests tracked for this agent. Exclude it from computation.
			continue
		}

		observedAgentsCount++

		// Same bucket-spread gate as AgentStats: agents with requests
		// concentrated in fewer time buckets aren't representative
		// enough to contribute to the cluster baseline.
		if snap.filledBuckets < minFilledBuckets {
			continue
		}

		qualifiedAgentsCount++
		qualifiedAgentsTotalRequestsCount += agentRequestsCount
		qualifiedAgentsTotalFaultyRequestsCount += snap.faultyRequestsCount

		if snap.successfulRequestsLatencyCount > 0 {
			agentAverageLatencyMs := snap.successfulRequestsLatencySumMs / snap.successfulRequestsLatencyCount
			qualifiedAgentsLatencySumMs += agentAverageLatencyMs
			agentAverageLatenciesMs = append(agentAverageLatenciesMs, agentAverageLatencyMs)
		}

		// An agent qualifies to be a contributor to faulty agents fraction only
		// if we have tracked enough requests to be statistically significant.
		if agentRequestsCount >= minErrorRateRequests {
			faultyContributorsCount++
			if float64(snap.faultyRequestsCount)/float64(agentRequestsCount) > faultyThreshold {
				faultyAgentsCount++
			}
		}
	}

	// Quorum gate: require that more than half of the agents we have any
	// data for actually pass the qualification gates. Without this, a
	// single freshly-qualifying agent in a cluster of mostly-unqualified
	// ones would produce a meaningless baseline.
	if qualifiedAgentsCount == 0 || qualifiedAgentsCount*2 < observedAgentsCount {
		return ClusterStats{}, false
	}

	// BaselineLatency averages over agents (each agent contributes equally
	// regardless of throughput) and only over agents with at least one
	// successful request — latency is undefined for an agent that errored
	// every request.
	var baselineLatencyMs int64
	if len(agentAverageLatenciesMs) > 0 {
		baselineLatencyMs = qualifiedAgentsLatencySumMs / int64(len(agentAverageLatenciesMs))
	}
	slowThresholdMs := int64(float64(baselineLatencyMs) * slowMultiplier)

	// BaselineErrorRate is request-weighted (totalFaultyRequests / totalRequests)
	// rather than a per-agent mean. With request-weighted, low-throughput
	// agents contribute proportionally to their request count, so their
	// inherent 1/N quantisation noise cannot pull the cluster baseline.
	var baselineErrorRate float64
	if qualifiedAgentsTotalRequestsCount > 0 {
		baselineErrorRate = float64(qualifiedAgentsTotalFaultyRequestsCount) / float64(qualifiedAgentsTotalRequestsCount)
	}

	// Compute slow and faulty agent fractions.
	var slowFraction, faultyFraction float64
	if len(agentAverageLatenciesMs) > 0 {
		var slowAgentsCount int64
		for _, latencyMs := range agentAverageLatenciesMs {
			if latencyMs > slowThresholdMs {
				slowAgentsCount++
			}
		}
		slowFraction = float64(slowAgentsCount) / float64(len(agentAverageLatenciesMs))
	}
	if faultyContributorsCount > 0 {
		faultyFraction = float64(faultyAgentsCount) / float64(faultyContributorsCount)
	}

	return ClusterStats{
		BaselineLatency:         time.Duration(baselineLatencyMs) * time.Millisecond,
		SlowThreshold:           time.Duration(slowThresholdMs) * time.Millisecond,
		SlowFraction:            slowFraction,
		SlowContributorsCount:   int64(len(agentAverageLatenciesMs)),
		BaselineErrorRate:       baselineErrorRate,
		FaultyThreshold:         faultyThreshold,
		FaultyFraction:          faultyFraction,
		FaultyContributorsCount: faultyContributorsCount,
	}, true
}

// getOrCreate returns the stats entry for nodeID, creating it on first use.
func (t *AverageAgentStatsTracker) getOrCreate(nodeID int32) *averageAgentStats {
	t.statsMu.RLock()
	s, ok := t.stats[nodeID]
	t.statsMu.RUnlock()
	if ok {
		return s
	}

	t.statsMu.Lock()
	defer t.statsMu.Unlock()
	if s, ok := t.stats[nodeID]; ok {
		return s
	}
	s = &averageAgentStats{}
	t.stats[nodeID] = s
	return s
}

// averageAgentStats holds the per-agent ring of stats buckets. Access is
// serialized by bucketsMu: bucket rotation under concurrent reads/writes is
// hard to make correct lock-free without careful encoding, and per-agent
// throughput (bounded by request rate) keeps mutex contention negligible.
type averageAgentStats struct {
	bucketsMu sync.Mutex
	buckets   [numStatsBuckets]averageAgentStatsBucket
}

// snapshot aggregates the buckets whose epochStart is within the last
// numStatsBuckets * bucketDuration. Buckets older than that range are
// ignored; their stale data will be overwritten on the next
// TrackAgentRequest that lands in the same ring slot.
func (s *averageAgentStats) snapshot(nowNs int64) averageAgentStatsSnapshot {
	currentEpoch := (nowNs / bucketDurationNs) * bucketDurationNs
	cutoff := currentEpoch - int64(numStatsBuckets-1)*bucketDurationNs
	var snap averageAgentStatsSnapshot

	s.bucketsMu.Lock()
	defer s.bucketsMu.Unlock()
	for _, b := range s.buckets {
		if b.successfulLatencyCount+b.faultyCount == 0 || b.epochStart < cutoff {
			continue
		}
		snap.successfulRequestsLatencySumMs += b.successfulLatencySumMs
		snap.successfulRequestsLatencyCount += b.successfulLatencyCount
		snap.faultyRequestsCount += b.faultyCount
		snap.filledBuckets++
	}
	return snap
}

// averageAgentStatsBucket holds the sums and counts for one time slice.
// successfulLatencyCount counts successful requests (the ones that contributed to
// successfulLatencySumMs); faultyCount counts errored requests. The bucket's total
// observation count is successfulLatencyCount + faultyCount.
type averageAgentStatsBucket struct {
	epochStart             int64 // bucket start in unix nanos; 0 = never written
	successfulLatencySumMs int64 // sum of successful request latencies
	successfulLatencyCount int64 // count of successful requests
	faultyCount            int64 // count of errored requests
}

// averageAgentStatsSnapshot summarises the buckets of one agent that fall
// within the current observation window.
type averageAgentStatsSnapshot struct {
	successfulRequestsLatencySumMs int64
	successfulRequestsLatencyCount int64
	faultyRequestsCount            int64
	filledBuckets                  int64
}

// totalRequestsCount returns the aggregate request count (successful +
// errored) covered by the snapshot.
func (s averageAgentStatsSnapshot) totalRequestsCount() int64 {
	return s.successfulRequestsLatencyCount + s.faultyRequestsCount
}

// toAgentStats converts the snapshot into the public AgentStats view. The
// caller is expected to have already verified the bucket-spread gate. The
// returned ErrorRate is the raw ratio over RequestCount; callers gating
// on small-sample noise (1/N quantisation) should consult RequestCount.
func (s averageAgentStatsSnapshot) toAgentStats() AgentStats {
	var lat time.Duration
	if s.successfulRequestsLatencyCount > 0 {
		lat = time.Duration(s.successfulRequestsLatencySumMs/s.successfulRequestsLatencyCount) * time.Millisecond
	}
	total := s.totalRequestsCount()
	var rate float64
	if total > 0 {
		rate = float64(s.faultyRequestsCount) / float64(total)
	}
	return AgentStats{Latency: lat, ErrorRate: rate, RequestCount: total}
}

// bucketIndex returns the ring slot for the given unix-nanosecond timestamp.
// Go's % operator preserves the sign of the dividend, so for nowNs < 0
// (only possible in tests with a fake clock seeded before the unix epoch)
// the raw modulo can be negative; normalize so the index is always in
// [0, numStatsBuckets).
func bucketIndex(nowNs int64) int64 {
	idx := (nowNs / bucketDurationNs) % numStatsBuckets
	if idx < 0 {
		idx += numStatsBuckets
	}
	return idx
}

// errorRateMinRequests returns the minimum totalCount required for an
// observed error rate to be reliably distinguishable from
// faultyThreshold. Below this, the smallest non-zero rate (1/N) is
// itself >= the threshold, so any single error would trip — too noisy
// to act on. faultyThreshold <= 0 disables the gate (any observation
// counts).
func errorRateMinRequests(faultyThreshold float64) int64 {
	if faultyThreshold <= 0 {
		return 0
	}
	n := int64(math.Ceil(1.0 / faultyThreshold))
	if n < 1 {
		return 1
	}
	return n
}
