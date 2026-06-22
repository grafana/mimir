package wgo

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"
)

// Per-agent observations accumulate into a sliding window of fixed-duration
// buckets. The window covers the last numStatsBuckets * bucketDuration of
// activity; requests outside the window are not evicted but simply ignored
// at read time, and their ring slot is lazily overwritten when a later
// request reuses it.
//
// Bucketing — rather than an EMA — gives the latency / error-rate queries
// time-aware statistics: a single concentrated burst of slow or errored
// requests cannot trigger hedging because the bucket-spread gate requires
// activity spread across multiple buckets. The window also
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
// The error-rate signal instead gets a volume-adaptive request-count floor
// derived per-call from the FaultyThreshold and the observed cluster volume
// (see errorRateMinRequests). AgentStats itself reports the raw ErrorRate
// plus a RequestCount so the caller can apply the same gate against its own
// threshold.
const (
	numStatsBuckets  = 6
	bucketDuration   = 10 * time.Second
	bucketDurationNs = int64(bucketDuration)

	// minFilledBuckets is the number of distinct time buckets that must
	// hold at least one request before an agent's stats are considered
	// representative, ensuring an agent's activity is spread across time
	// rather than concentrated in a single burst. It is kept low so that
	// an agent is still observable under backpressure (where each client
	// sees only a handful of requests per agent per window).
	//
	// The same gate is used for AgentStats (per-agent decisions) and for
	// admitting an agent to the cluster baseline computation: an agent
	// that contributes to the baseline is one whose own stats are also
	// available. It also doubles as the low-volume sample floor in
	// errorRateMinRequests: an agent that qualifies here has at least this
	// many requests, so the relaxed gate is always satisfiable.
	minFilledBuckets = 2
)

// AgentStatsReader is the read-only subset of AgentStatsTracker. Components
// that consume per-agent / cluster-wide stats but never write them (Hedger,
// RetryingProducer) take this narrower interface to keep concerns separated.
type AgentStatsReader interface {
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

// AgentStatsTracker is implemented by components that track per-agent and
// cluster-wide request stats (latency and error rate).
type AgentStatsTracker interface {
	AgentStatsReader

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
	// remains meaningful in that case.
	BaselineLatency time.Duration

	// SlowThreshold is the latency above which an agent is considered slow.
	SlowThreshold time.Duration

	// SlowFraction is the fraction of agents whose latency exceeds SlowThreshold.
	SlowFraction float64

	// SlowContributorsCount is the number of agents that contributed to
	// SlowFraction. Lets the caller scale fraction-based decisions to
	// cluster size.
	SlowContributorsCount int64

	// BaselineErrorRate is the typical error rate observed across the cluster's agents.
	BaselineErrorRate float64

	// FaultyThreshold is the error rate above which an agent is considered faulty.
	FaultyThreshold float64

	// AvgRequestsPerAgent is the average request count across observed agents.
	AvgRequestsPerAgent int64

	// FaultyFraction is the fraction of agents whose error rate exceeds FaultyThreshold.
	FaultyFraction float64

	// FaultyContributorsCount is the denominator of FaultyFraction. Lets the
	// caller scale fraction-based decisions to cluster size.
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

// NewAverageAgentStatsTracker returns a tracker with no recorded stats.
func NewAverageAgentStatsTracker() *AverageAgentStatsTracker {
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

// ClusterStats walks every agent and aggregates the cluster-wide view,
// returning false when there is no quorum of qualifying agents.
//
// slowMultiplier sets the slow threshold relative to the baseline latency: an
// agent is slow when its latency exceeds baseline*slowMultiplier. faultyThreshold
// is the error rate above which an agent is counted faulty. The meaning of each
// result field is documented on ClusterStats.
func (t *AverageAgentStatsTracker) ClusterStats(now time.Time, slowMultiplier, faultyThreshold float64) (ClusterStats, bool) {
	t.statsMu.RLock()
	defer t.statsMu.RUnlock()

	// Per-qualified-agent error stats, collected so the faulty count can be
	// deferred until AvgRequestsPerAgent is known (it drives the adaptive
	// per-agent sample gate in errorRateMinRequests).
	type agentErrorStat struct{ requests, faulty int64 }

	var (
		nowNs = now.UnixNano()

		// Per-agent latencies for agents with at least one successful
		// request — the only data needed to count slow agents. Agents
		// without a successful request are simply not appended.
		agentAverageLatenciesMs = make([]int64, 0, len(t.stats))

		// Agents count.
		observedAgentsCount  int64 // agents with any request in the window
		qualifiedAgentsCount int64 // agents that also pass the filled-buckets gate

		// Volume aggregation (drives the adaptive error-rate gate).
		observedAgentsTotalRequestsCount int64

		// Latency aggregation.
		qualifiedAgentsLatencySumMs int64 // sum of per-agent average latencies

		// Error-rate aggregation.
		qualifiedAgentsTotalRequestsCount       int64
		qualifiedAgentsTotalFaultyRequestsCount int64
		qualifiedAgentErrorStats                = make([]agentErrorStat, 0, len(t.stats))
	)

	for _, s := range t.stats {
		snap := s.snapshot(nowNs)
		agentRequestsCount := snap.totalRequestsCount()
		if agentRequestsCount == 0 {
			// No requests tracked for this agent. Exclude it from computation.
			continue
		}

		observedAgentsCount++
		observedAgentsTotalRequestsCount += agentRequestsCount

		// Same bucket-spread gate as AgentStats: agents with requests
		// concentrated in fewer time buckets aren't representative
		// enough to contribute to the cluster baseline.
		if snap.filledBuckets < minFilledBuckets {
			continue
		}

		qualifiedAgentsCount++
		qualifiedAgentsTotalRequestsCount += agentRequestsCount
		qualifiedAgentsTotalFaultyRequestsCount += snap.faultyRequestsCount
		qualifiedAgentErrorStats = append(qualifiedAgentErrorStats, agentErrorStat{requests: agentRequestsCount, faulty: snap.faultyRequestsCount})

		if snap.successfulRequestsLatencyCount > 0 {
			agentAverageLatencyMs := snap.successfulRequestsLatencySumMs / snap.successfulRequestsLatencyCount
			qualifiedAgentsLatencySumMs += agentAverageLatencyMs
			agentAverageLatenciesMs = append(agentAverageLatenciesMs, agentAverageLatencyMs)
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

	// Faulty agents are qualified agents whose error rate exceeds the
	// threshold, gated by the volume-adaptive minimum request count.
	avgRequestsPerAgent := observedAgentsTotalRequestsCount / observedAgentsCount
	minErrorRateRequests := errorRateMinRequests(ClusterStats{FaultyThreshold: faultyThreshold, AvgRequestsPerAgent: avgRequestsPerAgent})
	var faultyAgentsCount int64
	for _, a := range qualifiedAgentErrorStats {
		if a.requests >= minErrorRateRequests && float64(a.faulty)/float64(a.requests) > faultyThreshold {
			faultyAgentsCount++
		}
	}

	// SlowFraction is over agents with a measurable latency; FaultyFraction's
	// denominator is the observed agents (those with at least one request), so
	// it reflects the agents we currently have a health signal for rather than
	// treating idle or unknown agents as healthy.
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
	if observedAgentsCount > 0 {
		faultyFraction = float64(faultyAgentsCount) / float64(observedAgentsCount)
	}

	return ClusterStats{
		BaselineLatency:         time.Duration(baselineLatencyMs) * time.Millisecond,
		SlowThreshold:           time.Duration(slowThresholdMs) * time.Millisecond,
		SlowFraction:            slowFraction,
		SlowContributorsCount:   int64(len(agentAverageLatenciesMs)),
		BaselineErrorRate:       baselineErrorRate,
		FaultyThreshold:         faultyThreshold,
		AvgRequestsPerAgent:     avgRequestsPerAgent,
		FaultyFraction:          faultyFraction,
		FaultyContributorsCount: observedAgentsCount,
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

// errorRateMinRequests returns the minimum request count an agent must have
// before its observed error rate may be acted upon, adapted to how much traffic
// the cluster is actually seeing.
func errorRateMinRequests(stats ClusterStats) int64 {
	if stats.FaultyThreshold <= 0 {
		return 0
	}
	// base is the smallest count at which the coarsest non-zero error rate
	// (1/base) no longer exceeds the threshold on its own. Requiring it stops
	// one or two fluke errors on a barely-used agent from faking a high rate
	// and causing a false demotion.
	base := int64(math.Ceil(1.0 / stats.FaultyThreshold))
	if base < 1 {
		base = 1
	}
	// When the cluster is busy, the typical agent easily exceeds base, so we
	// hold every agent to base — a genuinely low-volume agent is itself the
	// anomaly and shouldn't be judged on sparse data.
	if stats.AvgRequestsPerAgent >= base {
		return base
	}
	// When the whole cluster is starved (backpressure), no agent can reach
	// base, so enforcing it would disable demotion for everyone. Relax to the
	// bar an agent already cleared to enter the cluster view (minFilledBuckets
	// requests, one per filled bucket), never above base. This trades some
	// precision for the ability to act at all; a fluke demotion is corrected by
	// the recovery probe within one ProbeInterval.
	return min(base, int64(minFilledBuckets))
}
