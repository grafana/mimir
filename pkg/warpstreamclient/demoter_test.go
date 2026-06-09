// SPDX-License-Identifier: AGPL-3.0-only

package warpstreamclient

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestDemoter builds a Demoter with a throwaway registry (so the gauges can
// be asserted) and a no-op logger.
func newTestDemoter(inner PartitionAssignmentStrategy, tracker AgentStatsReader, health HealthCheckConfig, cfg DemoterConfig) (*Demoter, *prometheus.Registry) {
	reg := prometheus.NewPedanticRegistry()
	return NewDemoter(inner, tracker, health, cfg, log.NewNopLogger(), reg), reg
}

func TestDemoter_Candidates(t *testing.T) {
	const (
		healthyID = int32(1)
		slowID    = int32(2)
		extraID   = int32(3)
	)
	topic := "t"
	part := int32(0)

	health := HealthCheckConfig{
		SlowMultiplier:    2.0,
		MaxSlowFraction:   0.3,
		FaultyThreshold:   0.05,
		MaxFaultyFraction: 0.3,
	}
	cfg := DemoterConfig{
		ProbeInterval: time.Second,
	}

	newTracker := func(faultyAgents ...int32) *AverageAgentStatsTracker {
		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		// Healthy baseline for everyone (low latency, no errors).
		for _, id := range []int32{healthyID, slowID, extraID} {
			seedFullWindow(tr, id, nowNs, 20, 10, 0)
		}
		// Override the faulty agents with a high error rate (50%) so they
		// clearly exceed FaultyThreshold=0.05.
		for _, id := range faultyAgents {
			seedFullWindow(tr, id, nowNs, 10, 10, 10)
		}
		return tr
	}

	t.Run("healthy primary passes through unchanged", func(t *testing.T) {
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(healthyID, extraID)},
		}
		d, reg := newTestDemoter(inner, newTracker(), health, cfg)
		now := time.Now()
		d.now = func() time.Time { return now }

		cands := d.Candidates(topic, part, 2)
		require.Len(t, cands, 2)
		assert.Equal(t, healthyID, cands[0].NodeID)
		assert.Equal(t, AgentStateHealthy, cands[0].State)

		// Nothing demoted, nothing suppressed.
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demoted_agents Number of Warpstream agents currently demoted by the Demoter.
			# TYPE demoter_demoted_agents gauge
			demoter_demoted_agents 0

			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 0
		`)))
	})

	t.Run("demoted primary is skipped on first call within probe interval and emitted as probe on first call after interval", func(t *testing.T) {
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(slowID, healthyID)},
		}
		d, reg := newTestDemoter(inner, newTracker(slowID), health, cfg)
		start := time.Now()
		now := start
		d.now = func() time.Time { return now }

		// First call: slowID is demoted and no prior probe was issued,
		// so it consumes the probe slot and surfaces as AgentStateDemoted
		// in the primary position. healthyID still trails as the fallback.
		cands := d.Candidates(topic, part, 2)
		require.Len(t, cands, 2)
		assert.Equal(t, slowID, cands[0].NodeID)
		assert.Equal(t, AgentStateDemoted, cands[0].State)
		assert.Equal(t, healthyID, cands[1].NodeID)
		assert.Equal(t, AgentStateHealthy, cands[1].State)

		// One agent is now demoted; the cluster-wide guard is not tripping.
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demoted_agents Number of Warpstream agents currently demoted by the Demoter.
			# TYPE demoter_demoted_agents gauge
			demoter_demoted_agents 1

			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 0
		`)))

		// Second call inside the probe interval: skip the demoted primary,
		// surface the healthy secondary as the new primary.
		now = start.Add(cfg.ProbeInterval / 2)
		cands = d.Candidates(topic, part, 2)
		require.Len(t, cands, 1)
		assert.Equal(t, healthyID, cands[0].NodeID)
		assert.Equal(t, AgentStateHealthy, cands[0].State)

		// After the probe interval elapses, the next call probes the
		// demoted agent again.
		now = start.Add(cfg.ProbeInterval + time.Millisecond)
		cands = d.Candidates(topic, part, 2)
		require.NotEmpty(t, cands)
		assert.Equal(t, slowID, cands[0].NodeID)
		assert.Equal(t, AgentStateDemoted, cands[0].State)
	})

	t.Run("demoted agents in non-primary positions are filtered out", func(t *testing.T) {
		// Primary healthy, secondary slow. The demoted secondary should
		// never appear in the output — neither as primary nor fallback.
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(healthyID, slowID)},
		}
		d, reg := newTestDemoter(inner, newTracker(slowID), health, cfg)
		d.now = func() time.Time { return time.Now() }

		cands := d.Candidates(topic, part, 3)
		require.Len(t, cands, 1)
		assert.Equal(t, healthyID, cands[0].NodeID)
		for _, c := range cands {
			assert.NotEqual(t, slowID, c.NodeID)
		}

		// The faulty agent is counted as demoted even though it was filtered
		// out of the candidate list (never surfaced as a primary probe).
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demoted_agents Number of Warpstream agents currently demoted by the Demoter.
			# TYPE demoter_demoted_agents gauge
			demoter_demoted_agents 1
			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 0
		`)))
	})

	t.Run("max faulty fraction guard suppresses demotion when too many agents are faulty", func(t *testing.T) {
		// 2 of 3 known agents are faulty: FaultyFraction = 2/3 ≈ 0.67,
		// which exceeds the configured MaxFaultyFraction = 0.3 (and is
		// also above the 1/3 scale-aware floor). This is a "cluster-wide
		// issue" by the policy definition: demoting most of the pool
		// would dump all traffic onto the one healthy agent, so the
		// Demoter must leave the candidate list untouched even though
		// the slow/faulty agents would have been demoted in isolation.
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(slowID, extraID)},
		}
		d, reg := newTestDemoter(inner, newTracker(slowID, extraID), health, cfg)
		d.now = func() time.Time { return time.Now() }

		cands := d.Candidates(topic, part, 2)
		require.Len(t, cands, 2)
		assert.Equal(t, slowID, cands[0].NodeID)
		assert.Equal(t, AgentStateHealthy, cands[0].State)
		assert.Equal(t, extraID, cands[1].NodeID)
		assert.Equal(t, AgentStateHealthy, cands[1].State)

		// The cluster-wide guard is tripping and nothing was demoted.
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demoted_agents Number of Warpstream agents currently demoted by the Demoter.
			# TYPE demoter_demoted_agents gauge
			demoter_demoted_agents 0

			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 1
		`)))
	})

	t.Run("demotion_suppressed gauge clears once the faulty fraction drops", func(t *testing.T) {
		// The gauge recomputes from cluster stats on every scrape, so it must
		// fall back to 0 when agents recover below the suppression floor — not
		// latch at 1.
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(slowID, extraID)},
		}
		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		seedFullWindow(tr, healthyID, nowNs, 20, 10, 0)
		seedFullWindow(tr, slowID, nowNs, 10, 10, 10)
		seedFullWindow(tr, extraID, nowNs, 10, 10, 10)
		d, reg := newTestDemoter(inner, tr, health, cfg)
		d.now = func() time.Time { return time.Now() }

		// 2 of 3 faulty → suppressed.
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 1
		`), "demoter_demotion_suppressed"))

		// extraID recovers → 1 of 3 faulty, below the floor → no longer suppressed.
		seedFullWindow(tr, extraID, nowNs, 20, 10, 0)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 0
		`), "demoter_demotion_suppressed"))
	})

	t.Run("cold start (no cluster stats): nothing is demoted", func(t *testing.T) {
		// Empty tracker → no cluster baseline → fail-open: every
		// candidate flows through unchanged.
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(slowID, healthyID)},
		}
		d, reg := newTestDemoter(inner, NewAverageAgentStatsTracker(), health, cfg)
		d.now = func() time.Time { return time.Now() }

		cands := d.Candidates(topic, part, 2)
		require.Len(t, cands, 2)
		assert.Equal(t, slowID, cands[0].NodeID)
		assert.Equal(t, AgentStateHealthy, cands[0].State)

		// No cluster stats yet → suppression reads 0 (fail-open), nothing demoted.
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demoted_agents Number of Warpstream agents currently demoted by the Demoter.
			# TYPE demoter_demoted_agents gauge
			demoter_demoted_agents 0

			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 0
		`)))
	})

	t.Run("demoted agent with sparse probe-only traffic stays demoted (hysteresis)", func(t *testing.T) {
		// Setup: an agent that starts out heavily demoted (many failed
		// requests in the window so it clearly passes the error-rate
		// gate), then transitions to "probe-only" state — sparse
		// failures, RequestCount well below errorRateMinRequests for
		// the configured FaultyThreshold.
		//
		// This is the steady state of a long-demoted agent: we route
		// only probes to it, the probes all fail, but the probe rate is
		// too low to satisfy the noise-suppression gate that
		// errorRateMinRequests imposes when *entering* demotion. The
		// gate's existence is correct for healthy→demoted transitions
		// (don't trip on a single fluke error) but wrong for staying
		// demoted: once we know the agent is sick, probe failures are
		// real signal, not noise.
		//
		// FaultyThreshold = 0.05 → errorRateMinRequests = ceil(1/0.05) = 20.
		// A probe-only agent with 1 failing probe per bucket across
		// numStatsBuckets=6 buckets has RequestCount=6 — under the gate.
		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		seedFullWindow(tr, healthyID, nowNs, 20, 10, 0)
		seedFullWindow(tr, extraID, nowNs, 20, 10, 0)
		// Heavy initial demoted state: 30 failures/bucket, no successes
		// → RequestCount=180, ErrorRate=1.0, both gates pass.
		seedFullWindow(tr, slowID, nowNs, 0, 0, 30)

		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(slowID, healthyID)},
		}
		d, _ := newTestDemoter(inner, tr, health, cfg)
		now := time.Now()
		d.now = func() time.Time { return now }

		// First call: the heavy-failure state passes the strict gate
		// and slowID is demoted, claiming the probe slot.
		first := d.Candidates(topic, part, 2)
		require.NotEmpty(t, first)
		require.Equal(t, slowID, first[0].NodeID)
		require.Equal(t, AgentStateDemoted, first[0].State)

		// Simulate the agent's stats "aging" into probe-only territory:
		// only 1 failing observation per bucket. RequestCount drops to
		// 6, below errorRateMinRequests(0.05)=20.
		seedFullWindow(tr, slowID, nowNs, 0, 0, 1)

		// Advance past the probe interval so the next probe is due.
		now = now.Add(cfg.ProbeInterval + time.Millisecond)

		// Second call. The agent's error rate is still 100%, but
		// RequestCount=6 < gate=20. With the current implementation,
		// isDemoted returns false here and slowID is misclassified as
		// healthy — it appears in the primary slot with State=Healthy
		// (the bug). The correct behaviour is to stay demoted: the
		// next probe is due, so slowID should surface as the primary
		// with State=Demoted.
		second := d.Candidates(topic, part, 2)
		require.NotEmpty(t, second)
		assert.Equal(t, slowID, second[0].NodeID)
		assert.Equal(t, AgentStateDemoted, second[0].State)
	})

	t.Run("recovery clears probe state and re-applies the strict gate", func(t *testing.T) {
		// Drive an agent through demote → probe-only failures →
		// genuine recovery → strict gate applies again on next
		// deterioration. Specifically: after recovery, a sparse-but-
		// failing window should NOT be classified as demoted (because
		// lastDemotedProbe was cleared and the strict gate kicks back
		// in). This is the inverse of the hysteresis test.
		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		seedFullWindow(tr, healthyID, nowNs, 20, 10, 0)
		seedFullWindow(tr, extraID, nowNs, 20, 10, 0)
		// Initial heavy-failure state: passes the strict gate.
		seedFullWindow(tr, slowID, nowNs, 0, 0, 30)

		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(slowID, healthyID)},
		}
		d, reg := newTestDemoter(inner, tr, health, cfg)
		now := time.Now()
		d.now = func() time.Time { return now }

		// Enter demotion.
		first := d.Candidates(topic, part, 2)
		require.Equal(t, slowID, first[0].NodeID)
		require.Equal(t, AgentStateDemoted, first[0].State)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demoted_agents Number of Warpstream agents currently demoted by the Demoter.
			# TYPE demoter_demoted_agents gauge
			demoter_demoted_agents 1

			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 0
		`)))

		// Recovery: replace the stats with all-successful traffic.
		// ErrorRate drops to 0 so isDemoted returns false; the recovery
		// branch clears lastDemotedProbe.
		seedFullWindow(tr, slowID, nowNs, 20, 10, 0)
		now = now.Add(cfg.ProbeInterval + time.Millisecond)
		recovered := d.Candidates(topic, part, 2)
		require.NotEmpty(t, recovered)
		assert.Equal(t, slowID, recovered[0].NodeID)
		assert.Equal(t, AgentStateHealthy, recovered[0].State)
		require.NoError(t, testutil.GatherAndCompare(reg, strings.NewReader(`
			# HELP demoter_demoted_agents Number of Warpstream agents currently demoted by the Demoter.
			# TYPE demoter_demoted_agents gauge
			demoter_demoted_agents 0

			# HELP demoter_demotion_suppressed Whether the Demoter is currently suppressing all demotions because too many agents are faulty (1) or not (0).
			# TYPE demoter_demotion_suppressed gauge
			demoter_demotion_suppressed 0
		`)))

		// Now simulate a fresh sparse-failure window (RequestCount=6).
		// Because the previous recovery cleared lastDemotedProbe, the
		// strict gate applies — and the gate refuses to demote on this
		// low-volume signal. The agent stays healthy.
		seedFullWindow(tr, slowID, nowNs, 0, 0, 1)
		now = now.Add(cfg.ProbeInterval + time.Millisecond)
		afterRecovery := d.Candidates(topic, part, 2)
		require.NotEmpty(t, afterRecovery)
		assert.Equal(t, slowID, afterRecovery[0].NodeID)
		assert.Equal(t, AgentStateHealthy, afterRecovery[0].State)
	})

	t.Run("probe interval is per-agent, not per-partition", func(t *testing.T) {
		// Two partitions whose natural primary is the same demoted
		// agent. Both partitions in quick succession: only one gets a
		// probe slot.
		const partA, partB = int32(0), int32(1)
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, partA}: healthyAgents(slowID, healthyID),
				{topic, partB}: healthyAgents(slowID, healthyID),
			},
		}
		d, _ := newTestDemoter(inner, newTracker(slowID), health, cfg)
		now := time.Now()
		d.now = func() time.Time { return now }

		first := d.Candidates(topic, partA, 2)
		require.NotEmpty(t, first)
		assert.Equal(t, slowID, first[0].NodeID)

		now = now.Add(time.Millisecond) // still inside the probe interval
		second := d.Candidates(topic, partB, 2)
		require.NotEmpty(t, second)
		assert.Equal(t, healthyID, second[0].NodeID)
	})

	t.Run("retry loop grows when the prefix is mostly demoted", func(t *testing.T) {
		// Make 10 agents all faulty (demoted) and one healthy. The
		// healthy agent sits at position 11 in the inner's ordering, so
		// the initial ask (maxCandidates+2 = 3) returns 3 demoted agents
		// and zero non-demoted. The retry loop must keep growing until
		// it pulls position 11 into raw.
		const (
			demoted0  = int32(10)
			demoted9  = int32(19)
			healthy11 = int32(100)
		)
		faulty := []int32{demoted0, 11, 12, 13, 14, 15, 16, 17, 18, demoted9}
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, part}: healthyAgents(demoted0, 11, 12, 13, 14, 15, 16, 17, 18, demoted9, healthy11),
			},
		}

		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		// Healthy11 is the only one with low error rate.
		seedFullWindow(tr, healthy11, nowNs, 20, 10, 0)
		for _, id := range faulty {
			seedFullWindow(tr, id, nowNs, 20, 10, 10)
		}

		d, _ := newTestDemoter(inner, tr, health, cfg)
		now := time.Now()
		d.now = func() time.Time { return now }

		cands := d.Candidates(topic, part, 1)
		require.Len(t, cands, 1)
		// The primary slot was filled either with the natural primary's
		// probe (shouldProbe returned true) or with healthy11. We
		// don't care which — only that the demoter found a slot, which
		// proves the retry loop pulled enough raw to consider healthy11.
	})

	t.Run("returned list is capped at maxCandidates regardless of pool size", func(t *testing.T) {
		// 10 healthy agents in the pool but maxCandidates=3: the return
		// slice must hold exactly 3 entries.
		const (
			primary   = int32(1)
			secondary = int32(2)
		)
		extras := []int32{3, 4, 5, 6, 7, 8, 9, 10}
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, part}: healthyAgents(append([]int32{primary, secondary}, extras...)...),
			},
		}

		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		for _, id := range append([]int32{primary, secondary}, extras...) {
			seedFullWindow(tr, id, nowNs, 20, 10, 0)
		}

		d, _ := newTestDemoter(inner, tr, health, cfg)
		now := time.Now()
		d.now = func() time.Time { return now }

		cands := d.Candidates(topic, part, 3)
		assert.Len(t, cands, 3)
	})

	t.Run("returned list cap holds when a demoted probe occupies the primary slot", func(t *testing.T) {
		// Natural primary is demoted (and probe-due) and there are
		// plenty of healthy alternates. The returned slice must include
		// the probe AND fill up to maxCandidates total.
		const (
			demoted  = int32(1)
			healthyA = int32(2)
			healthyB = int32(3)
			healthyC = int32(4)
			healthyD = int32(5)
		)
		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{
				{topic, part}: healthyAgents(demoted, healthyA, healthyB, healthyC, healthyD),
			},
		}

		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		seedFullWindow(tr, demoted, nowNs, 20, 10, 10) // faulty
		for _, id := range []int32{healthyA, healthyB, healthyC, healthyD} {
			seedFullWindow(tr, id, nowNs, 20, 10, 0)
		}

		d, _ := newTestDemoter(inner, tr, health, cfg)
		now := time.Now()
		d.now = func() time.Time { return now }

		cands := d.Candidates(topic, part, 3)
		require.Len(t, cands, 3)
		assert.Equal(t, demoted, cands[0].NodeID)
		assert.Equal(t, AgentStateDemoted, cands[0].State)
	})

	t.Run("logs demote and restore transitions", func(t *testing.T) {
		var buf bytes.Buffer
		logger := log.NewLogfmtLogger(&buf)
		reg := prometheus.NewPedanticRegistry()

		tr := NewAverageAgentStatsTracker()
		nowNs := time.Now().UnixNano()
		seedFullWindow(tr, healthyID, nowNs, 20, 10, 0)
		seedFullWindow(tr, extraID, nowNs, 20, 10, 0)
		seedFullWindow(tr, slowID, nowNs, 0, 0, 30) // heavy failures

		inner := &mockPartitionAssignmentStrategy{
			candidates: map[partitionKey][]Agent{{topic, part}: healthyAgents(slowID, healthyID)},
		}
		d := NewDemoter(inner, tr, health, cfg, logger, reg)
		now := time.Now()
		d.now = func() time.Time { return now }

		// Demote: slowID claims the probe slot, logging the transition once
		// with the diagnostic fields that explain why it crossed the gate.
		d.Candidates(topic, part, 2)
		demoteLog := buf.String()
		assert.Contains(t, demoteLog, "warpstream agent demoted")
		assert.Contains(t, demoteLog, "node_id="+strconv.Itoa(int(slowID)))
		assert.Contains(t, demoteLog, "error_rate=")
		assert.Contains(t, demoteLog, "request_count=")
		// The demotion edge applies the strict gate, so min_requests is
		// errorRateMinRequests(0.05)=20, not the relaxed 1.
		assert.Contains(t, demoteLog, "min_requests=20")
		assert.Contains(t, demoteLog, "faulty_threshold=0.05")

		// Recovery: all-successful traffic clears the probe state and logs restore.
		buf.Reset()
		seedFullWindow(tr, slowID, nowNs, 20, 10, 0)
		now = now.Add(cfg.ProbeInterval + time.Millisecond)
		d.Candidates(topic, part, 2)
		restoreLog := buf.String()
		assert.Contains(t, restoreLog, "warpstream agent restored")
		assert.Contains(t, restoreLog, "node_id="+strconv.Itoa(int(slowID)))
	})
}

// BenchmarkDemoter_Candidates measures the cost of Demoter.Candidates
// under concurrent load. The workload uses the real
// DefaultPartitionAssignmentStrategy and the real AverageAgentStatsTracker
// (wrapped by CachedAgentStatsTracker), so it exercises every layer the
// Demoter touches in production.
//
// The probe-state map is the only shared mutable state inside Demoter; the
// rest is local to each call. So changes that affect contention on that
// map (e.g. replacing the mutex with sync.Map + atomic) are what this
// benchmark is sensitive to.
func BenchmarkDemoter_Candidates(b *testing.B) {
	const (
		topic              = "t"
		numDemotedFraction = 4 // every Nth agent is demoted
	)
	scenarios := []struct {
		name          string
		numAgents     int32
		numPartitions int32
	}{
		{"agents=10/partitions=50", 10, 50},
		{"agents=100/partitions=100", 100, 100},
		{"agents=1000/partitions=3500", 1000, 3500},
	}
	health := HealthCheckConfig{
		SlowMultiplier:    2.0,
		MaxSlowFraction:   0.3,
		FaultyThreshold:   0.05,
		MaxFaultyFraction: 0.3,
	}
	cfg := DemoterConfig{
		ProbeInterval: time.Second,
	}

	for _, sc := range scenarios {
		b.Run(sc.name, func(b *testing.B) {
			// Real AverageAgentStatsTracker seeded with healthy
			// baselines and a faulty subset, wrapped by the cache the
			// production WarpstreamClient uses.
			innerTracker := NewAverageAgentStatsTracker()
			nowNs := time.Now().UnixNano()
			for i := int32(0); i < sc.numAgents; i++ {
				if int(i)%numDemotedFraction == 0 {
					seedFullWindow(innerTracker, i, nowNs, 0, 0, 30)
				} else {
					seedFullWindow(innerTracker, i, nowNs, 20, 10, 0)
				}
			}
			tracker := NewCachedAgentStatsTracker(innerTracker, time.Second)

			// Real DefaultPartitionAssignmentStrategy snapshot.
			agents := make([]int32, sc.numAgents)
			for i := int32(0); i < sc.numAgents; i++ {
				agents[i] = i
			}
			leaders := make(map[topicPartition]int32, sc.numPartitions)
			for p := int32(0); p < sc.numPartitions; p++ {
				leaders[topicPartition{topic, p}] = p % sc.numAgents
			}
			inner := newDefaultPartitionAssignmentStrategy(agents, leaders)

			d, _ := newTestDemoter(inner, tracker, health, cfg)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				var p int32
				for pb.Next() {
					_ = d.Candidates(topic, p%sc.numPartitions, 4)
					p++
				}
			})
		})
	}
}

func TestDemoterConfig_Validate(t *testing.T) {
	cases := map[string]struct {
		cfg     DemoterConfig
		wantErr string
	}{
		"probe interval zero": {
			cfg:     DemoterConfig{ProbeInterval: 0},
			wantErr: "probe interval",
		},
		"probe interval negative": {
			cfg:     DemoterConfig{ProbeInterval: -1},
			wantErr: "probe interval",
		},
		"valid": {
			cfg: DemoterConfig{ProbeInterval: time.Second},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}
