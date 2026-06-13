// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"testing"
	"time"
)

// Each TestScenario_* below runs against both clients and asserts only on
// the WarpstreamClient. The kgo run is reported as a reference baseline.
// All scenarios are gated by the WARPSTREAM_INTEGRATION env var via the
// shared skipIfNotIntegration check inside runScenario.

// TestScenario_AllHealthy: every agent at healthy latency. Every record
// must succeed via the primary; no hedge fires.
func TestScenario_AllHealthy(t *testing.T) {
	ws := runScenario(t, "all healthy", healthyBehaviors())
	assertHighSuccessRate(t, ws, 1.0)
}

// TestScenario_OneFastFailingAgent: broker 1 returns NotLeaderForPartition
// immediately. The cascade path retries on another agent.
func TestScenario_OneFastFailingAgent(t *testing.T) {
	bh := healthyBehaviors()
	bh[1] = brokerBehavior{fail: true}
	ws := runScenario(t, "1 fast-failing agent", bh)
	assertHighSuccessRate(t, ws, 1.0)
}

// TestScenario_OneSlowAgent: broker 1's per-request latency jumps to avg
// 1 s (max 3 s). The Hedger flags the primary as slow, fires a hedge, and
// the fallback wins the race.
func TestScenario_OneSlowAgent(t *testing.T) {
	bh := healthyBehaviors()
	bh[1] = brokerBehavior{latencyFn: slowAgentLatency}
	ws := runScenario(t, "1 slow agent", bh)
	assertHighSuccessRate(t, ws, 1.0)
}

// TestScenario_TwoFastFailingAgents: brokers 1 and 2 fail. Cascade retries
// on healthy agents.
func TestScenario_TwoFastFailingAgents(t *testing.T) {
	bh := healthyBehaviors()
	bh[1] = brokerBehavior{fail: true}
	bh[2] = brokerBehavior{fail: true}
	ws := runScenario(t, "2 fast-failing agents", bh)
	assertHighSuccessRate(t, ws, 1.0)
}

// TestScenario_TwoSlowAgents: brokers 1 and 2 are slow. Hedge fires for
// both; fallbacks win the race.
func TestScenario_TwoSlowAgents(t *testing.T) {
	bh := healthyBehaviors()
	bh[1] = brokerBehavior{latencyFn: slowAgentLatency}
	bh[2] = brokerBehavior{latencyFn: slowAgentLatency}
	ws := runScenario(t, "2 slow agents", bh)
	assertHighSuccessRate(t, ws, 1.0)
}

// TestScenario_OnePercentFailureAllAgents: every agent has a 1% random
// hard-failure probability on top of healthy latency.
func TestScenario_OnePercentFailureAllAgents(t *testing.T) {
	bh := healthyBehaviors()
	for i := int32(0); i < integrationClusterSize; i++ {
		b := bh[i]
		b.failRate = 0.01
		bh[i] = b
	}
	ws := runScenario(t, "1% failure rate across all agents", bh)
	assertHighSuccessRate(t, ws, 0.95)
}

// TestScenario_OnePercentTimeoutAllAgents: every agent has a 1%
// per-request probability of an extra ~10 s delay (= WriteTimeout). The
// burst length matches the flush deadline, so a hit on the primary can't
// be recovered within budget. With 50 partitions per request and 1% per
// partition, P(no partition trips a burst) = 0.99^50 ≈ 0.605, so the
// app-level success rate sits around 60%.
func TestScenario_OnePercentTimeoutAllAgents(t *testing.T) {
	bh := healthyBehaviors()
	for i := int32(0); i < integrationClusterSize; i++ {
		bh[i] = brokerBehavior{
			latencyFn: withBurst(healthyLatency, 0.01, 10*time.Second),
		}
	}
	ws := runScenario(t, "1% timeouts across all agents", bh)
	assertHighSuccessRate(t, ws, 0.50)
}

// TestScenario_OnePercentSlowAllAgents: every agent has a 1% per-request
// probability of an extra ~3 s slow burst (within the per-attempt deadline
// but slow enough that the hedge timer fires).
func TestScenario_OnePercentSlowAllAgents(t *testing.T) {
	bh := healthyBehaviors()
	for i := int32(0); i < integrationClusterSize; i++ {
		bh[i] = brokerBehavior{
			latencyFn: withBurst(healthyLatency, 0.01, 3*time.Second),
		}
	}
	ws := runScenario(t, "1% slow bursts across all agents", bh)
	assertHighSuccessRate(t, ws, 0.95)
}

// TestScenario_25PercentSlowAgents: 25% of agents are permanently slow
// (avg 1 s, max 3 s) while the rest stay at healthy latency. The hedge
// fallback should steer most traffic onto the healthy majority.
func TestScenario_25PercentSlowAgents(t *testing.T) {
	bh := healthyBehaviors()
	slowCount := integrationClusterSize / 4
	for i := int32(0); i < slowCount; i++ {
		bh[i] = brokerBehavior{latencyFn: slowAgentLatency}
	}
	ws := runScenario(t, "25% slow agents", bh)
	assertHighSuccessRate(t, ws, 1.0)
}

// TestScenario_GCSSlowOutage reproduces an incident where object storage
// degraded asymmetrically across agents:
//   - 50% of agents stay healthy
//   - 40% degrade to avg ≈ 700 ms / max ≈ 4 s
//   - 10% degrade to avg ≈ 2.5 s / max ≈ 10 s
//
// With a 5 s application-request timeout and 50 partitions per request,
// the bad-agent fraction (10%) maps to ~5 partitions per request whose
// primary always routes to a bad agent. We expect a large initial failure
// spike; the Demoter should kick in within ~30 s of the swap (3 buckets ×
// 10 s) and reroute traffic away from the worst offenders.
func TestScenario_GCSSlowOutage(t *testing.T) {
	bh := healthyBehaviors()
	// First 10% of brokers are very bad; next 40% are moderately slow;
	// remainder stay healthy.
	badCount := integrationClusterSize / 10
	modCount := (integrationClusterSize * 4) / 10
	for i := int32(0); i < badCount; i++ {
		bh[i] = brokerBehavior{latencyFn: gcsBadLatency}
	}
	for i := badCount; i < badCount+modCount; i++ {
		bh[i] = brokerBehavior{latencyFn: gcsModerateLatency}
	}
	ws := runScenario(t, "GCS slow outage (10% bad, 40% moderate)", bh)
	// The exact recovery curve depends on Demoter timing; the assertion
	// is intentionally loose. Re-tune once the report is reviewed.
	assertHighSuccessRate(t, ws, 0.20)
}
