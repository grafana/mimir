// SPDX-License-Identifier: AGPL-3.0-only

package integration

import (
	"math"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// brokerBehavior overrides per-broker Produce response behavior.
// latencyFn draws an artificial response latency per request; fail forces
// every request to fail with NotLeaderForPartition; failRate is the random
// per-request hard-failure probability when fail is false.
type brokerBehavior struct {
	latencyFn func(*rand.Rand) time.Duration
	fail      bool
	failRate  float64
}

// triangular draws a duration from a triangular distribution defined by
// (min, mode, max). A reasonable approximation of real Warpstream agent
// response times: clustered around a mode with a long right tail.
func triangular(rng *rand.Rand, min, mode, max time.Duration) time.Duration {
	if max <= min {
		return min
	}
	u := rng.Float64()
	span := float64(max - min)
	mid := float64(mode-min) / span
	var pick float64
	if u < mid {
		pick = float64(min) + math.Sqrt(u*span*float64(mode-min))
	} else {
		pick = float64(max) - math.Sqrt((1-u)*span*float64(max-mode))
	}
	return time.Duration(pick)
}

// withBurst wraps base so each draw has burstRate probability of being
// extended by burst.
func withBurst(base func(*rand.Rand) time.Duration, burstRate float64, burst time.Duration) func(*rand.Rand) time.Duration {
	return func(rng *rand.Rand) time.Duration {
		d := base(rng)
		if rng.Float64() < burstRate {
			d += burst
		}
		return d
	}
}

// healthyLatency models a Warpstream agent under normal load: avg ≈ 400 ms,
// min 100 ms, max 1 s. mode=min squeezes mass toward the floor so the long
// tail tops out near 1 s — matches production traces for healthy agents.
func healthyLatency(rng *rand.Rand) time.Duration {
	return triangular(rng, 100*time.Millisecond, 100*time.Millisecond, time.Second)
}

// slowAgentLatency models a degraded agent: avg ≈ 1 s, max 3 s.
func slowAgentLatency(rng *rand.Rand) time.Duration {
	return triangular(rng, 500*time.Millisecond, time.Second, 3*time.Second)
}

// gcsModerateLatency models an agent backed by slow object-storage:
// avg ≈ 700 ms, max ≈ 4 s. Built as healthy base + occasional burst so the
// tail stays heavy rather than smearing across all requests.
func gcsModerateLatency(rng *rand.Rand) time.Duration {
	return withBurst(healthyLatency, 0.10, 3*time.Second)(rng)
}

// gcsBadLatency models a severely degraded agent: avg ≈ 2.5 s, max ≈ 10 s.
// A quarter of all requests hit a multi-second burst.
func gcsBadLatency(rng *rand.Rand) time.Duration {
	return withBurst(healthyLatency, 0.25, 9*time.Second)(rng)
}

// healthyBehaviors returns a baseline behaviors map where every agent runs
// at production-like healthy latency. Scenarios mutate a copy.
func healthyBehaviors() map[int32]brokerBehavior {
	out := map[int32]brokerBehavior{}
	for i := int32(0); i < integrationClusterSize; i++ {
		out[i] = brokerBehavior{latencyFn: healthyLatency}
	}
	return out
}

// behaviorsSwapper holds the currently-active per-broker behaviors and
// exposes atomic read/write. The kfake control function and the per-client
// latency hooks read through it, so a single Store call swaps the entire
// cluster's behavior between phases (warmup → scenario) without locking.
type behaviorsSwapper struct {
	p atomic.Pointer[map[int32]brokerBehavior]
}

func newBehaviorsSwapper(initial map[int32]brokerBehavior) *behaviorsSwapper {
	s := &behaviorsSwapper{}
	s.swap(initial)
	return s
}

func (s *behaviorsSwapper) swap(b map[int32]brokerBehavior) {
	s.p.Store(&b)
}

func (s *behaviorsSwapper) load() map[int32]brokerBehavior {
	if m := s.p.Load(); m != nil {
		return *m
	}
	return nil
}

// TestTriangular verifies that the triangular helper produces draws inside
// [min, max] with a mean close to the analytic (min+mode+max)/3. The bounds
// are loose enough to be stable across runs but tight enough to catch a
// regression where the distribution collapses or escapes its range (which
// is what an earlier hand-rolled sqrt approximation did).
func TestTriangular(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name           string
		min, mode, max time.Duration
	}{
		{"healthy", 250 * time.Millisecond, 500 * time.Millisecond, time.Second},
		{"slow", 500 * time.Millisecond, time.Second, 3 * time.Second},
		{"degenerate", 100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
		{"min-at-mode", 100 * time.Millisecond, 100 * time.Millisecond, time.Second},
		{"max-at-mode", 100 * time.Millisecond, time.Second, time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(1, 2))
			const n = 100_000
			var sum time.Duration
			for i := 0; i < n; i++ {
				d := triangular(rng, tc.min, tc.mode, tc.max)
				require.GreaterOrEqualf(t, d, tc.min, "draw below min (i=%d)", i)
				require.LessOrEqualf(t, d, tc.max, "draw above max (i=%d)", i)
				sum += d
			}
			mean := sum / n
			expected := (tc.min + tc.mode + tc.max) / 3
			tolerance := time.Duration(0.05 * float64(tc.max-tc.min))
			if tolerance < time.Millisecond {
				tolerance = time.Millisecond
			}
			require.InDeltaf(t, float64(expected), float64(mean), float64(tolerance),
				"empirical mean %v outside %v±%v", mean, expected, tolerance)
		})
	}
}

func TestWithBurst_AddsExpectedExtraLatency(t *testing.T) {
	t.Parallel()
	base := func(*rand.Rand) time.Duration { return 100 * time.Millisecond }
	burst := withBurst(base, 0.5, 500*time.Millisecond)

	rng := rand.New(rand.NewPCG(7, 9))
	const n = 20_000
	var sum, bursts time.Duration
	for i := 0; i < n; i++ {
		d := burst(rng)
		sum += d
		if d > 100*time.Millisecond {
			bursts++
		}
	}
	mean := sum / n
	// Expected mean = 100ms + 0.5 * 500ms = 350ms; allow ±10ms.
	require.InDelta(t, float64(350*time.Millisecond), float64(mean), float64(10*time.Millisecond),
		"empirical mean %v not near 350ms", mean)
	// Expected burst rate ≈ 50%, allow ±2%.
	rate := float64(bursts) / float64(n)
	require.InDelta(t, 0.5, rate, 0.02, "empirical burst rate %.3f not near 0.5", rate)
}

func TestHealthyBehaviors_OneEntryPerBroker(t *testing.T) {
	t.Parallel()
	b := healthyBehaviors()
	require.Equal(t, int(integrationClusterSize), len(b))
	for i := int32(0); i < integrationClusterSize; i++ {
		got, ok := b[i]
		require.True(t, ok, "broker %d missing", i)
		require.NotNil(t, got.latencyFn, "broker %d has nil latencyFn", i)
	}
}

func TestBehaviorsSwapper_LoadReturnsLastStore(t *testing.T) {
	t.Parallel()
	first := map[int32]brokerBehavior{1: {fail: true}}
	s := newBehaviorsSwapper(first)
	loaded := s.load()
	require.Contains(t, loaded, int32(1))
	require.True(t, loaded[1].fail)

	second := map[int32]brokerBehavior{2: {fail: true}, 3: {failRate: 0.5}}
	s.swap(second)
	loaded = s.load()
	require.NotContains(t, loaded, int32(1))
	require.Contains(t, loaded, int32(2))
	require.Contains(t, loaded, int32(3))
	require.Equal(t, 0.5, loaded[3].failRate)
}
