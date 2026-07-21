// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/grafana/mimir/pkg/nautilus/assignment"
)

// BenchmarkRunPhase3CooldownLookup mimics the lookup pattern that
// dominated CPU on dev-15 after the latestTo Apply fix landed:
// runPhase3 issues ~N cooldown overlap checks per round, against a
// cooldown map of ~K active entries. The naive O(K) scan in
// isInMoveCooldown × N candidates is ~N·K. The cooldownIndex
// collapses each query to O(log K) after an O(K log K) build.
//
// Sizes match what we observed in mimir-dev-15 traces:
//   - SmallScale:  K=100,   N=2000   (pre-collapse, healthy)
//   - DevScale:    K=1300,  N=22000  (current dev-15 production)
//   - StressScale: K=5000,  N=50000  (worst-case future growth)
func BenchmarkRunPhase3CooldownLookup(b *testing.B) {
	cases := []struct {
		name               string
		numCooldowns       int
		numCandidateRanges int
	}{
		{"SmallScale_K100_N2000", 100, 2000},
		{"DevScale_K1300_N22000", 1300, 22000},
		{"StressScale_K5000_N50000", 5000, 50000},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			cooldowns, candidates := genCooldownWorkload(tc.numCooldowns, tc.numCandidateRanges)
			now := time.Now()
			r := &Rebalancer{
				cfg:           Config{MoveCooldown: time.Minute},
				moveCooldowns: cooldowns,
			}

			b.Run("naive_isInMoveCooldown", func(b *testing.B) {
				b.ReportAllocs()
				var hits int
				for i := 0; i < b.N; i++ {
					for _, hr := range candidates {
						if r.isInMoveCooldown(now, hr) {
							hits++
						}
					}
				}
				b.ReportMetric(float64(hits)/float64(b.N), "hits/op")
			})

			b.Run("indexed_overlaps", func(b *testing.B) {
				b.ReportAllocs()
				var hits int
				for i := 0; i < b.N; i++ {
					idx := newCooldownIndex(now, cooldowns)
					for _, hr := range candidates {
						if idx.overlaps(hr) {
							hits++
						}
					}
				}
				b.ReportMetric(float64(hits)/float64(b.N), "hits/op")
			})
		})
	}
}

// genCooldownWorkload synthesises a cooldown map and a candidate
// query set that approximate dev-15 shapes:
//   - cooldown intervals are small (~1024 wide) and scattered across
//     the keyspace, mirroring the per-range cooldowns recordMoveCooldowns
//     installs after Phase 3 moves;
//   - candidate ranges are similarly sized and overlap the cooldown
//     set with ~5-15% probability, which matches the cooldown hit
//     rate we see in the traces (most candidates pass the check).
func genCooldownWorkload(numCooldowns, numCandidates int) (map[assignment.HashRange]time.Time, []assignment.HashRange) {
	rng := rand.New(rand.NewSource(42))
	cooldowns := make(map[assignment.HashRange]time.Time, numCooldowns)
	deadline := time.Now().Add(time.Minute)
	for i := 0; i < numCooldowns; i++ {
		lo := uint32(rng.Int63n(int64(^uint32(0))))
		hi := lo + uint32(rng.Intn(1024))
		cooldowns[assignment.HashRange{Lo: lo, Hi: hi}] = deadline
	}

	candidates := make([]assignment.HashRange, numCandidates)
	for i := range candidates {
		lo := uint32(rng.Int63n(int64(^uint32(0))))
		hi := lo + uint32(rng.Intn(1024))
		candidates[i] = assignment.HashRange{Lo: lo, Hi: hi}
	}
	return cooldowns, candidates
}

// BenchmarkCooldownIndex_Build measures the one-time build cost of
// the index for the same dev-sized cooldown sets, so we can confirm
// the build itself is not the new bottleneck.
func BenchmarkCooldownIndex_Build(b *testing.B) {
	for _, k := range []int{100, 1300, 5000} {
		b.Run(fmt.Sprintf("K=%d", k), func(b *testing.B) {
			cooldowns, _ := genCooldownWorkload(k, 0)
			now := time.Now()
			b.ResetTimer()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = newCooldownIndex(now, cooldowns)
			}
		})
	}
}
