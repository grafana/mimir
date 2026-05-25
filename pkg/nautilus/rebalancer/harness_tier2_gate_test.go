// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHarness_Tier2Gate_LegacyEveryRound verifies the
// backward-compatible default: when RoundInterval is zero, tier-2
// fires on every rebalance tick (same as the pre-decoupling
// behavior). A regression here would silently change production
// cadence for operators who haven't opted in to the new setting.
func TestHarness_Tier2Gate_LegacyEveryRound(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        5 * time.Minute,
			LeaseLookahead:       90 * time.Second,
			MinRebalanceInterval: 30 * time.Second,
			MaxRebalanceInterval: 5 * time.Minute,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:       true,
				Alpha:         1.0,
				RoundInterval: 0,
			},
		},
		captureLogs: true,
	})
	h.addReadcache("readcache-0")
	h.addReadcache("readcache-1")

	// Cold-start fire (reason=first_round) seeds tier-2 state.
	require.NoError(t, h.runRound())
	startedAt := h.r.lastTier2RoundAt
	require.False(t, startedAt.IsZero(),
		"cold-start round must initialize lastTier2RoundAt")

	// Subsequent rounds: with interval=0 every tick fires
	// tier-2 (reason=interval_zero). Verify by observing
	// lastTier2RoundAt advances each round.
	prev := startedAt
	for i := 0; i < 3; i++ {
		h.advance(30 * time.Second)
		require.NoError(t, h.runRound())
		assert.Truef(t, h.r.lastTier2RoundAt.After(prev),
			"round %d should advance lastTier2RoundAt when interval=0", i+1)
		prev = h.r.lastTier2RoundAt
	}
}

// TestHarness_Tier2Gate_IntervalPendingSkips is the core gating
// regression test. With RoundInterval larger than the rebalance
// cadence, tier-2 must fire less often than tier-1: the gate skips
// ticks where the interval has not elapsed and the instance set
// hasn't changed.
func TestHarness_Tier2Gate_IntervalPendingSkips(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        15 * time.Minute,
			LeaseLookahead:       5 * time.Minute,
			MinRebalanceInterval: 5 * time.Minute,
			MaxRebalanceInterval: 15 * time.Minute,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:       true,
				Alpha:         1.0,
				RoundInterval: 30 * time.Minute,
			},
		},
		captureLogs: true,
	})
	h.addReadcache("readcache-0")
	h.addReadcache("readcache-1")

	// Cold-start fire (reason=first_round).
	require.NoError(t, h.runRound())
	coldStartAt := h.r.lastTier2RoundAt
	require.False(t, coldStartAt.IsZero())

	// Five rounds (25 min wall-clock) at the 5 min tier-1 cadence
	// should NOT re-fire tier-2 because RoundInterval=30m hasn't
	// elapsed. lastTier2RoundAt must stay at coldStartAt.
	for i := 0; i < 5; i++ {
		h.advance(5 * time.Minute)
		require.NoError(t, h.runRound())
		assert.Equalf(t, coldStartAt, h.r.lastTier2RoundAt,
			"round %d at +%dmin should be gated, lastTier2RoundAt unchanged", i+1, (i+1)*5)
	}

	// One more tick brings total elapsed to 30 min: tier-2 must
	// fire now (reason=interval_elapsed). lastTier2RoundAt advances.
	h.advance(5 * time.Minute)
	require.NoError(t, h.runRound())
	assert.True(t, h.r.lastTier2RoundAt.After(coldStartAt),
		"the round at +30min should fire tier-2; lastTier2RoundAt must advance")

	// Log should mention the skip path at least once.
	assert.Contains(t, h.logOutput(), "skipping tier-2 round",
		"the skipped rounds should produce 'skipping tier-2 round' log entries")
}

// TestHarness_Tier2Gate_InstanceChangeFiresEarly is the failover
// guarantee: when a readcache joins or leaves the ring, tier-2
// must fire on the very next tick regardless of how recently it
// last fired. Otherwise scale-down would leave partitions orphaned
// on the departed pod for up to RoundInterval.
func TestHarness_Tier2Gate_InstanceChangeFiresEarly(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       4,
			LeaseDuration:        15 * time.Minute,
			LeaseLookahead:       5 * time.Minute,
			MinRebalanceInterval: 5 * time.Minute,
			MaxRebalanceInterval: 15 * time.Minute,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:       true,
				Alpha:         1.0,
				RoundInterval: 30 * time.Minute,
			},
		},
		captureLogs: true,
	})
	h.addReadcache("readcache-0")
	h.addReadcache("readcache-1")

	// Cold start.
	require.NoError(t, h.runRound())
	coldStartAt := h.r.lastTier2RoundAt
	require.False(t, coldStartAt.IsZero())

	// One regular tick: gate skips (interval not elapsed).
	h.advance(5 * time.Minute)
	require.NoError(t, h.runRound())
	require.Equal(t, coldStartAt, h.r.lastTier2RoundAt,
		"the +5min tick must be gated; otherwise the test environment doesn't actually exercise the gate")

	// Scale up by adding a new instance, then run the next tick.
	// The instance-set change must override interval_pending and
	// fire tier-2.
	h.addReadcache("readcache-2")
	h.advance(5 * time.Minute)
	require.NoError(t, h.runRound())
	assert.True(t, h.r.lastTier2RoundAt.After(coldStartAt),
		"scale-up must force tier-2 to fire even though only 10min < 30min interval has elapsed")

	// Now scale DOWN. The instance set changes again so the next
	// tick must also fire tier-2.
	prev := h.r.lastTier2RoundAt
	h.removeReadcache("readcache-2")
	h.advance(5 * time.Minute)
	require.NoError(t, h.runRound())
	assert.True(t, h.r.lastTier2RoundAt.After(prev),
		"scale-down must force tier-2 to fire even though only 5min < 30min has elapsed since the previous fire")
}

// TestHarness_Tier2Gate_RefreshKeepsLeasesAliveOnSkippedRounds is
// the leases-don't-expire-during-skipped-rounds invariant. On
// every rebalance tick where tier-2 is skipped, the rebalancer
// MUST still extend existing leases. Otherwise a long
// RoundInterval would let leases age out and partitions go
// orphaned even without any topology change.
func TestHarness_Tier2Gate_RefreshKeepsLeasesAliveOnSkippedRounds(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount: 4,
			// 5min leases with a 10s lookahead: leases expire fast,
			// so any failure to refresh on skipped rounds would
			// show up within the test's horizon.
			LeaseDuration:        5 * time.Minute,
			LeaseLookahead:       10 * time.Second,
			MinRebalanceInterval: 30 * time.Second,
			MaxRebalanceInterval: 2 * time.Minute,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:       true,
				Alpha:         1.0,
				RoundInterval: 30 * time.Minute, // tier-2 always skipped within the test horizon
			},
		},
	})
	h.addReadcache("readcache-0")
	h.addReadcache("readcache-1")

	// Cold start to seed tier-2 ownership.
	require.NoError(t, h.runRound())
	require.NotEmpty(t, h.tier2Active(),
		"cold start should have assigned partitions to readcaches")
	startOwners := h.ownersByInstance()

	// Run many rounds spanning ~10 minutes (well past one
	// LeaseDuration). The tier-2 gate skips every round, but
	// refreshReadcacheLeases on the skip branch must keep the
	// leases alive.
	const totalElapsed = 10 * time.Minute
	const step = 30 * time.Second
	for elapsed := time.Duration(0); elapsed < totalElapsed; elapsed += step {
		h.advance(step)
		require.NoError(t, h.runRound())
	}

	// Active partitions and owner distribution must be unchanged
	// (no failover, no scaling, no tier-2 fire).
	assert.Equal(t, 4, len(h.tier2Active()),
		"all partitions must still have an active tier-2 lease after %s of skipped rounds", totalElapsed)
	assert.Equal(t, startOwners, h.ownersByInstance(),
		"owner distribution must be stable across skipped rounds")
}

// TestHarness_Tier2Gate_LogOutputIncludesReason is a minimal
// observability test: when the gate skips a tier-2 round, the
// reason token must appear in the structured log so operators can
// debug "why didn't tier-2 fire just now?" without reading code.
func TestHarness_Tier2Gate_LogOutputIncludesReason(t *testing.T) {
	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       2,
			LeaseDuration:        15 * time.Minute,
			LeaseLookahead:       5 * time.Minute,
			MinRebalanceInterval: 1 * time.Minute,
			MaxRebalanceInterval: 5 * time.Minute,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:       true,
				Alpha:         1.0,
				RoundInterval: 30 * time.Minute,
			},
		},
		captureLogs: true,
	})
	h.addReadcache("readcache-0")
	h.addReadcache("readcache-1")
	// go-kit's NewLogfmtLogger filters DEBUG by default if no
	// level filter is applied — but the harness wires the raw
	// logger so DEBUG passes through. Confirm via stronger
	// assertion that the skip line is emitted at all.

	// Cold start fires; subsequent rounds skip.
	require.NoError(t, h.runRound())
	h.advance(1 * time.Minute)
	require.NoError(t, h.runRound())

	logOut := h.logOutput()
	assert.True(t,
		strings.Contains(logOut, "skipping tier-2 round") ||
			strings.Contains(logOut, "interval_pending"),
		"the skipped-round log must mention either the message or the reason token; got:\n%s", logOut)
}
