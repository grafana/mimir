// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestReadcacheCoverageStaysFullAcrossRoundInterval is the dev-scale
// regression for the tier-2 lease-decay bug: 300 partitions, 36
// readcaches, RoundInterval=30m (tier-2 fires only at the interval
// boundary; every other wakeup is refresh-only). It ticks at the REAL
// nextRoundDelay cadence (driven by the tier-1 hash lease horizon) for
// past one full RoundInterval.
//
// Before the fix the refresh path used the raw LeaseLookahead and an
// ActiveAt(now)-only rebuild, so leases whose pre-issue window fell
// between two wakeups lapsed and were dropped permanently — coverage
// decayed below the partition count mid-interval ("stopped consuming
// then caught up" dips). With readcacheLeaseLookahead padding the
// lookahead by one tick and refreshReadcacheLeases reviving lapsed
// partitions, coverage must stay at numPartitions the whole time.
func TestReadcacheCoverageStaysFullAcrossRoundInterval(t *testing.T) {
	const numPartitions = 300
	const numInstances = 36

	h := newHarness(t, harnessOpts{
		cfg: Config{
			PartitionCount:       numPartitions,
			LeaseDuration:        5 * time.Minute,
			LeaseLookahead:       90 * time.Second,
			MinRebalanceInterval: 30 * time.Second,
			MaxRebalanceInterval: 5 * time.Minute,
			EntryRetention:       24 * time.Hour,
			ReadcacheSlicer: ReadcacheSlicerConfig{
				Enabled:        true,
				Alpha:          1.0,
				MovementBudget: 0.5,
				RoundInterval:  30 * time.Minute,
			},
		},
	})
	for i := 0; i < numInstances; i++ {
		h.addReadcache(fmt.Sprintf("readcache-%d", i))
	}

	// Cold-start fire seeds tier-2 ownership.
	require.NoError(t, h.runRound())
	require.Equal(t, numPartitions, len(h.tier2Active()),
		"cold start must cover every partition")
	start := h.clock.Now()

	// Run wakeups at the real nextRoundDelay cadence for 45 simulated
	// minutes (past one 30m RoundInterval), asserting full coverage at
	// every tick so any mid-interval decay is caught at the moment it
	// happens rather than only at the end.
	const horizon = 45 * time.Minute
	for h.clock.Now().Sub(start) < horizon {
		h.advance(h.r.nextRoundDelay(h.clock.Now()))
		require.NoError(t, h.runRound())
		require.Equalf(t, numPartitions, len(h.tier2Active()),
			"coverage decayed at sim-min %.1f: tier-2 must keep every partition leased across the RoundInterval",
			h.clock.Now().Sub(start).Minutes())
	}
}
