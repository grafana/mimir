// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import "time"

// tier2GateDecision is the result of deciding whether the tier-2
// readcache slicer should fire on this rebalance tick.
type tier2GateDecision struct {
	// fire is true when the tier-2 round should run.
	fire bool
	// reason is a short structured-log token describing why fire
	// took the value it did. Always populated.
	reason string
}

// shouldFireTier2 returns the tier-2 gate decision for a single
// rebalance tick. Inputs are:
//
//   - interval:        ReadcacheSlicer.RoundInterval, the minimum
//     wall-clock spacing between tier-2 fires.
//     Zero disables gating: every tick fires.
//   - now:             the wall-clock time of this rebalance tick.
//   - lastFireAt:      the wall-clock time of the most recent
//     successful tier-2 fire (zero if never).
//   - currentInstances: the active readcache instance set this tick,
//     already sorted (activeReadcacheInstances
//     sorts its output).
//   - lastInstances:   the instance set as of the most recent fire
//     (the value the caller will overwrite once
//     fire is true), already sorted.
//
// Behavior:
//
//   - First fire (lastFireAt is zero) always returns fire=true with
//     reason "first_round". Ensures cold-start cluster gets a
//     tier-2 round on the first tick.
//   - Instance-set change vs lastInstances returns fire=true with
//     reason "instances_changed". Failover and scale events must
//     not wait for the interval — orphaned partitions would have no
//     readcache owner until the interval elapsed.
//   - interval == 0 returns fire=true with reason "interval_zero"
//     (legacy / opt-out behavior).
//   - Otherwise fire=true iff now.Sub(lastFireAt) >= interval, with
//     reason "interval_elapsed"; false with reason
//     "interval_pending" when the interval has not elapsed.
//
// The decision is monotonic in the interval-elapsed branch: once
// fire=true, the caller must update lastFireAt before the next
// shouldFireTier2 call, otherwise it would fire again on every
// subsequent tick.
func shouldFireTier2(
	interval time.Duration,
	now time.Time,
	lastFireAt time.Time,
	currentInstances []string,
	lastInstances []string,
) tier2GateDecision {
	if lastFireAt.IsZero() {
		return tier2GateDecision{fire: true, reason: "first_round"}
	}
	if !sortedSlicesEqual(currentInstances, lastInstances) {
		return tier2GateDecision{fire: true, reason: "instances_changed"}
	}
	if interval <= 0 {
		return tier2GateDecision{fire: true, reason: "interval_zero"}
	}
	if now.Sub(lastFireAt) >= interval {
		return tier2GateDecision{fire: true, reason: "interval_elapsed"}
	}
	return tier2GateDecision{fire: false, reason: "interval_pending"}
}

// sortedSlicesEqual reports whether two pre-sorted string slices
// contain the same elements in the same order. Cheaper than
// constructing a set and matches the order activeReadcacheInstances
// emits.
func sortedSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
