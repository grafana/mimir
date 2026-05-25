// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShouldFireTier2_FirstRoundAlwaysFires(t *testing.T) {
	d := shouldFireTier2(30*time.Minute, time.Unix(1000, 0), time.Time{}, []string{"a"}, nil)
	assert.True(t, d.fire)
	assert.Equal(t, "first_round", d.reason)
}

func TestShouldFireTier2_FirstRoundFiresEvenWhenIntervalZero(t *testing.T) {
	// interval zero is the legacy "every tick" mode; first round
	// still gets the unambiguous "first_round" reason rather than
	// "interval_zero" so observability separates startup from steady.
	d := shouldFireTier2(0, time.Unix(1000, 0), time.Time{}, nil, nil)
	assert.True(t, d.fire)
	assert.Equal(t, "first_round", d.reason)
}

func TestShouldFireTier2_IntervalNotElapsed(t *testing.T) {
	now := time.Unix(1000, 0)
	last := now.Add(-5 * time.Minute)
	d := shouldFireTier2(30*time.Minute, now, last, []string{"a"}, []string{"a"})
	assert.False(t, d.fire)
	assert.Equal(t, "interval_pending", d.reason)
}

func TestShouldFireTier2_IntervalElapsed(t *testing.T) {
	now := time.Unix(1000, 0)
	last := now.Add(-30 * time.Minute)
	d := shouldFireTier2(30*time.Minute, now, last, []string{"a"}, []string{"a"})
	assert.True(t, d.fire)
	assert.Equal(t, "interval_elapsed", d.reason)
}

func TestShouldFireTier2_InstancesChangedOverridesPending(t *testing.T) {
	// Failover scenario: pod left the ring 1 minute after the last
	// tier-2 round. Interval has not elapsed but we must fire now
	// to re-home the orphaned partitions.
	now := time.Unix(1000, 0)
	last := now.Add(-1 * time.Minute)
	d := shouldFireTier2(30*time.Minute, now, last, []string{"a"}, []string{"a", "b"})
	assert.True(t, d.fire)
	assert.Equal(t, "instances_changed", d.reason)
}

func TestShouldFireTier2_InstancesAddedOverridesPending(t *testing.T) {
	// Scale-up: a new readcache joined the ring; spread load to it
	// now rather than waiting up to RoundInterval.
	now := time.Unix(1000, 0)
	last := now.Add(-1 * time.Minute)
	d := shouldFireTier2(30*time.Minute, now, last, []string{"a", "b", "c"}, []string{"a", "b"})
	assert.True(t, d.fire)
	assert.Equal(t, "instances_changed", d.reason)
}

func TestShouldFireTier2_SameInstancesIntervalZero(t *testing.T) {
	// Legacy mode (interval==0): every tick fires once we've had a
	// first round, with the explicit "interval_zero" reason.
	now := time.Unix(1000, 0)
	last := now.Add(-1 * time.Second)
	d := shouldFireTier2(0, now, last, []string{"a"}, []string{"a"})
	assert.True(t, d.fire)
	assert.Equal(t, "interval_zero", d.reason)
}

func TestShouldFireTier2_IdenticalInstanceOrderRequired(t *testing.T) {
	// The function relies on activeReadcacheInstances returning a
	// sorted slice; if a caller misuses it with unsorted slices
	// the comparison can spuriously trigger. Document that contract
	// here via assertion.
	now := time.Unix(1000, 0)
	last := now.Add(-1 * time.Minute)
	d := shouldFireTier2(30*time.Minute, now, last, []string{"b", "a"}, []string{"a", "b"})
	assert.True(t, d.fire, "unsorted inputs are reported as changed; callers must pre-sort")
	assert.Equal(t, "instances_changed", d.reason)
}
