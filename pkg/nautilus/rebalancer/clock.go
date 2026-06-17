// SPDX-License-Identifier: AGPL-3.0-only

package rebalancer

import "time"

// Clock is the time source consulted by the rebalancer for every
// decision that depends on "now": lease horizons, expiry checks,
// move cooldowns, round scheduling, and the timestamps recorded on
// new log entries. Production uses wallClock; tests substitute a
// controlled clock so a multi-round timeline can be driven
// deterministically without sleeping.
//
// Keep the surface minimal: only Now(). Round scheduling in
// running() uses a real time.Timer (not the Clock), so the loop
// stays decoupled from the rebalance-decision time source. Tests
// that exercise the loop call rebalance() directly with a fake
// Clock instead of running().
type Clock interface {
	Now() time.Time
}

// wallClock is the production Clock: time.Now() with no
// instrumentation. Constructed by default in New.
type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// now returns the current time per the rebalancer's injected Clock,
// falling back to wall time if no Clock is wired. The fallback
// exists so the many tests that construct &Rebalancer{...} directly
// (without going through New) don't need to also stamp in a clock
// just to call methods that consult one. New always installs
// wallClock{} explicitly; this is purely a safety net.
func (r *Rebalancer) now() time.Time {
	if r.clock == nil {
		return time.Now()
	}
	return r.clock.Now()
}
