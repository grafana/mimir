// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/math/rate_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package math

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRate(t *testing.T) {
	ticks := []struct {
		events int
		want   float64
	}{
		{60, 1},
		{30, 0.9},
		{0, 0.72},
		{60, 0.776},
		{0, 0.6208},
		{0, 0.49664},
		{0, 0.397312},
		{0, 0.3178496},
		{0, 0.25427968},
		{0, 0.203423744},
		{0, 0.1627389952},
	}
	r := NewEWMARate(0.2, time.Minute)

	for _, tick := range ticks {
		for e := 0; e < tick.events; e++ {
			r.Inc()
		}
		r.Tick()
		// We cannot do double comparison, because double operations on different
		// platforms may actually produce results that differ slightly.
		// There are multiple issues about this in Go's github, eg: 18354 or 20319.
		require.InDelta(t, tick.want, r.Rate(), 0.0000000001, "unexpected rate")
	}

	r = NewEWMARate(0.2, time.Minute)

	for _, tick := range ticks {
		r.Add(int64(tick.events))
		r.Tick()
		require.InDelta(t, tick.want, r.Rate(), 0.0000000001, "unexpected rate")
	}
}

func TestRateWithWarmup(t *testing.T) {
	ticks := []struct {
		events int64
		want   float64
	}{
		{120, 0},
		{0, 0},
		{60, 1},
		{120, 1.2},
	}
	r := NewEWMARateWithWarmup(0.2, time.Minute, 3)

	for _, tick := range ticks {
		r.Add(tick.events)
		r.Tick()
		require.InDelta(t, tick.want, r.Rate(), 0.0000000001, "unexpected rate")
	}
}

func TestRateWithZeroWarmupMatchesDefault(t *testing.T) {
	defaultRate := NewEWMARate(0.2, time.Minute)
	zeroWarmupRate := NewEWMARateWithWarmup(0.2, time.Minute, 0)

	for _, events := range []int64{60, 30, 0, 60} {
		defaultRate.Add(events)
		zeroWarmupRate.Add(events)
		defaultRate.Tick()
		zeroWarmupRate.Tick()

		require.InDelta(t, defaultRate.Rate(), zeroWarmupRate.Rate(), 0.0000000001)
	}
}
