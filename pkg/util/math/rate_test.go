// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/math/rate_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package math

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func TestNewEWMARateFromWindow(t *testing.T) {
	var samples = [100]float64{
		4599, 5711, 4746, 4621, 5037, 4218, 4925, 4281, 5207, 5203, 5594, 5149,
		4948, 4994, 6056, 4417, 4973, 4714, 4964, 5280, 5074, 4913, 4119, 4522,
		4631, 4341, 4909, 4750, 4663, 5167, 3683, 4964, 5151, 4892, 4171, 5097,
		3546, 4144, 4551, 6557, 4234, 5026, 5220, 4144, 5547, 4747, 4732, 5327,
		5442, 4176, 4907, 3570, 4684, 4161, 5206, 4952, 4317, 4819, 4668, 4603,
		4885, 4645, 4401, 4362, 5035, 3954, 4738, 4545, 5433, 6326, 5927, 4983,
		5364, 4598, 5071, 5231, 5250, 4621, 4269, 3953, 3308, 3623, 5264, 5322,
		5395, 4753, 4936, 5315, 5243, 5060, 4989, 4921, 4480, 3426, 3687, 4220,
		3197, 5139, 6101, 5279,
	}

	const window = 60
	e := NewEWMARateFromWindow(window, time.Second)
	// Warm up the ewma
	for i := 0; i < window; i++ {
		e.Tick()
	}
	for _, f := range samples {
		e.Add(int64(f * 100))
		e.Tick()
	}

	const exp = 4584.616105524594
	assert.InDelta(t, exp, e.Rate()/100, 0.00000001)
}
