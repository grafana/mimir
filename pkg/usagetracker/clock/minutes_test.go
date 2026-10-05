// SPDX-License-Identifier: AGPL-3.0-only

package clock

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMinutes(t *testing.T) {
	// t0 is at 4-hour boundary
	t0 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	t1 := t0.Add(30 * time.Minute)
	t2 := t0.Add(-30 * time.Minute)

	t.Run("greaterThan", func(t *testing.T) {
		for _, tc := range []struct {
			a, b     Minutes
			expected bool
		}{
			{a: ToMinutes(t0), b: ToMinutes(t0.Add(-5 * time.Minute)), expected: true},
			{a: ToMinutes(t1), b: ToMinutes(t1.Add(-5 * time.Minute)), expected: true},
			{a: ToMinutes(t2), b: ToMinutes(t2.Add(-5 * time.Minute)), expected: true},
			{a: ToMinutes(t0), b: ToMinutes(t0.Add(5 * time.Minute)), expected: false},
			{a: ToMinutes(t1), b: ToMinutes(t1.Add(5 * time.Minute)), expected: false},
			{a: ToMinutes(t2), b: ToMinutes(t2.Add(5 * time.Minute)), expected: false},

			{a: ToMinutes(t0), b: ToMinutes(t0.Add(-59 * time.Minute)), expected: true},
			{a: ToMinutes(t1), b: ToMinutes(t1.Add(-59 * time.Minute)), expected: true},
			{a: ToMinutes(t2), b: ToMinutes(t2.Add(-59 * time.Minute)), expected: true},
			{a: ToMinutes(t0), b: ToMinutes(t0.Add(59 * time.Minute)), expected: false},
			{a: ToMinutes(t1), b: ToMinutes(t1.Add(59 * time.Minute)), expected: false},
			{a: ToMinutes(t2), b: ToMinutes(t2.Add(59 * time.Minute)), expected: false},
		} {
			t.Run(fmt.Sprintf("%s > %s = %t", tc.a, tc.b, tc.expected), func(t *testing.T) {
				require.Equal(t, tc.expected, tc.a.GreaterThan(tc.b))
			})
		}
	})

	t.Run("all valid values", func(t *testing.T) {
		for a := Minutes(0); a < 120; a++ {
			for b := Minutes(0); b < 120; b++ {
				// How many minutes a is ahead of b on the two-hour clock face.
				ahead := (int(a) - int(b) + 120) % 120
				require.Equal(t, ahead > 0 && ahead < 60, a.GreaterThan(b), "%s > %s", a, b)
				require.Equal(t, ahead < 60, a.GreaterOrEqualThan(b), "%s >= %s", a, b)
			}
		}
	})
}
