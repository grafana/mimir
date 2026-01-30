// SPDX-License-Identifier: AGPL-3.0-only

package math

import (
	"math/bits"
)

// NextPowerTwo will return the next int greater than or equal to the given value which is a power of 2.
// eg 1 returns 2, 3 returns 4, 4 returns 4, 5 returns 8.
// Any value <= 1 will return 2.
func NextPowerTwo(n int) int {
	if n <= 1 {
		return 2
	}
	u := uint(n - 1)
	return 1 << bits.Len(u)
}
