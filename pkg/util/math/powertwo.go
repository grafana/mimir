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

// IsPowerOfTwo will return true if the given value is a power of 2.
// Note that 0 is not a power of 2, but 1 is a valid power of 2.
//
// Note - this is not a drop in replacement for pool.IsPowerOfTwo(),
// as this over function will return true for 0
func IsPowerOfTwo(n int) bool {
	if n == 0 {
		return false
	}
	return (n & (n - 1)) == 0
}
