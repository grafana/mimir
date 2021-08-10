// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/util/math/math.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package math

// Max returns the maximum of two ints
func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Min returns the minimum of two ints
func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Max64 returns the maximum of two int64s
func Max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// Min64 returns the minimum of two int64s
func Min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
