// SPDX-License-Identifier: AGPL-3.0-only

//go:build (!amd64 && !arm64) || nosimd

package tenantshard

import "unsafe"

// simdImpl names the scanExpired implementation compiled into this binary. It is only used
// for reporting in tests and benchmarks.
const simdImpl = "generic"

// scanExpired returns the index of the first group in p[:n] that holds at least one
// occupied, expired slot, or n when there is none.
func scanExpired(p *data, n int, lo, length uint8) int {
	for i, d := range unsafe.Slice(p, n) {
		if d.matchExpired(lo, length) != 0 {
			return i
		}
	}
	return n
}
