// SPDX-License-Identifier: AGPL-3.0-only

//go:build amd64 && !nosimd

package tenantshard

// simdImpl names the scanExpired implementation compiled into this binary. It is only used
// for reporting in tests and benchmarks.
const simdImpl = "sse2"

// scanExpired returns the index of the first group in p[:n] that holds at least one
// occupied, expired slot, or n when there is none. See matchExpiredWord for the predicate
// it evaluates, and expiry_amd64.s for the implementation.
//
//go:noescape
func scanExpired(p *data, n int, lo, length uint8) int
