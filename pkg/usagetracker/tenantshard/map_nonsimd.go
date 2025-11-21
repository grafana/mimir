// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/bits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

package tenantshard

import (
	"math/bits"
	"unsafe"
)

const (
	groupSize = 8
	// maxAvgGroupLoad was 7 in dolthub/swiss, but we trade in some memory for less CPU by having to check less entries.
	maxAvgGroupLoad = 4

	loBits uint64 = 0x0101010101010101
	hiBits uint64 = 0x8080808080808080
)

type bitset uint64

// match searches the given index for whole bytes that match the given prefix.
func (m *index) match(p prefix) bitset {
	// See: https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
	// e.g.,
	// if m = 0x123456c9c9777777 and p = 0xc9, then:
	//               loBits * p == 0xc9c9c9c9c9c9c9c9
	// and B = m ^ (loBits * p) == 0x1234560000777777
	// then    findZeroBytes(B) == 0x0000008080000000
	return findZeroBytes(castUint64(m) ^ (loBits * uint64(p)))
}

func (m *index) matchEmpty() bitset {
	return findZeroBytes(castUint64(m))
}

// nextMatch clears and returns the index corresponding to the next set bit in
// the given bitset. It is assumed that the given bitset is nonzero.
func nextMatch(b *bitset) uint32 {
	s := uint32(bits.TrailingZeros64(uint64(*b)))
	*b &= ^(1 << s) // clear bit s+1 from the right.
	return s >> 3   // div by 8 to obtain index [0, 8).
}

// findZeroBytes locates all zero bytes in the given word. Their presence is
// indicated by the returned bitset: if the i'th bit is set, then the byte
// starting at the i'th bit is zero.
func findZeroBytes(x uint64) bitset {
	return bitset(((x - loBits) & ^(x)) & hiBits)
}

func castUint64(m *index) uint64 {
	return *(*uint64)((unsafe.Pointer)(m))
}
