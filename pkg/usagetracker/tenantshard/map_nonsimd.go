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

func metaMatchH2(idx uint64, p prefix) bitset {
	// https://graphics.stanford.edu/~seander/bithacks.html##ValueInWord
	return hasZeroByte(idx ^ (loBits * uint64(p)))
}

func metaMatchEmpty(m uint64) bitset {
	return hasZeroByte(m)
}

func nextMatch(b *bitset) uint32 {
	s := uint32(bits.TrailingZeros64(uint64(*b)))
	*b &= ^(1 << s) // clear bit |s|
	return s >> 3   // div by 8
}

func hasZeroByte(x uint64) bitset {
	return bitset(((x - loBits) & ^(x)) & hiBits)
}

func castUint64(p unsafe.Pointer, offset int) uint64 {
	return *(*uint64)(unsafe.Add(p, offset))
}
