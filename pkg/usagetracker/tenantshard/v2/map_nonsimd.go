// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/bits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

//go:build !amd64.v3 || nosimd

package v2

import (
	"math/bits"
	"unsafe"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

const (
	groupSize = 8

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

// matchEmptyOrSpillmark searches the given index for slots that hold no data, i.e. that are either
// empty or hold a spillmark. Clearing the low bit of every byte maps both marks to zero.
// Note that this cannot be written on top of a plain zero-byte search: findZeroBytes reports a byte
// that holds 1 as zero when a borrow reaches it from a zero byte below, so a spillmark that follows
// an empty slot would be indistinguishable from an occupied one.
func (m *index) matchEmptyOrSpillmark() bitset {
	// TODO: see if we can optimize this, this likely overlaps with findZeroBytes logic.
	return findZeroBytes(castUint64(m) & (^loBits))
}

// matchOccupied matches the slots that hold data, i.e. neither empty nor spillmarks.
func (m *index) matchOccupied() bitset {
	return m.matchEmptyOrSpillmark() ^ bitset(hiBits)
}

// cleanupGroup removes the entries of the group that expired at watermark, and returns how many it removed.
func cleanupGroup(idx *index, d *data, watermark clock.Minutes) int {
	removed := 0
	occupied := idx.matchOccupied()
	for occupied != 0 {
		j := nextMatch(&occupied)
		if watermark.GreaterOrEqualThan(d[j].clockMinutes()) {
			removed++

			if j == last {
				// This is the last element, if it was previously set,
				// then group may have spilled to the next one.
				// We need to keep that signal, so we leave a spillmark here.
				d[j] = spillmark
				// We need to leave spillmark in the data because that's what iterator uses.
				idx[j] = spillmark
				// We don't need to touch the keys, because nobody will read them if index/data is a spillmark.
				// Keys are groups of uint64 that utilize an entire cache line, better to avoid touching them.
			} else {
				// This is not the last element, so just mark it as empty.
				d[j] = empty
				idx[j] = empty
			}
		}
	}
	return removed
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
