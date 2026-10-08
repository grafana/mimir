// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/dolthub/swiss/blob/master/bits.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: Dolthub, Inc.

package v2

import (
	"math/bits"
	"unsafe"
)

const (
	groupSize = 8

	loBits uint64 = 0x0101010101010101
	hiBits uint64 = 0x8080808080808080

	// spillmarkInLastSlot is a group with a spillmark in the last slot and empty slots everywhere else:
	// what clearSlots writes into the slots it removes.
	spillmarkInLastSlot uint64 = spillmark << (8 * last)
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

// expiredSlots returns a word with the high bit set in each byte of the data group x whose value expired, that is
// for which watermark.GreaterOrEqualThan(value) holds. w is the watermark in every byte: loBits * watermark.
// Empty and spillmark bytes may be reported too, so the caller must remove freeSlots from the result.
//
// It computes GreaterOrEqualThan for the 8 slots at once, with the same steps as clock.Minutes: each byte of
// the uint64 is one slot, and the arithmetic never lets a byte carry or borrow into the next one.
// Go computes aheadOf in int64 and the bytes wrap at 256 instead, but aheadOf is always in [-135, 255],
// and wrapping only moves [-135, -1] to [121, 255], which is not below 60 either. So the result is the
// same as GreaterOrEqualThan for every watermark and every value.
func expiredSlots(x, w uint64) uint64 {
	v := ^x // The clock.Minutes of every slot, see xorData.

	// d := watermark - v. Setting the high bit of each byte of w and clearing it in v means that no byte
	// borrows from the next one. The xor then restores the high bits. Note that ^v is x.
	d := ((w | hiBits) - (v &^ hiBits)) ^ ((w ^ x) & hiBits)
	// d < 0, that is watermark < v: the borrow out of the high bit of each byte.
	negative := ((^w & v) | (^(w ^ v) & d)) & hiBits
	// aheadOf := d + (d>>63)&120. 120 fits in the low 7 bits, so adding it to the low 7 bits of each
	// byte never carries into the next byte, and the xor adds the high bits back.
	aheadOf := ((d &^ hiBits) + (negative>>7)*120) ^ (d & hiBits)

	// aheadOf < 60: adding 128-60 to the low 7 bits sets the high bit when they are 60 or more,
	// and a byte that already has its high bit set is 128 or more.
	return ^(((aheadOf &^ hiBits) + loBits*(128-60)) | aheadOf) & hiBits
}

// freeSlots returns a word with the high bit set in each byte of the data group x that holds no value,
// that is an empty slot or a spillmark. See matchEmptyOrSpillmark for why this is exact.
// The data and the index always agree on which slots these are.
func freeSlots(x uint64) uint64 {
	return uint64(findZeroBytes(x &^ loBits))
}

// clearSlots removes the slots of the group that have the high bit set in remove, and returns how many it removed.
// x is the data group as it was loaded. Removed slots become empty in both idx and d, except for the last one,
// which becomes a spillmark: it keeps the signal that the group may have spilled into the next one.
// Keys are not touched: nobody reads the key of a slot whose index and data are empty or a spillmark,
// and keys are groups of uint64 that fill a whole cache line.
func clearSlots(idx *index, d *data, x, remove uint64) int {
	mask := (remove >> 7) * 0xff // 0xff in every byte to remove.
	setUint64Data(d, x&^mask|spillmarkInLastSlot&mask)
	setUint64(idx, castUint64(idx)&^mask|spillmarkInLastSlot&mask)
	return bits.OnesCount64(remove)
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

func setUint64(m *index, v uint64) {
	*(*uint64)((unsafe.Pointer)(m)) = v
}

func castUint64Data(d *data) uint64 {
	return *(*uint64)((unsafe.Pointer)(d))
}

func setUint64Data(d *data, v uint64) {
	*(*uint64)((unsafe.Pointer)(d)) = v
}
