// SPDX-License-Identifier: AGPL-3.0-only

package tenantshard

import (
	"math/bits"
	"unsafe"

	"github.com/grafana/mimir/pkg/usagetracker/clock"
)

// Finding the entries to evict during Cleanup used to be a per-slot loop with three
// unpredictable branches per slot, which dominated the cost of Cleanup. This file turns
// that test into a branchless, whole-group operation, so that groups holding nothing to
// evict are skipped without ever branching per slot.
//
// # Turning the clock comparison into a range check
//
// An entry is evicted when watermark.GreaterOrEqualThan(value) holds, where value is
// clock.Minutes in [0, 120) and watermark is also in [0, 120). Expanding
// clock.Minutes.GreaterThan, that condition is (watermark-value) mod 120 < 60, so the
// values that expire are the 60 consecutive minutes ending at the watermark, wrapping
// around the 120 minute clock face.
//
// Slots hold the value xor-ed (see xorData), that is x = 255-value, so the expiring
// values map to the byte range starting at lo = 255-watermark and running upwards,
// wrapping at 256. Two cases:
//
//   - watermark >= 59: the minutes do not wrap, and x lands in [lo, lo+59], entirely
//     inside [136, 255].
//   - watermark < 59: the minutes wrap, and x lands in [lo, 255] plus [136, 194-watermark].
//     Going up from lo and wrapping at 256, those two pieces are joined by [0, 135], which
//     holds no valid value, so the whole thing is still one range: [lo, lo+195].
//
// Both cases are therefore "uint8(x-lo) <= length", differing only in length. The second
// case sweeps up the empty (0) and tombstone (1) markers as a side effect, so the result
// is always intersected with a separate occupied test.
//
// # Domain
//
// This is exact for values and watermarks in [0, 120), which is what clock.ToMinutes
// produces. Snapshots are the only path that can introduce a byte outside that range, and
// loadSnapshot rejects those before they reach the map.

// expiredRange returns the range of xorData byte values that Cleanup must evict for the
// given watermark: a slot expires when uint8(x-lo) <= length and the slot is occupied.
func expiredRange(watermark clock.Minutes) (lo, length uint8) {
	lo = ^uint8(watermark) // 255 - watermark
	if watermark >= 59 {
		// The 60 minute window ending at the watermark does not wrap around the clock face.
		return lo, 59
	}
	return lo, 195
}

// matchExpired returns a bitset with the high bit of lane j set when slot j of the group
// is occupied and its value falls in the expired range described by lo and length.
func (d *data) matchExpired(lo, length uint8) bitset {
	return matchExpiredWord(castUint64Data(d), lo, length)
}

// matchExpiredWord is the portable implementation of matchExpired over one 8 byte group.
// It mirrors, lane for lane, what the assembly implementations compute with a byte-wise
// subtract, an unsigned compare and an occupied test.
func matchExpiredWord(x uint64, lo, length uint8) bitset {
	y := subBytes(x, lo)
	// Occupied means the slot holds neither empty (0) nor tombstone (1), that is x > 1,
	// which for bytes is the same as x &^ 1 being non-zero.
	notOccupied := findZeroBytes(x &^ loBits)
	return leBytes(y, length) &^ notOccupied
}

// subBytes subtracts n from every byte lane of x, wrapping at 256 per lane instead of
// borrowing into the next lane.
func subBytes(x uint64, n uint8) uint64 {
	nb := uint64(n) * loBits
	// Setting the high bit of every lane of x guarantees each lane is at least as large as
	// the corresponding lane of nb&^hiBits, so the subtraction never borrows across lanes.
	// The final xor restores the high bits to what a per-lane subtraction would produce.
	return ((x | hiBits) - (nb &^ hiBits)) ^ ((x ^ ^nb) & hiBits)
}

// leBytes sets the high bit of every byte lane of x that is less than or equal to n,
// unsigned, and clears the others.
func leBytes(x uint64, n uint8) bitset {
	// Compare the low 7 bits of each lane by borrowing into the spare high bit.
	low := (((uint64(n&0x7f) * loBits) | hiBits) - (x &^ hiBits)) & hiBits
	if n < 0x80 {
		// x <= n also requires the high bit of the lane to be clear.
		return bitset(low & ^x)
	}
	// Any lane with its high bit clear is below 0x80 and therefore below n.
	return bitset((^x & hiBits) | low)
}

// lastMatch clears and returns the index of the highest set lane in the given bitset.
// It is the counterpart of nextMatch and assumes the bitset is non-zero.
func lastMatch(b *bitset) uint32 {
	s := uint32(63 - bits.LeadingZeros64(uint64(*b)))
	*b &= ^(1 << s)
	return s >> 3
}

func castUint64Data(d *data) uint64 {
	return *(*uint64)(unsafe.Pointer(d))
}
