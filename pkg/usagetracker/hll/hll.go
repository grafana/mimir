// SPDX-License-Identifier: AGPL-3.0-only

// Package hll implements the HyperLogLog sketch that the usage-tracker uses to estimate how many distinct
// series a tenant sends, including the ones it rejects. The sketch is dense, with one byte per register,
// because it is built for speed: inserts don't allocate and a merge is a register-wise max over 8 registers at once.
package hll

import (
	"encoding/binary"
	"math"
	"math/bits"
)

const (
	// MinPrecision and MaxPrecision bound the precision p of a sketch, which has 2^p registers.
	// The relative standard error of an estimate is about 1.04/sqrt(2^p).
	MinPrecision = 4
	MaxPrecision = 16

	// highBits has the high bit of each byte of an uint64 set.
	highBits = 0x8080808080808080

	// alphaInf is 1/(2*ln(2)), the limit of the HyperLogLog bias correction constant for a large number of registers.
	alphaInf = 0.7213475204444817
)

// Sketch is a dense HyperLogLog sketch with 2^p one-byte registers.
// Each register holds the highest rank inserted for its index, between 0 and MaxRank(p).
type Sketch []uint8

// NewSketch returns an empty sketch with precision p, which must be between MinPrecision and MaxPrecision.
func NewSketch(p uint8) Sketch {
	return make(Sketch, 1<<p)
}

// MaxRank returns the highest rank that a register of a sketch with precision p can hold.
func MaxRank(p uint8) uint8 {
	return 65 - p
}

// Hash mixes value with salt using the SplitMix64 finalizer.
// Values that an attacker can choose, like series hashes, must be mixed with a secret salt before they are inserted:
// otherwise the attacker can choose values with high ranks, and a few of them inflate the estimate.
// Mixing also spreads values whose bits are not uniform, like the series hashes of one partition and shard.
func Hash(value, salt uint64) uint64 {
	z := (value ^ salt) + 0x9e3779b97f4a7c15
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// Insert adds h, a value already mixed with Hash, to the sketch.
func (s Sketch) Insert(h uint64) {
	s.insert(h, s.precision())
}

// InsertAll mixes each value with salt using Hash, and adds it to the sketch.
func (s Sketch) InsertAll(values []uint64, salt uint64) {
	p := s.precision()
	for _, v := range values {
		s.insert(Hash(v, salt), p)
	}
}

func (s Sketch) insert(h uint64, p uint) {
	// The top p bits choose the register, and the rank is the position of the first 1 in the bits after them.
	// The 1 at bit p-1 caps the rank at 65-p when all those bits are 0.
	// p is between MinPrecision and MaxPrecision, so masking the shifts with 63 changes nothing, but it saves
	// the checks for shifts by 64 or more on every insert.
	i := h >> ((64 - p) & 63)
	rank := uint8(bits.LeadingZeros64(h<<(p&63)|1<<((p-1)&63))) + 1
	// max compiles to a conditional move: a branch would mispredict often while the sketch is filling up.
	s[i] = max(s[i], rank)
}

// Merge sets each register of s to the max of itself and the same register of other, so that s estimates
// the union of the values of both. other must have the same precision as s.
func (s Sketch) Merge(other Sketch) {
	other = other[:len(s)]
	for i := 0; i+8 <= len(s); i += 8 {
		a := binary.LittleEndian.Uint64(s[i:])
		b := binary.LittleEndian.Uint64(other[i:])
		// Registers are below 0x80, so (a|0x80)-b doesn't borrow from the next byte, and its high bit is set where a >= b.
		ge := ((a | highBits) - b) & highBits
		mask := (ge >> 7) * 0xff
		binary.LittleEndian.PutUint64(s[i:], a&mask|b&^mask)
	}
}

// Valid returns whether every register of s holds a rank that a sketch with its precision can hold.
// Use it on sketches that come from outside the process before merging them: Merge relies on it.
func (s Sketch) Valid() bool {
	if len(s) < 1<<MinPrecision || len(s) > 1<<MaxPrecision || len(s)&(len(s)-1) != 0 {
		return false
	}
	maxRank := MaxRank(uint8(s.precision()))
	for _, r := range s {
		if r > maxRank {
			return false
		}
	}
	return true
}

// Estimate returns the estimated number of distinct values inserted into the sketch.
//
// It uses the improved raw estimator from Otmar Ertl, "New cardinality estimation algorithms for HyperLogLog
// sketches" (2017), which is also what Redis uses. Unlike the original HyperLogLog estimator, it needs neither
// linear counting nor empirical bias corrections for small cardinalities, and it has no bias where the original
// one switches to linear counting.
func (s Sketch) Estimate() float64 {
	q := 64 - int(s.precision())
	var histogram [64 + 2]int // Ranks go from 0 to q+1.
	for _, r := range s {
		histogram[r]++
	}

	m := float64(len(s))
	z := m * tau(1-float64(histogram[q+1])/m)
	for k := q; k >= 1; k-- {
		z = 0.5 * (z + float64(histogram[k]))
	}
	z += m * sigma(float64(histogram[0])/m)
	return alphaInf * m * m / z
}

func (s Sketch) precision() uint {
	return uint(bits.TrailingZeros(uint(len(s))))
}

// sigma is the sigma function of Ertl's estimator: x + sum(x^(2^k) * 2^(k-1)) for k >= 1.
func sigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y, z := 1.0, x
	for {
		x *= x
		prev := z
		z += x * y
		y += y
		if z == prev {
			return z
		}
	}
}

// tau is the tau function of Ertl's estimator: (1 - x - sum((1 - x^(2^-k))^2 * 2^-k)) / 3 for k >= 1.
func tau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y, z := 1.0, 1-x
	for {
		x = math.Sqrt(x)
		prev := z
		y *= 0.5
		z -= (1 - x) * (1 - x) * y
		if z == prev {
			return z / 3
		}
	}
}
